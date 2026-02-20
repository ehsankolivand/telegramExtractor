#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import sqlite3
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.tl.types import (
    MessageEntityMentionName,
    MessageEntityTextUrl,
    MessageEntityUrl,
    MessageEntityHashtag,
)

# =========================
# 1) HARD-CODED TARGET LINK
# =========================
TARGET_LINK = "https://t.me/"  # <-- REPLACE THIS with the t.me link to the topic or chat you want to export.

# Set to None to export the entire chat instead of a specific topic.
# If TARGET_LINK includes a message id, it is auto-detected unless overridden.
FORCE_TOPIC_ROOT_MSG_ID = None

# Download media? (default is off; only metadata is stored)
DOWNLOAD_MEDIA = False

# Message limit (None means all)
MAX_MESSAGES = None

# Output directory
OUTPUT_BASE_DIR = Path("./telegram_export")

# Telegram session name (session file is created next to this script)
SESSION_NAME = "tg_session"

# =========================
# Helpers
# =========================

def utc_iso(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def safe_slug(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    return s[:80] if s else "chat"

def parse_tme_link(link: str):
    """
    Supports:
    - https://t.me/username/123
    - https://t.me/username
    - https://t.me/+INVITEHASH
    - https://t.me/joinchat/INVITEHASH
    - https://t.me/c/123456/789 (internal link)
    """
    u = urlparse(link)
    path = (u.path or "").strip("/")
    parts = [p for p in path.split("/") if p]

    out = {
        "kind": None,            # public | invite | internal
        "entity_hint": None,     # username or something resolvable by get_entity
        "topic_or_msg_id": None, # int or None
        "invite_hash": None,
        "internal_chat": None,
        "internal_msg_id": None,
    }

    # invite link: t.me/+HASH or t.me/joinchat/HASH
    if parts and (parts[0].startswith("+") or parts[0] == "joinchat"):
        out["kind"] = "invite"
        if parts[0].startswith("+"):
            out["invite_hash"] = parts[0][1:]
        else:
            out["invite_hash"] = parts[1] if len(parts) > 1 else None
        return out

    # internal link: t.me/c/<internal_chat>/<msg_id>
    if len(parts) >= 3 and parts[0] == "c":
        out["kind"] = "internal"
        out["internal_chat"] = parts[1]
        try:
            out["internal_msg_id"] = int(parts[2])
        except:
            out["internal_msg_id"] = None
        return out

    # public: username or username/msgid
    if parts:
        out["kind"] = "public"
        out["entity_hint"] = parts[0]
        if len(parts) >= 2:
            try:
                out["topic_or_msg_id"] = int(parts[1])
            except:
                out["topic_or_msg_id"] = None
        return out

    return out

def extract_mentions_and_links(text: str, entities):
    text = text or ""
    mentions_usernames = sorted(set(re.findall(r"@([A-Za-z0-9_]{3,})", text)))
    mentions_user_ids = []
    links = []
    hashtags = []

    if entities:
        for e in entities:
            if isinstance(e, MessageEntityMentionName):
                # Text mention without @username, with user_id.
                try:
                    mentions_user_ids.append(int(e.user_id))
                except:
                    pass
            elif isinstance(e, MessageEntityTextUrl):
                # Hyperlinked text with an explicit URL.
                if getattr(e, "url", None):
                    links.append(e.url)
            elif isinstance(e, MessageEntityUrl):
                # URL entity inside text.
                # Exact substring extraction with UTF-16 offsets is tricky,
                # so regex fallback below supplements this.
                pass
            elif isinstance(e, MessageEntityHashtag):
                # Similar to URLs, hashtags are also collected with regex fallback.
                pass

    # Collect links via regex fallback.
    links += re.findall(r"(https?://[^\s\)\]\}]+)", text)
    links = sorted(set(links))

    # Collect hashtags via regex fallback.
    hashtags = sorted(set(re.findall(r"#([\w_]{2,})", text, flags=re.UNICODE)))

    return {
        "usernames": mentions_usernames,
        "user_ids": sorted(set(mentions_user_ids)),
        "links": links,
        "hashtags": hashtags,
    }

def ensure_db(db_path: Path):
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS msg_index (
            msg_id INTEGER PRIMARY KEY,
            sender TEXT,
            date_utc TEXT,
            preview TEXT
        )
    """)
    con.commit()
    return con

def db_get_preview(con, msg_id: int):
    cur = con.cursor()
    cur.execute("SELECT sender, date_utc, preview FROM msg_index WHERE msg_id=?", (msg_id,))
    row = cur.fetchone()
    if not row:
        return None
    return {"sender": row[0], "date_utc": row[1], "preview": row[2]}

def db_put_preview(con, msg_id: int, sender: str, date_utc: str, preview: str):
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO msg_index (msg_id, sender, date_utc, preview) VALUES (?,?,?,?)",
        (msg_id, sender, date_utc, preview),
    )
    con.commit()

async def get_sender_display(client, msg, sender_cache):
    sid = getattr(msg, "sender_id", None)
    if sid is None:
        return {"id": None, "name": None, "username": None, "type": None}

    if sid in sender_cache:
        return sender_cache[sid]

    sender = await msg.get_sender()
    name = None
    username = None
    stype = None

    if sender is None:
        name = None
        stype = None
    else:
        username = getattr(sender, "username", None)
        stype = sender.__class__.__name__
        # best-effort name
        first = getattr(sender, "first_name", None)
        last = getattr(sender, "last_name", None)
        title = getattr(sender, "title", None)  # channels
        if title:
            name = title
        else:
            name = " ".join([x for x in [first, last] if x]) or getattr(sender, "display_name", None)

    info = {"id": sid, "name": name, "username": username, "type": stype}
    sender_cache[sid] = info
    return info

def render_md_block(rec: dict) -> str:
    # Markdown frontmatter style (LLM-friendly).
    meta = {
        "id": rec.get("id"),
        "date_utc": rec.get("date_utc"),
        "from": rec.get("from", {}),
        "reply_to": rec.get("reply_to", {}),
        "mentions": rec.get("mentions", {}),
        "has_media": rec.get("has_media"),
        "media": rec.get("media"),
        "edited_utc": rec.get("edited_utc"),
        "is_service": rec.get("is_service"),
    }
    fm = yaml_like(meta)
    text = rec.get("text") or ""
    return f"---\n{fm}---\n{text}\n\n"

def yaml_like(obj, indent=0):
    # Minimal YAML serializer (without PyYAML dependency).
    sp = "  " * indent
    if obj is None:
        return f"{sp}null\n"
    if isinstance(obj, bool):
        return f"{sp}{'true' if obj else 'false'}\n"
    if isinstance(obj, (int, float)):
        return f"{sp}{obj}\n"
    if isinstance(obj, str):
        s = obj.replace("\r", "").strip("\n")
        # Use block style for multi-line strings.
        if "\n" in s:
            lines = s.split("\n")
            out = f"{sp}|-\n"
            for ln in lines:
                out += f"{sp}  {ln}\n"
            return out
        return f"{sp}{s}\n"
    if isinstance(obj, list):
        if not obj:
            return f"{sp}[]\n"
        out = ""
        for item in obj:
            if isinstance(item, (dict, list)):
                out += f"{sp}-\n{yaml_like(item, indent+1)}"
            else:
                out += f"{sp}- {str(item)}\n"
        return out
    if isinstance(obj, dict):
        if not obj:
            return f"{sp}{{}}\n"
        out = ""
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                out += f"{sp}{k}:\n{yaml_like(v, indent+1)}"
            else:
                out += f"{sp}{k}: {yaml_like(v, 0).strip()}\n"
        return out
    return f"{sp}{str(obj)}\n"

async def main():
    print("=== Telegram Topic Export (AI-friendly) ===")

    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")
    if not api_id:
        api_id = input("Enter TG_API_ID (from my.telegram.org): ").strip()
    if not api_hash:
        api_hash = input("Enter TG_API_HASH (from my.telegram.org): ").strip()

    # Telethon expects int api_id
    api_id_int = int(api_id)

    phone = os.getenv("TG_PHONE") or input("Enter your phone number (e.g. +98...): ").strip()

    link_info = parse_tme_link(TARGET_LINK)

    topic_root_msg_id = FORCE_TOPIC_ROOT_MSG_ID
    if topic_root_msg_id is None:
        topic_root_msg_id = link_info.get("topic_or_msg_id")

    entity_hint = link_info.get("entity_hint")
    if link_info["kind"] == "internal":
        # Internal links are harder to resolve reliably; use a public username link.
        raise RuntimeError("Internal t.me/c/... link detected. Please use a public username link like https://t.me/<username>/<msgid>.")

    out_slug = safe_slug(entity_hint or "invite_chat")
    out_dir = OUTPUT_BASE_DIR / f"{out_slug}_topic_{topic_root_msg_id or 'ALL'}"
    out_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = out_dir / "messages.jsonl"
    md_path = out_dir / "messages.md"
    schema_path = out_dir / "SCHEMA.md"
    db_path = out_dir / "message_index.sqlite"
    media_dir = out_dir / "media"
    if DOWNLOAD_MEDIA:
        media_dir.mkdir(parents=True, exist_ok=True)

    con = ensure_db(db_path)

    # Write schema doc (once)
    if not schema_path.exists():
        schema_path.write_text(
            "Schema (messages.jsonl)\n"
            "- id: message id\n"
            "- date_utc\n"
            "- from: {id, name, username, type}\n"
            "- text\n"
            "- reply_to: {msg_id, top_id, preview?}\n"
            "- mentions: {usernames, user_ids, links, hashtags}\n"
            "- has_media, media\n"
            "- edited_utc\n"
            "- is_service\n",
            encoding="utf-8"
        )

    client = TelegramClient(SESSION_NAME, api_id_int, api_hash)

    await client.connect()
    if not await client.is_user_authorized():
        try:
            await client.send_code_request(phone)
            code = input("Enter the Telegram login code you received: ").strip()
            try:
                await client.sign_in(phone=phone, code=code)
            except SessionPasswordNeededError:
                pw = input("Two-step verification enabled. Enter your password: ").strip()
                await client.sign_in(password=pw)
        except Exception as e:
            await client.disconnect()
            raise

    # Resolve entity
    entity = None
    if link_info["kind"] == "invite":
        # Invite links require hash-based join/import logic (optional).
        # Keep behavior simple here and require a public username link.
        raise RuntimeError("Invite link detected. Please replace TARGET_LINK with a public username link, or add join logic.")
    else:
        if not entity_hint:
            raise RuntimeError("Could not parse entity from TARGET_LINK. Please set TARGET_LINK to https://t.me/<username>/<msgid>.")
        entity = await client.get_entity(entity_hint)

    sender_cache = {}

    # Open output files (append-safe)
    jf = open(jsonl_path, "w", encoding="utf-8")
    mf = open(md_path, "w", encoding="utf-8")

    exported = 0

    # If topic is set, export the topic root message for full context.
    if topic_root_msg_id:
        root = await client.get_messages(entity, ids=topic_root_msg_id)
        if root:
            sender_info = await get_sender_display(client, root, sender_cache)
            text = root.message or ""
            mentions = extract_mentions_and_links(text, root.entities)

            rec_root = {
                "id": root.id,
                "date_utc": utc_iso(root.date),
                "from": sender_info,
                "text": text,
                "reply_to": {
                    "msg_id": getattr(root, "reply_to_msg_id", None),
                    "top_id": getattr(getattr(root, "reply_to", None), "reply_to_top_id", None),
                    "preview": None,
                },
                "mentions": mentions,
                "has_media": bool(root.media),
                "media": None,
                "edited_utc": utc_iso(getattr(root, "edit_date", None)),
                "is_service": bool(getattr(root, "action", None)),
                "is_topic_root": True,
            }

            # media meta
            if root.media:
                rec_root["media"] = {
                    "type": root.media.__class__.__name__,
                    "file_name": getattr(getattr(root, "file", None), "name", None),
                    "size": getattr(getattr(root, "file", None), "size", None),
                    "mime_type": getattr(getattr(root, "file", None), "mime_type", None),
                }

            preview = (text or "").replace("\n", " ").strip()[:200]
            db_put_preview(con, root.id, sender_info.get("name") or sender_info.get("username") or str(sender_info.get("id")), rec_root["date_utc"], preview)

            jf.write(json.dumps(rec_root, ensure_ascii=False) + "\n")
            mf.write(render_md_block(rec_root))
            exported += 1

    # Export messages
    it = client.iter_messages(
        entity,
        reply_to=topic_root_msg_id if topic_root_msg_id else None,
        reverse=True,          # oldest -> newest (better reply preview resolution)
        limit=MAX_MESSAGES
    )

    async for msg in it:
        try:
            sender_info = await get_sender_display(client, msg, sender_cache)
            text = msg.message or ""
            mentions = extract_mentions_and_links(text, msg.entities)

            reply_to_msg_id = getattr(msg, "reply_to_msg_id", None)
            reply_to_top_id = getattr(getattr(msg, "reply_to", None), "reply_to_top_id", None)

            reply_preview = None
            if reply_to_msg_id:
                reply_preview = db_get_preview(con, reply_to_msg_id)

            rec = {
                "id": msg.id,
                "date_utc": utc_iso(msg.date),
                "from": sender_info,
                "text": text,
                "reply_to": {
                    "msg_id": reply_to_msg_id,
                    "top_id": reply_to_top_id,
                    "preview": reply_preview,  # {sender, date_utc, preview} or None
                },
                "mentions": mentions,
                "has_media": bool(msg.media),
                "media": None,
                "edited_utc": utc_iso(getattr(msg, "edit_date", None)),
                "is_service": bool(getattr(msg, "action", None)),
            }

            # media meta / optional download
            if msg.media:
                rec["media"] = {
                    "type": msg.media.__class__.__name__,
                    "file_name": getattr(getattr(msg, "file", None), "name", None),
                    "size": getattr(getattr(msg, "file", None), "size", None),
                    "mime_type": getattr(getattr(msg, "file", None), "mime_type", None),
                }
                if DOWNLOAD_MEDIA:
                    try:
                        saved = await msg.download_media(file=str(media_dir / f"{msg.id}"))
                        rec["media"]["saved_path"] = str(saved) if saved else None
                    except Exception as e:
                        rec["media"]["download_error"] = str(e)

            # Store in DB so later reply messages can include a preview.
            preview = (text or "").replace("\n", " ").strip()[:200]
            sender_label = sender_info.get("name") or sender_info.get("username") or str(sender_info.get("id"))
            db_put_preview(con, msg.id, sender_label, rec["date_utc"], preview)

            jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
            mf.write(render_md_block(rec))

            exported += 1
            if exported % 500 == 0:
                print(f"Exported {exported} messages...")

        except FloodWaitError as e:
            # Respect Telegram rate limits.
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:
            # Skip per-message errors and continue.
            err = {
                "id": getattr(msg, "id", None),
                "error": str(e),
            }
            jf.write(json.dumps({"_error": err}, ensure_ascii=False) + "\n")

    jf.close()
    mf.close()
    con.close()
    await client.disconnect()

    print("\nDONE ✅")
    print(f"Output folder: {out_dir.resolve()}")
    print(f"- JSONL: {jsonl_path.name}")
    print(f"- Markdown: {md_path.name}")
    print(f"- Index DB: {db_path.name}")

if __name__ == "__main__":
    asyncio.run(main())
