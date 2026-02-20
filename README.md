# Telegram Topic Export Tools

This repository contains two scripts for exporting Telegram messages and preparing markdown output for downstream processing.

## Files

- `export_telegram_topic.py`: Exports a Telegram chat/topic to `JSONL` and `Markdown`, with optional media download and reply preview indexing via SQLite.
- `split_md_by_size.py`: Splits a large markdown export into smaller `.md` files by size while preserving message blocks when possible.

## Requirements

- Python 3.9+
- A Telegram API app from [my.telegram.org](https://my.telegram.org)

Install dependency:

```bash
pip install telethon
```

## Configure Export Script

Edit constants at the top of `export_telegram_topic.py`:

- `TARGET_LINK`: Telegram link to a public chat/topic message (for example: `https://t.me/<username>/<msgid>`).
- `FORCE_TOPIC_ROOT_MSG_ID`: Set a specific topic root message ID, or keep `None` to auto-detect from the link.
- `DOWNLOAD_MEDIA`: `True` to download media files, `False` to store metadata only.
- `MAX_MESSAGES`: Limit exported messages, or `None` for all.
- `OUTPUT_BASE_DIR`: Base output directory.
- `SESSION_NAME`: Telethon session file name.

## Run Export

Set credentials (or enter them interactively when prompted):

```bash
export TG_API_ID="YOUR_API_ID"
export TG_API_HASH="YOUR_API_HASH"
export TG_PHONE="+1234567890"
python export_telegram_topic.py
```

## Output Structure

Example output directory:

```text
telegram_export/<chat_slug>_topic_<topic_id_or_ALL>/
  messages.jsonl
  messages.md
  SCHEMA.md
  message_index.sqlite
  media/                  # only if DOWNLOAD_MEDIA=True
```

## Split Markdown by Size

```bash
python split_md_by_size.py /path/to/messages.md --out ./md_parts --max-mb 1.0 --prefix part_
```

Generated files include:

- `INDEX.txt`
- `manifest.json`
- `part_0001.md`, `part_0002.md`, ...
