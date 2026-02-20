#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import math
import os
import re
from pathlib import Path

def utf8_len(s: str) -> int:
    return len(s.encode("utf-8"))

def find_record_starts(md: str):
    """
    Records look like:
    ---
    id: ...
    ...
    ---
    text...

    We detect record starts by looking for:
      - file start with '---\n' followed by 'id:'
      - or 2+ newlines then '---\n' followed by 'id:'
    This avoids confusing the frontmatter closing '---' with a new record.
    """
    starts = []

    if md.startswith("---\n") and re.match(r"(?m)^id:\s", md[4:80] or ""):
        starts.append(0)

    # find occurrences of \n{2,} + (---\n) where next line begins with id:
    pat = re.compile(r"\n{2,}(---\n(?=id:\s))", re.MULTILINE)
    for m in pat.finditer(md):
        starts.append(m.start(1))  # position of the '---\n'

    starts = sorted(set(starts))
    return starts

def split_into_records(md: str):
    starts = find_record_starts(md)
    if not starts:
        # fallback: no recognizable records, return whole file as one "record"
        return [md]

    records = []
    for i, st in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(md)
        records.append(md[st:end])
    return records

def chunk_records_by_size(records, max_bytes: int):
    chunks = []
    cur = ""
    cur_bytes = 0

    def push_current():
        nonlocal cur, cur_bytes
        if cur:
            chunks.append(cur)
            cur = ""
            cur_bytes = 0

    for rec in records:
        rec_b = utf8_len(rec)

        # If a single record is bigger than max_bytes, we must split it (rare, but safe).
        if rec_b > max_bytes:
            push_current()
            # split large record by lines, keeping size <= max_bytes
            lines = rec.splitlines(keepends=True)
            part = ""
            part_b = 0
            for ln in lines:
                ln_b = utf8_len(ln)
                if part_b + ln_b > max_bytes and part:
                    chunks.append(part)
                    part = ""
                    part_b = 0
                part += ln
                part_b += ln_b
            if part:
                chunks.append(part)
            continue

        # Normal case: try to append record to current chunk
        if cur_bytes + rec_b > max_bytes and cur:
            push_current()

        cur += rec
        cur_bytes += rec_b

    push_current()
    return chunks

def main():
    ap = argparse.ArgumentParser(description="Split a large Markdown export into smaller .md files by size (preserving message blocks when possible).")
    ap.add_argument("input_md", help="Path to input .md file (e.g., messages.md)")
    ap.add_argument("--out", default="./md_parts", help="Output directory (default: ./md_parts)")
    ap.add_argument("--max-mb", type=float, default=1.0, help="Max size per part in MB (default: 1.0)")
    ap.add_argument("--prefix", default="part_", help="Output filename prefix (default: part_)")
    args = ap.parse_args()

    in_path = Path(args.input_md).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    md = in_path.read_text(encoding="utf-8", errors="replace")
    max_bytes = int(args.max_mb * 1024 * 1024)

    records = split_into_records(md)
    chunks = chunk_records_by_size(records, max_bytes=max_bytes)

    # Write parts
    digits = max(4, len(str(len(chunks))))
    manifest = {
        "source_file": str(in_path),
        "output_dir": str(out_dir),
        "max_mb": args.max_mb,
        "max_bytes": max_bytes,
        "parts": []
    }

    for idx, content in enumerate(chunks, start=1):
        fname = f"{args.prefix}{str(idx).zfill(digits)}.md"
        fpath = out_dir / fname
        # Ensure it ends with newline for nicer concatenation/reading
        if not content.endswith("\n"):
            content += "\n"
        fpath.write_text(content, encoding="utf-8")

        b = utf8_len(content)
        manifest["parts"].append({
            "file": fname,
            "bytes": b,
            "mb": round(b / (1024 * 1024), 4),
        })

    # Also write a simple index file
    index_lines = [
        f"Source: {in_path.name}",
        f"Total parts: {len(chunks)}",
        f"Max per part: {args.max_mb} MB ({max_bytes} bytes)",
        "",
    ]
    for p in manifest["parts"]:
        index_lines.append(f"- {p['file']}  ({p['mb']} MB)")

    (out_dir / "INDEX.txt").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    total_mb = round(sum(p["bytes"] for p in manifest["parts"]) / (1024 * 1024), 3)
    print("DONE ✅")
    print(f"Input:  {in_path}")
    print(f"Output: {out_dir}")
    print(f"Parts:  {len(chunks)}  (total ~{total_mb} MB)")
    print("Files:  INDEX.txt , manifest.json")

if __name__ == "__main__":
    main()
