#!/usr/bin/env python3
"""Merge a translation batch into frontend/js/i18n.jsx.

Batch = {"key": {"lang": "text", ...}}. Existing rows only gain the languages
they lack (en/es are never overwritten); absent keys are added as new rows.
"""
import json, re, sys

LANGS = "en es fr de it pt ru zh ja ko ar he ur hi".split()
PATH = "frontend/js/i18n.jsx"
ANCHOR = "  'nav.metrics':"

def main(batch_path):
    batch = json.load(open(batch_path, encoding="utf-8"))
    src = open(PATH, encoding="utf-8").read()
    added_rows, extended = [], 0
    for key, texts in batch.items():
        row = re.search(r"^(\s*'" + re.escape(key) + r"':\s*\{)(.*)(\},?\s*)$", src, re.M)
        if row:
            have = set(re.findall(r"(?:^|[\s,{])(" + "|".join(LANGS) + r"):", row.group(2)))
            extra = [l for l in LANGS if l not in have and l in texts]
            if not extra:
                continue
            body = row.group(2).rstrip().rstrip(",")
            body += ", " + ", ".join(f"{l}:{json.dumps(texts[l], ensure_ascii=False)}" for l in extra) + " "
            src = src[:row.start()] + row.group(1) + body + row.group(3) + src[row.end():]
            extended += 1
        else:
            missing = [l for l in ("en",) if l not in texts]
            if missing:
                sys.exit(f"{key}: new key without 'en'")
            cells = ", ".join(f"{l}:{json.dumps(texts[l], ensure_ascii=False)}" for l in LANGS if l in texts)
            added_rows.append(f"  '{key}': {{ {cells} }},\n")
    if added_rows:
        if src.count(ANCHOR) != 1:
            sys.exit("anchor not found")
        src = src.replace(ANCHOR, "".join(added_rows) + ANCHOR)
    open(PATH, "w", encoding="utf-8").write(src)
    print(f"extended {extended} rows, added {len(added_rows)} rows")

if __name__ == "__main__":
    main(sys.argv[1])
