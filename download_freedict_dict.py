import csv
import gzip
import re
from collections import defaultdict

INDEX_FILE = "freedict-eng-pol.index"
DICT_FILE = "freedict-eng-pol.dict.dz"
OUTPUT_FILE = "dictionary_pl_en_dict.csv"


def b64decode_dictd(s):
    """
    Dictd uses a custom base64 encoding.
    """
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
    value = 0

    for c in s:
        value = value * 64 + chars.index(c)

    return value


# Read dictionary data
with gzip.open(DICT_FILE, "rb") as f:
    dict_data = f.read()

pairs = []

with open(INDEX_FILE, "r", encoding="utf-8", errors="ignore") as f:
    for line in f:
        parts = line.strip().split("\t")

        if len(parts) < 3:
            continue

        word = parts[0].lower()

        offset = b64decode_dictd(parts[1])
        length = b64decode_dictd(parts[2])

        entry_bytes = dict_data[offset:offset + length]

        try:
            entry = entry_bytes.decode("utf-8", errors="ignore")
        except:
            continue

        # Remove dictd formatting
        entry = re.sub(r"<[^>]+>", "", entry)

        # Split possible translations
        translations = re.split(r"[;,/]", entry)

        for tr in translations:
            tr = tr.strip().lower()

            # Clean parentheses
            tr = re.sub(r"\(.*?\)", "", tr).strip()

            if tr and tr != word:
                # Skip junk
                if len(tr) < 2:
                    continue

                if any(x in tr for x in [
                    "{", "}", "[", "]", "<", ">",
                    "abbr.", "obs.", "fig.", "slang"
                ]):
                    continue

                # Skip extremely long phrases
                if len(tr.split()) > 4:
                    continue

                # Keep only mostly alphabetic entries
                letters = sum(c.isalpha() for c in tr)
                if letters / max(len(tr), 1) < 0.6:
                    continue
                pairs.append((word, tr))

print(f"Extracted {len(pairs)} raw pairs")

# Deduplicate/group
grouped = defaultdict(set)

for en, pl in pairs:
    grouped[en].add(pl)

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["english", "polish", "alternatives"])

    for en in sorted(grouped.keys()):
        pls = sorted(grouped[en])

        if not pls:
            continue

        writer.writerow([
            en,
            pls[0],
            "|".join(pls[1:5])
        ])

print(f"Saved {len(grouped)} entries to {OUTPUT_FILE}")