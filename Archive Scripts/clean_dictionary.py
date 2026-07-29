"""
clean_dictionary.py
====================
Cleans the FreeDict dictionary (dictionary_pl_en_dict.csv).
Input: comma-separated, columns = english, polish, alternatives.
Output: pl_en_dict_clean.csv with clean word-to-word pairs.

Cleaning:
  - Strips leading numbers (1., 2., i., ii.)
  - Removes bracketed notes [...]
  - Removes multi-word phrases (keeps only single Polish words)
  - Takes the first valid Polish word from each definition
  - Also checks the 'alternatives' column for additional translations
"""

import csv
import os
import re
from collections import defaultdict

DATA_DIR = "data"
INPUT_CSV = os.path.join(DATA_DIR, "dictionary_pl_en_dict.csv")
OUTPUT_CSV = os.path.join(DATA_DIR, "pl_en_dict_clean.csv")

POLISH_CHARS = set('ąćęłńóśźż')


def extract_polish_words(text):
    """Extract single Polish words from a definition cell."""
    if not text or not text.strip():
        return []
    
    words = []
    
    # Remove bracketed content
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\{.*?\}', '', text)
    text = re.sub(r'\(.*?\)', '', text)
    
    # Split on newlines, pipes, semicolons
    lines = re.split(r'[\n|]', text)
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Remove leading numbers
        line = re.sub(r'^[\d]+\.\s*', '', line)
        line = re.sub(r'^[ivxIVX]+\.\s*', '', line)
        line = re.sub(r'^[-–—]\s*', '', line)
        line = line.strip(' \t.,;')
        
        if not line:
            continue
        
        for word in line.split():
            word = word.strip('.,;:()[]{}"\'-–—?!')
            if len(word) < 2:
                continue
            if word.isdigit():
                continue
            if any(c in POLISH_CHARS for c in word) or \
               word.lower().endswith(('ać', 'eć', 'ić', 'yć', 'ować', 'enie', 
                                      'anie', 'ość', 'izm', 'yka', 'ik', 'ek', 
                                      'ka', 'ko', 'ny', 'ły', 'ia', 'ów', 'nąć',
                                      'cie', 'my', 'ciej', 'owa', 'owy', 'owe')):
                words.append(word.lower())
                break
    
    return words


def extract_from_alternatives(text):
    """Extract Polish words from the alternatives column (pipe-separated)."""
    if not text or not text.strip():
        return []
    
    words = []
    parts = text.split('|')
    for part in parts:
        part = part.strip().lower()
        # Remove pronunciation
        part = re.sub(r'[ˈˌˌːæɑɛɔəʌʃʒʧʤŋθð]+', '', part)
        part = part.strip()
        if len(part) >= 2 and any(c in POLISH_CHARS for c in part):
            words.append(part)
    
    return words


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    
    print(f"Reading: {INPUT_CSV}")
    
    pairs = []
    skipped = 0
    total = 0
    
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            
            en_word = row.get('english', '').strip().lower()
            pl_text = row.get('polish', '').strip()
            alt_text = row.get('alternatives', '').strip()
            
            # Skip metadata rows
            if not en_word or len(en_word) < 2:
                skipped += 1
                continue
            if en_word.startswith('00') or en_word.startswith('free'):
                skipped += 1
                continue
            
            # Extract from polish column
            pl_words = extract_polish_words(pl_text)
            
            # Also check alternatives column
            alt_words = extract_from_alternatives(alt_text)
            pl_words.extend(alt_words)
            
            if pl_words:
                for pw in pl_words:
                    pairs.append((en_word, pw))
            else:
                skipped += 1
    
    print(f"  Total rows: {total}")
    print(f"  Valid pairs: {total - skipped}")
    print(f"  Skipped: {skipped}")
    print(f"  Total word pairs: {len(pairs)}")
    
    # Group
    grouped = defaultdict(list)
    for en, pl in pairs:
        if pl not in grouped[en]:
            grouped[en].append(pl)
    
    # Write
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["english", "polish", "polish_alternatives"])
        for en_word in sorted(grouped.keys()):
            pl_words = grouped[en_word]
            main = pl_words[0]
            alts = "|".join(pl_words[1:6]) if len(pl_words) > 1 else ""
            writer.writerow([en_word, main, alts])
    
    print(f"\nWritten: {OUTPUT_CSV}")
    print(f"  Unique English entries: {len(grouped)}")
    
    # Sample
    print(f"\nFirst 25 entries:")
    with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for i, row in enumerate(reader):
            if i >= 25:
                break
            alt = f" ({row[2]})" if row[2] else ""
            print(f"  {row[0]:<20} -> {row[1]:<20}{alt}")