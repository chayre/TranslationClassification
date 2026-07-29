"""
check_dict_format.py
=====================
Quick diagnostic to see the actual format of dictionary_pl_en_dict.csv
"""

import os

DATA_DIR = "data"
INPUT_CSV = os.path.join(DATA_DIR, "dictionary_pl_en_dict.csv")

with open(INPUT_CSV, 'r', encoding='utf-8') as f:
    # Read first few lines raw
    print("First 5 lines (raw):")
    for i, line in enumerate(f):
        if i >= 5:
            break
        print(repr(line[:200]))
    
    print("\n" + "="*60)
    f.seek(0)
    
    # Check what delimiter is being used
    first_line = f.readline().strip()
    tabs = first_line.count('\t')
    commas = first_line.count(',')
    pipes = first_line.count('|')
    
    print(f"First line: {repr(first_line[:200])}")
    print(f"Tabs: {tabs}, Commas: {commas}, Pipes: {pipes}")