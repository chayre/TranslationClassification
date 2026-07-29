"""
download_fasttext_cc.py
=======================
Downloads fastText Common Crawl models for Polish and English.
These have much broader coverage than Wikipedia-trained models.
"""

import os
import urllib.request
import zipfile

DATA_DIR = r"C:\Users\CAyre\Documents\Coding\TranslationClassification\TranslationClassification\data"

MODELS = {
    "en": {
        "url": "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.vec.gz",
        "filename": "cc.en.300.vec.gz",
    },
    "pl": {
        "url": "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.pl.300.vec.gz",
        "filename": "cc.pl.300.vec.gz",
    },
}

import gzip
import shutil

for lang, info in MODELS.items():
    gz_path = os.path.join(DATA_DIR, info["filename"])
    vec_path = gz_path.replace(".gz", "")

    if os.path.exists(vec_path):
        print(f"Already exists: {vec_path}")
        continue

    print(f"Downloading {lang} model...")
    urllib.request.urlretrieve(info["url"], gz_path)
    print(f"  Downloaded to {gz_path}")

    print(f"  Extracting...")
    with gzip.open(gz_path, 'rb') as f_in:
        with open(vec_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(gz_path)
    print(f"  Done: {vec_path}\n")

print("All done!")