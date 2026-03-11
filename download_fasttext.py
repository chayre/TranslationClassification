"""
fastText Aligned Vector Downloader
===================================
Downloads pre-trained aligned fastText word vectors for two languages
into the data/ folder, ready for use with pipeline.py --model fasttext.

The aligned vectors are trained on Wikipedia and aligned into a shared
cross-lingual space using the RCSLS method (Joulin et al. 2018), meaning
vectors for the same concept in different languages are comparable directly.

Vectors are ~650 MB per language. 44 languages are available.

Available language codes (ISO 639-1):
  af, sq, ar, bg, bn, ca, hr, cs, da, nl, en, et, fi, fr, de, el,
  he, hi, hu, id, it, ja, ko, lv, lt, mk, ms, no, pl, pt, ro, ru,
  sk, sl, es, sv, tl, ta, th, tr, uk, vi, zh

Usage:
  python download_fasttext.py --lang-a no --lang-b en
  python download_fasttext.py --lang-a fr --lang-b de --outdir data
"""

import argparse
import os
import urllib.request

BASE_URL = "https://dl.fbaipublicfiles.com/fasttext/vectors-aligned/wiki.{lang}.align.vec"

AVAILABLE_LANGS = {
    "af", "sq", "ar", "bg", "bn", "ca", "hr", "cs", "da", "nl",
    "en", "et", "fi", "fr", "de", "el", "he", "hi", "hu", "id",
    "it", "ja", "ko", "lv", "lt", "mk", "ms", "no", "pl", "pt",
    "ro", "ru", "sk", "sl", "es", "sv", "tl", "ta", "th", "tr",
    "uk", "vi", "zh",
}


def download(lang, outdir):
    if lang not in AVAILABLE_LANGS:
        print(f"  ERROR: '{lang}' is not an available language code.")
        print(f"  Available codes: {', '.join(sorted(AVAILABLE_LANGS))}")
        return None

    url = BASE_URL.format(lang=lang)
    dest = os.path.join(outdir, f"wiki.{lang}.align.vec")

    if os.path.exists(dest):
        print(f"  Already exists: {dest} — skipping.")
        return dest

    print(f"  Downloading {lang} vectors from {url}")
    print(f"  Destination: {dest}")
    print(f"  (~650 MB, this may take a few minutes...)")

    def progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            pct = min(downloaded / total_size * 100, 100)
            bar = int(pct / 2)
            print(f"\r  [{'=' * bar}{' ' * (50 - bar)}] {pct:.1f}%", end="", flush=True)

    urllib.request.urlretrieve(url, dest, reporthook=progress)
    print()  # newline after progress bar
    print(f"  Done: {dest}")
    return dest


def main():
    parser = argparse.ArgumentParser(description="Download aligned fastText vectors")
    parser.add_argument("--lang-a", required=True, help="ISO 639-1 code for language A (e.g. 'no')")
    parser.add_argument("--lang-b", required=True, help="ISO 639-1 code for language B (e.g. 'en')")
    parser.add_argument("--outdir", default="data", help="Output directory (default: data/)")
    args = parser.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    print(f"Downloading fastText aligned vectors...\n")

    path_a = download(args.lang_a, args.outdir)
    print()
    path_b = download(args.lang_b, args.outdir)

    if path_a and path_b:
        print(f"\nAll done. Run the pipeline with:")
        print(f"  python pipeline.py --model fasttext \\")
        print(f"    --fasttext-vec-a {path_a} \\")
        print(f"    --fasttext-vec-b {path_b}")


if __name__ == "__main__":
    main()
