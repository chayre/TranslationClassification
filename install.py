"""
CDI Pipeline Installer
======================
Installs all dependencies in the correct order.
Run this instead of `pip install -r requirements.txt`.

Usage:
  python install.py              # core only (LaBSE + LLM false-cognate check)
  python install.py --fasttext   # core + fastText
"""

import argparse
import subprocess
import sys


def run(cmd, description):
    print(f"\n>>> {description}")
    print(f"    {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        print(f"\nERROR: Installation failed for: {description}")
        print("See error above. You may need to resolve this manually.")
        sys.exit(1)


def pip(*packages):
    run([sys.executable, "-m", "pip", "install", *packages], f"Installing {', '.join(packages)}")


def install_core():
    print("=" * 60)
    print("Installing core dependencies")
    print("=" * 60)
    pip("torch>=2.0.0")
    pip("numpy>=1.24.0")
    pip("sentence-transformers>=2.7.0")
    pip("openpyxl>=3.1.0")
    pip("anthropic>=0.20.0")
    pip("gensim>=4.3.0")
    print("\nCore dependencies installed.")


def install_fasttext():
    print("=" * 60)
    print("Installing fastText dependencies")
    print("=" * 60)
    # fasttext-langdetect provides pre-built wheels compatible with Python 3.13
    # fasttext-wheel requires compiling from source and does not support Python 3.13
    pip("fasttext-langdetect")
    print("\nfastText installed.")
    print("Next: download aligned vectors with:")
    print("  python download_fasttext.py --lang-a no --lang-b en")


def main():
    parser = argparse.ArgumentParser(description="CDI pipeline dependency installer")
    parser.add_argument("--fasttext", action="store_true", help="Install fastText dependencies")
    args = parser.parse_args()

    install_core()

    if args.fasttext:
        install_fasttext()

    print("\n" + "=" * 60)
    print("All done!")
    print("=" * 60)
    if not args.fasttext:
        print("\nOptional models available:")
        print("  python install.py --fasttext   # adds fastText support")


if __name__ == "__main__":
    main()
