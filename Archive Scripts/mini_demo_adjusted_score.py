"""
mini_demo.py
=============
Tiny semantic alignment demo with similarity profiles.

This version loads selectable Polish/English word lists from a text file.
By default, it expects the file here, relative to this script:

    data/mini-dict.txt

Usage examples:
    python mini_demo.py --list-1
    python mini_demo.py --list-2
    python mini_demo.py --list-all
    python mini_demo.py --list-all --dict data/mini-dict.txt
"""

import argparse
import sys
from pathlib import Path

import numpy as np
from scipy.stats import pearsonr

print("Step 1: Importing SentenceTransformer...", flush=True)
from sentence_transformers import SentenceTransformer
print("Done.\n", flush=True)


def unique_preserve_order(items):
    """Return items without duplicates while keeping the first occurrence."""
    seen = set()
    output = []
    for item in items:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def parse_key_value_line(line, line_number):
    """Parse a line like 'polish = english'."""
    if "=" not in line:
        raise ValueError(f"Line {line_number}: expected 'left = right', got: {line}")
    left, right = line.split("=", 1)
    left = left.strip()
    right = right.strip()
    if not left or not right:
        raise ValueError(f"Line {line_number}: both sides of '=' must have text, got: {line}")
    return left, right


def load_word_lists(dict_path):
    """
    Load word lists from mini-dict.txt.

    Expected format:

        [list-1]
        description = Original demo list

        pairs:
        pies = dog
        kot = cat

        associates:
        animal = zwierzę
        food = jedzenie

    In the 'pairs' section, each line is:
        Polish word = English word

    In the 'associates' section, each line is:
        English associate = Polish associate

    You may list multiple Polish associates with commas:
        building = budynek, konstrukcja
    """
    if not dict_path.exists():
        raise FileNotFoundError(
            f"Could not find dictionary file: {dict_path}\n"
            "Expected default location: data/mini-dict.txt relative to this script."
        )

    word_lists = {}
    current_list_name = None
    current_section = None

    with dict_path.open("r", encoding="utf-8-sig") as f:
        for line_number, raw_line in enumerate(f, start=1):
            line = raw_line.strip()

            # Ignore blank lines and comments.
            if not line or line.startswith("#"):
                continue

            # Start a new list block, e.g. [list-2].
            if line.startswith("[") and line.endswith("]"):
                current_list_name = line[1:-1].strip()
                if not current_list_name:
                    raise ValueError(f"Line {line_number}: empty list name.")
                if current_list_name in word_lists:
                    raise ValueError(f"Line {line_number}: duplicate list name: {current_list_name}")
                word_lists[current_list_name] = {
                    "description": "",
                    "polish_words": [],
                    "english_words": [],
                    "dictionary": {},
                }
                current_section = None
                continue

            if current_list_name is None:
                raise ValueError(f"Line {line_number}: content appears before a [list-name] header.")

            lower_line = line.lower()
            if lower_line == "pairs:":
                current_section = "pairs"
                continue
            if lower_line == "associates:":
                current_section = "associates"
                continue

            if lower_line.startswith("description"):
                key, value = parse_key_value_line(line, line_number)
                if key.lower() != "description":
                    raise ValueError(f"Line {line_number}: expected 'description = ...', got: {line}")
                word_lists[current_list_name]["description"] = value
                continue

            if current_section == "pairs":
                polish_word, english_word = parse_key_value_line(line, line_number)
                word_lists[current_list_name]["polish_words"].append(polish_word)
                word_lists[current_list_name]["english_words"].append(english_word)
                continue

            if current_section == "associates":
                english_associate, polish_associates_text = parse_key_value_line(line, line_number)
                polish_associates = [w.strip() for w in polish_associates_text.split(",") if w.strip()]
                if not polish_associates:
                    raise ValueError(f"Line {line_number}: associate must include at least one Polish word.")
                word_lists[current_list_name]["dictionary"][english_associate] = polish_associates
                continue

            raise ValueError(
                f"Line {line_number}: line is not inside 'pairs:' or 'associates:' section: {line}"
            )

    validate_word_lists(word_lists)
    return word_lists


def validate_word_lists(word_lists):
    """Catch missing or malformed lists early with helpful errors."""
    if not word_lists:
        raise ValueError("No word lists were found in the dictionary file.")

    for list_name, config in word_lists.items():
        polish_count = len(config["polish_words"])
        english_count = len(config["english_words"])
        associate_count = len(config["dictionary"])

        if polish_count == 0 or english_count == 0:
            raise ValueError(f"{list_name}: must contain at least one translation pair.")
        if polish_count != english_count:
            raise ValueError(
                f"{list_name}: pair mismatch. "
                f"Found {polish_count} Polish words and {english_count} English words."
            )
        if associate_count < 2:
            raise ValueError(
                f"{list_name}: must contain at least two associates for Pearson correlation."
            )


def combine_all_word_lists(word_lists):
    """Combine all configured lists into one large list/dictionary."""
    polish_words = []
    english_words = []
    dictionary = {}

    for config in word_lists.values():
        polish_words.extend(config["polish_words"])
        english_words.extend(config["english_words"])
        for en_word, pl_words in config["dictionary"].items():
            dictionary.setdefault(en_word, [])
            dictionary[en_word].extend(pl_words)
            dictionary[en_word] = unique_preserve_order(dictionary[en_word])

    return {
        "description": "Combined list containing all lists from mini-dict.txt",
        "polish_words": unique_preserve_order(polish_words),
        "english_words": unique_preserve_order(english_words),
        "dictionary": dictionary,
    }


def get_word_config(list_name, word_lists):
    """Choose one configured list or combine all lists."""
    if list_name == "list-all":
        return combine_all_word_lists(word_lists)
    return word_lists[list_name]


def parse_args(word_lists, default_dict_path):
    parser = argparse.ArgumentParser(
        description="Tiny semantic alignment demo with selectable Polish/English word lists."
    )
    parser.add_argument(
        "--dict",
        default=str(default_dict_path),
        help="Path to mini-dict.txt. Default: data/mini-dict.txt next to this script.",
    )

    group = parser.add_mutually_exclusive_group()
    for list_name, config in word_lists.items():
        group.add_argument(
            f"--{list_name}",
            action="store_const",
            const=list_name,
            dest="word_list",
            help=config.get("description") or f"Use {list_name}.",
        )
    group.add_argument(
        "--list-all",
        action="store_const",
        const="list-all",
        dest="word_list",
        help="Combine all available word lists and dictionaries.",
    )
    parser.set_defaults(word_list="list-1" if "list-1" in word_lists else next(iter(word_lists)))
    return parser.parse_args()


def get_dict_path_from_argv(default_dict_path):
    """
    Read only --dict first so we can load the txt file before building list-specific flags.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--dict", default=str(default_dict_path))
    known_args, _ = parser.parse_known_args()
    return Path(known_args.dict).expanduser().resolve()


def print_profile_table(words, vecs, assoc_vectors, col_labels, title):
    """Print a table of similarity profiles."""
    print(f"\n{title}")
    print(f"{'Word':<12}", end="")
    for label in col_labels:
        print(f"{label:>10}", end="")
    print()
    print("-" * (12 + 10 * len(col_labels)))

    for word in words:
        if word in vecs:
            sims = assoc_vectors @ vecs[word]
            print(f"{word:<12}", end="")
            for s in sims:
                print(f"{s:10.4f}", end="")
            print()
    print()


def main():
    script_dir = Path(__file__).resolve().parent
    default_dict_path = script_dir / "data" / "mini-dict.txt"

    dict_path = get_dict_path_from_argv(default_dict_path)
    word_lists = load_word_lists(dict_path)
    args = parse_args(word_lists, default_dict_path)

    # Resolve again after final arg parsing in case --dict was passed.
    dict_path = Path(args.dict).expanduser().resolve()
    word_lists = load_word_lists(dict_path)

    config = get_word_config(args.word_list, word_lists)
    polish_words = config["polish_words"]
    english_words = config["english_words"]
    dictionary = config["dictionary"]

    print(f"Selected word list: {args.word_list} ({config['description']})", flush=True)
    print(f"Dictionary file: {dict_path}\n", flush=True)

    print("Step 2: Gathering words...", flush=True)
    all_words = set(polish_words + english_words)
    for en, pl_list in dictionary.items():
        all_words.add(en)
        all_words.update(pl_list)
    all_words = list(all_words)
    print(f"  {len(all_words)} unique words\n", flush=True)

    print("Step 3: Loading LaBSE model...", flush=True)
    model = SentenceTransformer("sentence-transformers/LaBSE")
    print("  Model loaded.\n", flush=True)

    print("Step 4: Embedding words...", flush=True)
    embeddings = model.encode(
        all_words,
        normalize_embeddings=True,
        show_progress_bar=True,
        batch_size=8,
    )
    vecs = {w: embeddings[i] for i, w in enumerate(all_words)}
    print(f"  Embedded {len(vecs)} words.\n", flush=True)

    print("Step 5: Building associate vectors...", flush=True)
    en_labels = []
    pl_labels = []
    en_assoc_list = []
    pl_assoc_list = []

    for en, pl_list in dictionary.items():
        if en in vecs:
            pl_vecs_list = [vecs[w] for w in pl_list if w in vecs]
            if pl_vecs_list:
                en_assoc_list.append(vecs[en])
                pl_assoc_list.append(np.mean(pl_vecs_list, axis=0))
                en_labels.append(en)
                pl_labels.append(pl_list[0])

    en_assoc = np.array(en_assoc_list)
    pl_assoc = np.array(pl_assoc_list)
    print(f"  {len(en_labels)} associates")
    print(f"  English:  {en_labels}")
    print(f"  Polish:   {pl_labels}\n", flush=True)

    # ── Similarity profiles ──
    print("=" * 70)
    print("SIMILARITY PROFILES (cosine similarity to each associate)")
    print("=" * 70)

    print_profile_table(
        polish_words,
        vecs,
        pl_assoc,
        pl_labels,
        "POLISH WORDS -> POLISH ASSOCIATES",
    )

    print_profile_table(
        english_words,
        vecs,
        en_assoc,
        en_labels,
        "ENGLISH WORDS -> ENGLISH ASSOCIATES",
    )

    # ── R_c matrix ──
    print("=" * 70)
    print("SEMANTIC ALIGNMENT SCORES (Pearson R_c)")
    print("=" * 70)
    print(f"\n{'':>10}", end="")
    for en in english_words:
        print(f"{en:>10}", end="")
    print()
    print("-" * (10 + 10 * len(english_words)))

    for pl in polish_words:
        print(f"{pl:>10}", end="", flush=True)
        for en in english_words:
            if pl in vecs and en in vecs:
                sims_pl = pl_assoc @ vecs[pl]
                sims_en = en_assoc @ vecs[en]
                r_c, _ = pearsonr(sims_en, sims_pl)
                print(f"{r_c:10.4f}", end="", flush=True)
            else:
                print(f"{'N/A':>10}", end="", flush=True)
        print()


    # ── Adjusted score matrix ──
    print("\n" + "=" * 70)
    print("ADJUSTED SCORE (0.25*Rc + 0.75*Cosine_Sim)")
    print("=" * 70)
    print(f"\n{'':>10}", end="")
    for en in english_words:
        print(f"{en:>10}", end="")
    print()
    print("-" * (10 + 10 * len(english_words)))

    for pl in polish_words:
        print(f"{pl:>10}", end="", flush=True)
        for en in english_words:
            if pl in vecs and en in vecs:
                sims_pl = pl_assoc @ vecs[pl]
                sims_en = en_assoc @ vecs[en]
                r_c, _ = pearsonr(sims_en, sims_pl)
                cosine_sim = float(vecs[pl] @ vecs[en])
                adjusted_score = 0.25 * r_c + 0.75 * cosine_sim
                print(f"{adjusted_score:10.4f}", end="", flush=True)
            else:
                print(f"{'N/A':>10}", end="", flush=True)
        print()

    print("\nTop 3 adjusted matches:")
    for label, source_words, target_words in [
        ("Polish -> English", polish_words, english_words),
        ("English -> Polish", english_words, polish_words),
    ]:
        print(f"\n{label}:")
        for src in source_words:
            scores = []
            for tgt in target_words:
                if src in vecs and tgt in vecs:
                    v_src = vecs[src]
                    v_tgt = vecs[tgt]
                    sims_src = pl_assoc @ v_src if src in polish_words else en_assoc @ v_src
                    sims_tgt = en_assoc @ v_tgt if tgt in english_words else pl_assoc @ v_tgt
                    r_c, _ = pearsonr(sims_src, sims_tgt)
                    cosine_sim = float(v_src @ v_tgt)
                    adjusted_score = 0.5 * r_c + 0.5 * cosine_sim
                    scores.append((tgt, adjusted_score, r_c, cosine_sim))
            scores.sort(key=lambda x: x[1], reverse=True)
            top3 = "  ".join(
                f"{w}({score:.3f}; Rc={r_c:.3f}, cos={cos:.3f})"
                for w, score, r_c, cos in scores[:3]
            )
            print(f"  {src:<10} -> {top3}")

    # ── Top matches ──
    print("\n" + "=" * 70)
    print("TOP 3 MATCHES")
    print("=" * 70)

    for label, source_words, target_words in [
        ("Polish -> English", polish_words, english_words),
        ("English -> Polish", english_words, polish_words),
    ]:
        print(f"\n{label}:")
        for src in source_words:
            scores = []
            for tgt in target_words:
                if src in vecs and tgt in vecs:
                    v_src = vecs[src]
                    v_tgt = vecs[tgt]
                    sims_src = pl_assoc @ v_src if src in polish_words else en_assoc @ v_src
                    sims_tgt = en_assoc @ v_tgt if tgt in english_words else pl_assoc @ v_tgt
                    r_c, _ = pearsonr(sims_src, sims_tgt)
                    scores.append((tgt, r_c))
            scores.sort(key=lambda x: x[1], reverse=True)
            top3 = "  ".join(f"{w}({s:.3f})" for w, s in scores[:3])
            print(f"  {src:<10} -> {top3}")

    print("\nDone!", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        sys.exit(1)
