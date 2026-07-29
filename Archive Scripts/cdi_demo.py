"""
cdi_demo.py
===========
CDI semantic alignment demo using similarity-profile correlations.

This version is designed for the larger CDI dictionary file, such as:

    data/big-cdi-dict.txt

The dictionary text file should use this format:

    [list-8]
    description = CDI words

    pairs:
    aparat = camera
    apteka = pharmacy

    associates:
    photo = zdjęcie
    lens = obiektyw

In the pairs section:
    Polish word = English word

In the associates section:
    English associate = Polish associate

What this script does:
    - Loads one list from the CDI dictionary file.
    - Prunes duplicate associate words.
    - Prunes associates that also appear in the pair list.
    - Prints the final associate dictionary size after pruning.
    - Embeds words with LaBSE.
    - Scores matches using Pearson correlation, cosine similarity, or an adjusted blend.
    - Prints correct then incorrect top-ranked matches for each translation direction.
    - Reports accuracy: percent of words whose true pair was ranked #1.
    - Lists incorrect top-ranked words and their top 3 matches.

Usage examples:
    python cdi_demo.py
    python cdi_demo.py --dict data/big-cdi-dict.txt
    python cdi_demo.py --list-8
    python cdi_demo.py --word-list list-8 --direction both
    python cdi_demo.py --score adjusted
    python cdi_demo.py --score adjusted --adjusted-rc-weight 0.5 --adjusted-cosine-weight 0.5
    python cdi_demo.py --dry-run
"""

from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np
from scipy.stats import pearsonr

Pair = Tuple[str, str]
AssociateEntry = Tuple[str, List[str], int]
# Match tuple: target word, selected score, Pearson R_c, direct cosine similarity
Match = Tuple[str, float, float, float]


def normalize_key(text: str) -> str:
    """Normalize text for duplicate checks without changing displayed text."""
    return " ".join(text.strip().casefold().split())


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    """Return items without duplicates while keeping the first occurrence."""
    seen = set()
    output = []
    for item in items:
        key = normalize_key(item)
        if key not in seen:
            seen.add(key)
            output.append(item)
    return output


def parse_key_value_line(line: str, line_number: int) -> Tuple[str, str]:
    """Parse a line like 'left = right'."""
    if "=" not in line:
        raise ValueError(f"Line {line_number}: expected 'left = right', got: {line}")
    left, right = line.split("=", 1)
    left = left.strip()
    right = right.strip()
    if not left or not right:
        raise ValueError(f"Line {line_number}: both sides of '=' must have text, got: {line}")
    return left, right


def default_dict_path(script_dir: Path) -> Path:
    """
    Prefer the original project layout: data/big-cdi-dict.txt.
    Fall back to big-cdi-dict.txt next to this script.
    """
    data_path = script_dir / "data" / "big-cdi-dict.txt"
    same_dir_path = script_dir / "big-cdi-dict.txt"
    if data_path.exists():
        return data_path
    if same_dir_path.exists():
        return same_dir_path
    return data_path


def load_word_lists(dict_path: Path) -> Dict[str, dict]:
    """Load all configured CDI lists from the text dictionary file."""
    if not dict_path.exists():
        raise FileNotFoundError(
            f"Could not find dictionary file: {dict_path}\n"
            "Expected default location: data/big-cdi-dict.txt relative to this script."
        )

    word_lists: Dict[str, dict] = {}
    current_list_name = None
    current_section = None

    with dict_path.open("r", encoding="utf-8-sig") as f:
        for line_number, raw_line in enumerate(f, start=1):
            line = raw_line.strip()

            # Ignore blank lines and comments.
            if not line or line.startswith("#"):
                continue

            # Start a new list block, e.g. [list-8].
            if line.startswith("[") and line.endswith("]"):
                current_list_name = line[1:-1].strip()
                if not current_list_name:
                    raise ValueError(f"Line {line_number}: empty list name.")
                if current_list_name in word_lists:
                    raise ValueError(f"Line {line_number}: duplicate list name: {current_list_name}")
                word_lists[current_list_name] = {
                    "description": "",
                    "pairs": [],
                    "associate_entries": [],
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

            # Treat description specially only before a pairs/associates section.
            # Inside associates, lines such as "description = opis" are valid associates.
            if current_section is None and lower_line.startswith("description"):
                key, value = parse_key_value_line(line, line_number)
                if key.lower() != "description":
                    raise ValueError(f"Line {line_number}: expected 'description = ...', got: {line}")
                word_lists[current_list_name]["description"] = value
                continue

            if current_section == "pairs":
                polish_word, english_word = parse_key_value_line(line, line_number)
                word_lists[current_list_name]["pairs"].append((polish_word, english_word))
                continue

            if current_section == "associates":
                english_associate, polish_associates_text = parse_key_value_line(line, line_number)
                polish_associates = [w.strip() for w in polish_associates_text.split(",") if w.strip()]
                if not polish_associates:
                    raise ValueError(f"Line {line_number}: associate must include at least one Polish word.")
                word_lists[current_list_name]["associate_entries"].append(
                    (english_associate, polish_associates, line_number)
                )
                continue

            raise ValueError(
                f"Line {line_number}: line is not inside 'pairs:' or 'associates:' section: {line}"
            )

    validate_word_lists(word_lists)
    return word_lists


def validate_word_lists(word_lists: Dict[str, dict]) -> None:
    """Catch missing or malformed lists early with helpful errors."""
    if not word_lists:
        raise ValueError("No word lists were found in the dictionary file.")

    for list_name, config in word_lists.items():
        pair_count = len(config["pairs"])
        associate_count = len(config["associate_entries"])

        if pair_count == 0:
            raise ValueError(f"{list_name}: must contain at least one translation pair.")
        if associate_count < 2:
            raise ValueError(
                f"{list_name}: must contain at least two associates for Pearson correlation."
            )


def combine_all_word_lists(word_lists: Dict[str, dict]) -> dict:
    """Combine all configured lists into one large list."""
    pairs: List[Pair] = []
    associate_entries: List[AssociateEntry] = []

    for config in word_lists.values():
        pairs.extend(config["pairs"])
        associate_entries.extend(config["associate_entries"])

    return {
        "description": "Combined list containing all lists from the CDI dictionary file",
        "pairs": pairs,
        "associate_entries": associate_entries,
    }


def get_word_config(list_name: str, word_lists: Dict[str, dict]) -> dict:
    """Choose one configured list or combine all lists."""
    if list_name == "list-all":
        return combine_all_word_lists(word_lists)
    return word_lists[list_name]


def get_dict_path_from_argv(default_path: Path) -> Path:
    """Read only --dict first so we can load the txt file before building list-specific flags."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--dict", default=str(default_path))
    known_args, _ = parser.parse_known_args()
    return Path(known_args.dict).expanduser().resolve()


def parse_args(word_lists: Dict[str, dict], default_path: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CDI semantic alignment demo using profile correlation, cosine, or adjusted scores."
    )
    parser.add_argument(
        "--dict",
        default=str(default_path),
        help="Path to big-cdi-dict.txt. Default: data/big-cdi-dict.txt next to this script.",
    )
    parser.add_argument(
        "--word-list",
        choices=list(word_lists.keys()) + ["list-all"],
        default=None,
        help="Word list to use, e.g. list-8. You can also use --list-8 style flags.",
    )
    parser.add_argument(
        "--direction",
        choices=["pl-en", "en-pl", "both"],
        default="both",
        help="Which direction to evaluate. Default: both.",
    )
    parser.add_argument(
        "--score",
        choices=["rc", "correlation", "cosine", "adjusted"],
        default="rc",
        help=(
            "Scoring method for ranking matches. "
            "rc/correlation = Pearson profile correlation; "
            "cosine = direct embedding cosine; "
            "adjusted = adjusted-rc-weight*R_c + adjusted-cosine-weight*cosine. "
            "Default: rc."
        ),
    )
    parser.add_argument(
        "--adjusted-rc-weight",
        type=float,
        default=0.5,
        help="R_c weight used when --score adjusted. Default: 0.5.",
    )
    parser.add_argument(
        "--adjusted-cosine-weight",
        type=float,
        default=0.5,
        help="Cosine similarity weight used when --score adjusted. Default: 0.5.",
    )
    parser.add_argument(
        "--model",
        default="sentence-transformers/LaBSE",
        help="SentenceTransformer model name. Default: sentence-transformers/LaBSE.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Embedding batch size. Default: 32.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load and prune the dictionary, print counts, then stop before loading the model.",
    )

    # Keep compatibility with the old dynamic style, e.g. --list-8.
    group = parser.add_mutually_exclusive_group()
    for list_name, config in word_lists.items():
        group.add_argument(
            f"--{list_name}",
            action="store_const",
            const=list_name,
            dest="dynamic_word_list",
            help=config.get("description") or f"Use {list_name}.",
        )
    group.add_argument(
        "--list-all",
        action="store_const",
        const="list-all",
        dest="dynamic_word_list",
        help="Combine all available word lists.",
    )

    args = parser.parse_args()

    if args.word_list and args.dynamic_word_list:
        parser.error("Use either --word-list or --list-N, not both.")

    if args.dynamic_word_list:
        args.word_list = args.dynamic_word_list
    elif args.word_list is None:
        args.word_list = "list-8" if "list-8" in word_lists else next(iter(word_lists))

    if args.adjusted_rc_weight < 0 or args.adjusted_cosine_weight < 0:
        parser.error("Adjusted score weights must be non-negative.")
    if args.score == "adjusted" and (args.adjusted_rc_weight + args.adjusted_cosine_weight) == 0:
        parser.error("At least one adjusted score weight must be greater than zero.")

    return args

def build_answer_maps(pairs: Sequence[Pair]) -> Tuple[List[str], List[str], Dict[str, set], Dict[str, set]]:
    """
    Build ordered unique candidate lists and answer maps.

    Some CDI entries repeat a Polish or English form with more than one translation.
    A top match is counted as correct if it appears in that source word's set of true pairs.
    """
    polish_words = unique_preserve_order(pl for pl, _ in pairs)
    english_words = unique_preserve_order(en for _, en in pairs)

    pl_to_en = defaultdict(set)
    en_to_pl = defaultdict(set)
    for pl, en in pairs:
        pl_to_en[pl].add(en)
        en_to_pl[en].add(pl)

    return polish_words, english_words, dict(pl_to_en), dict(en_to_pl)


def prune_associates(
    associate_entries: Sequence[AssociateEntry],
    polish_words: Sequence[str],
    english_words: Sequence[str],
) -> Tuple[Dict[str, List[str]], dict]:
    """
    Prune associates before embedding.

    Removes:
      - duplicate English associate keys;
      - duplicate Polish associate words;
      - English associates that are also English pair words;
      - English associates that are also Polish pair words;
      - Polish associates that are also Polish pair words;
      - Polish associates that are also English pair words.

    The cross-language pair-word checks are intentional because LaBSE embeds all words
    in one shared space, and the point is to keep the associate anchor list separate
    from the tested pair list.
    """
    pair_word_keys = {normalize_key(w) for w in polish_words} | {normalize_key(w) for w in english_words}

    pruned: Dict[str, List[str]] = {}
    seen_english_associates = set()
    seen_polish_associates = set()

    stats = {
        "raw_entries": len(associate_entries),
        "kept_entries": 0,
        "removed_duplicate_english_associate": 0,
        "removed_english_associate_in_pair_list": 0,
        "removed_polish_associate_in_pair_list": 0,
        "removed_duplicate_polish_associate": 0,
        "removed_empty_after_polish_pruning": 0,
    }

    for english_associate, polish_associates, _line_number in associate_entries:
        en_key = normalize_key(english_associate)

        if en_key in seen_english_associates:
            stats["removed_duplicate_english_associate"] += 1
            continue
        seen_english_associates.add(en_key)

        if en_key in pair_word_keys:
            stats["removed_english_associate_in_pair_list"] += 1
            continue

        kept_polish_associates: List[str] = []
        for polish_associate in polish_associates:
            pl_key = normalize_key(polish_associate)

            if pl_key in pair_word_keys:
                stats["removed_polish_associate_in_pair_list"] += 1
                continue
            if pl_key in seen_polish_associates:
                stats["removed_duplicate_polish_associate"] += 1
                continue

            seen_polish_associates.add(pl_key)
            kept_polish_associates.append(polish_associate)

        if not kept_polish_associates:
            stats["removed_empty_after_polish_pruning"] += 1
            continue

        pruned[english_associate] = kept_polish_associates

    stats["kept_entries"] = len(pruned)
    return pruned, stats


def safe_pearsonr(a: np.ndarray, b: np.ndarray) -> float:
    """Pearson r that returns NaN instead of raising for constant vectors."""
    if len(a) < 2 or len(b) < 2:
        return math.nan
    if float(np.std(a)) == 0.0 or float(np.std(b)) == 0.0:
        return math.nan
    r_c, _ = pearsonr(a, b)
    return float(r_c)


def format_score(score: float) -> str:
    if math.isnan(score):
        return "nan"
    return f"{score:.4f}"


def score_label(args: argparse.Namespace) -> str:
    """Human-readable label for the selected score."""
    if args.score in {"rc", "correlation"}:
        return "Pearson R_c"
    if args.score == "cosine":
        return "Cosine similarity"
    return f"Adjusted score ({args.adjusted_rc_weight:g}*R_c + {args.adjusted_cosine_weight:g}*cosine)"


def compute_selected_score(
    r_c: float,
    cosine_sim: float,
    score_mode: str,
    adjusted_rc_weight: float,
    adjusted_cosine_weight: float,
) -> float:
    """Return the score used for ranking based on the selected scoring mode."""
    if score_mode in {"rc", "correlation"}:
        return r_c
    if score_mode == "cosine":
        return cosine_sim
    if score_mode == "adjusted":
        if math.isnan(r_c) or math.isnan(cosine_sim):
            return math.nan
        return adjusted_rc_weight * r_c + adjusted_cosine_weight * cosine_sim
    raise ValueError(f"Unknown score mode: {score_mode}")


def rank_matches(
    source_words: Sequence[str],
    target_words: Sequence[str],
    source_assoc_vectors: np.ndarray,
    target_assoc_vectors: np.ndarray,
    vecs: Dict[str, np.ndarray],
    score_mode: str,
    adjusted_rc_weight: float,
    adjusted_cosine_weight: float,
) -> Dict[str, List[Match]]:
    """Rank target words for every source word using the selected scoring method."""
    rankings: Dict[str, List[Match]] = {}

    profile_cache = {}
    for word in set(source_words) | set(target_words):
        if word not in vecs:
            continue
        if word in source_words:
            profile_cache[("source", word)] = source_assoc_vectors @ vecs[word]
        if word in target_words:
            profile_cache[("target", word)] = target_assoc_vectors @ vecs[word]

    for src in source_words:
        if src not in vecs or ("source", src) not in profile_cache:
            rankings[src] = []
            continue

        scores: List[Match] = []
        sims_src = profile_cache[("source", src)]
        for tgt in target_words:
            if tgt not in vecs or ("target", tgt) not in profile_cache:
                continue
            sims_tgt = profile_cache[("target", tgt)]
            r_c = safe_pearsonr(sims_src, sims_tgt)
            cosine_sim = float(vecs[src] @ vecs[tgt])
            selected_score = compute_selected_score(
                r_c,
                cosine_sim,
                score_mode,
                adjusted_rc_weight,
                adjusted_cosine_weight,
            )
            scores.append((tgt, selected_score, r_c, cosine_sim))

        scores.sort(key=lambda item: (-math.inf if math.isnan(item[1]) else item[1]), reverse=True)
        rankings[src] = scores

    return rankings


def evaluate_rankings(
    rankings: Dict[str, List[Match]],
    answer_map: Dict[str, set],
) -> Tuple[int, int, float, List[dict], List[dict]]:
    """Evaluate whether the top-ranked match is one of the source word's true pairs."""
    total = 0
    correct = 0
    correct_items = []
    incorrect_items = []

    for src, matches in rankings.items():
        valid_targets = answer_map.get(src, set())
        if not valid_targets or not matches:
            continue

        total += 1
        item = {
            "source": src,
            "expected": sorted(valid_targets),
            "top3": matches[:3],
        }
        top_target = matches[0][0]
        if top_target in valid_targets:
            correct += 1
            correct_items.append(item)
        else:
            incorrect_items.append(item)

    percent = (correct / total * 100.0) if total else 0.0
    return correct, total, percent, correct_items, incorrect_items


def print_pruning_summary(stats: dict) -> None:
    print("Associate pruning summary:")
    print(f"  Raw associate entries: {stats['raw_entries']}")
    print(f"  Removed duplicate English associates: {stats['removed_duplicate_english_associate']}")
    print(f"  Removed English associates also in pair list: {stats['removed_english_associate_in_pair_list']}")
    print(f"  Removed Polish associates also in pair list: {stats['removed_polish_associate_in_pair_list']}")
    print(f"  Removed duplicate Polish associates: {stats['removed_duplicate_polish_associate']}")
    print(f"  Removed entries empty after Polish pruning: {stats['removed_empty_after_polish_pruning']}")
    print(f"  Associate dictionary size after pruning: {stats['kept_entries']}\n")


def format_match(match: Match) -> str:
    target, score, r_c, cosine_sim = match
    return (
        f"{target} "
        f"(score={format_score(score)})"
    )


def print_match_items(title: str, items: List[dict], empty_message: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
    if not items:
        print(f"{empty_message}\n")
        return

    for item in items:
        expected = ", ".join(item["expected"])
        top3 = ", ".join(format_match(match) for match in item["top3"])
        print(f"{item['source']} | expected: {expected} | top 3: {top3}")
    print()


def print_direction_results(
    direction_title: str,
    score_name: str,
    correct: int,
    total: int,
    percent: float,
    correct_items: List[dict],
    incorrect_items: List[dict],
) -> None:
    """Print results as correct first, then incorrect, for one translation direction."""
    print("\n" + "#" * 80)
    print(f"{direction_title} results by {score_name}")
    print("#" * 80)
    print_accuracy(f"{direction_title} accuracy", correct, total, percent)

    print_match_items(
        f"Correct {direction_title} top-ranked matches",
        correct_items,
        "No correct top-ranked matches.",
    )
    print_match_items(
        f"Incorrect {direction_title} top-ranked matches",
        incorrect_items,
        "None. All evaluated words had their true pair ranked highest.",
    )


def print_accuracy(title: str, correct: int, total: int, percent: float) -> None:
    print(f"{title}: {correct}/{total} correct ({percent:.2f}%)")

def main() -> int:
    script_dir = Path(__file__).resolve().parent
    default_path = default_dict_path(script_dir)

    first_dict_path = get_dict_path_from_argv(default_path)
    word_lists = load_word_lists(first_dict_path)
    args = parse_args(word_lists, default_path)

    # Resolve again after final arg parsing in case --dict was passed.
    dict_path = Path(args.dict).expanduser().resolve()
    word_lists = load_word_lists(dict_path)
    config = get_word_config(args.word_list, word_lists)

    pairs: List[Pair] = config["pairs"]
    polish_words, english_words, pl_to_en, en_to_pl = build_answer_maps(pairs)

    print(f"Selected word list: {args.word_list} ({config['description']})", flush=True)
    print(f"Dictionary file: {dict_path}", flush=True)
    print(f"Pair rows: {len(pairs)}", flush=True)
    print(f"Unique Polish pair words: {len(polish_words)}", flush=True)
    print(f"Unique English pair words: {len(english_words)}\n", flush=True)

    dictionary, prune_stats = prune_associates(
        config["associate_entries"],
        polish_words,
        english_words,
    )
    print_pruning_summary(prune_stats)

    if len(dictionary) < 2:
        raise ValueError("Need at least two associates after pruning for Pearson correlation.")

    if args.dry_run:
        print("Dry run complete. Model was not loaded.")
        return 0

    print("Step 2: Gathering words...", flush=True)
    all_words = set(polish_words + english_words)
    for en, pl_list in dictionary.items():
        all_words.add(en)
        all_words.update(pl_list)
    all_words = sorted(all_words)
    print(f"  {len(all_words)} unique words to embed\n", flush=True)

    print("Step 3: Importing SentenceTransformer...", flush=True)
    from sentence_transformers import SentenceTransformer
    print("  Import done.\n", flush=True)

    print("Step 4: Loading LaBSE model...", flush=True)
    model = SentenceTransformer(args.model)
    print("  Model loaded.\n", flush=True)

    print("Step 5: Embedding words...", flush=True)
    embeddings = model.encode(
        all_words,
        normalize_embeddings=True,
        show_progress_bar=True,
        batch_size=args.batch_size,
    )
    vecs = {w: embeddings[i] for i, w in enumerate(all_words)}
    print(f"  Embedded {len(vecs)} words.\n", flush=True)

    print("Step 6: Building associate vectors...", flush=True)
    en_assoc_list = []
    pl_assoc_list = []

    for en, pl_list in dictionary.items():
        if en not in vecs:
            continue
        pl_vecs_list = [vecs[w] for w in pl_list if w in vecs]
        if pl_vecs_list:
            en_assoc_list.append(vecs[en])
            pl_assoc_list.append(np.mean(pl_vecs_list, axis=0))

    en_assoc = np.array(en_assoc_list)
    pl_assoc = np.array(pl_assoc_list)
    print(f"  Associate vector pairs used: {len(en_assoc)}\n", flush=True)

    if len(en_assoc) < 2 or len(pl_assoc) < 2:
        raise ValueError("Need at least two usable associate vector pairs for Pearson correlation.")

    all_correct = 0
    all_total = 0

    if args.direction in {"pl-en", "both"}:
        pl_en_rankings = rank_matches(
            source_words=polish_words,
            target_words=english_words,
            source_assoc_vectors=pl_assoc,
            target_assoc_vectors=en_assoc,
            vecs=vecs,
            score_mode=args.score,
            adjusted_rc_weight=args.adjusted_rc_weight,
            adjusted_cosine_weight=args.adjusted_cosine_weight,
        )
        correct, total, percent, correct_items, incorrect_items = evaluate_rankings(pl_en_rankings, pl_to_en)
        print_direction_results(
            "Polish -> English",
            score_label(args),
            correct,
            total,
            percent,
            correct_items,
            incorrect_items,
        )
        all_correct += correct
        all_total += total

    if args.direction in {"en-pl", "both"}:
        en_pl_rankings = rank_matches(
            source_words=english_words,
            target_words=polish_words,
            source_assoc_vectors=en_assoc,
            target_assoc_vectors=pl_assoc,
            vecs=vecs,
            score_mode=args.score,
            adjusted_rc_weight=args.adjusted_rc_weight,
            adjusted_cosine_weight=args.adjusted_cosine_weight,
        )
        correct, total, percent, correct_items, incorrect_items = evaluate_rankings(en_pl_rankings, en_to_pl)
        print_direction_results(
            "English -> Polish",
            score_label(args),
            correct,
            total,
            percent,
            correct_items,
            incorrect_items,
        )
        all_correct += correct
        all_total += total

    if args.direction == "both":
        overall_percent = (all_correct / all_total * 100.0) if all_total else 0.0
        print_accuracy("Overall bidirectional accuracy", all_correct, all_total, overall_percent)

    print(f"Scoring method used: {score_label(args)}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
