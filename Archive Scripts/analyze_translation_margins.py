"""
analyze_translation_margins.py
==============================

Companion analysis script for mini_demo.py / mini_demo_adjusted_score.py.

For each selected list in data/mini-dict.txt, this script calculates how well each
true translation pair separates from the next-best wrong match.

Main output:
    margin = true_translation_score - best_wrong_translation_score

For example, for Polish -> English:
    margin(pies) = score(pies, dog) - max(score(pies, every English word except dog))

It saves:
    1. A CSV of all scores and margins.
    2. A summary chart by direction.
    3. One chart per source word.

Example usage:
    python analyze_translation_margins.py
    python analyze_translation_margins.py --metric rc
    python analyze_translation_margins.py --metric adjusted
    python analyze_translation_margins.py --lists list-1 list-2 list-3 list-4 list-5
    python analyze_translation_margins.py --script mini_demo.py --dict data/mini-dict.txt
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import re
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import pearsonr
from sentence_transformers import SentenceTransformer


Record = Dict[str, object]


def import_demo_script(script_path: Path):
    """Import the existing demo script so we can reuse its mini-dict parser."""
    if not script_path.exists():
        raise FileNotFoundError(f"Could not find script: {script_path}")

    spec = importlib.util.spec_from_file_location("translation_demo_module", script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not import script: {script_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    required = ["load_word_lists", "get_word_config"]
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise AttributeError(
            f"The script {script_path} is missing required functions: {', '.join(missing)}"
        )

    return module


def list_sort_key(name: str):
    """Sort list-1, list-2, ..., list-10 in numeric order when possible."""
    match = re.fullmatch(r"list-(\d+)", name)
    if match:
        return (0, int(match.group(1)))
    return (1, name)


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    output = []
    for item in items:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


def gather_all_words(word_lists: dict, demo_module, selected_lists: List[str]) -> List[str]:
    """Gather every target word and associate word needed across all selected lists."""
    words: List[str] = []
    for list_name in selected_lists:
        config = demo_module.get_word_config(list_name, word_lists)
        words.extend(config["polish_words"])
        words.extend(config["english_words"])
        for en_associate, pl_associates in config["dictionary"].items():
            words.append(en_associate)
            words.extend(pl_associates)
    return unique_preserve_order(words)


def build_vectors(model_name: str, all_words: List[str], batch_size: int) -> Dict[str, np.ndarray]:
    print(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name)

    print(f"Embedding {len(all_words)} unique words...")
    embeddings = model.encode(
        all_words,
        normalize_embeddings=True,
        show_progress_bar=True,
        batch_size=batch_size,
    )
    return {word: embeddings[i] for i, word in enumerate(all_words)}


def build_associate_vectors(config: dict, vecs: Dict[str, np.ndarray]):
    """Build parallel English and Polish associate vectors for one list."""
    en_labels = []
    pl_labels = []
    en_assoc_list = []
    pl_assoc_list = []

    for en_associate, pl_associates in config["dictionary"].items():
        if en_associate not in vecs:
            continue

        pl_vecs = [vecs[w] for w in pl_associates if w in vecs]
        if not pl_vecs:
            continue

        en_labels.append(en_associate)
        pl_labels.append(pl_associates[0])
        en_assoc_list.append(vecs[en_associate])
        pl_assoc_list.append(np.mean(pl_vecs, axis=0))

    if len(en_assoc_list) < 2:
        raise ValueError("Need at least two usable associates for Pearson correlation.")

    return (
        np.array(en_assoc_list),
        np.array(pl_assoc_list),
        en_labels,
        pl_labels,
    )


def score_pair(
    source_word: str,
    target_word: str,
    source_language: str,
    vecs: Dict[str, np.ndarray],
    en_assoc: np.ndarray,
    pl_assoc: np.ndarray,
    metric: str,
    rc_weight: float,
    cos_weight: float,
) -> Tuple[float, float, float]:
    """Return selected score, Rc, and cosine similarity for one candidate pair."""
    if source_language == "pl":
        source_profile = pl_assoc @ vecs[source_word]
        target_profile = en_assoc @ vecs[target_word]
    elif source_language == "en":
        source_profile = en_assoc @ vecs[source_word]
        target_profile = pl_assoc @ vecs[target_word]
    else:
        raise ValueError("source_language must be 'pl' or 'en'.")

    r_c, _ = pearsonr(source_profile, target_profile)
    cosine_sim = float(vecs[source_word] @ vecs[target_word])

    if metric == "rc":
        score = float(r_c)
    elif metric == "cosine":
        score = cosine_sim
    elif metric == "adjusted":
        score = rc_weight * float(r_c) + cos_weight * cosine_sim
    else:
        raise ValueError("metric must be one of: rc, cosine, adjusted")

    return score, float(r_c), cosine_sim


def analyze_list(
    list_name: str,
    config: dict,
    vecs: Dict[str, np.ndarray],
    metric: str,
    rc_weight: float,
    cos_weight: float,
) -> List[Record]:
    """Calculate true-vs-next-best margins for one configured list."""
    english_words = config["english_words"]
    polish_words = config["polish_words"]
    assoc_count = len(config["dictionary"])

    en_assoc, pl_assoc, _, _ = build_associate_vectors(config, vecs)

    records: List[Record] = []

    # Polish -> English margins
    for i, pl_word in enumerate(polish_words):
        gold_en = english_words[i]
        candidate_scores = []

        for en_word in english_words:
            score, r_c, cosine_sim = score_pair(
                pl_word,
                en_word,
                "pl",
                vecs,
                en_assoc,
                pl_assoc,
                metric,
                rc_weight,
                cos_weight,
            )
            candidate_scores.append((en_word, score, r_c, cosine_sim))

        records.append(make_record(list_name, assoc_count, "pl_to_en", pl_word, gold_en, candidate_scores))

    # English -> Polish margins
    for i, en_word in enumerate(english_words):
        gold_pl = polish_words[i]
        candidate_scores = []

        for pl_word in polish_words:
            score, r_c, cosine_sim = score_pair(
                en_word,
                pl_word,
                "en",
                vecs,
                en_assoc,
                pl_assoc,
                metric,
                rc_weight,
                cos_weight,
            )
            candidate_scores.append((pl_word, score, r_c, cosine_sim))

        records.append(make_record(list_name, assoc_count, "en_to_pl", en_word, gold_pl, candidate_scores))

    return records


def make_record(
    list_name: str,
    assoc_count: int,
    direction: str,
    source_word: str,
    gold_word: str,
    candidate_scores: List[Tuple[str, float, float, float]],
) -> Record:
    """Convert scored candidates into one margin record."""
    candidate_scores = sorted(candidate_scores, key=lambda x: x[1], reverse=True)

    gold_rows = [row for row in candidate_scores if row[0] == gold_word]
    if not gold_rows:
        raise ValueError(f"Gold word {gold_word!r} missing for source {source_word!r}.")

    gold_score, gold_rc, gold_cos = gold_rows[0][1], gold_rows[0][2], gold_rows[0][3]
    wrong_rows = [row for row in candidate_scores if row[0] != gold_word]
    best_wrong_word, best_wrong_score, best_wrong_rc, best_wrong_cos = wrong_rows[0]

    rank = 1 + sum(1 for _, score, _, _ in candidate_scores if score > gold_score)
    margin = gold_score - best_wrong_score

    return {
        "list_name": list_name,
        "associate_count": assoc_count,
        "direction": direction,
        "source_word": source_word,
        "gold_word": gold_word,
        "gold_score": gold_score,
        "gold_rc": gold_rc,
        "gold_cosine": gold_cos,
        "best_wrong_word": best_wrong_word,
        "best_wrong_score": best_wrong_score,
        "best_wrong_rc": best_wrong_rc,
        "best_wrong_cosine": best_wrong_cos,
        "margin": margin,
        "rank": rank,
        "is_top_match": rank == 1,
    }


def write_csv(records: List[Record], csv_path: Path):
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "list_name",
        "associate_count",
        "direction",
        "source_word",
        "gold_word",
        "gold_score",
        "gold_rc",
        "gold_cosine",
        "best_wrong_word",
        "best_wrong_score",
        "best_wrong_rc",
        "best_wrong_cosine",
        "margin",
        "rank",
        "is_top_match",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def plot_summary(records: List[Record], out_dir: Path, metric: str):
    """Create one summary chart per direction with all source words."""
    out_dir.mkdir(parents=True, exist_ok=True)
    directions = sorted(set(str(r["direction"]) for r in records))

    for direction in directions:
        subset = [r for r in records if r["direction"] == direction]
        words = sorted(set(str(r["source_word"]) for r in subset))

        plt.figure(figsize=(12, 7))
        for word in words:
            word_rows = [r for r in subset if r["source_word"] == word]
            word_rows.sort(key=lambda r: (int(r["associate_count"]), str(r["list_name"])))
            x = [int(r["associate_count"]) for r in word_rows]
            y = [float(r["margin"]) for r in word_rows]
            plt.plot(x, y, marker="o", label=word)

        plt.axhline(0, linestyle="--", linewidth=1)
        plt.title(f"Translation margin by associate count ({direction}, {metric})")
        plt.xlabel("Number of associates")
        plt.ylabel("Gold score minus best wrong score")
        plt.legend(loc="best", fontsize="small")
        plt.tight_layout()
        plt.savefig(out_dir / f"summary_{direction}_{metric}.png", dpi=200)
        plt.close()


def plot_each_word(records: List[Record], out_dir: Path, metric: str):
    """Create one chart per source word and direction."""
    word_dir = out_dir / "by_word"
    word_dir.mkdir(parents=True, exist_ok=True)

    keys = sorted(set((str(r["direction"]), str(r["source_word"])) for r in records))
    for direction, word in keys:
        subset = [r for r in records if r["direction"] == direction and r["source_word"] == word]
        subset.sort(key=lambda r: (int(r["associate_count"]), str(r["list_name"])))

        x = [int(r["associate_count"]) for r in subset]
        y = [float(r["margin"]) for r in subset]
        labels = [str(r["list_name"]) for r in subset]

        plt.figure(figsize=(8, 5))
        plt.plot(x, y, marker="o")
        plt.axhline(0, linestyle="--", linewidth=1)

        for x_i, y_i, label in zip(x, y, labels):
            plt.annotate(label, (x_i, y_i), textcoords="offset points", xytext=(5, 5), fontsize=8)

        plt.title(f"{word}: translation margin by associate count ({direction}, {metric})")
        plt.xlabel("Number of associates")
        plt.ylabel("Gold score minus best wrong score")
        plt.tight_layout()

        safe_word = re.sub(r"[^\w\-]+", "_", word, flags=re.UNICODE)
        plt.savefig(word_dir / f"{direction}_{safe_word}_{metric}.png", dpi=200)
        plt.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Chart translation-score margins across mini-dict lists."
    )
    parser.add_argument(
        "--script",
        default="mini_demo.py",
        help="Path to the existing demo script. Default: mini_demo.py",
    )
    parser.add_argument(
        "--dict",
        default="data/mini-dict.txt",
        help="Path to mini-dict.txt. Default: data/mini-dict.txt",
    )
    parser.add_argument(
        "--lists",
        nargs="+",
        default=None,
        help="Specific lists to analyze, e.g. --lists list-1 list-2 list-5. Default: all list-* sections.",
    )
    parser.add_argument(
        "--metric",
        choices=["rc", "cosine", "adjusted"],
        default="rc",
        help="Which score to use for the margin. Default: rc.",
    )
    parser.add_argument(
        "--rc-weight",
        type=float,
        default=0.25,
        help="Rc weight for adjusted metric. Default: 0.25.",
    )
    parser.add_argument(
        "--cos-weight",
        type=float,
        default=0.75,
        help="Cosine weight for adjusted metric. Default: 0.75.",
    )
    parser.add_argument(
        "--model",
        default="sentence-transformers/LaBSE",
        help="SentenceTransformer model name. Default: sentence-transformers/LaBSE",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=8,
        help="Embedding batch size. Default: 8.",
    )
    parser.add_argument(
        "--out-dir",
        default="data/margin-analysis",
        help="Output folder for CSV and charts. Default: data/margin-analysis",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    script_path = Path(args.script).expanduser().resolve()
    dict_path = Path(args.dict).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    demo_module = import_demo_script(script_path)
    word_lists = demo_module.load_word_lists(dict_path)

    if args.lists:
        selected_lists = args.lists
    else:
        selected_lists = sorted(
            [name for name in word_lists.keys() if re.fullmatch(r"list-\d+", name)],
            key=list_sort_key,
        )

    if not selected_lists:
        raise ValueError("No lists selected or found.")

    missing = [name for name in selected_lists if name not in word_lists]
    if missing:
        raise ValueError(f"These lists were not found in {dict_path}: {', '.join(missing)}")

    print("Analyzing lists:", ", ".join(selected_lists))
    print("Metric:", args.metric)
    if args.metric == "adjusted":
        print(f"Adjusted formula: {args.rc_weight}*Rc + {args.cos_weight}*Cosine_Sim")

    all_words = gather_all_words(word_lists, demo_module, selected_lists)
    vecs = build_vectors(args.model, all_words, args.batch_size)

    records: List[Record] = []
    for list_name in selected_lists:
        config = demo_module.get_word_config(list_name, word_lists)
        list_records = analyze_list(
            list_name,
            config,
            vecs,
            args.metric,
            args.rc_weight,
            args.cos_weight,
        )
        records.extend(list_records)

    csv_path = out_dir / f"translation_margins_{args.metric}.csv"
    write_csv(records, csv_path)
    plot_summary(records, out_dir, args.metric)
    plot_each_word(records, out_dir, args.metric)

    print("\nDone.")
    print(f"CSV saved to: {csv_path}")
    print(f"Charts saved to: {out_dir}")


if __name__ == "__main__":
    main()
