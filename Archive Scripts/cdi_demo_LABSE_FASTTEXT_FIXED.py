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
    English associate alternative(s) = Polish associate alternative(s)

Comma or slash can separate alternatives on either side. For example:
    picture, photo = obraz, zdjęcie
    quick/fast = prędko/szybko

What this script does:
    - Loads one list from the CDI dictionary file.
    - Merges overlapping associate translations into bilingual many-to-many groups.
    - Keeps associates even if they also appear in the pair list.
    - Computes cosine to each alternative separately, then averages the cosine scores.
    - Prints the final bilingual associate-group size after consolidation.
    - Embeds words with LaBSE or FastText vectors.
    - Scores matches using Pearson correlation, cosine similarity, or an adjusted blend.
    - Can compute profile correlations using all associates, only positive-overlap associates, or strongest-N associates.
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
    python cdi_demo.py --profile-filter positive
    python cdi_demo.py --profile-filter top-n --top-n 100
    python cdi_demo.py --embedding labse
    python cdi_demo.py --embedding fasttext --pl-vectors data/cc.pl.300.vec --en-vectors data/cc.en.300.vec
    python cdi_demo.py --dry-run
"""

from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
from scipy.stats import pearsonr

Pair = Tuple[str, str]
# One associates row: English alternatives, Polish alternatives, source line number.
AssociateEntry = Tuple[List[str], List[str], int]
# One merged bilingual semantic dimension: English alternatives, Polish alternatives.
AssociateGroup = Tuple[List[str], List[str]]
# Match tuple: target word, selected score, Pearson R_c, direct cosine similarity.
Match = Tuple[str, float, float, float]


@dataclass(frozen=True)
class AssociateProfileSide:
    """Flattened associate vectors plus boundaries for averaging cosine scores by group."""

    vectors: np.ndarray
    group_starts: np.ndarray
    group_counts: np.ndarray


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


def parse_alternatives(text: str, line_number: int, side_name: str) -> List[str]:
    """Parse comma- or slash-separated alternatives, preserving first-seen display text."""
    # Both delimiters occur in CDI-style files. Spaces and hyphens remain part of a phrase.
    raw_items = text.replace("/", ",").split(",")
    items = unique_preserve_order(item.strip() for item in raw_items if item.strip())
    if not items:
        raise ValueError(f"Line {line_number}: {side_name} must include at least one item.")
    return items


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
                polish_text, english_text = parse_key_value_line(line, line_number)
                polish_alternatives = parse_alternatives(polish_text, line_number, "Polish pair side")
                english_alternatives = parse_alternatives(english_text, line_number, "English pair side")
                # Every cross-language combination is accepted as a true translation. Each
                # alternative is still evaluated as its own word rather than as a phrase/vector.
                for polish_word in polish_alternatives:
                    for english_word in english_alternatives:
                        word_lists[current_list_name]["pairs"].append((polish_word, english_word))
                continue

            if current_section == "associates":
                english_text, polish_text = parse_key_value_line(line, line_number)
                english_associates = parse_alternatives(
                    english_text, line_number, "English associate side"
                )
                polish_associates = parse_alternatives(
                    polish_text, line_number, "Polish associate side"
                )
                word_lists[current_list_name]["associate_entries"].append(
                    (english_associates, polish_associates, line_number)
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
        "--profile-filter",
        choices=["all", "positive", "top-n"],
        default="all",
        help=(
            "Which associate dimensions to use when computing Pearson R_c profiles. "
            "all = use all associate dimensions; "
            "positive = keep only dimensions where both source and target have positive cosine similarities; "
            "top-n = keep the strongest N dimensions by average source/target cosine. "
            "Default: all."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Number of associate dimensions to keep when --profile-filter top-n. Default: 100.",
    )
    parser.add_argument(
        "--embedding",
        choices=["labse", "fasttext"],
        default="labse",
        help=(
            "Embedding backend. labse uses SentenceTransformer; fasttext loads static vectors "
            "from --pl-vectors and --en-vectors. Default: labse."
        ),
    )
    parser.add_argument(
        "--model",
        default="sentence-transformers/LaBSE",
        help="SentenceTransformer model name used by --embedding labse.",
    )
    parser.add_argument(
        "--pl-vectors",
        default=None,
        help=(
            "Polish FastText .vec file. The default "
            "is data/cc.pl.300.vec relative to the script when that file exists."
        ),
    )
    parser.add_argument(
        "--en-vectors",
        default=None,
        help=(
            "English FastText .vec file. The default "
            "is data/cc.en.300.vec relative to the script when that file exists."
        ),
    )
    parser.add_argument(
        "--allow-static-crosslingual-cosine",
        action="store_true",
        help=(
            "Allow direct Polish-English cosine for static vectors. Use only when the Polish "
            "and English vectors are already aligned in the same cross-lingual space."
        ),
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
        help="Load and consolidate the dictionary, print counts, then stop before loading the model.",
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
    if args.top_n < 2:
        parser.error("--top-n must be at least 2 because Pearson correlation needs at least two dimensions.")
    if args.embedding == "fasttext":
        script_dir = Path(__file__).resolve().parent
        default_pl = script_dir / "data" / "cc.pl.300.vec"
        default_en = script_dir / "data" / "cc.en.300.vec"
        if args.pl_vectors is None and default_pl.exists():
            args.pl_vectors = str(default_pl)
        if args.en_vectors is None and default_en.exists():
            args.en_vectors = str(default_en)
        if not args.pl_vectors or not args.en_vectors:
            parser.error(
                "--embedding fasttext requires --pl-vectors and --en-vectors. "
                "FastText defaults are detected automatically only when data/cc.pl.300.vec "
                "and data/cc.en.300.vec exist next to the script."
            )
        if args.score in {"cosine", "adjusted"} and not args.allow_static_crosslingual_cosine:
            parser.error(
                "Direct Polish-English cosine is not valid for separate monolingual static spaces. "
                "Use --score rc, or provide aligned cross-lingual vectors and add "
                "--allow-static-crosslingual-cosine."
            )

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


def build_associate_groups(
    associate_entries: Sequence[AssociateEntry],
) -> Tuple[List[AssociateGroup], dict]:
    """
    Build symmetric many-to-many bilingual associate groups.

    Each associate row may contain alternatives on either side. Rows that overlap
    through any English or Polish associate are merged into one connected bilingual
    group. This prevents duplicate dimensions while retaining every observed
    translation alternative instead of arbitrarily dropping later mappings.
    """
    parent: Dict[Tuple[str, str], Tuple[str, str]] = {}
    rank: Dict[Tuple[str, str], int] = {}
    display: Dict[Tuple[str, str], str] = {}
    first_seen: Dict[Tuple[str, str], int] = {}
    raw_row_keys = set()
    duplicate_rows = 0
    order_counter = 0

    def make_node(language: str, word: str) -> Tuple[str, str]:
        nonlocal order_counter
        node = (language, normalize_key(word))
        if node not in parent:
            parent[node] = node
            rank[node] = 0
            display[node] = word
            first_seen[node] = order_counter
            order_counter += 1
        return node

    def find(node: Tuple[str, str]) -> Tuple[str, str]:
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != node:
            next_node = parent[node]
            parent[node] = root
            node = next_node
        return root

    def union(a: Tuple[str, str], b: Tuple[str, str]) -> None:
        root_a = find(a)
        root_b = find(b)
        if root_a == root_b:
            return
        if rank[root_a] < rank[root_b]:
            root_a, root_b = root_b, root_a
        parent[root_b] = root_a
        if rank[root_a] == rank[root_b]:
            rank[root_a] += 1

    for english_alternatives, polish_alternatives, _line_number in associate_entries:
        en_words = unique_preserve_order(english_alternatives)
        pl_words = unique_preserve_order(polish_alternatives)
        row_key = (
            tuple(sorted(normalize_key(word) for word in en_words)),
            tuple(sorted(normalize_key(word) for word in pl_words)),
        )
        if row_key in raw_row_keys:
            duplicate_rows += 1
        else:
            raw_row_keys.add(row_key)

        nodes = [make_node("en", word) for word in en_words]
        nodes.extend(make_node("pl", word) for word in pl_words)
        anchor = nodes[0]
        for node in nodes[1:]:
            union(anchor, node)

    components: Dict[Tuple[str, str], List[Tuple[str, str]]] = defaultdict(list)
    for node in parent:
        components[find(node)].append(node)

    ordered_components = sorted(
        components.values(),
        key=lambda nodes: min(first_seen[node] for node in nodes),
    )
    groups: List[AssociateGroup] = []
    largest_group_size = 0
    for nodes in ordered_components:
        english_nodes = sorted(
            (node for node in nodes if node[0] == "en"),
            key=lambda node: first_seen[node],
        )
        polish_nodes = sorted(
            (node for node in nodes if node[0] == "pl"),
            key=lambda node: first_seen[node],
        )
        if not english_nodes or not polish_nodes:
            # This cannot occur for valid rows, but keep the invariant explicit.
            continue
        english_words = [display[node] for node in english_nodes]
        polish_words = [display[node] for node in polish_nodes]
        groups.append((english_words, polish_words))
        largest_group_size = max(largest_group_size, len(nodes))

    stats = {
        "raw_entries": len(associate_entries),
        "exact_duplicate_rows": duplicate_rows,
        "unique_english_associates": sum(1 for node in parent if node[0] == "en"),
        "unique_polish_associates": sum(1 for node in parent if node[0] == "pl"),
        "kept_entries": len(groups),
        "rows_merged_into_groups": max(0, len(associate_entries) - len(groups)),
        "largest_group_size": largest_group_size,
    }
    return groups, stats


def l2_normalize(vector: np.ndarray) -> Optional[np.ndarray]:
    """Return a float32 unit vector, or None for a zero/invalid vector."""
    vector = np.asarray(vector, dtype=np.float32)
    norm = float(np.linalg.norm(vector))
    if not np.isfinite(norm) or norm == 0.0:
        return None
    return vector / norm


def candidate_tokenizations(text: str) -> List[List[str]]:
    """Generate conservative tokenizations for static word-vector lookup."""
    normalized = " ".join(text.strip().split())
    candidates = [[normalized]]
    if " " in normalized or "-" in normalized:
        pieces = [piece for piece in normalized.replace("-", " ").split() if piece]
        if pieces:
            candidates.append(pieces)
    return candidates


def load_text_vectors_subset(path: Path, needed_tokens: set[str]) -> Dict[str, np.ndarray]:
    """Stream a FastText .vec file and retain only requested tokens."""
    if not path.exists():
        raise FileNotFoundError(f"Vector file not found: {path}")

    needed_exact = set(needed_tokens)
    needed_casefold = {token.casefold() for token in needed_tokens}
    found: Dict[str, np.ndarray] = {}
    expected_dim: Optional[int] = None

    file_size_gb = path.stat().st_size / (1024 ** 3)
    print(f"  Streaming {path} ({file_size_gb:.2f} GB)...", flush=True)

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        first = handle.readline().strip()
        first_parts = first.split()
        has_header = len(first_parts) == 2 and all(part.isdigit() for part in first_parts)
        if has_header:
            expected_dim = int(first_parts[1])
        else:
            handle.seek(0)

        for line_number, line in enumerate(handle, start=2 if has_header else 1):
            parts = line.rstrip().split()
            if len(parts) < 3:
                continue
            token = parts[0]
            token_key = token.casefold()
            if token not in needed_exact and token_key not in needed_casefold:
                continue
            try:
                vector = np.asarray(parts[1:], dtype=np.float32)
            except ValueError:
                continue
            if expected_dim is None:
                expected_dim = len(vector)
            if len(vector) != expected_dim:
                continue
            normalized = l2_normalize(vector)
            if normalized is not None:
                found[token] = normalized
                found.setdefault(token_key, normalized)

    print(f"  Loaded {len({k for k in found if k in needed_exact or k.casefold() in needed_casefold})} matching token keys.", flush=True)
    return found


def build_static_phrase_vectors(
    words: Sequence[str],
    token_vectors: Dict[str, np.ndarray],
) -> Tuple[Dict[str, np.ndarray], List[str]]:
    """Build word/phrase vectors; multiword expressions are means of available token vectors."""
    output: Dict[str, np.ndarray] = {}
    missing: List[str] = []

    def lookup(token: str) -> Optional[np.ndarray]:
        variants = [token, token.casefold(), token.lower()]
        for variant in variants:
            if variant in token_vectors:
                return np.asarray(token_vectors[variant], dtype=np.float32)
        return None

    for word in words:
        built = None
        for tokenization in candidate_tokenizations(word):
            pieces = [lookup(token) for token in tokenization]
            pieces = [piece for piece in pieces if piece is not None]
            if len(pieces) == len(tokenization) and pieces:
                built = l2_normalize(np.mean(pieces, axis=0))
                if built is not None:
                    break
        if built is None:
            missing.append(word)
        else:
            output[word] = built
    return output, missing


def load_embedding_backend(
    args: argparse.Namespace,
    polish_vocab: Sequence[str],
    english_vocab: Sequence[str],
) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray], bool]:
    """Load LaBSE or FastText embeddings and return language-specific vectors."""
    if args.embedding == "labse":
        print("Step 3: Importing SentenceTransformer...", flush=True)
        from sentence_transformers import SentenceTransformer
        print("  Import done.\n", flush=True)
        print("Step 4: Loading LaBSE model...", flush=True)
        model = SentenceTransformer(args.model)
        print("  Model loaded.\n", flush=True)
        combined = unique_preserve_order(list(polish_vocab) + list(english_vocab))
        print("Step 5: Embedding words with LaBSE...", flush=True)
        embeddings = model.encode(
            combined,
            normalize_embeddings=True,
            show_progress_bar=True,
            batch_size=args.batch_size,
        )
        shared = {word: np.asarray(embeddings[i], dtype=np.float32) for i, word in enumerate(combined)}
        return ({word: shared[word] for word in polish_vocab if word in shared},
                {word: shared[word] for word in english_vocab if word in shared},
                True)

    pl_path = Path(args.pl_vectors).expanduser().resolve()
    en_path = Path(args.en_vectors).expanduser().resolve()
    print(f"Step 3: Loading {args.embedding} vectors...", flush=True)

    pl_tokens = {token for word in polish_vocab for toks in candidate_tokenizations(word) for token in toks}
    en_tokens = {token for word in english_vocab for toks in candidate_tokenizations(word) for token in toks}
    pl_tokens_map = load_text_vectors_subset(pl_path, pl_tokens)
    en_tokens_map = load_text_vectors_subset(en_path, en_tokens)
    pl_vecs, pl_missing = build_static_phrase_vectors(polish_vocab, pl_tokens_map)
    en_vecs, en_missing = build_static_phrase_vectors(english_vocab, en_tokens_map)

    print(f"  Polish vectors built: {len(pl_vecs)}/{len(polish_vocab)}")
    print(f"  English vectors built: {len(en_vecs)}/{len(english_vocab)}")
    if pl_missing:
        print(f"  Missing Polish words/phrases: {len(pl_missing)} (first 20: {', '.join(pl_missing[:20])})")
    if en_missing:
        print(f"  Missing English words/phrases: {len(en_missing)} (first 20: {', '.join(en_missing[:20])})")
    print()
    return pl_vecs, en_vecs, bool(args.allow_static_crosslingual_cosine)


def build_associate_profile_sides(
    groups: Sequence[AssociateGroup],
    polish_vecs: Dict[str, np.ndarray],
    english_vecs: Dict[str, np.ndarray],
) -> Tuple[AssociateProfileSide, AssociateProfileSide, dict]:
    """
    Build aligned profile structures without averaging embeddings.

    Every alternative vector remains separate. During profile calculation, cosine
    similarities are computed to all available alternatives and those scalar cosine
    values are averaged within the bilingual associate group.
    """
    english_flat: List[np.ndarray] = []
    polish_flat: List[np.ndarray] = []
    english_starts: List[int] = []
    polish_starts: List[int] = []
    english_counts: List[int] = []
    polish_counts: List[int] = []

    stats = {
        "input_groups": len(groups),
        "used_groups": 0,
        "dropped_missing_english_group": 0,
        "dropped_missing_polish_group": 0,
        "missing_english_alternatives": 0,
        "missing_polish_alternatives": 0,
        "partially_available_groups": 0,
    }

    for english_words, polish_words in groups:
        available_english = [english_vecs[word] for word in english_words if word in english_vecs]
        available_polish = [polish_vecs[word] for word in polish_words if word in polish_vecs]
        missing_english = len(english_words) - len(available_english)
        missing_polish = len(polish_words) - len(available_polish)
        stats["missing_english_alternatives"] += missing_english
        stats["missing_polish_alternatives"] += missing_polish

        if not available_english:
            stats["dropped_missing_english_group"] += 1
            continue
        if not available_polish:
            stats["dropped_missing_polish_group"] += 1
            continue
        if missing_english or missing_polish:
            stats["partially_available_groups"] += 1

        english_starts.append(len(english_flat))
        polish_starts.append(len(polish_flat))
        english_counts.append(len(available_english))
        polish_counts.append(len(available_polish))
        english_flat.extend(available_english)
        polish_flat.extend(available_polish)
        stats["used_groups"] += 1

    if not english_flat or not polish_flat:
        raise ValueError("No usable bilingual associate groups remain after vector lookup.")

    english_side = AssociateProfileSide(
        vectors=np.asarray(english_flat, dtype=np.float32),
        group_starts=np.asarray(english_starts, dtype=np.int64),
        group_counts=np.asarray(english_counts, dtype=np.float32),
    )
    polish_side = AssociateProfileSide(
        vectors=np.asarray(polish_flat, dtype=np.float32),
        group_starts=np.asarray(polish_starts, dtype=np.int64),
        group_counts=np.asarray(polish_counts, dtype=np.float32),
    )
    return english_side, polish_side, stats


def compute_similarity_profile(
    side: AssociateProfileSide,
    word_vector: np.ndarray,
) -> np.ndarray:
    """Compute each cosine separately, then average cosine scores within each group."""
    individual_cosines = side.vectors @ word_vector
    group_sums = np.add.reduceat(individual_cosines, side.group_starts)
    return np.asarray(group_sums / side.group_counts, dtype=np.float32)


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


def profile_filter_label(args: argparse.Namespace) -> str:
    """Human-readable label for selected profile filtering."""
    if args.profile_filter == "all":
        return "all associate dimensions"
    if args.profile_filter == "positive":
        return "positive-overlap associate dimensions only"
    return f"strongest {args.top_n} associate dimensions"


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


def filter_profile_pair(
    sims_src: np.ndarray,
    sims_tgt: np.ndarray,
    profile_filter: str,
    top_n: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Select which associate dimensions to keep before Pearson R_c is calculated.

    all: keep every associate dimension.
    positive: keep only dimensions where both source and target similarities are > 0.
    top-n: keep the N strongest dimensions by the average of source and target similarities.
    """
    if profile_filter == "all":
        return sims_src, sims_tgt

    if profile_filter == "positive":
        mask = (sims_src > 0) & (sims_tgt > 0)
        return sims_src[mask], sims_tgt[mask]

    if profile_filter == "top-n":
        if len(sims_src) == 0 or len(sims_tgt) == 0:
            return sims_src[:0], sims_tgt[:0]
        n = min(top_n, len(sims_src))
        # Average strength rewards dimensions that are strong for both words,
        # rather than dimensions that are extreme for only one side.
        strength = (sims_src + sims_tgt) / 2.0
        top_indices = np.argsort(strength)[-n:]
        # Sort selected dimensions back into original associate order so the
        # profile remains interpretable/reproducible.
        top_indices = np.sort(top_indices)
        return sims_src[top_indices], sims_tgt[top_indices]

    raise ValueError(f"Unknown profile filter: {profile_filter}")


def rank_matches(
    source_words: Sequence[str],
    target_words: Sequence[str],
    source_assoc_side: AssociateProfileSide,
    target_assoc_side: AssociateProfileSide,
    source_vecs: Dict[str, np.ndarray],
    target_vecs: Dict[str, np.ndarray],
    direct_cosine_available: bool,
    score_mode: str,
    adjusted_rc_weight: float,
    adjusted_cosine_weight: float,
    profile_filter: str,
    top_n: int,
) -> Dict[str, List[Match]]:
    """Rank target words for every source word using the selected scoring method."""
    rankings: Dict[str, List[Match]] = {}

    profile_cache = {}
    for word in source_words:
        if word in source_vecs:
            profile_cache[("source", word)] = compute_similarity_profile(
                source_assoc_side, source_vecs[word]
            )
    for word in target_words:
        if word in target_vecs:
            profile_cache[("target", word)] = compute_similarity_profile(
                target_assoc_side, target_vecs[word]
            )

    for src in source_words:
        if src not in source_vecs or ("source", src) not in profile_cache:
            rankings[src] = []
            continue

        scores: List[Match] = []
        sims_src = profile_cache[("source", src)]
        for tgt in target_words:
            if tgt not in target_vecs or ("target", tgt) not in profile_cache:
                continue
            sims_tgt = profile_cache[("target", tgt)]
            filtered_src, filtered_tgt = filter_profile_pair(
                sims_src,
                sims_tgt,
                profile_filter,
                top_n,
            )
            r_c = safe_pearsonr(filtered_src, filtered_tgt)
            cosine_sim = (
                float(source_vecs[src] @ target_vecs[tgt])
                if direct_cosine_available
                else math.nan
            )
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
    print("Associate grouping summary:")
    print(f"  Raw associate rows: {stats['raw_entries']}")
    print(f"  Exact duplicate rows found: {stats['exact_duplicate_rows']}")
    print(f"  Unique English associate forms: {stats['unique_english_associates']}")
    print(f"  Unique Polish associate forms: {stats['unique_polish_associates']}")
    print("  Removed associates because they were in pair list: 0 (disabled; pair words are kept)")
    print(f"  Rows consolidated into bilingual groups: {stats['rows_merged_into_groups']}")
    print(f"  Bilingual associate groups after consolidation: {stats['kept_entries']}")
    print(f"  Largest bilingual group (both languages combined): {stats['largest_group_size']} forms\n")


def print_profile_build_summary(stats: dict) -> None:
    print("Associate profile-vector summary:")
    print(f"  Input bilingual groups: {stats['input_groups']}")
    print(f"  Usable bilingual groups: {stats['used_groups']}")
    print(f"  Groups dropped with no English vector: {stats['dropped_missing_english_group']}")
    print(f"  Groups dropped with no Polish vector: {stats['dropped_missing_polish_group']}")
    print(f"  Missing English alternatives within groups: {stats['missing_english_alternatives']}")
    print(f"  Missing Polish alternatives within groups: {stats['missing_polish_alternatives']}")
    print(f"  Partially available groups retained: {stats['partially_available_groups']}\n")


def format_match(match: Match) -> str:
    target, score, r_c, cosine_sim = match
    return (
        f"{target} "
        f"(score={format_score(score)}, Rc={format_score(r_c)}, cos={format_score(cosine_sim)})"
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

    associate_groups, prune_stats = build_associate_groups(config["associate_entries"])
    print_pruning_summary(prune_stats)
    print(f"Profile filter: {profile_filter_label(args)}")
    if args.score == "adjusted":
        print(
            f"Adjusted score weights: Rc={args.adjusted_rc_weight:g}, "
            f"cosine={args.adjusted_cosine_weight:g}"
        )
    print()

    if len(associate_groups) < 2:
        raise ValueError("Need at least two bilingual associate groups for Pearson correlation.")

    if args.dry_run:
        print("Dry run complete. Model was not loaded.")
        return 0

    print("Step 2: Gathering words by language...", flush=True)
    polish_vocab = set(polish_words)
    english_vocab = set(english_words)
    for english_alternatives, polish_alternatives in associate_groups:
        english_vocab.update(english_alternatives)
        polish_vocab.update(polish_alternatives)
    polish_vocab = sorted(polish_vocab)
    english_vocab = sorted(english_vocab)
    print(f"  Polish words/phrases needed: {len(polish_vocab)}")
    print(f"  English words/phrases needed: {len(english_vocab)}\n", flush=True)

    pl_vecs, en_vecs, direct_cosine_available = load_embedding_backend(
        args,
        polish_vocab,
        english_vocab,
    )

    print("Step 6: Building bilingual associate profiles...", flush=True)
    en_assoc_side, pl_assoc_side, profile_stats = build_associate_profile_sides(
        associate_groups,
        polish_vecs=pl_vecs,
        english_vecs=en_vecs,
    )
    print_profile_build_summary(profile_stats)

    if profile_stats["used_groups"] < 2:
        raise ValueError("Need at least two usable associate groups for Pearson correlation.")

    all_correct = 0
    all_total = 0

    if args.direction in {"pl-en", "both"}:
        pl_en_rankings = rank_matches(
            source_words=polish_words,
            target_words=english_words,
            source_assoc_side=pl_assoc_side,
            target_assoc_side=en_assoc_side,
            source_vecs=pl_vecs,
            target_vecs=en_vecs,
            direct_cosine_available=direct_cosine_available,
            score_mode=args.score,
            adjusted_rc_weight=args.adjusted_rc_weight,
            adjusted_cosine_weight=args.adjusted_cosine_weight,
            profile_filter=args.profile_filter,
            top_n=args.top_n,
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
            source_assoc_side=en_assoc_side,
            target_assoc_side=pl_assoc_side,
            source_vecs=en_vecs,
            target_vecs=pl_vecs,
            direct_cosine_available=direct_cosine_available,
            score_mode=args.score,
            adjusted_rc_weight=args.adjusted_rc_weight,
            adjusted_cosine_weight=args.adjusted_cosine_weight,
            profile_filter=args.profile_filter,
            top_n=args.top_n,
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

    print(f"Embedding backend used: {args.embedding}")
    print(f"Scoring method used: {score_label(args)}")
    print(f"Profile filter used for Pearson R_c: {profile_filter_label(args)}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
