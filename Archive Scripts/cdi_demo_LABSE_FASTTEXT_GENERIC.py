"""
cdi_demo_LANGUAGE_GENERIC.py
============================
CDI semantic alignment demo using bilingual similarity-profile correlations.

Dictionary format
-----------------
The first non-comment metadata line declares the two language codes:

    languages = pl, en

The first code is the LEFT language and the second code is the RIGHT language.
Every row in both ``pairs:`` and ``associates:`` must follow that same order:

    [list-8]
    description = CDI words

    pairs:
    aparat = camera
    apteka = pharmacy

    associates:
    zdjęcie = photo
    obiektyw = lens

Alternatives may appear on either side, separated by commas or slashes:

    obraz, zdjęcie = picture, photo
    prędko/szybko = quick/fast

The script does not contain fixed language names. It reads the language codes from
``languages = ...`` and uses them for vector-file discovery, output labels, and
direction aliases.

Important behavior
------------------
- Multiple alternatives are never averaged as embedding vectors.
- Cosine is computed separately for each available alternative, then those scalar
  cosine values are averaged within the bilingual associate group.
- Alternatives are supported symmetrically on both sides.
- Overlapping associate rows are consolidated into many-to-many bilingual groups.
- LaBSE and FastText are supported.
- Pearson R_c can use all dimensions, positive-overlap dimensions, or strongest-N.

Examples
--------
    python cdi_demo_LANGUAGE_GENERIC.py --dict data/big-cdi-dict.txt --list-8 --embedding labse --score rc
    python cdi_demo_LANGUAGE_GENERIC.py --dict data/big-cdi-dict.txt --list-8 --embedding fasttext --score rc --profile-filter top-n --top-n 100
    python cdi_demo_LANGUAGE_GENERIC.py --direction pl-en
    python cdi_demo_LANGUAGE_GENERIC.py --direction left-right
    python cdi_demo_LANGUAGE_GENERIC.py --dry-run
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
# One associates row: left-language alternatives, right-language alternatives, source line number.
AssociateEntry = Tuple[List[str], List[str], int]
# One merged bilingual semantic dimension: left-language alternatives, right-language alternatives.
AssociateGroup = Tuple[List[str], List[str]]
# Match tuple: target word, selected score, Pearson R_c, direct cosine similarity.
Match = Tuple[str, float, float, float]


@dataclass(frozen=True)
class LanguageConfig:
    """Language codes declared by the dictionary's top-level ``languages`` tag."""

    left_code: str
    right_code: str

    @property
    def left_label(self) -> str:
        return self.left_code.upper()

    @property
    def right_label(self) -> str:
        return self.right_code.upper()

    @property
    def left_to_right_alias(self) -> str:
        return f"{self.left_code}-{self.right_code}"

    @property
    def right_to_left_alias(self) -> str:
        return f"{self.right_code}-{self.left_code}"


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


def load_word_lists(dict_path: Path) -> Tuple[LanguageConfig, Dict[str, dict]]:
    """Load dictionary metadata and every configured CDI list."""
    if not dict_path.exists():
        raise FileNotFoundError(
            f"Could not find dictionary file: {dict_path}\n"
            "Expected default location: data/big-cdi-dict.txt relative to this script."
        )

    languages: Optional[LanguageConfig] = None
    word_lists: Dict[str, dict] = {}
    current_list_name: Optional[str] = None
    current_section: Optional[str] = None

    with dict_path.open("r", encoding="utf-8-sig") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()

            if not line or line.startswith("#"):
                continue

            # Global metadata must appear before the first [list-name] header.
            if current_list_name is None and "=" in line:
                key, value = parse_key_value_line(line, line_number)
                if key.casefold() == "languages":
                    if languages is not None:
                        raise ValueError(f"Line {line_number}: duplicate languages tag.")
                    codes = parse_alternatives(value, line_number, "languages tag")
                    if len(codes) != 2:
                        raise ValueError(
                            f"Line {line_number}: languages must contain exactly two codes, "
                            f"for example 'languages = pl, en'."
                        )
                    if normalize_key(codes[0]) == normalize_key(codes[1]):
                        raise ValueError(f"Line {line_number}: language codes must be different.")
                    languages = LanguageConfig(codes[0], codes[1])
                    continue

            if line.startswith("[") and line.endswith("]"):
                if languages is None:
                    raise ValueError(
                        f"Line {line_number}: add 'languages = <left-code>, <right-code>' "
                        "before the first list header."
                    )
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
                raise ValueError(
                    f"Line {line_number}: only the global 'languages = ...' tag may appear "
                    "before the first [list-name] header."
                )

            lower_line = line.casefold()
            if lower_line == "pairs:":
                current_section = "pairs"
                continue
            if lower_line == "associates:":
                current_section = "associates"
                continue

            if current_section is None and lower_line.startswith("description"):
                key, value = parse_key_value_line(line, line_number)
                if key.casefold() != "description":
                    raise ValueError(f"Line {line_number}: expected 'description = ...', got: {line}")
                word_lists[current_list_name]["description"] = value
                continue

            if current_section == "pairs":
                left_text, right_text = parse_key_value_line(line, line_number)
                left_alternatives = parse_alternatives(left_text, line_number, "left pair side")
                right_alternatives = parse_alternatives(right_text, line_number, "right pair side")
                # Every cross-language combination is accepted as a true translation.
                # Each alternative is still evaluated independently.
                for left_word in left_alternatives:
                    for right_word in right_alternatives:
                        word_lists[current_list_name]["pairs"].append((left_word, right_word))
                continue

            if current_section == "associates":
                left_text, right_text = parse_key_value_line(line, line_number)
                left_associates = parse_alternatives(left_text, line_number, "left associate side")
                right_associates = parse_alternatives(right_text, line_number, "right associate side")
                word_lists[current_list_name]["associate_entries"].append(
                    (left_associates, right_associates, line_number)
                )
                continue

            raise ValueError(
                f"Line {line_number}: line is not inside 'pairs:' or 'associates:' section: {line}"
            )

    if languages is None:
        raise ValueError(
            "Dictionary is missing the required top-level tag: "
            "languages = <left-code>, <right-code>"
        )

    validate_word_lists(word_lists)
    return languages, word_lists

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


def parse_args(
    word_lists: Dict[str, dict],
    default_path: Path,
    languages: LanguageConfig,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CDI semantic alignment using profile correlation, cosine, or adjusted scores."
    )
    parser.add_argument(
        "--dict",
        default=str(default_path),
        help="Path to the tagged dictionary file. Default: data/big-cdi-dict.txt next to this script.",
    )
    parser.add_argument(
        "--word-list",
        choices=list(word_lists.keys()) + ["list-all"],
        default=None,
        help="Word list to use, e.g. list-8. You can also use --list-8 style flags.",
    )

    direction_choices = unique_preserve_order(
        [
            "left-right",
            "right-left",
            languages.left_to_right_alias,
            languages.right_to_left_alias,
            "both",
        ]
    )
    parser.add_argument(
        "--direction",
        choices=direction_choices,
        default="both",
        help=(
            "Direction to evaluate. Use left-right/right-left, the dictionary-derived aliases "
            f"{languages.left_to_right_alias}/{languages.right_to_left_alias}, or both. Default: both."
        ),
    )
    parser.add_argument(
        "--score",
        choices=["rc", "correlation", "cosine", "adjusted"],
        default="rc",
        help=(
            "Scoring method for ranking matches. rc/correlation = Pearson profile correlation; "
            "cosine = direct embedding cosine; adjusted = adjusted-rc-weight*R_c + "
            "adjusted-cosine-weight*cosine. Default: rc."
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
        help="Cosine weight used when --score adjusted. Default: 0.5.",
    )
    parser.add_argument(
        "--profile-filter",
        choices=["all", "positive", "top-n"],
        default="all",
        help=(
            "Associate dimensions used for Pearson R_c. all = every dimension; "
            "positive = dimensions where both profile values are positive; "
            "top-n = strongest N dimensions by average profile similarity. Default: all."
        ),
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Dimensions retained when --profile-filter top-n. Default: 100.",
    )
    parser.add_argument(
        "--embedding",
        choices=["labse", "fasttext"],
        default="labse",
        help=(
            "Embedding backend. LaBSE uses SentenceTransformer; FastText uses the files "
            "from --left-vectors and --right-vectors. Default: labse."
        ),
    )
    parser.add_argument(
        "--model",
        default="sentence-transformers/LaBSE",
        help="SentenceTransformer model name used by --embedding labse.",
    )
    parser.add_argument(
        "--left-vectors",
        default=None,
        help=(
            f"Static-vector file for the left language ({languages.left_code}). The automatic "
            f"default is data/cc.{languages.left_code}.300.vec when present."
        ),
    )
    parser.add_argument(
        "--right-vectors",
        default=None,
        help=(
            f"Static-vector file for the right language ({languages.right_code}). The automatic "
            f"default is data/cc.{languages.right_code}.300.vec when present."
        ),
    )
    parser.add_argument(
        "--allow-static-crosslingual-cosine",
        action="store_true",
        help=(
            "Allow direct cross-language cosine for static vectors. Use only when the two "
            "vector files are already aligned in one cross-lingual space."
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
        help="Load and consolidate the dictionary, print counts, then stop before loading embeddings.",
    )

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

    if args.direction in {"left-right", languages.left_to_right_alias}:
        args.direction = "left-right"
    elif args.direction in {"right-left", languages.right_to_left_alias}:
        args.direction = "right-left"

    if args.adjusted_rc_weight < 0 or args.adjusted_cosine_weight < 0:
        parser.error("Adjusted score weights must be non-negative.")
    if args.score == "adjusted" and (args.adjusted_rc_weight + args.adjusted_cosine_weight) == 0:
        parser.error("At least one adjusted score weight must be greater than zero.")
    if args.top_n < 2:
        parser.error("--top-n must be at least 2 because Pearson correlation needs two dimensions.")

    if args.embedding == "fasttext":
        script_dir = Path(__file__).resolve().parent

        def find_default_vector(code: str) -> Optional[Path]:
            candidates = unique_preserve_order([code, code.casefold()])
            for candidate_code in candidates:
                candidate = script_dir / "data" / f"cc.{candidate_code}.300.vec"
                if candidate.exists():
                    return candidate
            return None

        default_left = find_default_vector(languages.left_code)
        default_right = find_default_vector(languages.right_code)
        if args.left_vectors is None and default_left is not None:
            args.left_vectors = str(default_left)
        if args.right_vectors is None and default_right is not None:
            args.right_vectors = str(default_right)
        if not args.left_vectors or not args.right_vectors:
            parser.error(
                "--embedding fasttext requires both --left-vectors and --right-vectors when "
                "automatic data/cc.<language-code>.300.vec discovery does not find both files."
            )
        if args.score in {"cosine", "adjusted"} and not args.allow_static_crosslingual_cosine:
            parser.error(
                "Direct cross-language cosine is not valid for separate monolingual static spaces. "
                "Use --score rc, or provide aligned vectors and add "
                "--allow-static-crosslingual-cosine."
            )

    return args

def build_answer_maps(
    pairs: Sequence[Pair],
) -> Tuple[List[str], List[str], Dict[str, set], Dict[str, set]]:
    """Build ordered candidate lists and true-translation maps for both directions."""
    left_words = unique_preserve_order(left for left, _ in pairs)
    right_words = unique_preserve_order(right for _, right in pairs)

    left_to_right = defaultdict(set)
    right_to_left = defaultdict(set)
    for left, right in pairs:
        left_to_right[left].add(right)
        right_to_left[right].add(left)

    return left_words, right_words, dict(left_to_right), dict(right_to_left)

def build_associate_groups(
    associate_entries: Sequence[AssociateEntry],
) -> Tuple[List[AssociateGroup], dict]:
    """
    Build symmetric many-to-many bilingual associate groups.

    Each row may contain alternatives on either side. Rows connected by any shared
    same-language associate are merged into one bilingual component. This retains
    every observed translation alternative without duplicating semantic dimensions.
    """
    parent: Dict[Tuple[str, str], Tuple[str, str]] = {}
    rank: Dict[Tuple[str, str], int] = {}
    display: Dict[Tuple[str, str], str] = {}
    first_seen: Dict[Tuple[str, str], int] = {}
    raw_row_keys = set()
    duplicate_rows = 0
    order_counter = 0

    def make_node(side: str, word: str) -> Tuple[str, str]:
        nonlocal order_counter
        node = (side, normalize_key(word))
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

    for left_alternatives, right_alternatives, _line_number in associate_entries:
        left_words = unique_preserve_order(left_alternatives)
        right_words = unique_preserve_order(right_alternatives)
        row_key = (
            tuple(sorted(normalize_key(word) for word in left_words)),
            tuple(sorted(normalize_key(word) for word in right_words)),
        )
        if row_key in raw_row_keys:
            duplicate_rows += 1
        else:
            raw_row_keys.add(row_key)

        nodes = [make_node("left", word) for word in left_words]
        nodes.extend(make_node("right", word) for word in right_words)
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
        left_nodes = sorted(
            (node for node in nodes if node[0] == "left"),
            key=lambda node: first_seen[node],
        )
        right_nodes = sorted(
            (node for node in nodes if node[0] == "right"),
            key=lambda node: first_seen[node],
        )
        if not left_nodes or not right_nodes:
            continue
        groups.append(
            ([display[node] for node in left_nodes], [display[node] for node in right_nodes])
        )
        largest_group_size = max(largest_group_size, len(nodes))

    stats = {
        "raw_entries": len(associate_entries),
        "exact_duplicate_rows": duplicate_rows,
        "unique_left_associates": sum(1 for node in parent if node[0] == "left"),
        "unique_right_associates": sum(1 for node in parent if node[0] == "right"),
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
    left_vocab: Sequence[str],
    right_vocab: Sequence[str],
    languages: LanguageConfig,
) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray], bool]:
    """Load LaBSE or static vectors and return one vector dictionary per language."""
    if args.embedding == "labse":
        print("Step 3: Importing SentenceTransformer...", flush=True)
        from sentence_transformers import SentenceTransformer
        print("  Import done.\n", flush=True)
        print("Step 4: Loading LaBSE model...", flush=True)
        model = SentenceTransformer(args.model)
        print("  Model loaded.\n", flush=True)
        combined = unique_preserve_order(list(left_vocab) + list(right_vocab))
        print("Step 5: Embedding words with LaBSE...", flush=True)
        embeddings = model.encode(
            combined,
            normalize_embeddings=True,
            show_progress_bar=True,
            batch_size=args.batch_size,
        )
        shared = {
            word: np.asarray(embeddings[index], dtype=np.float32)
            for index, word in enumerate(combined)
        }
        return (
            {word: shared[word] for word in left_vocab if word in shared},
            {word: shared[word] for word in right_vocab if word in shared},
            True,
        )

    left_path = Path(args.left_vectors).expanduser().resolve()
    right_path = Path(args.right_vectors).expanduser().resolve()
    print(f"Step 3: Loading {args.embedding} vectors...", flush=True)

    left_tokens = {
        token
        for word in left_vocab
        for tokenization in candidate_tokenizations(word)
        for token in tokenization
    }
    right_tokens = {
        token
        for word in right_vocab
        for tokenization in candidate_tokenizations(word)
        for token in tokenization
    }
    left_token_map = load_text_vectors_subset(left_path, left_tokens)
    right_token_map = load_text_vectors_subset(right_path, right_tokens)
    left_vecs, left_missing = build_static_phrase_vectors(left_vocab, left_token_map)
    right_vecs, right_missing = build_static_phrase_vectors(right_vocab, right_token_map)

    print(f"  {languages.left_label} vectors built: {len(left_vecs)}/{len(left_vocab)}")
    print(f"  {languages.right_label} vectors built: {len(right_vecs)}/{len(right_vocab)}")
    if left_missing:
        print(
            f"  Missing {languages.left_label} words/phrases: {len(left_missing)} "
            f"(first 20: {', '.join(left_missing[:20])})"
        )
    if right_missing:
        print(
            f"  Missing {languages.right_label} words/phrases: {len(right_missing)} "
            f"(first 20: {', '.join(right_missing[:20])})"
        )
    print()
    return left_vecs, right_vecs, bool(args.allow_static_crosslingual_cosine)

def build_associate_profile_sides(
    groups: Sequence[AssociateGroup],
    left_vecs: Dict[str, np.ndarray],
    right_vecs: Dict[str, np.ndarray],
) -> Tuple[AssociateProfileSide, AssociateProfileSide, dict]:
    """
    Build aligned profile structures without averaging embeddings.

    Each alternative vector stays separate. Profile construction computes cosine to
    every available alternative and averages the resulting scalar cosine values.
    """
    left_flat: List[np.ndarray] = []
    right_flat: List[np.ndarray] = []
    left_starts: List[int] = []
    right_starts: List[int] = []
    left_counts: List[int] = []
    right_counts: List[int] = []

    stats = {
        "input_groups": len(groups),
        "used_groups": 0,
        "dropped_missing_left_group": 0,
        "dropped_missing_right_group": 0,
        "missing_left_alternatives": 0,
        "missing_right_alternatives": 0,
        "partially_available_groups": 0,
    }

    for left_words, right_words in groups:
        available_left = [left_vecs[word] for word in left_words if word in left_vecs]
        available_right = [right_vecs[word] for word in right_words if word in right_vecs]
        missing_left = len(left_words) - len(available_left)
        missing_right = len(right_words) - len(available_right)
        stats["missing_left_alternatives"] += missing_left
        stats["missing_right_alternatives"] += missing_right

        if not available_left:
            stats["dropped_missing_left_group"] += 1
            continue
        if not available_right:
            stats["dropped_missing_right_group"] += 1
            continue
        if missing_left or missing_right:
            stats["partially_available_groups"] += 1

        left_starts.append(len(left_flat))
        right_starts.append(len(right_flat))
        left_counts.append(len(available_left))
        right_counts.append(len(available_right))
        left_flat.extend(available_left)
        right_flat.extend(available_right)
        stats["used_groups"] += 1

    if not left_flat or not right_flat:
        raise ValueError("No usable bilingual associate groups remain after vector lookup.")

    left_side = AssociateProfileSide(
        vectors=np.asarray(left_flat, dtype=np.float32),
        group_starts=np.asarray(left_starts, dtype=np.int64),
        group_counts=np.asarray(left_counts, dtype=np.float32),
    )
    right_side = AssociateProfileSide(
        vectors=np.asarray(right_flat, dtype=np.float32),
        group_starts=np.asarray(right_starts, dtype=np.int64),
        group_counts=np.asarray(right_counts, dtype=np.float32),
    )
    return left_side, right_side, stats

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


def print_pruning_summary(stats: dict, languages: LanguageConfig) -> None:
    print("Associate grouping summary:")
    print(f"  Raw associate rows: {stats['raw_entries']}")
    print(f"  Exact duplicate rows found: {stats['exact_duplicate_rows']}")
    print(f"  Unique {languages.left_label} associate forms: {stats['unique_left_associates']}")
    print(f"  Unique {languages.right_label} associate forms: {stats['unique_right_associates']}")
    print("  Removed associates because they were in pair list: 0 (disabled; pair words are kept)")
    print(f"  Rows consolidated into bilingual groups: {stats['rows_merged_into_groups']}")
    print(f"  Bilingual associate groups after consolidation: {stats['kept_entries']}")
    print(f"  Largest bilingual group (both languages combined): {stats['largest_group_size']} forms\n")

def print_profile_build_summary(stats: dict, languages: LanguageConfig) -> None:
    print("Associate profile-vector summary:")
    print(f"  Input bilingual groups: {stats['input_groups']}")
    print(f"  Usable bilingual groups: {stats['used_groups']}")
    print(
        f"  Groups dropped with no {languages.left_label} vector: "
        f"{stats['dropped_missing_left_group']}"
    )
    print(
        f"  Groups dropped with no {languages.right_label} vector: "
        f"{stats['dropped_missing_right_group']}"
    )
    print(
        f"  Missing {languages.left_label} alternatives within groups: "
        f"{stats['missing_left_alternatives']}"
    )
    print(
        f"  Missing {languages.right_label} alternatives within groups: "
        f"{stats['missing_right_alternatives']}"
    )
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
    languages, word_lists = load_word_lists(first_dict_path)
    args = parse_args(word_lists, default_path, languages)

    # Resolve again after full argument parsing in case --dict was supplied.
    dict_path = Path(args.dict).expanduser().resolve()
    final_languages, word_lists = load_word_lists(dict_path)
    if final_languages != languages:
        # This is mainly defensive; --dict is already observed during the first pass.
        languages = final_languages
    config = get_word_config(args.word_list, word_lists)

    pairs: List[Pair] = config["pairs"]
    left_words, right_words, left_to_right, right_to_left = build_answer_maps(pairs)

    print(f"Selected word list: {args.word_list} ({config['description']})", flush=True)
    print(f"Dictionary file: {dict_path}", flush=True)
    print(
        f"Languages: {languages.left_code} (left) and {languages.right_code} (right)",
        flush=True,
    )
    print(f"Expanded pair mappings: {len(pairs)}", flush=True)
    print(f"Unique {languages.left_label} pair words: {len(left_words)}", flush=True)
    print(f"Unique {languages.right_label} pair words: {len(right_words)}\n", flush=True)

    associate_groups, grouping_stats = build_associate_groups(config["associate_entries"])
    print_pruning_summary(grouping_stats, languages)
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
    left_vocab = set(left_words)
    right_vocab = set(right_words)
    for left_alternatives, right_alternatives in associate_groups:
        left_vocab.update(left_alternatives)
        right_vocab.update(right_alternatives)
    left_vocab = sorted(left_vocab)
    right_vocab = sorted(right_vocab)
    print(f"  {languages.left_label} words/phrases needed: {len(left_vocab)}")
    print(f"  {languages.right_label} words/phrases needed: {len(right_vocab)}\n", flush=True)

    left_vecs, right_vecs, direct_cosine_available = load_embedding_backend(
        args,
        left_vocab,
        right_vocab,
        languages,
    )

    print("Step 6: Building bilingual associate profiles...", flush=True)
    left_assoc_side, right_assoc_side, profile_stats = build_associate_profile_sides(
        associate_groups,
        left_vecs=left_vecs,
        right_vecs=right_vecs,
    )
    print_profile_build_summary(profile_stats, languages)

    if profile_stats["used_groups"] < 2:
        raise ValueError("Need at least two usable associate groups for Pearson correlation.")

    all_correct = 0
    all_total = 0

    if args.direction in {"left-right", "both"}:
        left_right_rankings = rank_matches(
            source_words=left_words,
            target_words=right_words,
            source_assoc_side=left_assoc_side,
            target_assoc_side=right_assoc_side,
            source_vecs=left_vecs,
            target_vecs=right_vecs,
            direct_cosine_available=direct_cosine_available,
            score_mode=args.score,
            adjusted_rc_weight=args.adjusted_rc_weight,
            adjusted_cosine_weight=args.adjusted_cosine_weight,
            profile_filter=args.profile_filter,
            top_n=args.top_n,
        )
        correct, total, percent, correct_items, incorrect_items = evaluate_rankings(
            left_right_rankings,
            left_to_right,
        )
        direction_title = f"{languages.left_label} -> {languages.right_label}"
        print_direction_results(
            direction_title,
            score_label(args),
            correct,
            total,
            percent,
            correct_items,
            incorrect_items,
        )
        all_correct += correct
        all_total += total

    if args.direction in {"right-left", "both"}:
        right_left_rankings = rank_matches(
            source_words=right_words,
            target_words=left_words,
            source_assoc_side=right_assoc_side,
            target_assoc_side=left_assoc_side,
            source_vecs=right_vecs,
            target_vecs=left_vecs,
            direct_cosine_available=direct_cosine_available,
            score_mode=args.score,
            adjusted_rc_weight=args.adjusted_rc_weight,
            adjusted_cosine_weight=args.adjusted_cosine_weight,
            profile_filter=args.profile_filter,
            top_n=args.top_n,
        )
        correct, total, percent, correct_items, incorrect_items = evaluate_rankings(
            right_left_rankings,
            right_to_left,
        )
        direction_title = f"{languages.right_label} -> {languages.left_label}"
        print_direction_results(
            direction_title,
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

    print(f"Language codes used: {languages.left_code}, {languages.right_code}")
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
