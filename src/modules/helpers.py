# -*- coding: utf-8 -*-

import os
import re
from typing import List, Optional, Tuple, Dict


# ----------------------------- File reading ----------------------------- #
def try_read_text_file_with_encoding(path: str) -> Tuple[List[str], Optional[str]]:
    """
    Robust text reader that tries several encodings and returns (lines, encoding_used).
    If it falls back to 'replace' mode, returns (lines, None) to signal that encoding is uncertain.
    """
    encodings = ["utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return [ln.rstrip("\n") for ln in f], enc
        except Exception:
            continue
    # last permissive attempt
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return [ln.rstrip("\n") for ln in f], None


def try_read_text_file_lines(path: str) -> Optional[List[str]]:
    """
    Same as above but returns only the lines (used by PrePostRelations.loaders).
    """
    try:
        lines, _ = try_read_text_file_with_encoding(path)
        return lines
    except Exception:
        return None


# ----------------------------- Parsing helpers ----------------------------- #
def find_all_subnetwork_headers(lines: List[str]) -> List[int]:
    """Return indices of every line that starts with 'SubNetwork'."""
    return [i for i, ln in enumerate(lines) if ln.strip().startswith("SubNetwork")]


def extract_mo_from_subnetwork_line(line: str) -> Optional[str]:
    """
    Extract MO/table name from the 'SubNetwork,...,<MO>' line.
    Rule: last token after the last comma. Falls back to last whitespace token.
    """
    if not line:
        return None
    if "," in line:
        last = line.strip().split(",")[-1].strip()
        return last or None
    toks = line.strip().split()
    return toks[-1].strip() if toks else None


def split_line(line: str, sep: Optional[str]) -> List[str]:
    """
    Split a line by a provided separator; if sep is None, split by whitespace.
    """
    if sep is None:
        return re.split(r"\s+", line.strip())
    return line.split(sep)


def make_unique_columns(cols: List[str]) -> List[str]:
    """
    Return a list of column names made unique by appending .1, .2, ... to duplicates.
    """
    seen: Dict[str, int] = {}
    unique = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            unique.append(c)
        else:
            seen[c] += 1
            unique.append(f"{c}.{seen[c]}")
    return unique


# ----------------------------- Excel sheet helpers ----------------------------- #
def sanitize_sheet_name(name: str) -> str:
    """
    Excel sheet name constraints: max 31 chars
    """
    name = re.sub(r'[:\\/?*\[\]]', "_", name)
    name = name.strip().strip("'")
    return (name or "Sheet")[:31]


def unique_sheet_name(base: str, used: set) -> str:
    """
    Make a unique sheet name within 'used' set by adding ' (k)' or '_NN' suffix.
    """
    if base not in used:
        return base
    for k in range(1, 1000):
        suffix = f" ({k})"
        cand = (base[: max(0, 31 - len(suffix))] + suffix)
        if cand not in used:
            return cand
    i, cand = 1, base
    while cand in used:
        cand = f"{base[:28]}_{i:02d}"
        i += 1
    return cand
