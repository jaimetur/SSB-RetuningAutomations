# -*- coding: utf-8 -*-

import re
from typing import List, Optional, Dict

import pandas as pd


# ============================ FREQ UTILS ============================

def base_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.split("-", n=1).str[0].fillna("").str.strip()


def extract_gu_freq_base(s: pd.Series) -> pd.Series:
    base = base_series(s)
    fallback = s.astype(str).str.extract(r"(\d+)", expand=False)
    return base.where(base != "", fallback).fillna("").astype(str)


def extract_nr_freq_base(s: pd.Series) -> pd.Series:
    got = s.astype(str).str.extract(r"NRFreqRelation\s*=\s*(\d+)", expand=False)
    return got.fillna(base_series(s)).fillna("").astype(str)


def detect_freq_column(table_name: str, columns: List[str]) -> Optional[str]:
    if table_name == "GUtranCellRelation" and "GUtranFreqRelationId" in columns:
        return "GUtranFreqRelationId"
    for c in columns:
        lc = c.lower()
        if "freqrelation" in lc or "freq" in lc:
            return c
    return None


def detect_key_columns(table_name: str, columns: List[str], freq_col: Optional[str]) -> List[str]:
    preferred = {
        "GUtranCellRelation": ["GUtranCellRelationId", "neighborCellRef"],
        "NRCellRelation": ["NRCellRelationId", "neighborCellRef"],
    }
    for cand in preferred.get(table_name, []):
        if cand in columns:
            return [cand]
    id_like = [c for c in columns if c.lower().endswith("id")]
    if freq_col and freq_col in id_like:
        id_like.remove(freq_col)
    id_like = [c for c in id_like if c not in ("Pre/Post", "Date")]
    if id_like:
        return id_like[:2]
    if "neighborCellRef" in columns:
        return ["neighborCellRef"]
    remaining = [c for c in columns if c not in ("Pre/Post", "Date")]
    return remaining[:1] if remaining else []


def enforce_gu_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols_required = ["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=cols_required)
    out = df.copy()
    for c in cols_required:
        if c not in out.columns:
            out[c] = ""
    other = [c for c in out.columns if c not in cols_required]
    return out[cols_required + other]


def enforce_nr_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    cols_required = ["NodeId", "NRCellCUId", "NRCellRelationId"]
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=cols_required)
    out = df.copy()
    for c in cols_required:
        if c not in out.columns:
            out[c] = ""
    other = [c for c in out.columns if c not in cols_required]
    return out[cols_required + other]


# --- NEW HELPERS FROM CONFIGURATION AUDIT MODULE ---

def resolve_column_case_insensitive(
    df: pd.DataFrame,
    candidates: List[str],
) -> Optional[str]:
    """
    Resolve a column name by trying several candidates, case-insensitive
    and ignoring underscores/spaces.
    """
    if df is None or df.empty:
        return None

    def _canon(s: str) -> str:
        return re.sub(r"[\s_]+", "", str(s).strip().lower())

    cols = list(df.columns)
    canon_map = {_canon(c): c for c in cols}
    for cand in candidates:
        key = _canon(cand)
        if key in canon_map:
            return canon_map[key]
    # Fallback: startswith-based match
    for cand in candidates:
        key = _canon(cand)
        for c in cols:
            if _canon(c).startswith(key):
                return c
    return None


def parse_int_frequency(value: object) -> Optional[int]:
    """
    Try to parse a frequency/ARFCN value as integer from the leading numeric part
    of the string (before any non-digit chars like '-' or spaces).

    Examples:
      - '653952-30-20-0-1' -> 653952
      - '648672 some text' -> 648672
      - '  647328'         -> 647328
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    m = re.match(r"^(\d+)", s)
    if not m:
        return None

    try:
        return int(m.group(1))
    except Exception:
        return None


def is_n77_from_string(value: object) -> bool:
    """
    Determine if a cell can be considered N77 based on ARFCN/SSB value.

    Rule:
    - Parse integer frequency from the string (leading digits).
    - Return True if it's within [646600, 660000].
    """
    freq = parse_int_frequency(value)
    return bool(freq is not None and 646600 <= freq <= 660000)



def extract_sync_frequencies(value: str):
    """Extract all ARFCN values from GUtranSyncSignalFrequency=XXXX-xx patterns."""
    if not value or not isinstance(value, str):
        return set()
    matches = re.findall(r"GUtranSyncSignalFrequency=(\d+)-", value)
    return set(matches)



def extract_ssb_from_profile_ref(profile_ref: object, side: str) -> Optional[int]:
    """
    Extract SSB/ARFCN integer from a mcpcPCellNrFreqRelProfileRef string.

    Expected patterns:
      - "xxxx_647328"  -> side="suffix"
      - "647328_xxxx"  -> side="prefix"

    Args:
      profile_ref: Raw value from dataframe (can be None/NaN/str).
      side: "prefix" (before first underscore) or "suffix" (after last underscore).

    Returns:
      int if a numeric token is found on the requested side, otherwise None.
    """
    if profile_ref is None:
        return None

    s = str(profile_ref).strip()
    if not s or "_" not in s:
        return None

    parts = [p.strip() for p in s.split("_") if p.strip()]
    if not parts:
        return None

    token = parts[0] if side == "prefix" else parts[-1]
    if not token.isdigit():
        return None

    try:
        return int(token)
    except Exception:
        return None


def detect_profile_ref_ssb_side(profile_ref: object, old_ssb: int, new_ssb: int) -> str:
    """
    Detect whether SSB is encoded as prefix or suffix in the profile ref.

    Returns:
      - "prefix" if the left token matches old/new SSB
      - "suffix" if the right token matches old/new SSB
      - "" if it cannot be detected
    """
    prefix_val = extract_ssb_from_profile_ref(profile_ref, "prefix")
    if prefix_val in {old_ssb, new_ssb}:
        return "prefix"

    suffix_val = extract_ssb_from_profile_ref(profile_ref, "suffix")
    if suffix_val in {old_ssb, new_ssb}:
        return "suffix"

    return ""


def build_expected_profile_ref_clone_by_side(old_profile_ref: str, old_ssb: int, new_ssb: int, side: str) -> str:
    """
    Build the expected cloned profile reference replacing old SSB with new SSB on the requested side.

    Examples:
      - side="suffix": "430090_648672" -> "430090_647328"
      - side="prefix": "648672_430090" -> "647328_430090"
    """
    s = str(old_profile_ref).strip()
    if not s or "_" not in s:
        return s

    parts = [p for p in s.split("_")]
    if not parts:
        return s

    if side == "prefix":
        if parts[0].strip().isdigit() and int(parts[0].strip()) == int(old_ssb):
            parts[0] = str(new_ssb)
        return "_".join(parts)

    # side == "suffix"
    if parts[-1].strip().isdigit() and int(parts[-1].strip()) == int(old_ssb):
        parts[-1] = str(new_ssb)
    return "_".join(parts)
