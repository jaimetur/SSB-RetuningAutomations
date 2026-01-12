# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from typing import List, Optional, Tuple, Dict, Iterable

import pandas as pd

SUMMARY_RE = re.compile(r"^\s*\d+\s+instance\(s\)\s*$", re.IGNORECASE)


# ============================ PARSING ============================

def find_all_subnetwork_headers(lines: List[str]) -> List[int]:
    return [i for i, ln in enumerate(lines) if ln.strip().startswith("SubNetwork")]


def extract_mo_from_subnetwork_line(line: str) -> Optional[str]:
    if not line:
        return None
    if "," in line:
        last = line.strip().split(",")[-1].strip()
        return last or None
    toks = line.strip().split()
    return toks[-1].strip() if toks else None


def detect_data_separator(probe_lines: Iterable[str]) -> Optional[str]:
    probe = [ln for ln in probe_lines if ln.strip() and not SUMMARY_RE.match(ln)]
    if any("\t" in ln for ln in probe):
        return "\t"
    if any("," in ln for ln in probe):
        return ","
    return None


def split_line(line: str, sep: Optional[str]) -> List[str]:
    if sep is None:
        return re.split(r"\s+", line.strip())
    return line.split(sep)


def make_unique_columns(cols: List[str]) -> List[str]:
    """
    Ensure column names are unique by appending a numeric suffix when needed.
    """
    seen: Dict[str, int] = {}
    unique: List[str] = []
    for c in cols:
        base = c or "Col"
        if base not in seen:
            seen[base] = 0
            unique.append(base)
        else:
            seen[base] += 1
            unique.append(f"{base}_{seen[base]}")
    return unique

# def make_unique_columns(cols: List[str]) -> List[str]:
#     seen: Dict[str, int] = {}
#     out = []
#     for c in cols:
#         if c not in seen:
#             seen[c] = 0
#             out.append(c)
#         else:
#             seen[c] += 1
#             out.append(f"{c}.{seen[c]}")
#     return out


def parse_table_slice_from_subnetwork(lines: List[str], header_idx: int, end_idx: int) -> pd.DataFrame:
    # 1) Data header: first non-empty, non-summary line after SubNetwork
    data_header_idx = None
    for j in range(header_idx + 1, end_idx):
        ln = lines[j]
        if not ln.strip() or SUMMARY_RE.match(ln):
            continue
        data_header_idx = j
        break
    if data_header_idx is None:
        return pd.DataFrame()

    header_line = lines[data_header_idx].strip()

    # 2) Separator inside the slice
    probe_end = min(end_idx, data_header_idx + 50)
    sep = detect_data_separator(lines[data_header_idx:probe_end])

    # 3) Header cols
    header_cols = [c.strip() for c in (split_line(header_line, sep))]
    header_cols = make_unique_columns(header_cols)

    # 4) Rows
    rows: List[List[str]] = []
    for j in range(data_header_idx + 1, end_idx):
        ln = lines[j]
        if not ln.strip() or SUMMARY_RE.match(ln):
            continue
        parts = [p.strip() for p in split_line(ln, sep)]
        if len(parts) < len(header_cols):
            parts += [""] * (len(header_cols) - len(parts))
        elif len(parts) > len(header_cols):
            parts = parts[:len(header_cols)]
        rows.append(parts)

    df = pd.DataFrame(rows, columns=header_cols)
    df = df.replace({"nan": "", "NaN": "", "None": "", "none": "", "NULL": "", "null": ""}).dropna(how="all")
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()
    return df


def fallback_header_index(
    valid_lines: List[str],
    all_lines: List[str],
    summary_re,
) -> Optional[int]:
    """
    Best-effort detection of a tabular header line when no explicit 'SubNetwork' header is found.
    """
    any_tab = any("\t" in ln for ln in valid_lines)
    sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid_lines) else None)

    for i, ln in enumerate(all_lines):
        if not ln.strip() or summary_re.match(ln):
            continue
        if sep == "\t" and "\t" in ln:
            return i
        if sep == "," and "," in ln:
            return i
        if sep is None:
            # First non-empty, non-summary line becomes header
            return i
    return None


def parse_log_lines(
    lines: List[str],
    summary_re,
    forced_header_idx: Optional[int] = None,
) -> Tuple[pd.DataFrame, str]:
    """
    Parse a list of lines into a DataFrame.

    - Tries to infer separator (tab / comma / whitespace).
    - Skips lines matching SUMMARY_RE.
    - Normalizes columns and trims rows beyond Excel's maximum if needed.
    """
    valid = [ln for ln in lines if ln.strip() and not summary_re.match(ln)]
    header_idx = forced_header_idx
    if header_idx is None:
        header_idx = fallback_header_index(valid, lines, summary_re)
    if header_idx is None:
        return pd.DataFrame(), "No header detected"

    header_line = lines[header_idx].strip()
    any_tab = any("\t" in ln for ln in valid)
    data_sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid) else None)

    if header_line.startswith("SubNetwork"):
        header_cols = [c.strip() for c in header_line.split(",")]
    else:
        header_cols = [
            c.strip()
            for c in (
                header_line.split(data_sep)
                if data_sep
                else re.split(r"\s+", header_line.strip())
            )
        ]
    header_cols = make_unique_columns(header_cols)

    rows: List[List[str]] = []
    for ln in lines[header_idx + 1 :]:
        if not ln.strip() or summary_re.match(ln):
            continue
        parts = [
            p.strip()
            for p in (
                ln.split(data_sep)
                if data_sep
                else re.split(r"\s+", ln.strip())
            )
        ]
        if len(parts) < len(header_cols):
            parts += [""] * (len(header_cols) - len(parts))
        elif len(parts) > len(header_cols):
            parts = parts[: len(header_cols)]
        rows.append(parts)

    df = pd.DataFrame(rows, columns=header_cols)
    df = df.replace({"nan": "", "NaN": "", "None": "", "NULL": ""}).dropna(how="all")
    for c in df.columns:
        df[c] = df[c].astype(str).str.strip()

    note = (
        "Header=SubNetwork-comma"
        if header_line.startswith("SubNetwork")
        else (
            "Tab-separated"
            if data_sep == "\t"
            else ("Comma-separated" if data_sep == "," else "Whitespace-separated")
        )
    )
    return df, note


def find_subnetwork_header_index(lines: List[str], summary_re=None) -> Optional[int]:
    """
    Find the index of the first line starting with 'SubNetwork'.
    """
    for i, ln in enumerate(lines):
        if ln.strip().startswith("SubNetwork"):
            return i
    return None


def extract_mo_name_from_previous_line(
    lines: List[str],
    header_idx: Optional[int],
) -> Optional[str]:
    """
    Try to infer the MO name from the line immediately before the header.
    """
    if header_idx is None or header_idx == 0:
        return None
    prev = lines[header_idx - 1].strip()
    if not prev:
        return None
    if "," in prev:
        last = prev.split(",")[-1].strip()
        return last or None
    toks = prev.split()
    return toks[-1].strip() if toks else None


def cap_rows(
    df: pd.DataFrame,
    note: str,
    max_rows_excel: int = 1_048_576,
) -> Tuple[pd.DataFrame, str]:
    """
    Cap the number of rows to Excel's maximum and append a note if trimming occurs.
    """
    if len(df) > max_rows_excel:
        df = df.iloc[:max_rows_excel, :].copy()
        note = (note + " | " if note else "") + f"Trimmed to {max_rows_excel} rows"
    return df, note


def normalize_ref(s: str) -> str:
    return str(s).replace(" ", "").strip()


def extract_gnbcucp_segment(nrcell_ref: str) -> str:
    """
    Extract GNBCUCPFunction segment from a full nRCellRef string.

    Example:
      '...,GNBCUCPFunction=1,NRNetwork=1,ExternalGNBCUCPFunction=auto311_480_3_2509535,ExternalNRCellCU=auto41116222186'
      -> 'GNBCUCPFunction=1,NRNetwork=1,ExternalGNBCUCPFunction=auto311_480_3_2509535,ExternalNRCellCU=auto41116222186'
    """
    if not isinstance(nrcell_ref, str):
        return ""
    pos = nrcell_ref.find("GNBCUCPFunction=")
    if pos == -1:
        return ""
    return nrcell_ref[pos:].strip()


def resolve_nrcell_ref(row: pd.Series, relations_lookup: Dict[tuple, pd.Series]) -> str:
    """
    Prefer nRCellRef from relations_df; if empty, fallback to value in disc row.
    """
    key = (
        str(row.get("NodeId", "")).strip(),
        str(row.get("NRCellCUId", "")).strip(),
        str(row.get("NRCellRelationId", "")).strip(),
    )
    rel_row = relations_lookup.get(key)
    candidates = []
    if rel_row is not None:
        candidates.append(rel_row.get("nRCellRef"))
    candidates.append(row.get("nRCellRef"))

    for v in candidates:
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except TypeError:
            pass
        s = str(v).strip()
        if not s or s.lower() == "nan":
            continue
        return s
    return ""


def normalize_market_name(name: str) -> str:
    """
    Normalize a market folder name so that, for example,
    '231_Indiana', '231-Indiana' and 'Indiana' match.

    Used only for matching PRE/POST markets.
    """
    s = name.strip().lower()
    # Strip leading digits + separators (underscore, hyphen, space)
    s = re.sub(r"^\d+[_\-\s]*", "", s)
    return s


def normalize_csv_list(text: str) -> str:
    """Normalize a comma-separated text into 'a,b,c' without extra spaces/empties."""
    if not text:
        return ""
    items = [t.strip() for t in text.split(",")]
    items = [t for t in items if t]
    return ",".join(items)


def parse_arfcn_csv_to_set(
    csv_text: Optional[str],
    default_values: List[int],
    label: str,
) -> set:
    """
    Helper to parse a CSV string into a set of integers.

    - If csv_text is empty or all values are invalid, fall back to default_values.
    - Logs warnings for invalid tokens.
    """
    values: List[int] = []
    if csv_text:
        for token in csv_text.split(","):
            tok = token.strip()
            if not tok:
                continue
            try:
                values.append(int(tok))
            except ValueError:
                print(f"[Configuration Audit] [WARNING] Ignoring invalid ARFCN '{tok}' in {label} list.")

    if not values:
        return set(default_values)

    return set(values)


def build_expected_profile_ref_clone(old_profile_ref: object, old_ssb: int, new_ssb: int) -> str:
    """
    Build the expected clone profile reference by replacing the OLD SSB integer with the NEW SSB integer
    only when it appears as a standalone number (not part of a longer digit sequence).

    Example:
      McpcPCellNrFreqRelProfile=430090_648672 -> McpcPCellNrFreqRelProfile=430090_647328
    """
    if old_profile_ref is None:
        return ""

    s = str(old_profile_ref).strip()
    if not s:
        return ""

    return re.sub(rf"(?<!\d){int(old_ssb)}(?!\d)", str(int(new_ssb)), s)
