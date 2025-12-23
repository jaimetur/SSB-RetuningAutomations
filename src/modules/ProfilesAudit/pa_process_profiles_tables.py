# -*- coding: utf-8 -*-
import re
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency


# ----------------------------- Profiles tables (OLD/NEW SSB replica + param equality) -----------------------------
def process_profiles_tables(
    dfs_by_table: Dict[str, pd.DataFrame],
    add_row,
    n77_ssb_pre: object,
    n77_ssb_post: object,
) -> None:
    """
    Validate that for each profiles table, every row whose MOid contains SSB-pre has a matching replica row
    in the same table where MOid has SSB-post (SSB-pre replaced by SSB-post) and all other parameters are identical,
    except for reservedBy which is ignored.

    It appends two SummaryAudit rows per table:
      - Profiles Inconsistencies: missing new SSB replica
      - Profiles Discrepancies: new replica exists but parameters differ
    """
    profile_tables: List[Tuple[str, str]] = [
        ("McpcPCellNrFreqRelProfileUeCfg", "McpcPCellNrFreqRelProfileId"),
        ("McpcPCellProfileUeCfg", "McpcPCellProfileId"),
        ("UlQualMcpcMeasCfg", "UlQualMcpcMeasCfgId"),
        ("McpcPSCellProfileUeCfg", "McpcPSCellProfileId"),
        ("McfbCellProfile", "McfbCellProfileId"),
        ("McfbCellProfileUeCfg", "McfbCellProfileId"),
        ("TrStSaCellProfile", "TrStSaCellProfileId"),
        ("TrStSaCellProfileUeCfg", "TrStSaCellProfileId"),
        ("McpcPCellEUtranFreqRelProfile", "McpcPCellEUtranFreqRelProfileId"),
        ("McpcPCellEUtranFreqRelProfileUeCfg", "McpcPCellEUtranFreqRelProfileId"),
        ("UeMCEUtranFreqRelProfile", "UeMCEUtranFreqRelProfileId"),
        ("UeMCEUtranFreqRelProfileUeCfg", "UeMCEUtranFreqRelProfileId"),
    ]

    ssb_pre_int = _safe_parse_int(n77_ssb_pre)
    ssb_post_int = _safe_parse_int(n77_ssb_post)

    for table_name, moid_col_name in profile_tables:
        df = dfs_by_table.get(table_name)
        _process_single_profiles_table(df, table_name, moid_col_name, add_row, ssb_pre_int, ssb_post_int)


def _process_single_profiles_table(
    df: Optional[pd.DataFrame],
    table_name: str,
    moid_col_name: str,
    add_row,
    ssb_pre_int: Optional[int],
    ssb_post_int: Optional[int],
) -> None:
    """
    Process one profiles table with a given MOid column name.
    """
    metric_missing = f"Profiles with old N77 SSB ({ssb_pre_int}) but not new N77 SSB ({ssb_post_int}) (from {table_name})"
    metric_discr = f"Profiles with old N77 SSB ({ssb_pre_int}) and new N77 SSB ({ssb_post_int}) but with param discrepancies (from {table_name})"

    try:
        if df is None or df.empty:
            add_row(table_name, "Profiles Inconsistencies", metric_missing, "Table not found or empty")
            add_row(table_name, "Profiles Discrepancies", metric_discr, "Table not found or empty")
            return

        if ssb_pre_int is None or ssb_post_int is None:
            add_row(table_name, "Profiles Inconsistencies", metric_missing, f"ERROR: Invalid SSB-pre/SSB-post values ({ssb_pre_int}, {ssb_post_int})")
            add_row(table_name, "Profiles Discrepancies", metric_discr, f"ERROR: Invalid SSB-pre/SSB-post values ({ssb_pre_int}, {ssb_post_int})")
            return

        node_col = resolve_column_case_insensitive(df, ["NodeId"])
        moid_col = resolve_column_case_insensitive(df, [moid_col_name])
        reserved_col = resolve_column_case_insensitive(df, ["reservedBy", "ReservedBy"])

        if not node_col or not moid_col:
            add_row(table_name, "Profiles Inconsistencies", metric_missing, "N/A", "NodeId / MOid column missing")
            add_row(table_name, "Profiles Discrepancies", metric_discr, "N/A", "NodeId / MOid column missing")
            return

        work = df.copy()
        work[node_col] = work[node_col].astype(str).str.strip()
        work[moid_col] = work[moid_col].astype(str).str.strip()

        # If this is a *UeCfg table and we can resolve the corresponding UeCfgId column, pair rows by (NodeId, UeCfgId).
        # This matches the user's requirement: rows with the same key (NodeId, XxxUeCfgId) must come in pairs where only the ProfileId changes old->new.
        uecfg_col = _resolve_uecfg_id_col_for_profile_id(work, moid_col_name)
        if uecfg_col:
            work[uecfg_col] = work[uecfg_col].astype(str).str.strip()
            _process_profiles_table_by_pair_key(work, table_name, node_col, moid_col, uecfg_col, reserved_col, add_row, ssb_pre_int, ssb_post_int, metric_missing, metric_discr)
            return

        compare_cols = [c for c in work.columns if c not in {moid_col, reserved_col}]

        # OLD behavior (fallback): pre/post detection directly on MOid and matching expected post MOid inside the same table.
        # This can overcount discrepancies in UeCfg tables where the correct key is (NodeId, UeCfgId).
        pre_mask = work[moid_col].map(lambda v: _contains_int_token(str(v), ssb_pre_int))
        post_mask = work[moid_col].map(lambda v: _contains_int_token(str(v), ssb_post_int))

        pre_rows = work.loc[pre_mask].copy()
        post_rows = work.loc[post_mask].copy()

        if pre_rows.empty:
            add_row(table_name, "Profiles Inconsistencies", metric_missing, 0, "")
            add_row(table_name, "Profiles Discrepancies", metric_discr, 0, "")
            return

        post_index_exact: Set[Tuple[str, Tuple[Optional[str], ...]]] = set()
        post_by_moid: Dict[str, List[Dict[str, Optional[str]]]] = {}

        for _, r in post_rows.iterrows():
            moid_val = str(r[moid_col]).strip()
            normalized = _normalize_row_for_compare(r, compare_cols)
            post_index_exact.add((moid_val, tuple(normalized.get(c) for c in compare_cols)))
            post_by_moid.setdefault(moid_val, []).append(normalized)

        missing_count = 0
        discrepancy_count = 0
        missing_nodes: Set[str] = set()
        discrepancy_nodes_to_cols: Dict[str, Set[str]] = {}

        for _, pre in pre_rows.iterrows():
            pre_moid = str(pre[moid_col]).strip()
            expected_post_moid = _replace_int_token(pre_moid, ssb_pre_int, ssb_post_int)

            pre_norm = _normalize_row_for_compare(pre, compare_cols)
            exact_key = (expected_post_moid, tuple(pre_norm.get(c) for c in compare_cols))

            if exact_key in post_index_exact:
                continue

            node_val = str(pre.get(node_col, "")).strip()

            candidates = post_by_moid.get(expected_post_moid, [])
            if not candidates:
                missing_count += 1
                if node_val:
                    missing_nodes.add(node_val)
                continue

            discrepancy_count += 1
            diff_cols = _best_diff_columns(pre_norm, candidates, compare_cols)
            if node_val:
                discrepancy_nodes_to_cols.setdefault(node_val, set()).update(diff_cols)

        missing_nodes_str = ", ".join(sorted(missing_nodes))
        add_row(table_name, "Profiles Inconsistencies", metric_missing, missing_count, missing_nodes_str)

        discrepancy_extra = _format_discrepancy_extrainfo(discrepancy_nodes_to_cols)
        add_row(table_name, "Profiles Discrepancies", metric_discr, discrepancy_count, discrepancy_extra)

    except Exception as ex:
        add_row(table_name, "Profiles Inconsistencies", metric_missing, f"ERROR: {ex}")
        add_row(table_name, "Profiles Discrepancies", metric_discr, f"ERROR: {ex}")


def _process_profiles_table_by_pair_key(
    work: pd.DataFrame,
    table_name: str,
    node_col: str,
    profile_id_col: str,
    uecfg_col: str,
    reserved_col: Optional[str],
    add_row,
    ssb_pre_int: int,
    ssb_post_int: int,
    metric_missing: str,
    metric_discr: str,
) -> None:
    """
    Pair rows by (NodeId, UeCfgId). For each "old-only" row, expect a matching "new-only" row in the same key group
    where the ProfileId has old->new replacement and all other columns (except reservedBy and ProfileId) are identical.
    """
    compare_cols = [c for c in work.columns if c not in {profile_id_col, reserved_col}]

    # Avoid false positives when a ProfileId contains BOTH old and new (e.g. "648672_647328").
    # Only treat as PRE if it contains old and NOT new, and as POST if it contains new and NOT old.
    def _is_old_only(v: object) -> bool:
        s = "" if v is None else str(v)
        return _contains_int_token(s, ssb_pre_int) and not _contains_int_token(s, ssb_post_int)

    def _is_new_only(v: object) -> bool:
        s = "" if v is None else str(v)
        return _contains_int_token(s, ssb_post_int) and not _contains_int_token(s, ssb_pre_int)

    pre_rows = work.loc[work[profile_id_col].map(_is_old_only)].copy()
    post_rows = work.loc[work[profile_id_col].map(_is_new_only)].copy()

    if pre_rows.empty:
        add_row(table_name, "Profiles Inconsistencies", metric_missing, 0, "")
        add_row(table_name, "Profiles Discrepancies", metric_discr, 0, "")
        return

    # Index POST rows by (key, profile_id, signature)
    post_exact: Set[Tuple[Tuple[str, str], str, Tuple[Optional[str], ...]]] = set()
    post_by_key_and_profile: Dict[Tuple[str, str], Dict[str, List[Dict[str, Optional[str]]]]] = {}

    for _, r in post_rows.iterrows():
        key = (str(r.get(node_col, "")).strip(), str(r.get(uecfg_col, "")).strip())
        pid = str(r.get(profile_id_col, "")).strip()
        norm = _normalize_row_for_compare(r, compare_cols)
        sig = tuple(norm.get(c) for c in compare_cols)
        post_exact.add((key, pid, sig))
        post_by_key_and_profile.setdefault(key, {}).setdefault(pid, []).append(norm)

    missing_count = 0
    discrepancy_count = 0
    missing_nodes: Set[str] = set()
    discrepancy_nodes_to_cols: Dict[str, Set[str]] = {}

    for _, pre in pre_rows.iterrows():
        key = (str(pre.get(node_col, "")).strip(), str(pre.get(uecfg_col, "")).strip())
        pre_pid = str(pre.get(profile_id_col, "")).strip()
        expected_pid = _replace_int_token(pre_pid, ssb_pre_int, ssb_post_int)

        pre_norm = _normalize_row_for_compare(pre, compare_cols)
        sig = tuple(pre_norm.get(c) for c in compare_cols)

        if (key, expected_pid, sig) in post_exact:
            continue

        node_val = key[0]

        candidates = post_by_key_and_profile.get(key, {}).get(expected_pid, [])
        if not candidates:
            missing_count += 1
            if node_val:
                missing_nodes.add(node_val)
            continue

        discrepancy_count += 1
        diff_cols = _best_diff_columns(pre_norm, candidates, compare_cols)
        if node_val:
            discrepancy_nodes_to_cols.setdefault(node_val, set()).update(diff_cols)

    missing_nodes_str = ", ".join(sorted(missing_nodes))
    add_row(table_name, "Profiles Inconsistencies", metric_missing, missing_count, missing_nodes_str)

    discrepancy_extra = _format_discrepancy_extrainfo(discrepancy_nodes_to_cols)
    add_row(table_name, "Profiles Discrepancies", metric_discr, discrepancy_count, discrepancy_extra)


def _resolve_uecfg_id_col_for_profile_id(df: pd.DataFrame, profile_id_col_name: str) -> Optional[str]:
    """
    Heuristic for *UeCfg tables: if profile id column is Xxx...Id, try to locate Xxx...UeCfgId.
    Returns the resolved column name in df, or None if not found.
    """
    if not profile_id_col_name:
        return None

    if profile_id_col_name.lower().endswith("id"):
        expected = f"{profile_id_col_name[:-2]}UeCfgId"
    else:
        expected = f"{profile_id_col_name}UeCfgId"

    return resolve_column_case_insensitive(df, [expected])


def _safe_parse_int(value: object) -> Optional[int]:
    """
    Convert an input value (int/str/etc) to an int using parse_int_frequency when possible.
    """
    parsed = parse_int_frequency(value)
    if parsed is not None:
        return int(parsed)
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _contains_int_token(text: str, number: int) -> bool:
    """
    Return True if `text` contains the exact integer token `number` not surrounded by other digits.
    """
    if text is None:
        return False
    s = str(text)
    pattern = rf"(?<!\d){re.escape(str(number))}(?!\d)"
    return re.search(pattern, s) is not None


def _replace_int_token(text: str, old_number: int, new_number: int) -> str:
    """
    Replace exact integer token `old_number` by `new_number` in `text` (token not surrounded by other digits).
    """
    s = "" if text is None else str(text)
    pattern = rf"(?<!\d){re.escape(str(old_number))}(?!\d)"
    return re.sub(pattern, str(new_number), s)


def _normalize_value(value: object) -> Optional[str]:
    """
    Normalize values so comparisons are stable across types (NaN/None, ints as '123', floats like 123.0 as '123').
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, bool):
        return str(value)

    if isinstance(value, (int,)):
        return str(int(value))

    if isinstance(value, (float,)):
        if value.is_integer():
            return str(int(value))
        return str(value).strip()

    return str(value).strip()


def _normalize_row_for_compare(row: pd.Series, compare_cols: List[str]) -> Dict[str, Optional[str]]:
    """
    Build a normalized dict for all compare columns.
    """
    normalized: Dict[str, Optional[str]] = {}
    for c in compare_cols:
        normalized[c] = _normalize_value(row.get(c))
    return normalized


def _best_diff_columns(
    pre_norm: Dict[str, Optional[str]],
    post_candidates: List[Dict[str, Optional[str]]],
    compare_cols: List[str],
) -> Set[str]:
    """
    For a pre row and multiple post candidates (same MOid after replacement), compute the smallest set of differing columns.
    """
    best: Set[str] = set(compare_cols)
    best_len = len(best)

    for cand in post_candidates:
        diffs: Set[str] = set()
        for c in compare_cols:
            if pre_norm.get(c) != cand.get(c):
                diffs.add(str(c))
        if len(diffs) < best_len:
            best = diffs
            best_len = len(diffs)
            if best_len == 0:
                break

    return best


def _format_discrepancy_extrainfo(discrepancy_nodes_to_cols: Dict[str, Set[str]]) -> str:
    """
    Format: NodeA (Col1, Col2), NodeB (ColX)
    """
    if not discrepancy_nodes_to_cols:
        return ""

    parts: List[str] = []
    for node in sorted(discrepancy_nodes_to_cols.keys()):
        cols = sorted(discrepancy_nodes_to_cols.get(node, set()))
        cols_str = ", ".join(cols) if cols else ""
        parts.append(f"{node} ({cols_str})" if cols_str else f"{node} (UnknownColumn)")
    return ", ".join(parts)
