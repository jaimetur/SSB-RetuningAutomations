# -*- coding: utf-8 -*-

"""
Post Step2 Cleanup Consistency Checks (Profiles-related).

This module ONLY implements the 2 checks requested (Profiles Inconsistencies):

1) NRCellCU
   Metric:
     "NR nodes with the new N77 SSB (<post>) and NRCellCU Ref parameters to Profiles with the old SSB name (from NRCellCU table)"
   Logic (scoped ONLY by nodes_post):
     For all rows whose NodeId is in nodes_post, verify that the following ref columns
     do NOT contain the old SSB (pre) as an integer token:
        - mcpcPCellProfileRef
        - mcpcPSCellProfileRef
        - mcfbCellProfileRef
        - trStSaCellProfileRef

2) EUtranFreqRelation
   Metric:
     "NR nodes with the new N77 SSB (<post>) and EUtranFreqRelation Ref parameters to Profiles with the old SSB name (from EUtranFreqRelation table)"
   Logic (scoped ONLY by nodes_post):
     For all rows whose NodeId is in nodes_post, verify that the following ref columns
     do NOT contain the old SSB (pre) as an integer token:
        - mcpcPCellEUtranFreqRelProfileRef
        - UeMCEUtranFreqRelProfile

Notes:
- Filtering is done ONLY by nodes_post (no filtering by nRFrequencyRef).
- If nodes_post is empty, the module outputs N/A for both metrics (no extra "Post Step2 checks" error row).
- add_row signature expected:
    add_row(category: str, subcategory: str, metric: str, value: object, extra: str = "")
"""

import re
from typing import Optional, Set, Iterable, List

import pandas as pd

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency


def cc_post_step2(
    df_nr_cell_cu: Optional[pd.DataFrame],
    df_eutran_freq_rel: Optional[pd.DataFrame],
    add_row,
    n77_ssb_pre: object,
    n77_ssb_post: object,
    nodes_post: Optional[Iterable[object]] = None,
) -> None:
    """
    Append Post Step2 Cleanup checks to SummaryAudit via add_row(category, subcategory, metric, value, extra).

    Filtering is done ONLY by nodes_post (NodeId in nodes_post).
    """
    ssb_pre = _safe_parse_int(n77_ssb_pre)
    ssb_post = _safe_parse_int(n77_ssb_post)

    # ------------------------------------------------------------------
    # Normalize nodes_post (scope)
    # ------------------------------------------------------------------
    nodes_post_set: Set[str] = set()
    if nodes_post:
        for n in nodes_post:
            s = "" if n is None else str(n).strip()
            if s:
                nodes_post_set.add(s)

    # Metrics (must match the table text)
    metric_nrcellcu = f"NR nodes with the new N77 SSB ({ssb_post}) and NRCellCU Ref parameters to Profiles with the old SSB name (from NRCellCU table)"
    metric_eutran = f"NR nodes with the new N77 SSB ({ssb_post}) and EUtranFreqRelation Ref parameters to Profiles with the old SSB name (from EUtranFreqRelation table)"

    # Basic validation of SSBs
    if ssb_pre is None or ssb_post is None:
        add_row("NRCellCU", "Profiles Inconsistencies", metric_nrcellcu, 0, f"Invalid SSB values ({ssb_pre}, {ssb_post})")
        add_row("EUtranFreqRelation", "Profiles Inconsistencies", metric_eutran, 0, f"Invalid SSB values ({ssb_pre}, {ssb_post})")
        return

    # If nodes_post empty, do NOT add a separate error row; just mark both metrics as N/A
    if not nodes_post_set:
        add_row("NRCellCU", "Profiles Inconsistencies", metric_nrcellcu, 0, "nodes_post is empty (checks are scoped ONLY by nodes_post)")
        add_row("EUtranFreqRelation", "Profiles Inconsistencies", metric_eutran, 0, "nodes_post is empty (checks are scoped ONLY by nodes_post)")
        return

    # ------------------------------------------------------------------
    # 1) NRCellCU check (scoped by nodes_post)
    # ------------------------------------------------------------------
    _check_nrcellcu_profile_refs(
        df_nr_cell_cu=df_nr_cell_cu,
        add_row=add_row,
        ssb_pre=ssb_pre,
        ssb_post=ssb_post,
        nodes_post=nodes_post_set,
        metric_text=metric_nrcellcu,
    )

    # ------------------------------------------------------------------
    # 2) EUtranFreqRelation check (scoped by nodes_post)
    # ------------------------------------------------------------------
    _check_eutranfreqrelation_profile_refs(
        df=df_eutran_freq_rel,
        add_row=add_row,
        ssb_pre=ssb_pre,
        ssb_post=ssb_post,
        nodes_post=nodes_post_set,
        metric_text=metric_eutran,
    )


# =====================================================================
#                           INTERNAL HELPERS
# =====================================================================

def _safe_parse_int(value: object) -> Optional[int]:
    parsed = parse_int_frequency(value)
    if parsed is not None:
        return int(parsed)
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _contains_int_token(text: object, number: int) -> bool:
    if text is None:
        return False
    s = str(text)
    pattern = rf"(?<!\d){re.escape(str(number))}(?!\d)"
    return re.search(pattern, s) is not None


def _format_nodes(nodes: Set[str]) -> str:
    return ", ".join(sorted(nodes))


def _normalize_node_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


# =====================================================================
#                           CHECKS IMPLEMENTATION
# =====================================================================

def _check_nrcellcu_profile_refs(
    df_nr_cell_cu: Optional[pd.DataFrame],
    add_row,
    ssb_pre: int,
    ssb_post: int,
    nodes_post: Set[str],
    metric_text: str,
) -> None:
    category = "NRCellCU"
    subcategory = "Profiles Inconsistencies"

    if df_nr_cell_cu is None or df_nr_cell_cu.empty:
        add_row(category, subcategory, metric_text, "Table not found or empty", "")
        return

    node_col = resolve_column_case_insensitive(df_nr_cell_cu, ["NodeId"])
    if not node_col:
        add_row(category, subcategory, metric_text, "N/A", "Missing NodeId column in NRCellCU")
        return

    # Ref columns to check (case-insensitive)
    ref_cols_candidates: List[List[str]] = [
        ["mcpcPCellProfileRef"],
        ["mcpcPSCellProfileRef", "mcpcPsCellProfileRef"],
        ["mcfbCellProfileRef", "McfbCellProfileRef"],
        ["trStSaCellProfileRef", "TrStSaCellProfileRef"],
    ]

    ref_cols: List[str] = []
    for cands in ref_cols_candidates:
        c = resolve_column_case_insensitive(df_nr_cell_cu, cands)
        if c:
            ref_cols.append(c)

    if not ref_cols:
        add_row(category, subcategory, metric_text, "N/A", "Missing profile ref columns in NRCellCU")
        return

    work = df_nr_cell_cu.copy()
    work[node_col] = _normalize_node_series(work[node_col])

    # Scope ONLY by nodes_post
    work = work.loc[work[node_col].isin(nodes_post)].copy()

    if work.empty:
        add_row(category, subcategory, metric_text, 0, "")
        return

    bad_nodes: Set[str] = set()
    for _, r in work.iterrows():
        node = str(r.get(node_col, "")).strip()
        for c in ref_cols:
            if c in work.columns:
                if _contains_int_token(r.get(c), ssb_pre):
                    if node:
                        bad_nodes.add(node)
                    break

    add_row(category, subcategory, metric_text, len(bad_nodes), _format_nodes(bad_nodes))


def _check_eutranfreqrelation_profile_refs(
    df: Optional[pd.DataFrame],
    add_row,
    ssb_pre: int,
    ssb_post: int,
    nodes_post: Set[str],
    metric_text: str,
) -> None:
    category = "EUtranFreqRelation"
    subcategory = "Profiles Inconsistencies"

    if df is None or df.empty:
        add_row(category, subcategory, metric_text, "Table not found or empty", "")
        return

    node_col = resolve_column_case_insensitive(df, ["NodeId"])
    if not node_col:
        add_row(category, subcategory, metric_text, "N/A", "Missing NodeId in EUtranFreqRelation")
        return

    col1 = resolve_column_case_insensitive(df, ["mcpcPCellEUtranFreqRelProfileRef"])
    col2 = resolve_column_case_insensitive(df, ["UeMCEUtranFreqRelProfile", "ueMCEUtranFreqRelProfile"])

    ref_cols: List[str] = []
    if col1:
        ref_cols.append(col1)
    if col2:
        ref_cols.append(col2)

    if not ref_cols:
        add_row(category, subcategory, metric_text, "N/A", "Missing required profile ref columns in EUtranFreqRelation")
        return

    work = df.copy()
    work[node_col] = _normalize_node_series(work[node_col])

    # Scope ONLY by nodes_post
    work = work.loc[work[node_col].isin(nodes_post)].copy()

    if work.empty:
        add_row(category, subcategory, metric_text, 0, "")
        return

    bad_nodes: Set[str] = set()
    for _, r in work.iterrows():
        node = str(r.get(node_col, "")).strip()
        for c in ref_cols:
            if c in work.columns:
                if _contains_int_token(r.get(c), ssb_pre):
                    if node:
                        bad_nodes.add(node)
                    break

    add_row(category, subcategory, metric_text, len(bad_nodes), _format_nodes(bad_nodes))
