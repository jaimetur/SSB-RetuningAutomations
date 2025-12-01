# -*- coding: utf-8 -*-

from typing import List, Dict, Any
from openpyxl.styles import PatternFill, Alignment, Font

import pandas as pd
from pandas import DataFrame, Series

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency, is_n77_from_string, extract_sync_frequencies

# =====================================================================
#                        SUMMARY AUDIT BUILDER
# =====================================================================

def build_summary_audit(
    df_nr_cell_du: pd.DataFrame,
    df_nr_freq: pd.DataFrame,
    df_nr_freq_rel: pd.DataFrame,
    df_nr_cell_rel: pd.DataFrame,
    df_freq_prio_nr: pd.DataFrame,
    df_gu_sync_signal_freq: pd.DataFrame,
    df_gu_freq_rel: pd.DataFrame,
    df_gu_cell_rel: pd.DataFrame,
    df_nr_sector_carrier: pd.DataFrame,
    df_endc_distr_profile: pd.DataFrame,
    n77_ssb_pre: int,
    n77_ssb_post: int,
    n77b_ssb: int,
    allowed_n77_ssb_pre,
    allowed_n77_arfcn_pre,
    allowed_n77_ssb_post,
    allowed_n77_arfcn_post,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Build a synthetic 'SummaryAudit' table with high-level checks:

      - N77 detection on NRCellDU and NRSectorCarrier.
      - NR/LTE nodes where specific SSBs (old_arfcn / new_arfcn) are defined.
      - NR/LTE nodes with SSBs not in {old_arfcn, new_arfcn}.
      - Cardinality limits per cell and per node.
      - EndcDistrProfile gUtranFreqRef values.

    Notes:
      - N77 cells are those with SSB/SSB in range [646600-660000].
      - This function is best-effort and should not raise exceptions; any error is
        represented as a row in the resulting dataframe.
    """

    allowed_n77_ssb_pre_set = {int(v) for v in (allowed_n77_ssb_pre or [])}
    allowed_n77_arfcn_pre_set = {int(v) for v in (allowed_n77_arfcn_pre or [])}
    allowed_n77_ssb_post_set = {int(v) for v in (allowed_n77_ssb_post or [])}
    allowed_n77_arfcn_post_set = {int(v) for v in (allowed_n77_arfcn_post or [])}

    rows: List[Dict[str, object]] = []

    # Detailed parameter mismatching rows to build Excel sheets "Summary NR Param Missmatching" and
    # "Summary LTE Param Missmatching"
    param_mismatch_rows_nr: List[Dict[str, object]] = []
    param_mismatch_rows_gu: List[Dict[str, object]] = []

    param_mismatch_columns_nr = [
        "Layer",  # "NR"
        "Table",  # "NRFreqRelation"
        "NodeId",
        "GNBCUCPFunctionId",
        "NRCellCUId",
        "NRFreqRelationId",
        "Parameter",
        "OldSSB",
        "NewSSB",
        "OldValue",
        "NewValue",
    ]

    param_mismatch_columns_gu = [
        "Layer",  # "LTE"
        "Table",  # "GUtranFreqRelation"
        "NodeId",
        "EUtranCellId",  # generic LTE cell id
        "GUtranFreqRelationId",
        "Parameter",
        "OldSSB",
        "NewSSB",
        "OldValue",
        "NewValue",
    ]

    def is_not_old_not_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq not in (n77_ssb_pre, n77_ssb_post)

    def series_only_not_old_not_new(series) -> bool:
        return all(is_not_old_not_new(v) for v in series)

    def is_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == n77_ssb_post

    def is_old(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == n77_ssb_pre
    
    def has_value(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq is not None

    def is_n77_ssb_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_pre_set if freq is not None else False

    def is_n77_ssb_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_post_set if freq is not None else False

    def is_n77_arfcn_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_pre_set if freq is not None else False

    def is_n77_arfcn_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_post_set if freq is not None else False

    def all_n77_arfcn_in_pre(series: pd.Series) -> bool:
        freqs = series.map(parse_int_frequency)
        freqs_valid = {f for f in freqs if f is not None}
        # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_arfcn_pre_set
        return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_arfcn_pre_set)

    def all_n77_arfcn_in_post(series: pd.Series) -> bool:
        freqs = series.map(parse_int_frequency)
        freqs_valid = {f for f in freqs if f is not None}
        # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_arfcn_post_set
        return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_arfcn_post_set)

    def add_row(
            category: str,
            subcategory: str,
            metric: str,
            value: object,
            extra: str = "",
    ) -> None:
        rows.append(
            {
                "Category": category,
                "SubCategory": subcategory,
                "Metric": metric,
                "Value": value,
                "ExtraInfo": extra,
            }
        )

    # ----------------------------- NRCellDU (N77 detection + allowed SSB + LowMidBand/mmWave) -----------------------------
    def process_nr_cell_du():
        try:
            if df_nr_cell_du is not None and not df_nr_cell_du.empty:
                node_col = resolve_column_case_insensitive(df_nr_cell_du, ["NodeId"])
                ssb_col = resolve_column_case_insensitive(df_nr_cell_du, ["ssbFrequency"])

                if node_col and ssb_col:
                    work = df_nr_cell_du[[node_col, ssb_col]].copy()

                    # Ensure NodeId is treated consistently
                    work[node_col] = work[node_col].astype(str).str.strip()

                    # ------------------------------------------------------------------
                    # LowMidBand / mmWave node classification
                    #   - Cells with SSB in [2_000_000, 2_100_000] -> mmWave cells
                    #   - Cells with any other SSB (valid int)     -> LowMidBand cells
                    #   - A node "should" be only one type; if both appear, it is mixed.
                    # ------------------------------------------------------------------
                    work["_ssb_int_"] = work[ssb_col].map(parse_int_frequency)
                    valid_rows = work.loc[work["_ssb_int_"].notna()].copy()

                    if not valid_rows.empty:
                        # Flag mmWave cells
                        valid_rows["_is_mmwave_"] = valid_rows["_ssb_int_"].between(2_000_000, 2_100_000, inclusive="both")

                        nodes_with_nr_cells = sorted(valid_rows[node_col].astype(str).unique())
                        mmwave_nodes: list[str] = []
                        lowmid_nodes: list[str] = []
                        mixed_nodes: list[str] = []

                        for node, series in valid_rows.groupby(node_col)["_is_mmwave_"]:
                            has_mmwave = bool(series.any())
                            has_lowmid = bool((~series).any())
                            node_str = str(node)

                            if has_mmwave and not has_lowmid:
                                mmwave_nodes.append(node_str)
                            elif has_lowmid and not has_mmwave:
                                lowmid_nodes.append(node_str)
                            elif has_mmwave and has_lowmid:
                                mixed_nodes.append(node_str)

                        # Summary rows for NRCellDU LowMidBand / mmWave node types
                        add_row(
                            "NRCellDU",
                            "NR Frequency Audit",
                            "NR Nodes with ssbFrequency (from NRCellDU table)",
                            len(nodes_with_nr_cells),
                            ", ".join(nodes_with_nr_cells),
                        )
                        add_row(
                            "NRCellDU",
                            "NR Frequency Audit",
                            "NR LowMidBand Nodes (from NRCellDU table)",
                            len(lowmid_nodes),
                            ", ".join(sorted(lowmid_nodes)),
                        )
                        add_row(
                            "NRCellDU",
                            "NR Frequency Audit",
                            "NR mmWave Nodes (from NRCellDU table)",
                            len(mmwave_nodes),
                            ", ".join(sorted(mmwave_nodes)),
                        )

                        # Optional: nodes having both LowMidBand and mmWave cells
                        if mixed_nodes:
                            add_row(
                                "NRCellDU",
                                "NR Frequency Audit",
                                "NR Nodes with both LowMidBand and mmWave NR cells (from NRCellDU table)",
                                len(mixed_nodes),
                                ", ".join(sorted(mixed_nodes)),
                            )

                    # ------------------------------------------------------------------
                    # Existing N77 logic (kept as it was)
                    # ------------------------------------------------------------------
                    # N77 cells = those having at least one SSB in N77 band (646600-660000)
                    mask_n77 = work[ssb_col].map(is_n77_from_string)
                    n77_rows = work.loc[mask_n77].copy()

                    if not n77_rows.empty:
                        # NR Frequency Audit: NR nodes with N77 SSB in band (646600-660000) (from NRCellDU table)
                        n77_nodes = sorted(n77_rows[node_col].astype(str).unique())

                        add_row(
                            "NRCellDU",
                            "NR Frequency Audit",
                            "NR nodes with N77 SSB in band (646600-660000) (from NRCellDU table)",
                            len(n77_nodes),
                            ", ".join(n77_nodes),
                        )

                        # NR nodes whose ALL N77 SSBs are in Pre-Retune allowed list (from NRCellDU table)
                        if allowed_n77_ssb_pre_set:
                            grouped_n77 = n77_rows.groupby(node_col)[ssb_col]

                            def all_n77_ssb_in_pre(series: pd.Series) -> bool:
                                freqs = series.map(parse_int_frequency)
                                freqs_valid = {f for f in freqs if f is not None}
                                # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_ssb_pre_set
                                return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_ssb_pre_set)

                            pre_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_ssb_in_pre(series))
                            allowed_pre_str = ", ".join(str(v) for v in sorted(allowed_n77_ssb_pre_set))

                            add_row(
                                "NRCellDU",
                                "NR Frequency Audit",
                                f"NR nodes with N77 SSB in Pre-Retune allowed list ({allowed_pre_str}) (from NRCellDU table)",
                                len(pre_nodes),
                                ", ".join(pre_nodes),
                            )

                        # NR nodes whose ALL N77 SSBs are in Post-Retune allowed list (from NRCellDU table)
                        if allowed_n77_ssb_post_set:
                            grouped_n77 = n77_rows.groupby(node_col)[ssb_col]

                            def all_n77_ssb_in_post(series: pd.Series) -> bool:
                                freqs = series.map(parse_int_frequency)
                                freqs_valid = {f for f in freqs if f is not None}
                                # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_ssb_post_set
                                return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_ssb_post_set)

                            post_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_ssb_in_post(series))
                            allowed_post_str = ", ".join(str(v) for v in sorted(allowed_n77_ssb_post_set))

                            add_row(
                                "NRCellDU",
                                "NR Frequency Audit",
                                f"NR nodes with N77 SSB in Post-Retune allowed list ({allowed_post_str}) (from NRCellDU table)",
                                len(post_nodes),
                                ", ".join(post_nodes),
                            )

                        # NR Frequency Inconsistencies: NR SSB not in pre nor post allowed lists
                        if allowed_n77_ssb_pre_set or allowed_n77_ssb_post_set:
                            allowed_union = set(allowed_n77_ssb_pre_set) | set(allowed_n77_ssb_post_set)

                            def _is_not_in_union_ssb(v: object) -> bool:
                                freq = parse_int_frequency(v)
                                return freq is not None and freq not in allowed_union

                            bad_rows = n77_rows.loc[n77_rows[ssb_col].map(_is_not_in_union_ssb)]

                            # Unique nodes with at least one SSB not in pre/post allowed lists
                            bad_nodes = sorted(bad_rows[node_col].astype(str).unique())

                            # Build a unique (NodeId, SSB) list to avoid duplicated lines in ExtraInfo
                            unique_pairs = sorted(
                                {(str(r[node_col]).strip(), str(r[ssb_col]).strip()) for _, r in bad_rows.iterrows()}
                            )

                            extra = "; ".join(f"{node}: {ssb}" for node, ssb in unique_pairs)

                            add_row(
                                "NRCellDU",
                                "NR Frequency Inconsistencies",
                                "NR nodes with N77 SSB not in Pre/Post Retune allowed lists (from NRCellDU table)",
                                len(bad_nodes),
                                extra,
                            )
                        else:
                            add_row(
                                "NRCellDU",
                                "NR Frequency Inconsistencies",
                                "NR nodes with N77 SSB not in Pre/Post Retune allowed lists (no pre/post allowed lists configured) (from NRCellDU table)",
                                "N/A",
                            )
                    else:
                        add_row(
                            "NRCellDU",
                            "NR Frequency Audit",
                            "NRCellDU table has no N77 rows",
                            0,
                        )
                else:
                    add_row(
                        "NRCellDU",
                        "NR Frequency Audit",
                        "NRCellDU table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NRCellDU",
                    "NR Frequency Audit",
                    "NRCellDU table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NRCellDU",
                "NR Frequency Audit",
                "Error while checking NRCellDU",
                f"ERROR: {ex}",
            )

    # ----------------------------- NRFrequency (OLD/NEW SSB on N77 rows) -----------------------------
    def process_nr_freq():
        try:
            if df_nr_freq is not None and not df_nr_freq.empty:
                node_col = resolve_column_case_insensitive(df_nr_freq, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_nr_freq, ["arfcnValueNRDl", "NRFrequencyId", "nRFrequencyId"])

                if node_col and arfcn_col:
                    work = df_nr_freq[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    # Only consider N77 rows
                    n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                    if not n77_work.empty:
                        grouped = n77_work.groupby(node_col)[arfcn_col]

                        # NR Frequency Audit: ALL nodes (not only N77) with any non-empty SSB (from NRFrequency table)
                        all_nodes_with_freq = sorted(df_nr_freq.loc[df_nr_freq[arfcn_col].map(has_value), node_col].astype(str).unique())
                        add_row(
                            "NRFrequency",
                            "NR Frequency Audit",
                            f"NR nodes with N77 SSB defined (from NRFrequency table)",
                            len(all_nodes_with_freq),
                            ", ".join(all_nodes_with_freq),
                        )

                        # NR Frequency Audit: NR nodes with the old N77 SSB (from NRFrequency table)
                        old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                        add_row(
                            "NRFrequency",
                            "NR Frequency Audit",
                            f"NR nodes with the old N77 SSB ({n77_ssb_pre}) (from NRFrequency table)",
                            len(old_nodes),
                            ", ".join(old_nodes),
                        )

                        # NR Frequency Audit: NR nodes with the new N77 SSB (from NRFrequency table)
                        new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                        add_row(
                            "NRFrequency",
                            "NR Frequency Audit",
                            f"NR nodes with the new N77 SSB ({n77_ssb_post}) (from NRFrequency table)",
                            len(new_nodes),
                            ", ".join(new_nodes),
                        )

                        # NEW: check nodes that have old_ssb and also new_ssb vs those missing the new_arfcn (from NRFrequency table)
                        old_set = set(old_nodes)
                        new_set = set(new_nodes)

                        nodes_old_and_new = sorted(old_set & new_set)
                        add_row(
                            "NRFrequency",
                            "NR Frequency Audit",
                            f"NR nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from NRFrequency table)",
                            len(nodes_old_and_new),
                            ", ".join(nodes_old_and_new),
                        )

                        nodes_old_without_new = sorted(old_set - new_set)
                        add_row(
                            "NRFrequency",
                            "NR Frequency Audit",
                            f"NR nodes with the old N77 SSB ({n77_ssb_pre}) but without the new N77 SSB ({n77_ssb_post}) (from NRFrequency table)",
                            len(nodes_old_without_new),
                            ", ".join(nodes_old_without_new),
                        )

                        # NR Frequency Inconsistencies: NR nodes with the N77 SSB not in (old_freq, new_freq) (from NRFrequency table)
                        not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                        add_row(
                            "NRFrequency",
                            "NR Frequency Inconsistencies",
                            f"NR nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post}) (from NRFrequency table)",
                            len(not_old_not_new_nodes),
                            ", ".join(not_old_not_new_nodes),
                        )
                    else:
                        add_row(
                            "NRFrequency",
                            "NR Frequency Audit",
                            "NRFrequency table has no N77 rows",
                            0,
                        )
                else:
                    add_row(
                        "NRFrequency",
                        "NR Frequency Audit",
                        "NRFrequency table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NRFrequency",
                    "NR Frequency Audit",
                    "NRFrequency table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NRFrequency",
                "NR Frequency Audit",
                "Error while checking NRFrequency",
                f"ERROR: {ex}",
            )

    # ----------------------------- NRFreqRelation (OLD/NEW SSB on NR rows) -----------------------------
    def process_nr_freq_rel():
        try:
            if df_nr_freq_rel is not None and not df_nr_freq_rel.empty:
                node_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRFreqRelationId"])
                gnb_col = resolve_column_case_insensitive(df_nr_freq_rel, ["GNBCUCPFunctionId"])

                if node_col and arfcn_col:
                    work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    # Solo filas N77 (según el string de NRFreqRelationId)
                    n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                    if not n77_work.empty:
                        grouped = n77_work.groupby(node_col)[arfcn_col]

                        # NR Frequency Audit: NR nodes with the old N77 SSB (from NRFreqRelation table)
                        old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Audit",
                            f"NR nodes with the old N77 SSB ({n77_ssb_pre}) (from NRFreqRelation table)",
                            len(old_nodes),
                            ", ".join(old_nodes),
                        )

                        # NR Frequency Audit: NR nodes with the new N77 SSB (from NRFreqRelation table)
                        new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Audit",
                            f"NR nodes with the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)",
                            len(new_nodes),
                            ", ".join(new_nodes),
                        )

                        # NEW: node-level check old_ssb vs new_ssb presence
                        old_set = set(old_nodes)
                        new_set = set(new_nodes)

                        nodes_old_and_new = sorted(old_set & new_set)
                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Audit",
                            f"NR nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)",
                            len(nodes_old_and_new),
                            ", ".join(nodes_old_and_new),
                        )

                        nodes_old_without_new = sorted(old_set - new_set)
                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Audit",
                            f"NR nodes with the old N77 SSB ({n77_ssb_pre}) but without the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)",
                            len(nodes_old_without_new),
                            ", ".join(nodes_old_without_new),
                        )

                        # NR Frequency Inconsistencies: NR nodes with the SSB not in ({old_ssb}, {new_ssb}) (from NRFreqRelation table)
                        not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Inconsistencies",
                            f"NR nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post}) (from NRFreqRelation table)",
                            len(not_old_not_new_nodes),
                            ", ".join(not_old_not_new_nodes),
                        )

                        # NEW: nodes where NRFreqRelationId contains new SSB but has extra characters (e.g. 'auto_647328')
                        post_freq_str = str(n77_ssb_post)
                        pattern_work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                        pattern_work[node_col] = pattern_work[node_col].astype(str)
                        pattern_work[arfcn_col] = pattern_work[arfcn_col].astype(str)

                        mask_contains_post = pattern_work[arfcn_col].str.contains(post_freq_str, na=False)

                        def _has_same_int_and_extra(value: object) -> bool:
                            s = str(value).strip()
                            int_val = parse_int_frequency(s)
                            return int_val == n77_ssb_post and s != post_freq_str

                        mask_bad_pattern = mask_contains_post & pattern_work[arfcn_col].map(_has_same_int_and_extra)

                        bad_pattern_nodes = sorted(pattern_work.loc[mask_bad_pattern, node_col].astype(str).unique())

                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Inconsistencies",
                            f"NR nodes with Auto-created NRFreqRelationId to new N77 SSB ({n77_ssb_post}) but not following VZ naming convention (e.g. with extra characters: 'auto_{n77_ssb_post}')",
                            len(bad_pattern_nodes),
                            ", ".join(bad_pattern_nodes),
                        )

                        # NEW: cell-level check on NRCellCUId: presence and parameter equality (except some columns),
                        cell_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRCellCUId", "NRCellId", "CellId"])
                        rel_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRCellRelationId"])

                        if cell_col:
                            full = df_nr_freq_rel.copy()
                            full[node_col] = full[node_col].astype(str)
                            full[cell_col] = full[cell_col].astype(str)
                            if rel_col:
                                full[rel_col] = full[rel_col].astype(str)

                            # Restrict to N77 rows (based on SSB inside NRFreqRelationId)
                            mask_n77_full = full[arfcn_col].map(is_n77_from_string)
                            full_n77 = full.loc[mask_n77_full].copy()
                            full_n77["_arfcn_int_"] = full_n77[arfcn_col].map(parse_int_frequency)

                            old_mask = full_n77["_arfcn_int_"] == n77_ssb_pre
                            new_mask = full_n77["_arfcn_int_"] == n77_ssb_post

                            cells_with_old = set(full_n77.loc[old_mask, cell_col].astype(str))
                            cells_with_new = set(full_n77.loc[new_mask, cell_col].astype(str))

                            cells_both = sorted(cells_with_old & cells_with_new)
                            cells_old_without_new = sorted(cells_with_old - cells_with_new)

                            add_row(
                                "NRFreqRelation",
                                "NR Frequency Audit",
                                f"NR cells with the old N77 SSB ({n77_ssb_pre}) and the new SSB ({n77_ssb_post}) (from NRFreqRelation table)",
                                len(cells_both),
                                ", ".join(cells_both),
                            )

                            add_row(
                                "NRFreqRelation",
                                "NR Frequency Audit",
                                f"NR cells with the old N77 SSB ({n77_ssb_pre}) but without new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)",
                                len(cells_old_without_new),
                                ", ".join(cells_old_without_new),
                            )

                            # Parameter equality check (ignoring ID/reference columns and helper columns)
                            cols_to_ignore = {arfcn_col, "_arfcn_int_"}
                            for name in full_n77.columns:
                                lname = str(name).lower()
                                if lname in {"nrfreqrelationid", "nrfrequencyref", "reservedby"}:
                                    cols_to_ignore.add(name)

                            bad_cells_params = []

                            for cell_id in cells_both:
                                cell_rows = full_n77.loc[
                                    full_n77[cell_col].astype(str) == cell_id
                                    ].copy()
                                old_rows = cell_rows.loc[cell_rows["_arfcn_int_"] == n77_ssb_pre]
                                new_rows = cell_rows.loc[cell_rows["_arfcn_int_"] == n77_ssb_post]

                                if old_rows.empty or new_rows.empty:
                                    continue

                                if rel_col:
                                    # Only compares pairs OLD/NEW with same NRCellRelationId
                                    rel_old = set(old_rows[rel_col].astype(str))
                                    rel_new = set(new_rows[rel_col].astype(str))
                                    common_rels = rel_old & rel_new

                                    for rel_id in common_rels:
                                        old_rel = old_rows.loc[old_rows[rel_col].astype(str) == rel_id]
                                        new_rel = new_rows.loc[new_rows[rel_col].astype(str) == rel_id]

                                        if old_rel.empty or new_rel.empty:
                                            continue

                                        old_clean = (
                                            old_rel.drop(columns=list(cols_to_ignore), errors="ignore")
                                            .drop_duplicates()
                                            .reset_index(drop=True)
                                        )
                                        new_clean = (
                                            new_rel.drop(columns=list(cols_to_ignore), errors="ignore")
                                            .drop_duplicates()
                                            .reset_index(drop=True)
                                        )

                                        # Alinear columnas y ordenar filas
                                        old_clean = old_clean.reindex(sorted(old_clean.columns), axis=1)
                                        new_clean = new_clean.reindex(sorted(new_clean.columns), axis=1)

                                        sort_cols = list(old_clean.columns)
                                        old_clean = old_clean.sort_values(by=sort_cols).reset_index(drop=True)
                                        new_clean = new_clean.sort_values(by=sort_cols).reset_index(drop=True)

                                        if not old_clean.equals(new_clean):
                                            # Take first row of each side to report parameter-level differences
                                            old_row = old_clean.iloc[0]
                                            new_row = new_clean.iloc[0]

                                            def _values_differ(a: object, b: object) -> bool:
                                                return (pd.isna(a) and not pd.isna(b)) or (not pd.isna(a) and pd.isna(b)) or (a != b)

                                            node_val = ""
                                            gnb_val = ""
                                            nrcell_val = ""
                                            nrfreqrel_val = ""
                                            try:
                                                base_row = old_rel.iloc[0]
                                                node_val = str(base_row[node_col]) if node_col in base_row.index else ""
                                                if gnb_col and gnb_col in base_row.index:
                                                    gnb_val = str(base_row[gnb_col])
                                                if cell_col in base_row.index:
                                                    nrcell_val = str(base_row[cell_col])
                                                if arfcn_col in base_row.index:
                                                    nrfreqrel_val = str(base_row[arfcn_col])
                                            except Exception:
                                                node_val = ""

                                            for col_name in sort_cols:
                                                old_val = old_row[col_name]
                                                new_val = new_row[col_name]
                                                if _values_differ(old_val, new_val):
                                                    param_mismatch_rows_nr.append(
                                                        {
                                                            "Layer": "NR",
                                                            "Table": "NRFreqRelation",
                                                            "NodeId": node_val,
                                                            "GNBCUCPFunctionId": gnb_val,
                                                            "NRCellCUId": nrcell_val,
                                                            "NRFreqRelationId": nrfreqrel_val,
                                                            "Parameter": str(col_name),
                                                            "OldSSB": n77_ssb_pre,
                                                            "NewSSB": n77_ssb_post,
                                                            "OldValue": "" if pd.isna(old_val) else str(old_val),
                                                            "NewValue": "" if pd.isna(new_val) else str(new_val),
                                                        }
                                                    )

                                            bad_cells_params.append(str(cell_id))
                                            # con una relación que falle es suficiente para marcar la celda
                                            break

                                else:
                                    # Fallback: sin NRCellRelationId, se compara todo el bloque OLD vs NEW
                                    old_clean = (
                                        old_rows.drop(columns=list(cols_to_ignore), errors="ignore")
                                        .drop_duplicates()
                                        .reset_index(drop=True)
                                    )
                                    new_clean = (
                                        new_rows.drop(columns=list(cols_to_ignore), errors="ignore")
                                        .drop_duplicates()
                                        .reset_index(drop=True)
                                    )

                                    old_clean = old_clean.reindex(sorted(old_clean.columns), axis=1)
                                    new_clean = new_clean.reindex(sorted(new_clean.columns), axis=1)

                                    sort_cols = list(old_clean.columns)
                                    old_clean = old_clean.sort_values(by=sort_cols).reset_index(drop=True)
                                    new_clean = new_clean.sort_values(by=sort_cols).reset_index(drop=True)

                                    if not old_clean.equals(new_clean):
                                        old_row = old_clean.iloc[0]
                                        new_row = new_clean.iloc[0]

                                        def _values_differ(a: object, b: object) -> bool:
                                            return (pd.isna(a) and not pd.isna(b)) or (not pd.isna(a) and pd.isna(b)) or (a != b)

                                        node_val = ""
                                        gnb_val = ""
                                        nrcell_val = ""
                                        nrfreqrel_val = ""
                                        try:
                                            base_row = old_rows.iloc[0]
                                            node_val = str(base_row[node_col]) if node_col in base_row.index else ""
                                            if gnb_col and gnb_col in base_row.index:
                                                gnb_val = str(base_row[gnb_col])
                                            if cell_col in base_row.index:
                                                nrcell_val = str(base_row[cell_col])
                                            if arfcn_col in base_row.index:
                                                nrfreqrel_val = str(base_row[arfcn_col])
                                        except Exception:
                                            node_val = ""

                                        for col_name in sort_cols:
                                            old_val = old_row[col_name]
                                            new_val = new_row[col_name]
                                            if _values_differ(old_val, new_val):
                                                param_mismatch_rows_nr.append(
                                                    {
                                                        "Layer": "NR",
                                                        "Table": "NRFreqRelation",
                                                        "NodeId": node_val,
                                                        "GNBCUCPFunctionId": gnb_val,
                                                        "NRCellCUId": nrcell_val,
                                                        "NRFreqRelationId": nrfreqrel_val,
                                                        "Parameter": str(col_name),
                                                        "OldSSB": n77_ssb_pre,
                                                        "NewSSB": n77_ssb_post,
                                                        "OldValue": "" if pd.isna(old_val) else str(old_val),
                                                        "NewValue": "" if pd.isna(new_val) else str(new_val),
                                                    }
                                                )

                                        bad_cells_params.append(str(cell_id))

                            bad_cells_params = sorted(set(bad_cells_params))

                            add_row(
                                "NRFreqRelation",
                                "NR Frequency Inconsistencies",
                                f"NR cells with mismatching params between old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table). See 'Summary NR Param Mismatching' sheet for more details",
                                len(bad_cells_params),
                                ", ".join(bad_cells_params),
                            )
                        else:
                            add_row(
                                "NRFreqRelation",
                                "NR Frequency Audit",
                                "NRFreqRelation cell-level check skipped (NRCellCUId/NRCellId/CellId missing)",
                                "N/A",
                            )
                    else:
                        add_row(
                            "NRFreqRelation",
                            "NR Frequency Audit",
                            "NRFreqRelation table has no N77 rows",
                            0,
                        )
                else:
                    add_row(
                        "NRFreqRelation",
                        "NR Frequency Audit",
                        "NRFreqRelation table present but SSB/NodeId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NRFreqRelation",
                    "NR Frequency Audit",
                    "NRFreqRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NRFreqRelation",
                "NR Frequency Audit",
                "Error while checking NRFreqRelation",
                f"ERROR: {ex}",
            )

    # ----------------------------- NRSectorCarrier (N77 + allowed SSB) -----------------------------
    def process_nr_sector_carrier():
        try:
            if df_nr_sector_carrier is not None and not df_nr_sector_carrier.empty:
                node_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["arfcnDL"])

                if node_col and arfcn_col:
                    work = df_nr_sector_carrier[[node_col, arfcn_col]].copy()

                    work[node_col] = work[node_col].astype(str).str.strip()

                    # N77 nodes = those having at least one SSB in N77 band (646600-660000)
                    mask_n77 = work[arfcn_col].map(is_n77_from_string)
                    n77_rows = work.loc[mask_n77].copy()

                    # NR Frequency Audit: NR nodes with SSB in N77 band (646600-660000) (from NRSectorCarrier table)
                    n77_nodes = sorted(n77_rows[node_col].astype(str).unique())

                    add_row(
                        "NRSectorCarrier",
                        "NR Frequency Audit",
                        "NR nodes with N77 SSB in band (646600-660000) (from NRSectorCarrier table)",
                        len(n77_nodes),
                        ", ".join(n77_nodes),
                    )

                    # NR nodes whose ALL N77 SSBs are in Pre-Retune allowed list (from NRSectorCarrier table)
                    if allowed_n77_arfcn_pre_set:
                        grouped_n77 = n77_rows.groupby(node_col)[arfcn_col]

                        pre_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_arfcn_in_pre(series))
                        allowed_pre_str = ", ".join(str(v) for v in sorted(allowed_n77_arfcn_pre_set))

                        add_row(
                            "NRSectorCarrier",
                            "NR Frequency Audit",
                            f"NR nodes with N77 SSB in Pre-Retune allowed list ({allowed_pre_str}) (from NRSectorCarrier table)",
                            len(pre_nodes),
                            ", ".join(pre_nodes),
                        )

                    # NR nodes whose ALL N77 SSBs are in Post-Retune allowed list (from NRSectorCarrier table)
                    if allowed_n77_arfcn_post_set:
                        grouped_n77 = n77_rows.groupby(node_col)[arfcn_col]

                        post_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_arfcn_in_post(series))
                        allowed_post_str = ", ".join(str(v) for v in sorted(allowed_n77_arfcn_post_set))

                        add_row(
                            "NRSectorCarrier",
                            "NR Frequency Audit",
                            f"NR nodes with N77 SSB in Post-Retune allowed list ({allowed_post_str}) (from NRSectorCarrier table)",
                            len(post_nodes),
                            ", ".join(post_nodes),
                        )

                    # NR Frequency Inconsistencies: NR SSB not in pre nor post allowed lists
                    if allowed_n77_arfcn_pre_set or allowed_n77_arfcn_post_set:
                        allowed_union = set(allowed_n77_arfcn_pre_set) | set(allowed_n77_arfcn_post_set)

                        def _is_not_in_union(v: object) -> bool:
                            freq = parse_int_frequency(v)
                            return freq is not None and freq not in allowed_union

                        bad_rows = n77_rows.loc[n77_rows[arfcn_col].map(_is_not_in_union)]

                        # Unique nodes with at least one SSB not in pre/post allowed lists
                        bad_nodes = sorted(bad_rows[node_col].astype(str).unique())

                        # Build a unique (NodeId, SSB) list to avoid duplicated lines in ExtraInfo
                        unique_pairs = sorted(
                            {(str(r[node_col]).strip(), str(r[arfcn_col]).strip()) for _, r in bad_rows.iterrows()}
                        )

                        extra = "; ".join(f"{node}: {arfcn}" for node, arfcn in unique_pairs)

                        add_row(
                            "NRSectorCarrier",
                            "NR Frequency Inconsistencies",
                            "NR nodes with N77 SSB not in Pre/Post Retune allowed lists (from NRSectorCarrier table)",
                            len(bad_nodes),
                            extra,
                        )
                    else:
                        add_row(
                            "NRSectorCarrier",
                            "NR Frequency Inconsistencies",
                            "NR nodes with N77 SSB not in Pre/Post Retune allowed lists (no pre/post allowed lists configured) (from NRSectorCarrier table)",
                            "N/A",
                        )
                else:
                    add_row(
                        "NRSectorCarrier",
                        "NR Frequency Audit",
                        "NRSectorCarrier table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NRSectorCarrier",
                    "NR Frequency Audit",
                    "NRSectorCarrier table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NRSectorCarrier",
                "NR Frequency Audit",
                "Error while checking NRSectorCarrier",
                f"ERROR: {ex}",
            )


    # ------------------------------------- NRCellRelations --------------------------------------------
    def process_nr_cell_relation():
        try:
            if df_nr_cell_rel is not None and not df_nr_cell_rel.empty:
                node_col = resolve_column_case_insensitive(df_nr_cell_rel, ["NodeId"])
                freq_col = resolve_column_case_insensitive(df_nr_cell_rel, ["nRFreqRelationRef", "NRFreqRelationRef"])

                # Helper embedded inside the main function
                def _extract_freq_from_nrfreqrelationref(value: object) -> int | None:
                    """Extract NRFreqRelation integer from NRCellRelation reference string."""
                    if value is None:
                        return None
                    s = str(value)
                    key = "NRFreqRelation="
                    idx = s.rfind(key)
                    if idx == -1:
                        return None
                    substr = s[idx + len(key):]
                    digits = []
                    for ch in substr:
                        if ch.isdigit():
                            digits.append(ch)
                        else:
                            break
                    if not digits:
                        return None
                    try:
                        return int("".join(digits))
                    except Exception:
                        return None

                if node_col and freq_col:
                    work = df_nr_cell_rel[[node_col, freq_col]].copy()
                    work[node_col] = work[node_col].astype(str).str.strip()
                    work["_freq_int_"] = work[freq_col].map(_extract_freq_from_nrfreqrelationref)

                    # Use your variables for pre/post frequencies
                    old_ssb = n77_ssb_pre
                    new_ssb = n77_ssb_post

                    count_old = int((work["_freq_int_"] == old_ssb).sum())
                    count_new = int((work["_freq_int_"] == new_ssb).sum())

                    add_row(
                        "NRCellRelation",
                        "NR Frequency Audit",
                        f"NR cellRelations to old N77 SSB ({old_ssb}) (from NRCellRelation table)",
                        count_old,
                    )
                    add_row(
                        "NRCellRelation",
                        "NR Frequency Audit",
                        f"NR cellRelations to new N77 SSB ({new_ssb}) (from NRCellRelation table)",
                        count_new,
                    )
                else:
                    add_row(
                        "NRCellRelation",
                        "NR Frequency Audit",
                        "NRCellRelation table present but NodeId / nRFreqRelationRef column missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NRCellRelation",
                    "NR Frequency Audit",
                    "NRCellRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NRCellRelation",
                "NR Frequency Audit",
                "Error while checking NRCellRelation",
                f"ERROR: {ex}",
            )

    # ----------------------------- LTE GUtranSyncSignalFrequency (OLD/NEW SSB + LowMidBand/mmWave) -----------------------------
    def process_gu_sync_signal_freq():
        try:
            if df_gu_sync_signal_freq is not None and not df_gu_sync_signal_freq.empty:
                node_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["arfcn", "arfcnDL"])

                if node_col and arfcn_col:
                    work = df_gu_sync_signal_freq[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str).str.strip()

                    # ------------------------------------------------------------------
                    # LowMidBand / mmWave node classification on LTE side
                    #   - Rows with SSB in [2_000_000, 2_100_000] -> mmWave
                    #   - Rows with any other valid SSB          -> LowMidBand
                    #   - A node "should" be only one type; if both appear, it is mixed.
                    # ------------------------------------------------------------------
                    work["_arfcn_int_"] = work[arfcn_col].map(parse_int_frequency)
                    valid_rows = work.loc[work["_arfcn_int_"].notna()].copy()

                    if not valid_rows.empty:
                        valid_rows["_is_mmwave_"] = valid_rows["_arfcn_int_"].between(2_000_000, 2_100_000, inclusive="both")

                        nodes_with_lte_sync = sorted(valid_rows[node_col].astype(str).unique())
                        mmwave_nodes: list[str] = []
                        lowmid_nodes: list[str] = []
                        mixed_nodes: list[str] = []

                        for node, series in valid_rows.groupby(node_col)["_is_mmwave_"]:
                            has_mmwave = bool(series.any())
                            has_lowmid = bool((~series).any())
                            node_str = str(node)

                            if has_mmwave and not has_lowmid:
                                mmwave_nodes.append(node_str)
                            elif has_lowmid and not has_mmwave:
                                lowmid_nodes.append(node_str)
                            elif has_mmwave and has_lowmid:
                                mixed_nodes.append(node_str)

                        # Summary rows for LTE LowMidBand / mmWave node types
                        add_row(
                            "GUtranSyncSignalFrequency",
                            "LTE Frequency Audit",
                            "LTE Nodes with GUtranSyncSignalFrequency (from GUtranSyncSignalFrequency table)",
                            len(nodes_with_lte_sync),
                            ", ".join(nodes_with_lte_sync),
                        )
                        add_row(
                            "GUtranSyncSignalFrequency",
                            "LTE Frequency Audit",
                            "LTE LowMidBand Nodes (from GUtranSyncSignalFrequency table)",
                            len(lowmid_nodes),
                            ", ".join(sorted(lowmid_nodes)),
                        )
                        add_row(
                            "GUtranSyncSignalFrequency",
                            "LTE Frequency Audit",
                            "LTE mmWave Nodes (from GUtranSyncSignalFrequency table)",
                            len(mmwave_nodes),
                            ", ".join(sorted(mmwave_nodes)),
                        )

                        # Optional: nodes having both LowMidBand and mmWave SSBs
                        if mixed_nodes:
                            add_row(
                                "GUtranSyncSignalFrequency",
                                "LTE Frequency Audit",
                                "LTE Nodes with both LowMidBand and mmWave GUtranSyncSignalFrequency SSBs (from GUtranSyncSignalFrequency table)",
                                len(mixed_nodes),
                                ", ".join(sorted(mixed_nodes)),
                            )

                    # ------------------------------------------------------------------
                    # Existing logic: old/new SSB checks on LTE side
                    # ------------------------------------------------------------------
                    grouped = work.groupby(node_col)[arfcn_col]

                    # LTE nodes with GUtranSyncSignalFrequency defined (from GUtranSyncSignalFrequency table)
                    all_nodes_with_freq = sorted(
                        work.loc[work[arfcn_col].map(has_value), node_col].astype(str).unique()
                    )
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        "LTE nodes with GUtranSyncSignalFrequency defined (from GUtranSyncSignalFrequency table)",
                        len(all_nodes_with_freq),
                        ", ".join(all_nodes_with_freq),
                    )

                    # LTE Frequency Audit: LTE nodes with the old N77 SSB (from GUtranSyncSignalFrequency table)
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        f"LTE nodes with the old N77 SSB ({n77_ssb_pre}) (from GUtranSyncSignalFrequency table)",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # LTE Frequency Audit: LTE nodes with the new N77 SSB (from GUtranSyncSignalFrequency table)
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        f"LTE nodes with the new N77 SSB ({n77_ssb_post}) (from GUtranSyncSignalFrequency table)",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # NEW: nodes with old_ssb that should also have new_ssb
                    old_set = set(old_nodes)
                    new_set = set(new_nodes)

                    nodes_old_and_new = sorted(old_set & new_set)
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        f"LTE nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from GUtranSyncSignalFrequency table)",
                        len(nodes_old_and_new),
                        ", ".join(nodes_old_and_new),
                    )

                    nodes_old_without_new = sorted(old_set - new_set)
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        f"LTE nodes with the old N77 SSB ({n77_ssb_pre}) but without the new N77 SSB ({n77_ssb_post}) (from GUtranSyncSignalFrequency table)",
                        len(nodes_old_without_new),
                        ", ".join(nodes_old_without_new),
                    )

                    # LTE Frequency Inconsistencies: LTE nodes with the SSB not in ({old_arfcn}, {new_arfcn}) (from GUtranSyncSignalFrequency table)
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Inconsistencies",
                        f"LTE nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post}) (from GUtranSyncSignalFrequency table)",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )
                else:
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        "GUtranSyncSignalFrequency table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "GUtranSyncSignalFrequency",
                    "LTE Frequency Audit",
                    "GUtranSyncSignalFrequency table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "GUtranSyncSignalFrequency",
                "LTE Frequency Audit",
                "Error while checking GUtranSyncSignalFrequency",
                f"ERROR: {ex}",
            )

    # ----------------------------- LTE GUtranFreqRelation (OLD/NEW SSB) -----------------------------
    def process_gu_freq_rel():
        try:
            if df_gu_freq_rel is not None and not df_gu_freq_rel.empty:
                node_col = resolve_column_case_insensitive(df_gu_freq_rel, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_gu_freq_rel, ["GUtranFreqRelationId", "gUtranFreqRelationId"])

                if node_col and arfcn_col:
                    work = df_gu_freq_rel[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    grouped = work.groupby(node_col)[arfcn_col]

                    # LTE Frequency Audit: LTE nodes with the old N77 SSB (from GUtranFreqRelation table)
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Audit",
                        f"LTE nodes with the old N77 SSB ({n77_ssb_pre}) (from GUtranFreqRelation table)",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # LTE Frequency Audit: LTE nodes with the new N77 SSB (from GUtranFreqRelation table)
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Audit",
                        f"LTE nodes with the new N77 SSB ({n77_ssb_post}) (from GUtranFreqRelation table)",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # NEW: node-level check old_arfcn vs new_arfcn presence
                    old_set = set(old_nodes)
                    new_set = set(new_nodes)

                    nodes_old_and_new = sorted(old_set & new_set)
                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Audit",
                        f"LTE nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from GUtranFreqRelation table)",
                        len(nodes_old_and_new),
                        ", ".join(nodes_old_and_new),
                    )

                    nodes_old_without_new = sorted(old_set - new_set)
                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Audit",
                        f"LTE nodes with the old N77 SSB ({n77_ssb_pre}) but without the new SSB ({n77_ssb_post}) (from GUtranFreqRelation table)",
                        len(nodes_old_without_new),
                        ", ".join(nodes_old_without_new),
                    )

                    # LTE Frequency Inconsistencies: LTE nodes with the SSB not in ({old_ssb}, {new_ssb}) (from GUtranFreqRelation table)
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Inconsistencies",
                        f"LTE nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post}) (from GUtranFreqRelation table)",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )

                    # NEW: LTE nodes whose GUtranFreqRelationId containing post SSB do not follow pattern new_arfcn-*-*-*-* (3 hyphens)
                    post_freq_str = str(n77_ssb_post)
                    pattern_work = df_gu_freq_rel[[node_col, arfcn_col]].copy()
                    pattern_work[node_col] = pattern_work[node_col].astype(str)
                    pattern_work[arfcn_col] = pattern_work[arfcn_col].astype(str)

                    # Only rows whose GUtranFreqRelationId string contains the new SSB
                    mask_contains_post = pattern_work[arfcn_col].str.contains(post_freq_str, na=False)

                    def has_four_hyphens(value: object) -> bool:
                        s = str(value)
                        return s.count("-") == 4

                    mask_bad_pattern = mask_contains_post & ~pattern_work[arfcn_col].map(has_four_hyphens)

                    # Build detailed list: NodeId + GUtranFreqRelationId
                    bad_rows = pattern_work.loc[mask_bad_pattern].copy()
                    bad_nodes = sorted(bad_rows[node_col].astype(str).unique())

                    unique_pairs = sorted(
                        {(str(r[node_col]).strip(), str(r[arfcn_col]).strip()) for _, r in bad_rows.iterrows()}
                    )
                    extra_bad = "; ".join(f"{node}: {rel_id}" for node, rel_id in unique_pairs)

                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Inconsistencies",
                        f"LTE nodes with Auto-created GUtranFreqRelationId to new N77 SSB ({n77_ssb_post}) but not following VZ naming convention ({n77_ssb_post}-30-20-0-1) (from GUtranFreqRelation table)",
                        len(bad_nodes),
                        extra_bad,
                    )

                    # NEW: LTE cell-level check using GUtranFreqRelationId strings old_arfcn-30-20-0-1 / new_arfcn-30-20-0-1 (from GUtranFreqRelation table)
                    cell_col_gu = resolve_column_case_insensitive(df_gu_freq_rel, ["EUtranCellFDDId", "EUtranCellId", "CellId", "GUCellId"])
                    expected_old_rel_id = f"{n77_ssb_pre}-30-20-0-1"
                    expected_new_rel_id = f"{n77_ssb_post}-30-20-0-1"

                    if cell_col_gu:
                        full = df_gu_freq_rel.copy()
                        full[node_col] = full[node_col].astype(str)
                        full[cell_col_gu] = full[cell_col_gu].astype(str)
                        full[arfcn_col] = full[arfcn_col].astype(str)

                        mask_old_rel = full[arfcn_col] == expected_old_rel_id
                        mask_new_rel = full[arfcn_col] == expected_new_rel_id

                        cells_with_old = set(full.loc[mask_old_rel, cell_col_gu].astype(str))
                        cells_with_new = set(full.loc[mask_new_rel, cell_col_gu].astype(str))

                        cells_both = sorted(cells_with_old & cells_with_new)
                        cells_old_without_new = sorted(cells_with_old - cells_with_new)

                        add_row(
                            "GUtranFreqRelation",
                            "LTE Frequency Audit",
                            f"LTE cells with GUtranFreqRelationId {expected_old_rel_id} and {expected_new_rel_id} (from GUtranFreqRelation table)",
                            len(cells_both),
                            ", ".join(cells_both),
                        )

                        add_row(
                            "GUtranFreqRelation",
                            "LTE Frequency Audit",
                            f"LTE cells with GUtranFreqRelationId {expected_old_rel_id} but without {expected_new_rel_id} (from GUtranFreqRelation table)",
                            len(cells_old_without_new),
                            ", ".join(cells_old_without_new),
                        )

                        # Parameter equality check (ignoring ID/reference columns)
                        cols_to_ignore = {arfcn_col}
                        for name in full.columns:
                            lname = str(name).lower()
                            if lname in {"gutranfreqrelationid", "gutransyncsignalfrequencyref"}:
                                cols_to_ignore.add(name)

                        bad_cells_params = []
                        for cell_id in cells_both:
                            cell_rows = full.loc[full[cell_col_gu].astype(str) == cell_id].copy()
                            old_rows = cell_rows.loc[cell_rows[arfcn_col] == expected_old_rel_id]
                            new_rows = cell_rows.loc[cell_rows[arfcn_col] == expected_new_rel_id]

                            if old_rows.empty or new_rows.empty:
                                continue

                            old_clean = old_rows.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True)
                            new_clean = new_rows.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True)

                            # Align column order
                            old_clean = old_clean.reindex(sorted(old_clean.columns), axis=1)
                            new_clean = new_clean.reindex(sorted(new_clean.columns), axis=1)

                            # Sort rows by all columns to make comparison order-independent
                            sort_cols = list(old_clean.columns)
                            old_clean = old_clean.sort_values(by=sort_cols).reset_index(drop=True)
                            new_clean = new_clean.sort_values(by=sort_cols).reset_index(drop=True)

                            if not old_clean.equals(new_clean):
                                old_row = old_clean.iloc[0]
                                new_row = new_clean.iloc[0]

                                def _values_differ(a: object, b: object) -> bool:
                                    return (pd.isna(a) and not pd.isna(b)) or (not pd.isna(a) and pd.isna(b)) or (a != b)

                                node_val = ""
                                try:
                                    node_val = str(cell_rows[node_col].iloc[0])
                                except Exception:
                                    node_val = ""

                                for col_name in sort_cols:
                                    old_val = old_row[col_name]
                                    new_val = new_row[col_name]
                                    if _values_differ(old_val, new_val):
                                        param_mismatch_rows_gu.append(
                                            {
                                                "Layer": "LTE",
                                                "Table": "GUtranFreqRelation",
                                                "NodeId": node_val,
                                                "EUtranCellId": str(cell_id),
                                                "GUtranFreqRelationId": expected_new_rel_id,
                                                "Parameter": str(col_name),
                                                "OldSSB": n77_ssb_pre,
                                                "NewSSB": n77_ssb_post,
                                                "OldValue": "" if pd.isna(old_val) else str(old_val),
                                                "NewValue": "" if pd.isna(new_val) else str(new_val),
                                            }
                                        )

                                bad_cells_params.append(str(cell_id))

                        bad_cells_params = sorted(set(bad_cells_params))

                        add_row(
                            "GUtranFreqRelation",
                            "LTE Frequency Inconsistencies",
                            f"LTE cells with mismatching params between GUtranFreqRelationId {expected_old_rel_id} and {expected_new_rel_id} (from GUtranFreqRelation table). See 'Summary LTE Param Mismatching' sheet for more details",
                            len(bad_cells_params),
                            ", ".join(bad_cells_params),
                        )
                    else:
                        add_row(
                            "GUtranFreqRelation",
                            "LTE Frequency Audit",
                            "GUtranFreqRelation cell-level check skipped (EUtranCellFDDId/EUtranCellId/CellId/GUCellId missing)",
                            "N/A",
                        )
                else:
                    add_row(
                        "GUtranFreqRelation",
                        "LTE Frequency Audit",
                        "GUtranFreqRelation table present but SSB/NodeId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "GUtranFreqRelation",
                    "LTE Frequency Audit",
                    "GUtranFreqRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "GUtranFreqRelation",
                "LTE Frequency Audit",
                "Error while checking GUtranFreqRelation",
                f"ERROR: {ex}",
            )

    # ------------------------------------- GUtranCellRelation --------------------------------------------
    def process_gu_cell_relation():
        try:
            if df_gu_cell_rel is not None and not df_gu_cell_rel.empty:
                node_col = resolve_column_case_insensitive(df_gu_cell_rel, ["NodeId"])
                freq_col = resolve_column_case_insensitive(df_gu_cell_rel, ["GUtranFreqRelationId"])

                if node_col and freq_col:
                    work = df_gu_cell_rel[[node_col, freq_col]].copy()
                    work[node_col] = work[node_col].astype(str).str.strip()

                    # Extract numeric frequency from GUtranFreqRelationId (part before first '-')
                    work["_freq_int_"] = work[freq_col].map(lambda v: parse_int_frequency(str(v).split("-", 1)[0]) if pd.notna(v) else None)

                    count_old = int((work["_freq_int_"] == n77_ssb_pre).sum())
                    count_new = int((work["_freq_int_"] == n77_ssb_post).sum())

                    add_row(
                        "GUtranCellRelation",
                        "LTE Frequency Audit",
                        f"LTE cellRelations to old N77 SSB ({n77_ssb_pre}) (from GUtranCellRelation table)",
                        count_old,
                    )
                    add_row(
                        "GUtranCellRelation",
                        "LTE Frequency Audit",
                        f"LTE cellRelations to new N77 SSB ({n77_ssb_post}) (from GUtranCellRelation table)",
                        count_new,
                    )
                else:
                    add_row(
                        "GUtranCellRelation",
                        "LTE Frequency Audit",
                        "GUtranCellRelation table present but NodeId / GUtranFreqRelationId column missing",
                        "N/A",
                    )
            else:
                add_row(
                    "GUtranCellRelation",
                    "LTE Frequency Audit",
                    "GUtranCellRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "GUtranCellRelation",
                "LTE Frequency Audit",
                "Error while checking GUtranCellRelation",
                f"ERROR: {ex}",
            )

    # ----------------------------- EndcDistrProfile gUtranFreqRef -----------------------------
    def process_endc_distr_profile():
        try:
            if df_endc_distr_profile is not None and not df_endc_distr_profile.empty:
                node_col_edp = resolve_column_case_insensitive(df_endc_distr_profile, ["NodeId"])
                ref_col = resolve_column_case_insensitive(df_endc_distr_profile, ["gUtranFreqRef"])
                if node_col_edp and ref_col:
                    work = df_endc_distr_profile[[node_col_edp, ref_col]].copy()
                    work[node_col_edp] = work[node_col_edp].astype(str)
                    work[ref_col] = work[ref_col].astype(str)

                    # extract_sync_frequencies returns a set of strings (e.g. {"648672", "653952"})
                    freq_sets = work[ref_col].map(extract_sync_frequencies)

                    # Make sure we compare strings with strings
                    expected_old = str(n77_ssb_pre)
                    expected_new = str(n77_ssb_post)
                    expected_n77b = str(n77b_ssb)

                    # Nodes with GUtranSyncSignalFrequency containing old_arfcn and n77b_ssb (from EndcDistrProfile table)
                    mask_old_pair = freq_sets.map(
                        lambda s: (expected_old in s) and (expected_n77b in s)
                    )
                    old_nodes = sorted(
                        work.loc[mask_old_pair, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile",
                        "ENDC Audit",
                        f"Nodes with gUtranFreqRef containing the old N77 SSB ({n77_ssb_pre}) and the N77B SSB ({n77b_ssb}) (from EndcDistrProfile table)",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # Nodes with GUtranSyncSignalFrequency containing new_arfcn and n77b_ssb (from EndcDistrProfile table)
                    mask_new_pair = freq_sets.map(
                        lambda s: (expected_new in s) and (expected_n77b in s)
                    )
                    new_nodes = sorted(
                        work.loc[mask_new_pair, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile",
                        "ENDC Audit",
                        f"Nodes with gUtranFreqRef containing the new N77 SSB ({n77_ssb_post}) and the N77B SSB ({n77b_ssb}) (from EndcDistrProfile table)",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # Inconsistencies:
                    # - rows where neither old_arfcn nor new_arfcn is present (from EndcDistrProfile table)
                    #   OR
                    # - rows where n77b_ssb is not present (from EndcDistrProfile table)
                    mask_inconsistent = freq_sets.map(
                        lambda s: ((expected_old not in s and expected_new not in s) or (expected_n77b not in s))
                    )
                    bad_nodes = sorted(
                        work.loc[mask_inconsistent, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile",
                        "ENDC Inconsistencies",
                        f"Nodes with gUtranFreqRef not containing N77 SSBs ({n77_ssb_pre} or {n77_ssb_post}) together with N77B SSB ({n77b_ssb}) (from EndcDistrProfile table)",
                        len(bad_nodes),
                        ", ".join(bad_nodes),
                    )
                else:
                    add_row(
                        "EndcDistrProfile",
                        "ENDC Audit",
                        "EndcDistrProfile table present but NodeId/gUtranFreqRef missing",
                        "N/A",
                    )
            else:
                add_row(
                    "EndcDistrProfile",
                    "ENDC Audit",
                    "EndcDistrProfile table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "EndcDistrProfile",
                "ENDC Audit",
                "Error while checking EndcDistrProfile gUtranFreqRef",
                f"ERROR: {ex}",
            )

    # ----------------------------- FreqPrioNR (RATFreqPrioId on N77 only) -----------------------------
    def process_freq_prio_nr():
        try:
            if df_freq_prio_nr is not None and not df_freq_prio_nr.empty:
                node_col = resolve_column_case_insensitive(df_freq_prio_nr, ["NodeId"])
                freq_col = resolve_column_case_insensitive(df_freq_prio_nr, ["FreqPrioNRId"])
                ratfreqprio_col = resolve_column_case_insensitive(df_freq_prio_nr, ["RATFreqPrioId"])

                if node_col and freq_col and ratfreqprio_col:
                    work = df_freq_prio_nr[[node_col, freq_col, ratfreqprio_col]].copy()

                    # Normalize NodeId and RATFreqPrioId for consistent comparison
                    work[node_col] = work[node_col].astype(str).str.strip()
                    work[ratfreqprio_col] = work[ratfreqprio_col].astype(str).str.strip().str.lower()

                    # Parse frequency as integer to compare specific SSBs
                    work["_freq_int_"] = work[freq_col].map(parse_int_frequency)

                    # ------------------------------------------------------------------
                    # New checks: old SSB (648672) vs new SSB (647328) in FreqPrioNR
                    # ------------------------------------------------------------------
                    old_ssb = n77_ssb_pre
                    new_ssb = n77_ssb_post

                    old_rows = work.loc[work["_freq_int_"] == old_ssb].copy()
                    new_rows = work.loc[work["_freq_int_"] == new_ssb].copy()

                    old_nodes = set(old_rows[node_col].astype(str))
                    new_nodes = set(new_rows[node_col].astype(str))

                    nodes_old_only = sorted(old_nodes - new_nodes)
                    nodes_both = sorted(old_nodes & new_nodes)

                    # LTE nodes with the old N77 SSB (648672) but without the new SSB (647328)
                    add_row(
                        "FreqPrioNR",
                        "ENDC Audit",
                        f"LTE nodes with the old N77 SSB ({old_ssb}) but without the new N77 SSB ({new_ssb}) (from FreqPrioNR table)",
                        len(nodes_old_only),
                        ", ".join(nodes_old_only),
                    )

                    # LTE nodes with both, the old SSB (648672) and the new SSB (647328)
                    add_row(
                        "FreqPrioNR",
                        "ENDC Audit",
                        f"LTE nodes with both, the old N77 SSB ({old_ssb}) and the new N77 SSB ({new_ssb}) (from FreqPrioNR table)",
                        len(nodes_both),
                        ", ".join(nodes_both),
                    )

                    # LTE cells with mismatching params between FreqPrioNR 648672 and 647328
                    cell_col = resolve_column_case_insensitive(
                        df_freq_prio_nr,
                        [
                            "EUtranCellFDDId",
                            "EUtranCellId",
                            "EutranCellId",
                            "Cellname",
                            "CellId",
                            "LTECellId",
                        ],
                    )

                    mismatching_cells_details: list[str] = []

                    if cell_col:
                        # Work only with rows for old/new SSB and non-null LTE cell
                        cell_work = df_freq_prio_nr[[node_col, cell_col, freq_col, ratfreqprio_col]].copy()
                        cell_work[node_col] = cell_work[node_col].astype(str).str.strip()
                        cell_work[cell_col] = cell_work[cell_col].astype(str).str.strip()
                        cell_work[ratfreqprio_col] = cell_work[ratfreqprio_col].astype(str).str.strip().str.lower()
                        cell_work["_freq_int_"] = cell_work[freq_col].map(parse_int_frequency)

                        cell_work = cell_work.loc[
                            cell_work["_freq_int_"].isin([old_ssb, new_ssb]) & (cell_work[cell_col] != "")
                            ].copy()

                        # Build mapping {(node, cell): {freq: row}}
                        pairs: Dict[tuple, Dict[int, pd.Series]] = {}
                        for _, row in cell_work.iterrows():
                            key = (row[node_col], row[cell_col])
                            freq_val = row["_freq_int_"]
                            if freq_val not in (old_ssb, new_ssb):
                                continue
                            freq_map = pairs.setdefault(key, {})
                            if freq_val not in freq_map:
                                freq_map[freq_val] = row

                        # Compare params for cells that have both SSBs
                        for (node_id, lte_cell), freq_map in pairs.items():
                            if old_ssb in freq_map and new_ssb in freq_map:
                                row_old = freq_map[old_ssb]
                                row_new = freq_map[new_ssb]
                                diff_cols: list[str] = []

                                for col in cell_work.columns:
                                    if col in {node_col, cell_col, freq_col, "_freq_int_"}:
                                        continue
                                    val_old = row_old[col]
                                    val_new = row_new[col]

                                    if pd.isna(val_old) and pd.isna(val_new):
                                        continue
                                    if pd.isna(val_old) != pd.isna(val_new):
                                        diff_cols.append(col)
                                    elif str(val_old) != str(val_new):
                                        diff_cols.append(col)

                                if diff_cols:
                                    mismatching_cells_details.append(
                                        f"{node_id}/{lte_cell}: {', '.join(sorted(diff_cols))}"
                                    )

                    add_row(
                        "FreqPrioNR",
                        "ENDC Inconsistencies",
                        f"LTE cells with mismatching params between FreqPrioNR {old_ssb} and {new_ssb}",
                        len(mismatching_cells_details),
                        "; ".join(mismatching_cells_details),
                    )

                    # Keep only N77 rows (from FreqPrioNR table)
                    mask_n77 = work[freq_col].map(is_n77_from_string)
                    n77_work = work.loc[mask_n77].copy()

                    if not n77_work.empty:
                        # N77 nodes with RATFreqPrioId = "fwa" (from FreqPrioNR table)
                        mask_fwa = n77_work[ratfreqprio_col] == "fwa"
                        fwa_nodes = sorted(n77_work.loc[mask_fwa, node_col].astype(str).unique())

                        add_row(
                            "FreqPrioNR",
                            "ENDC Audit",
                            "NR nodes with RATFreqPrioId = 'fwa' in N77 band (from FreqPrioNR table)",
                            len(fwa_nodes),
                            ", ".join(fwa_nodes),
                        )

                        # N77 nodes with RATFreqPrioId = "publicsafety" (from FreqPrioNR table)
                        mask_publicsafety = n77_work[ratfreqprio_col] == "publicsafety"
                        publicsafety_nodes = sorted(n77_work.loc[mask_publicsafety, node_col].astype(str).unique())

                        add_row(
                            "FreqPrioNR",
                            "ENDC Audit",
                            "NR nodes with RATFreqPrioId = 'publicsafety' in N77 band (from FreqPrioNR table)",
                            len(publicsafety_nodes),
                            ", ".join(publicsafety_nodes),
                        )

                        # N77 nodes with any RATFreqPrioId different from "fwa" / "publicsafety" (from FreqPrioNR table)
                        mask_other = ~(mask_fwa | mask_publicsafety)
                        other_nodes = sorted(n77_work.loc[mask_other, node_col].astype(str).unique())

                        add_row(
                            "FreqPrioNR",
                            "ENDC Inconsistencies",
                            "NR nodes with RATFreqPrioId different from 'fwa'/'publicsafety' in N77 band (from FreqPrioNR table)",
                            len(other_nodes),
                            ", ".join(other_nodes),
                        )
                    else:
                        add_row(
                            "FreqPrioNR",
                            "ENDC Audit",
                            "FreqPrioNR table has no N77 rows (based on FreqPrioNRId)",
                            0,
                        )
                else:
                    add_row(
                        "FreqPrioNR",
                        "EndcProfileAudit",
                        "FreqPrioNR table present but NodeId/FreqPrioNRId/RATFreqPrioId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "FreqPrioNR",
                    "ENDC Audit",
                    "FreqPrioNR table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "FreqPrioNR",
                "ENDC Audit",
                "Error while checking FreqPrioNR",
                f"ERROR: {ex}",
            )

    # ----------------------------- CARDINALITY LIMITS -----------------------------
    def process_cardinalities():

        # Max 64 NRFrequency per node
        try:
            if df_nr_freq is not None and not df_nr_freq.empty:
                node_col = resolve_column_case_insensitive(df_nr_freq, ["NodeId"])
                if node_col:
                    counts = df_nr_freq[node_col].astype(str).value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0
                    limit = 64

                    # AUDIT: show the maximum observed value and the worst offenders
                    at_limit_or_above = counts[counts >= limit]
                    if at_limit_or_above.empty and max_count > 0:
                        at_limit_or_above = counts[counts == max_count]
                    extra_audit = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.items())

                    add_row(
                        "Cardinality NRFrequency",
                        "Cardinality Audit",
                        "Max NRFrequency definitions per node (limit 64)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: nodes exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality NRFrequency",
                        "Cardinality Inconsistencies",
                        "NR nodes with NRFrequency definitions at limit 64",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality NRFrequency",
                        "Cardinality Audit",
                        "NRFrequency per node (NodeId missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality NRFrequency",
                    "Cardinality Audit",
                    "NRFrequency per node",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality NRFrequency",
                "Cardinality Audit",
                "Error while checking NRFrequency cardinality",
                f"ERROR: {ex}",
            )

        # Max 16 NRFreqRelation per NR cell
        try:
            if df_nr_freq_rel is not None and not df_nr_freq_rel.empty:
                cell_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRCellCUId", "NRCellId", "CellId"])
                if cell_col:
                    counts = df_nr_freq_rel[cell_col].value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0
                    limit = 16

                    # AUDIT: show the maximum observed value and the worst offenders
                    at_limit_or_above = counts[counts >= limit]
                    if at_limit_or_above.empty and max_count > 0:
                        at_limit_or_above = counts[counts == max_count]
                    extra_audit = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.items())

                    add_row(
                        "Cardinality NRFreqRelation",
                        "Cardinality Audit",
                        "Max NRFreqRelation per NR cell (limit 16)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: cells exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality NRFreqRelation",
                        "Cardinality Inconsistencies",
                        "NR cells with NRFreqRelation count at limit 16",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality NRFreqRelation",
                        "Cardinality Audit",
                        "NRFreqRelation per cell (required cell column missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality NRFreqRelation",
                    "Cardinality Audit",
                    "NRFreqRelation per cell",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality NRFreqRelation",
                "Cardinality Audit",
                "Error while checking NRFreqRelation cardinality",
                f"ERROR: {ex}",
            )

        # Max 24 GUtranSyncSignalFrequency per node
        try:
            if df_gu_sync_signal_freq is not None and not df_gu_sync_signal_freq.empty:
                node_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["NodeId"])
                if node_col:
                    counts = df_gu_sync_signal_freq[node_col].astype(str).value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0
                    limit = 24

                    # AUDIT: show the maximum observed value and the worst offenders
                    at_limit_or_above = counts[counts >= limit]
                    if at_limit_or_above.empty and max_count > 0:
                        at_limit_or_above = counts[counts == max_count]
                    extra_audit = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.items())

                    add_row(
                        "Cardinality GUtranSyncSignalFrequency",
                        "Cardinality Audit",
                        "Max GUtranSyncSignalFrequency definitions per node (limit 24)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: nodes exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality GUtranSyncSignalFrequency",
                        "Cardinality Inconsistencies",
                        "LTE nodes with GUtranSyncSignalFrequency definitions at limit 24",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality GUtranSyncSignalFrequency",
                        "Cardinality Audit",
                        "GUtranSyncSignalFrequency per node (NodeId missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality GUtranSyncSignalFrequency",
                    "Cardinality Audit",
                    "GUtranSyncSignalFrequency per node",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality GUtranSyncSignalFrequency",
                "Cardinality Audit",
                "Error while checking GUtranSyncSignalFrequency cardinality",
                f"ERROR: {ex}",
            )

        # Max 16 GUtranFreqRelation per LTE cell
        try:
            if df_gu_freq_rel is not None and not df_gu_freq_rel.empty:
                cell_col_gu = resolve_column_case_insensitive(df_gu_freq_rel, ["EUtranCellFDDId", "EUtranCellId", "CellId", "GUCellId"])
                if cell_col_gu:
                    counts = df_gu_freq_rel[cell_col_gu].value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0
                    limit = 16

                    # AUDIT: show the maximum observed value and the worst offenders
                    at_limit_or_above = counts[counts >= limit]
                    if at_limit_or_above.empty and max_count > 0:
                        at_limit_or_above = counts[counts == max_count]
                    extra_audit = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.items())

                    add_row(
                        "Cardinality GUtranFreqRelation",
                        "Cardinality Audit",
                        "Max GUtranFreqRelation per LTE cell (limit 16)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: LTE cells exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality GUtranFreqRelation",
                        "Cardinality Inconsistencies",
                        "LTE cells with GUtranFreqRelation count at limit 16",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality GUtranFreqRelation",
                        "Cardinality Audit",
                        "GUtranFreqRelation per LTE cell (required cell column missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality GUtranFreqRelation",
                    "Cardinality Audit",
                    "GUtranFreqRelation per LTE cell",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality GUtranFreqRelation",
                "Cardinality Audit",
                "Error while checking GUtranFreqRelation cardinality",
                f"ERROR: {ex}",
            )


    # =======================================================================
    # ============================ MAIN CODE ================================
    # =======================================================================
    def main()-> tuple[DataFrame, DataFrame, DataFrame]:

        process_nr_freq()
        process_nr_freq_rel()
        process_nr_sector_carrier()
        process_nr_cell_du()
        process_nr_cell_relation()

        process_gu_sync_signal_freq()
        process_gu_freq_rel()
        process_gu_cell_relation()

        process_endc_distr_profile()
        process_freq_prio_nr()

        process_cardinalities()

        # If nothing was added, return at least an informational row
        if not rows:
            rows.append({
                "Category": "Info",
                "SubCategory": "Info",
                "Metric": "SummaryAudit",
                "Value": "No data available",
                "ExtraInfo": "",
            })

        # Build final DataFrame
        df = pd.DataFrame(rows)

        # Custom logical ordering for SummaryAudit (also drives PPT order)
        if not df.empty and all(col in df.columns for col in ["Category", "SubCategory", "Metric"]):
            # We now order only by (Category, SubCategory).
            # Inside each group, rows keep the insertion order.
            desired_order = [
                # NR Frequency NRCellDU
                ("NRCellDU", "NR Frequency Audit"),
                ("NRCellDU", "NR Frequency Inconsistencies"),

                # NR Frequency NRFrequency
                ("NRFrequency", "NR Frequency Audit"),
                ("NRFrequency", "NR Frequency Inconsistencies"),

                # NR Frequency NRFreqRelation
                ("NRFreqRelation", "NR Frequency Audit"),
                ("NRFreqRelation", "NR Frequency Inconsistencies"),

                # NR Frequency NRCellRelation
                ("NRCellRelation", "NR Frequency Audit"),
                ("NRCellRelation", "NR Frequency Inconsistencies"),

                # NR Frequency NRSectorCarrier
                ("NRSectorCarrier", "NR Frequency Audit"),
                ("NRSectorCarrier", "NR Frequency Inconsistencies"),

                # LTE Frequency GUtranSyncSignalFrequency
                ("GUtranSyncSignalFrequency", "LTE Frequency Audit"),
                ("GUtranSyncSignalFrequency", "LTE Frequency Inconsistencies"),

                # LTE Frequency GUtranSyncSignalFrequency
                ("GUtranFreqRelation", "LTE Frequency Audit"),
                ("GUtranFreqRelation", "LTE Frequency Inconsistencies"),

                # LTE Frequency GUtranCellRelation
                ("GUtranCellRelation", "LTE Frequency Audit"),
                ("GUtranCellRelation", "LTE Frequency Inconsistencies"),

                # EndcDistrProfile
                ("EndcDistrProfile", "ENDC Audit"),
                ("EndcDistrProfile", "ENDC Inconsistencies"),

                # FreqPrioNR
                ("FreqPrioNR", "ENDC Audit"),
                ("FreqPrioNR", "ENDC Inconsistencies"),

                # Cardinality NRFrequency
                ("Cardinality NRFrequency", "Cardinality Audit"),
                ("Cardinality NRFrequency", "Cardinality Inconsistencies"),

                # Cardinality NRFreqRelation
                ("Cardinality NRFreqRelation", "Cardinality Audit"),
                ("Cardinality NRFreqRelation", "Cardinality Inconsistencies"),

                # Cardinality GUtranSyncSignalFrequency
                ("Cardinality GUtranSyncSignalFrequency", "Cardinality Audit"),
                ("Cardinality GUtranSyncSignalFrequency", "Cardinality Inconsistencies"),

                # Cardinality GUtranFreqRelation
                ("Cardinality GUtranFreqRelation", "Cardinality Audit"),
                ("Cardinality GUtranFreqRelation", "Cardinality Inconsistencies"),

            ]

            # Map (Category, SubCategory) to an integer order
            order_map: Dict[tuple, int] = {key: idx for idx, key in enumerate(desired_order)}

            def order_row(r: pd.Series) -> int:
                """Return the desired order index for each row (or a large value if not explicitly listed)."""
                key = (
                    str(r.get("Category", "")),
                    str(r.get("SubCategory", "")),
                )
                return order_map.get(key, len(desired_order) + 100)

            df["__order__"] = df.apply(order_row, axis=1)
            df = (
                df.sort_values("__order__", kind="mergesort")  # stable: keeps insertion order inside each group
                .drop(columns=["__order__"])
                .reset_index(drop=True)
            )

        # Build Param Missmatching DataFrames (NR / LTE)
        if param_mismatch_rows_nr:
            df_param_mismatch_nr = pd.DataFrame(param_mismatch_rows_nr)
            for col_name in param_mismatch_columns_nr:
                if col_name not in df_param_mismatch_nr.columns:
                    df_param_mismatch_nr[col_name] = ""
            df_param_mismatch_nr = df_param_mismatch_nr[param_mismatch_columns_nr]
        else:
            df_param_mismatch_nr = pd.DataFrame(columns=param_mismatch_columns_nr)

        if param_mismatch_rows_gu:
            df_param_mismatch_gu = pd.DataFrame(param_mismatch_rows_gu)
            for col_name in param_mismatch_columns_gu:
                if col_name not in df_param_mismatch_gu.columns:
                    df_param_mismatch_gu[col_name] = ""
            df_param_mismatch_gu = df_param_mismatch_gu[param_mismatch_columns_gu]
        else:
            df_param_mismatch_gu = pd.DataFrame(columns=param_mismatch_columns_gu)

        return df, df_param_mismatch_nr, df_param_mismatch_gu

    # =======================================================================
    # ========================= END OF MAIN CODE ============================
    # =======================================================================

    df_summary, df_param_mismatch_nr, df_param_mismatch_gu = main()
    return df_summary, df_param_mismatch_nr, df_param_mismatch_gu

