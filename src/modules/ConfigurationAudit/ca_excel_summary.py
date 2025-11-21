# -*- coding: utf-8 -*-

from typing import List, Dict
import pandas as pd

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency, is_n77_from_string, extract_sync_frequencies


# =====================================================================
#              GENERIC HELPERS (columns, frequencies, pivots)
# =====================================================================


# =====================================================================
#                        SUMMARY AUDIT BUILDER
# =====================================================================

def build_summary_audit(
    df_nr_cell_du: pd.DataFrame,
    df_nr_freq: pd.DataFrame,
    df_nr_freq_rel: pd.DataFrame,
    df_freq_prio_nr: pd.DataFrame,
    df_gu_sync_signal_freq: pd.DataFrame,
    df_gu_freq_rel: pd.DataFrame,
    df_nr_sector_carrier: pd.DataFrame,
    df_endc_distr_profile: pd.DataFrame,
    old_ssb: int,
    new_ssb: int,
    n77b_ssb: int,
    allowed_n77_ssb_pre,
    allowed_n77_arfcn_pre,
    allowed_n77_ssb_post,
    allowed_n77_arfcn_post,
) -> pd.DataFrame:
    """
    Build a synthetic 'SummaryAudit' table with high-level checks:

      - N77 detection on NRCellDU and NRSectorCarrier.
      - NR/LTE nodes where specific ARFCNs (old_arfcn / new_arfcn) are defined.
      - NR/LTE nodes with ARFCNs not in {old_arfcn, new_arfcn}.
      - Cardinality limits per cell and per node.
      - EndcDistrProfile gUtranFreqRef values.

    Notes:
      - N77 cells are those with ARFCN/SSB in range [646600-660000].
      - This function is best-effort and should not raise exceptions; any error is
        represented as a row in the resulting dataframe.
    """

    allowed_n77_ssb_pre_set = {int(v) for v in (allowed_n77_ssb_pre or [])}
    allowed_n77_arfcn_pre_set = {int(v) for v in (allowed_n77_arfcn_pre or [])}
    allowed_n77_ssb_post_set = {int(v) for v in (allowed_n77_ssb_post or [])}
    allowed_n77_arfcn_post_set = {int(v) for v in (allowed_n77_arfcn_post or [])}

    rows: List[Dict[str, object]] = []

    def is_not_old_not_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq not in (old_ssb, new_ssb)

    def series_only_not_old_not_new(series) -> bool:
        return all(is_not_old_not_new(v) for v in series)

    def is_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == new_ssb

    def is_old(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == old_ssb
    
    def has_value(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq is not None

    def is_n77_ssb_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_pre_set if freq is not None else False

    def is_n77_ssb_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_postset if freq is not None else False

    def is_n77_arfcn_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_pre_set if freq is not None else False

    def is_n77_arfcn_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_post_set if freq is not None else False

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

    # ----------------------------- NRFrequency (OLD/NEW ARFCN on N77 rows) -----------------------------
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

                        # NR Frequency Audit: ALL nodes (not only N77) with any non-empty ARFCN (from NRFrequency table)
                        all_nodes_with_freq = sorted(df_nr_freq.loc[df_nr_freq[arfcn_col].map(has_value), node_col].astype(str).unique())
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with ARFCN defined (from NRFrequency table)",
                            len(all_nodes_with_freq),
                            ", ".join(all_nodes_with_freq),
                        )

                        # NR Frequency Audit: NR nodes with the old SSB (from NRFrequency table)
                        old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with the old SSB ({old_ssb}) (from NRFrequency table)",
                            len(old_nodes),
                            ", ".join(old_nodes),
                        )

                        # NR Frequency Audit: NR nodes with the new SSB (from NRFrequency table)
                        new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with the new SSB ({new_ssb}) (from NRFrequency table)",
                            len(new_nodes),
                            ", ".join(new_nodes),
                        )

                        # NEW: check nodes that have old_ssb and also new_ssb vs those missing the new_arfcn (from NRFrequency table)
                        old_set = set(old_nodes)
                        new_set = set(new_nodes)

                        nodes_old_and_new = sorted(old_set & new_set)
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with both, the old SSB ({old_ssb}) and the new SSB ({new_ssb}) (from NRFrequency table)",
                            len(nodes_old_and_new),
                            ", ".join(nodes_old_and_new),
                        )

                        nodes_old_without_new = sorted(old_set - new_set)
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFrequency",
                            f"NR nodes with the old SSB ({old_ssb}) but without the new SSB ({new_ssb}) (from NRFrequency table)",
                            len(nodes_old_without_new),
                            ", ".join(nodes_old_without_new),
                        )

                        # NR Frequency Inconsistencies: NR nodes with the N77 ARFCN not in (old_freq, new_freq) (from NRFrequency table)
                        not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFrequency",
                            f"NR nodes with the N77 ARFCN not in ({old_ssb}, {new_ssb}) (from NRFrequency table)",
                            len(not_old_not_new_nodes),
                            ", ".join(not_old_not_new_nodes),
                        )
                    else:
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            "NRFrequency table has no N77 rows",
                            0,
                        )
                else:
                    add_row(
                        "NR Frequency Audit",
                        "NRFrequency",
                        "NRFrequency table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NR Frequency Audit",
                    "NRFrequency",
                    "NRFrequency table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NR Frequency Audit",
                "NRFrequency",
                "Error while checking NRFrequency",
                f"ERROR: {ex}",
            )

    # ----------------------------- NRFreqRelation (OLD/NEW ARFCN on NR rows) -----------------------------
    def process_nr_freq_rel():
        try:
            if df_nr_freq_rel is not None and not df_nr_freq_rel.empty:
                node_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRFreqRelationId"])

                if node_col and arfcn_col:
                    work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                    if not n77_work.empty:
                        grouped = n77_work.groupby(node_col)[arfcn_col]

                        # NR Frequency Audit: NR nodes with the old SSB (from NRFreqRelation table)
                        old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFreqRelation",
                            f"NR nodes with the old SSB ({old_ssb}) (from NRFreqRelation table)",
                            len(old_nodes),
                            ", ".join(old_nodes),
                        )

                        # NR Frequency Audit: NR nodes with the new SSB (from NRFreqRelation table)
                        new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFreqRelation",
                            f"NR nodes with the new SSB ({new_ssb}) (from NRFreqRelation table)",
                            len(new_nodes),
                            ", ".join(new_nodes),
                        )

                        # NEW: node-level check old_arfcn vs new_arfcn presence
                        old_set = set(old_nodes)
                        new_set = set(new_nodes)

                        nodes_old_and_new = sorted(old_set & new_set)
                        add_row(
                            "NR Frequency Audit",
                            "NRFreqRelation",
                            f"NR nodes with both, the old SSB ({old_ssb}) and the new ARFCN ({new_ssb}) (from NRFreqRelation table)",
                            len(nodes_old_and_new),
                            ", ".join(nodes_old_and_new),
                        )

                        nodes_old_without_new = sorted(old_set - new_set)
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFreqRelation",
                            f"NR nodes with the old SSB ({old_ssb}) but without the new ARFCN ({new_ssb}) (from NRFreqRelation table)",
                            len(nodes_old_without_new),
                            ", ".join(nodes_old_without_new),
                        )

                        # NR Frequency Inconsistencies: NR nodes with the SSB not in ({old_arfcn}, {new_arfcn}) (from NRFreqRelation table)
                        not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFreqRelation",
                            f"NR nodes with the SSB not in ({old_ssb}, {new_ssb}) (from NRFreqRelation table)",
                            len(not_old_not_new_nodes),
                            ", ".join(not_old_not_new_nodes),
                        )

                        # NEW: nodes where NRFreqRelationId contains new ARFCN but has extra characters (e.g. 'auto_647328')
                        post_freq_str = str(new_ssb)
                        pattern_work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                        pattern_work[node_col] = pattern_work[node_col].astype(str)
                        pattern_work[arfcn_col] = pattern_work[arfcn_col].astype(str)

                        mask_contains_post = pattern_work[arfcn_col].str.contains(post_freq_str, na=False)

                        def _has_same_int_and_extra(value: object) -> bool:
                            s = str(value).strip()
                            int_val = parse_int_frequency(s)
                            return int_val == new_ssb and s != post_freq_str

                        mask_bad_pattern = mask_contains_post & pattern_work[arfcn_col].map(_has_same_int_and_extra)

                        bad_pattern_nodes = sorted(pattern_work.loc[mask_bad_pattern, node_col].astype(str).unique())

                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFreqRelation",
                            f"NR nodes with Auto-created NRFreqRelationId to new SSB ({new_ssb}) but not following VZ naming convention (e.g. with extra characters: 'auto_{new_ssb}')",
                            len(bad_pattern_nodes),
                            ", ".join(bad_pattern_nodes),
                        )

                        # NEW: cell-level check on NRCellCUId: presence and parameter equality (except some columns)
                        cell_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRCellCUId", "NRCellId", "CellId"])
                        if cell_col:
                            full = df_nr_freq_rel.copy()
                            full[node_col] = full[node_col].astype(str)
                            full[cell_col] = full[cell_col].astype(str)

                            # Restrict to N77 rows (based on ARFCN inside NRFreqRelationId)
                            mask_n77_full = full[arfcn_col].map(is_n77_from_string)
                            full_n77 = full.loc[mask_n77_full].copy()
                            full_n77["_arfcn_int_"] = full_n77[arfcn_col].map(parse_int_frequency)

                            old_mask = full_n77["_arfcn_int_"] == old_ssb
                            new_mask = full_n77["_arfcn_int_"] == new_ssb

                            cells_with_old = set(full_n77.loc[old_mask, cell_col].astype(str))
                            cells_with_new = set(full_n77.loc[new_mask, cell_col].astype(str))

                            cells_both = sorted(cells_with_old & cells_with_new)
                            cells_old_without_new = sorted(cells_with_old - cells_with_new)

                            add_row(
                                "NR Frequency Audit",
                                "NRFreqRelation",
                                f"NR cells with the old ARFCN ({old_ssb}) and the new ARFCN ({new_ssb}) (from NRFreqRelation table)",
                                len(cells_both),
                                ", ".join(cells_both),
                            )

                            add_row(
                                "NR Frequency Inconsistencies",
                                "NRFreqRelation",
                                f"NR cells with the old ARFCN ({old_ssb}) but without new ARFCN ({new_ssb}) (from NRFreqRelation table)",
                                len(cells_old_without_new),
                                ", ".join(cells_old_without_new),
                            )

                            # Parameter equality check (ignoring ID/reference columns)
                            cols_to_ignore = {arfcn_col}
                            for name in full_n77.columns:
                                lname = str(name).lower()
                                if lname in {"nrfreqrelationid", "nrfrequencyref", "reservedby"}:
                                    cols_to_ignore.add(name)

                            bad_cells_params = []
                            for cell_id in cells_both:
                                cell_rows = full_n77.loc[full_n77[cell_col].astype(str) == cell_id].copy()
                                old_rows = cell_rows.loc[cell_rows["_arfcn_int_"] == old_ssb]
                                new_rows = cell_rows.loc[cell_rows["_arfcn_int_"] == new_ssb]

                                if old_rows.empty or new_rows.empty:
                                    continue

                                old_clean = old_rows.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True)
                                new_clean = new_rows.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True)

                                old_clean = old_clean.reindex(sorted(old_clean.columns), axis=1)
                                new_clean = new_clean.reindex(sorted(new_clean.columns), axis=1)

                                if not old_clean.equals(new_clean):
                                    bad_cells_params.append(str(cell_id))

                            bad_cells_params = sorted(set(bad_cells_params))

                            add_row(
                                "NR Frequency Inconsistencies",
                                "NRFreqRelation",
                                f"NR cells with mismatching params between old ARFCN ({old_ssb}) and the new ARFCN ({new_ssb}) (from NRFreqRelation table)",
                                len(bad_cells_params),
                                ", ".join(bad_cells_params),
                            )
                        else:
                            add_row(
                                "NR Frequency Audit",
                                "NRFreqRelation",
                                "NRFreqRelation cell-level check skipped (NRCellCUId/NRCellId/CellId missing)",
                                "N/A",
                            )
                    else:
                        add_row(
                            "NR Frequency Audit",
                            "NRFreqRelation",
                            "NRFreqRelation table has no N77 rows",
                            0,
                        )
                else:
                    add_row(
                        "NR Frequency Audit",
                        "NRFreqRelation",
                        "NRFreqRelation table present but ARFCN/NodeId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NR Frequency Audit",
                    "NRFreqRelation",
                    "NRFreqRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NR Frequency Audit",
                "NRFreqRelation",
                "Error while checking NRFreqRelation",
                f"ERROR: {ex}",
            )

    # ----------------------------- NRSectorCarrier (N77 + allowed ARFCN) -----------------------------
    def process_nr_sector_carrier():
        try:
            if df_nr_sector_carrier is not None and not df_nr_sector_carrier.empty:
                node_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["arfcnDL"])

                if node_col and arfcn_col:
                    work = df_nr_sector_carrier[[node_col, arfcn_col]].copy()

                    work[node_col] = work[node_col].astype(str).str.strip()

                    # N77 nodes = those having at least one ARFCN in N77 band (646600-660000)
                    mask_n77 = work[arfcn_col].map(is_n77_from_string)
                    n77_rows = work.loc[mask_n77].copy()

                    # NR Frequency Audit: NR nodes with ARFCN in N77 band (646600-660000) (from NRSectorCarrier table)
                    n77_nodes = sorted(n77_rows[node_col].astype(str).unique())

                    add_row(
                        "NR Frequency Audit",
                        "NRSectorCarrier",
                        "NR nodes with ARFCN in N77 band (646600-660000) (from NRSectorCarrier table)",
                        len(n77_nodes),
                        ", ".join(n77_nodes),
                    )

                    # NR nodes with N77 ARFCN in Pre Retune-allowed list (from NRSectorCarrier table)
                    if allowed_n77_arfcn_pre_set:
                        mask_pre = n77_rows[arfcn_col].map(is_n77_arfcn_pre_allowed)
                        pre_nodes = sorted(n77_rows.loc[mask_pre, node_col].astype(str).unique())

                        add_row(
                            "NR Frequency Audit",
                            "NRSectorCarrier",
                            "NR nodes with N77 ARFCN in Pre-Retune allowed list (from NRSectorCarrier table)",
                            len(pre_nodes),
                            ", ".join(pre_nodes),
                        )

                    # NR nodes with N77 ARFCN in Post-Retune allowed list (from NRSectorCarrier table)
                    if allowed_n77_arfcn_post_set:
                        mask_post = n77_rows[arfcn_col].map(is_n77_arfcn_post_allowed)
                        post_nodes = sorted(n77_rows.loc[mask_post, node_col].astype(str).unique())

                        add_row(
                            "NR Frequency Audit",
                            "NRSectorCarrier",
                            "NR nodes with N77 ARFCN in Post-Retune allowed list (from NRSectorCarrier table)",
                            len(post_nodes),
                            ", ".join(post_nodes),
                        )

                    # NR Frequency Inconsistencies: NR ARFCN not in pre nor post allowed lists
                    if allowed_n77_arfcn_pre_set or allowed_n77_arfcn_post_set:
                        allowed_union = set(allowed_n77_arfcn_pre_set) | set(allowed_n77_arfcn_post_set)

                        def _is_not_in_union(v: object) -> bool:
                            freq = parse_int_frequency(v)
                            return freq is not None and freq not in allowed_union

                        bad_rows = n77_rows.loc[n77_rows[arfcn_col].map(_is_not_in_union)]

                        # Unique nodes with at least one ARFCN not in pre/post allowed lists
                        bad_nodes = sorted(bad_rows[node_col].astype(str).unique())

                        # Build a unique (NodeId, ARFCN) list to avoid duplicated lines in ExtraInfo
                        unique_pairs = sorted(
                            {(str(r[node_col]).strip(), str(r[arfcn_col]).strip()) for _, r in bad_rows.iterrows()}
                        )

                        extra = "; ".join(f"{node}: {arfcn}" for node, arfcn in unique_pairs)

                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRSectorCarrier",
                            "NR nodes with ARFCN not in Pre/Post Retune allowed lists (from NRSectorCarrier table)",
                            len(bad_nodes),
                            extra,
                        )
                    else:
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRSectorCarrier",
                            "NR nodes with ARFCN not in Pre/Post Retune allowed lists (no pre/post allowed lists configured) (from NRSectorCarrier table)",
                            "N/A",
                        )
                else:
                    add_row(
                        "NR Frequency Audit",
                        "NRSectorCarrier",
                        "NRSectorCarrier table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NR Frequency Audit",
                    "NRSectorCarrier",
                    "NRSectorCarrier table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NR Frequency Audit",
                "NRSectorCarrier",
                "Error while checking NRSectorCarrier",
                f"ERROR: {ex}",
            )

    # ----------------------------- NRCellDU (N77 detection) -----------------------------
    def process_nr_cell_du():
        try:
            if df_nr_cell_du is not None and not df_nr_cell_du.empty:
                node_col = resolve_column_case_insensitive(df_nr_cell_du, ["NodeId"])
                ssb_col = resolve_column_case_insensitive(df_nr_cell_du, ["ssbFrequency"])

                if node_col and ssb_col:
                    work = df_nr_cell_du[[node_col, ssb_col]].copy()

                    # Ensure NodeId is treated consistently
                    work[node_col] = work[node_col].astype(str).str.strip()

                    # Filter rows where SSB starts with '6' (N77)
                    mask_n77 = work[ssb_col].map(is_n77_from_string)

                    # Extract unique NodeId values
                    unique_nodes = sorted(work.loc[mask_n77, node_col].unique())

                    add_row(
                        "NR Frequency Audit",
                        "NRCellDU",
                        "NR nodes with SSB in N77 band (646600-660000) (from NRCellDU table)",
                        len(unique_nodes),
                        ", ".join(unique_nodes),
                    )
                else:
                    add_row(
                        "NR Frequency Audit",
                        "NRCellDU",
                        "NRCellDU table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NR Frequency Audit",
                    "NRCellDU",
                    "NRCellDU table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NR Frequency Audit",
                "NRCellDU",
                "Error while checking NRCellDU",
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

                    # Keep only N77 rows (from FreqPrioNR table)
                    mask_n77 = work[freq_col].map(is_n77_from_string)
                    n77_work = work.loc[mask_n77].copy()

                    if not n77_work.empty:
                        # N77 nodes with RATFreqPrioId = "fwa" (from FreqPrioNR table)
                        mask_fwa = n77_work[ratfreqprio_col] == "fwa"
                        fwa_nodes = sorted(n77_work.loc[mask_fwa, node_col].astype(str).unique())

                        add_row(
                            "NR Frequency Audit",
                            "FreqPrioNR",
                            "NR nodes with RATFreqPrioId = 'fwa' in N77 band (from FreqPrioNR table)",
                            len(fwa_nodes),
                            ", ".join(fwa_nodes),
                        )

                        # N77 nodes with RATFreqPrioId = "publicsafety" (from FreqPrioNR table)
                        mask_publicsafety = n77_work[ratfreqprio_col] == "publicsafety"
                        publicsafety_nodes = sorted(n77_work.loc[mask_publicsafety, node_col].astype(str).unique())

                        add_row(
                            "NR Frequency Audit",
                            "FreqPrioNR",
                            "NR nodes with RATFreqPrioId = 'publicsafety' in N77 band (from FreqPrioNR table)",
                            len(publicsafety_nodes),
                            ", ".join(publicsafety_nodes),
                        )

                        # N77 nodes with any RATFreqPrioId different from "fwa" / "publicsafety" (from FreqPrioNR table)
                        mask_other = ~(mask_fwa | mask_publicsafety)
                        other_nodes = sorted(n77_work.loc[mask_other, node_col].astype(str).unique())

                        add_row(
                            "NR Frequency Inconsistencies",
                            "FreqPrioNR",
                            "NR nodes with RATFreqPrioId different from 'fwa'/'publicsafety' in N77 band (from FreqPrioNR table)",
                            len(other_nodes),
                            ", ".join(other_nodes),
                        )
                    else:
                        add_row(
                            "NR Frequency Audit",
                            "FreqPrioNR",
                            "FreqPrioNR table has no N77 rows (based on FreqPrioNRId)",
                            0,
                        )
                else:
                    add_row(
                        "NR Frequency Audit",
                        "FreqPrioNR",
                        "FreqPrioNR table present but NodeId/FreqPrioNRId/RATFreqPrioId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "NR Frequency Audit",
                    "FreqPrioNR",
                    "FreqPrioNR table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "NR Frequency Audit",
                "FreqPrioNR",
                "Error while checking FreqPrioNR",
                f"ERROR: {ex}",
            )

    # ----------------------------- LTE GUtranSyncSignalFrequency (OLD/NEW ARFCN) -----------------------------
    def process_gu_sync_signal_freq():
        try:
            if df_gu_sync_signal_freq is not None and not df_gu_sync_signal_freq.empty:
                node_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["arfcn", "arfcnDL"])

                if node_col and arfcn_col:
                    work = df_gu_sync_signal_freq[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    # LTE nodes with any GUtranSyncSignalFrequency defined (from GUtranSyncSignalFrequency table)
                    all_nodes_with_freq = sorted(work.loc[work[arfcn_col].map(has_value), node_col].astype(str).unique())
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        "LTE nodes with GUtranSyncSignalFrequency defined:",
                        len(all_nodes_with_freq),
                        ", ".join(all_nodes_with_freq),
                    )

                    grouped = work.groupby(node_col)[arfcn_col]

                    # LTE Frequency Audit: LTE nodes with the old SSB (from GUtranSyncSignalFrequency table)
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the old SSB ({old_ssb}) (from GUtranSyncSignalFrequency table)",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # LTE Frequency Audit: LTE nodes with the new SSB (from GUtranSyncSignalFrequency table)
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the new SSB ({new_ssb}) (from GUtranSyncSignalFrequency table)",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # NEW: nodes with old_ssb that should also have new_ssb
                    old_set = set(old_nodes)
                    new_set = set(new_nodes)

                    nodes_old_and_new = sorted(old_set & new_set)
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with both, the old SSB ({old_ssb}) and the new SSB ({new_ssb}) (from GUtranSyncSignalFrequency table)",
                        len(nodes_old_and_new),
                        ", ".join(nodes_old_and_new),
                    )

                    nodes_old_without_new = sorted(old_set - new_set)
                    add_row(
                        "LTE Frequency Inconsistences",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the old SSB ({old_ssb}) but without the new SSB ({new_ssb}) (from GUtranSyncSignalFrequency table)",
                        len(nodes_old_without_new),
                        ", ".join(nodes_old_without_new),
                    )

                    # LTE Frequency Inconsistences: LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) (from GUtranSyncSignalFrequency table)
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row(
                        "LTE Frequency Inconsistences",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the SSB not in ({old_ssb}, {new_ssb}) (from GUtranSyncSignalFrequency table)",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )
                else:
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        "GUtranSyncSignalFrequency table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "LTE Frequency Audit",
                    "GUtranSyncSignalFrequency",
                    "GUtranSyncSignalFrequency table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "LTE Frequency Audit",
                "GUtranSyncSignalFrequency",
                "Error while checking GUtranSyncSignalFrequency",
                f"ERROR: {ex}",
            )

    # ----------------------------- LTE GUtranFreqRelation (OLD/NEW ARFCN) -----------------------------
    def process_gu_freq_rel():
        try:
            if df_gu_freq_rel is not None and not df_gu_freq_rel.empty:
                node_col = resolve_column_case_insensitive(df_gu_freq_rel, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_gu_freq_rel, ["GUtranFreqRelationId", "gUtranFreqRelationId"])

                if node_col and arfcn_col:
                    work = df_gu_freq_rel[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    grouped = work.groupby(node_col)[arfcn_col]

                    # LTE Frequency Audit: LTE nodes with the old SSB (from GUtranFreqRelation table)
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranFreqRelation",
                        f"LTE nodes with the old SSB ({old_ssb}) (from GUtranFreqRelation table)",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # LTE Frequency Audit: LTE nodes with the new SSB (from GUtranFreqRelation table)
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranFreqRelation",
                        f"LTE nodes with the new SSB ({new_ssb}) (from GUtranFreqRelation table)",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # NEW: node-level check old_arfcn vs new_arfcn presence
                    old_set = set(old_nodes)
                    new_set = set(new_nodes)

                    nodes_old_and_new = sorted(old_set & new_set)
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranFreqRelation",
                        f"LTE nodes with both, the old SSB ({old_ssb}) and the new SSB ({new_ssb}) (from GUtranFreqRelation table)",
                        len(nodes_old_and_new),
                        ", ".join(nodes_old_and_new),
                    )

                    nodes_old_without_new = sorted(old_set - new_set)
                    add_row(
                        "LTE Frequency Inconsistences",
                        "GUtranFreqRelation",
                        f"LTE nodes with the old SSB ({old_ssb}) but without the new SSB ({new_ssb}) (from GUtranFreqRelation table)",
                        len(nodes_old_without_new),
                        ", ".join(nodes_old_without_new),
                    )

                    # LTE Frequency Inconsistences: LTE nodes with the SSB not in ({old_ssb}, {new_ssb}) (from GUtranFreqRelation table)
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row(
                        "LTE Frequency Inconsistences",
                        "GUtranFreqRelation",
                        f"LTE nodes with the SSB not in ({old_ssb}, {new_ssb}) (from GUtranFreqRelation table)",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )

                    # NEW: LTE nodes whose GUtranFreqRelationId containing post SSB do not follow pattern new_arfcn-*-*-*-* (3 hyphens)
                    post_freq_str = str(new_ssb)
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
                        "LTE Frequency Inconsistences",
                        "GUtranFreqRelation",
                        f"LTE nodes with Auto-created GUtranFreqRelationId to new SSB ({new_ssb}) but not following VZ naming convention ({new_ssb}-30-20-0-1) (from GUtranFreqRelation table)",
                        len(bad_nodes),
                        extra_bad,
                    )

                    # NEW: LTE cell-level check using GUtranFreqRelationId strings old_arfcn-30-20-0-1 / new_arfcn-30-20-0-1 (from GUtranFreqRelation table)
                    cell_col_gu = resolve_column_case_insensitive(df_gu_freq_rel, ["EUtranCellFDDId", "EUtranCellId", "CellId", "GUCellId"])
                    expected_old_rel_id = f"{old_ssb}-30-20-0-1"
                    expected_new_rel_id = f"{new_ssb}-30-20-0-1"

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
                            "LTE Frequency Audit",
                            "GUtranFreqRelation",
                            f"LTE cells with GUtranFreqRelationId {expected_old_rel_id} and {expected_new_rel_id} (from GUtranFreqRelation table)",
                            len(cells_both),
                            ", ".join(cells_both),
                        )

                        add_row(
                            "LTE Frequency Inconsistences",
                            "GUtranFreqRelation",
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

                            old_clean = old_clean.reindex(sorted(old_clean.columns), axis=1)
                            new_clean = new_clean.reindex(sorted(new_clean.columns), axis=1)

                            if not old_clean.equals(new_clean):
                                bad_cells_params.append(str(cell_id))

                        bad_cells_params = sorted(set(bad_cells_params))

                        add_row(
                            "LTE Frequency Inconsistences",
                            "GUtranFreqRelation",
                            f"LTE cells with mismatching params between GUtranFreqRelationId {expected_old_rel_id} and {expected_new_rel_id} (from GUtranFreqRelation table)",
                            len(bad_cells_params),
                            ", ".join(bad_cells_params),
                        )
                    else:
                        add_row(
                            "LTE Frequency Audit",
                            "GUtranFreqRelation",
                            "GUtranFreqRelation cell-level check skipped (EUtranCellFDDId/EUtranCellId/CellId/GUCellId missing)",
                            "N/A",
                        )
                else:
                    add_row(
                        "LTE Frequency Audit",
                        "GUtranFreqRelation",
                        "GUtranFreqRelation table present but ARFCN/NodeId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "LTE Frequency Audit",
                    "GUtranFreqRelation",
                    "GUtranFreqRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "LTE Frequency Audit",
                "GUtranFreqRelation",
                "Error while checking GUtranFreqRelation",
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
                    expected_old = str(old_ssb)
                    expected_new = str(new_ssb)
                    expected_n77b = str(n77b_ssb)

                    # Nodes with GUtranSyncSignalFrequency containing old_arfcn and n77b_ssb (from EndcDistrProfile table)
                    mask_old_pair = freq_sets.map(
                        lambda s: (expected_old in s) and (expected_n77b in s)
                    )
                    old_nodes = sorted(
                        work.loc[mask_old_pair, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile Audit",
                        "EndcDistrProfile",
                        f"Nodes with gUtranFreqRef containing {old_ssb} and {n77b_ssb} (from EndcDistrProfile table)",
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
                        "EndcDistrProfile Audit",
                        "EndcDistrProfile",
                        f"Nodes with gUtranFreqRef containing {new_ssb} and {n77b_ssb} (from EndcDistrProfile table)",
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
                        "EndcDistrProfile Inconsistencies",
                        "EndcDistrProfile",
                        f"Nodes with gUtranFreqRef not containing ({old_ssb} or {new_ssb}) together with {n77b_ssb} (from EndcDistrProfile table)",
                        len(bad_nodes),
                        ", ".join(bad_nodes),
                    )
                else:
                    add_row(
                        "EndcDistrProfile Audit",
                        "EndcDistrProfile",
                        "EndcDistrProfile table present but NodeId/gUtranFreqRef missing",
                        "N/A",
                    )
            else:
                add_row(
                    "EndcDistrProfile Audit",
                    "EndcDistrProfile",
                    "EndcDistrProfile table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "EndcDistrProfile Audit",
                "EndcDistrProfile",
                "Error while checking EndcDistrProfile gUtranFreqRef",
                f"ERROR: {ex}",
            )

    # ----------------------------- CARDINALITY LIMITS -----------------------------
    def process_cardinalities():
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
                        "Cardinality Audit",
                        "Cardinality",
                        "Max NRFreqRelation per NR cell (limit 16)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: cells exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "NR cells with NRFreqRelation count at limit 16",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "NRFreqRelation per cell (required cell column missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality Audit",
                    "Cardinality",
                    "NRFreqRelation per cell",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality Audit",
                "Cardinality",
                "Error while checking NRFreqRelation cardinality",
                f"ERROR: {ex}",
            )

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
                        "Cardinality Audit",
                        "Cardinality",
                        "Max NRFrequency definitions per node (limit 64)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: nodes exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "NR nodes with NRFrequency definitions at limit 64",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "NRFrequency per node (NodeId missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality Audit",
                    "Cardinality",
                    "NRFrequency per node",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality Audit",
                "Cardinality",
                "Error while checking NRFrequency cardinality",
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
                        "Cardinality Audit",
                        "Cardinality",
                        "Max GUtranFreqRelation per LTE cell (limit 16)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: LTE cells exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "LTE cells with GUtranFreqRelation count at limit 16",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "GUtranFreqRelation per LTE cell (required cell column missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality Audit",
                    "Cardinality",
                    "GUtranFreqRelation per LTE cell",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality Audit",
                "Cardinality",
                "Error while checking GUtranFreqRelation cardinality",
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
                        "Cardinality Audit",
                        "Cardinality",
                        "Max GUtranSyncSignalFrequency definitions per node (limit 24)",
                        max_count,
                        extra_audit,
                    )

                    # INCONSISTENCIES: nodes exactly at the configured limit
                    at_limit = counts[counts == limit]
                    extra_incons = "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit.items())

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "LTE nodes with GUtranSyncSignalFrequency definitions at limit 24",
                        int(at_limit.size),
                        extra_incons,
                    )
                else:
                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "GUtranSyncSignalFrequency per node (NodeId missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "Cardinality Audit",
                    "Cardinality",
                    "GUtranSyncSignalFrequency per node",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "Cardinality Audit",
                "Cardinality",
                "Error while checking GUtranSyncSignalFrequency cardinality",
                f"ERROR: {ex}",
            )

    # =======================================================================
    # ============================ MAIN CODE ================================
    # =======================================================================
    def main()-> pd.DataFrame:

        process_nr_freq()
        process_nr_freq_rel()
        process_nr_cell_du()
        process_nr_sector_carrier()
        process_freq_prio_nr()

        process_gu_sync_signal_freq()
        process_gu_freq_rel()

        process_endc_distr_profile()
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
                # NR Frequency Audit
                ("NR Frequency Audit", "NRFrequency"),
                ("NR Frequency Audit", "NRFreqRelation"),
                ("NR Frequency Audit", "NRSectorCarrier"),
                ("NR Frequency Audit", "NRCellDU"),
                ("NR Frequency Audit", "FreqPrioNR"),

                # NR Frequency Inconsistencies
                ("NR Frequency Inconsistencies", "NRFrequency"),
                ("NR Frequency Inconsistencies", "NRFreqRelation"),
                ("NR Frequency Inconsistencies", "NRSectorCarrier"),
                ("NR Frequency Inconsistencies", "FreqPrioNR"),

                # LTE Frequency Audit
                ("LTE Frequency Audit", "GUtranSyncSignalFrequency"),
                ("LTE Frequency Audit", "GUtranFreqRelation"),

                # LTE Frequency Inconsistences
                ("LTE Frequency Inconsistences", "GUtranSyncSignalFrequency"),
                ("LTE Frequency Inconsistences", "GUtranFreqRelation"),

                # EndcDistrProfile
                ("EndcDistrProfile Audit", "EndcDistrProfile"),
                ("EndcDistrProfile Inconsistencies", "EndcDistrProfile"),

                # Cardinality
                ("Cardinality Audit", "Cardinality"),
                ("Cardinality Inconsistencies", "Cardinality"),
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

        return df
    # =======================================================================
    # ========================= END OF MAIN CODE ============================
    # =======================================================================

    df = main()
    return df