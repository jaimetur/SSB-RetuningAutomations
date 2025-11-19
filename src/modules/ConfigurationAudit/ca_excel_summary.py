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
    df_gu_sync_signal_freq: pd.DataFrame,
    df_gu_freq_rel: pd.DataFrame,
    df_nr_sector_carrier: pd.DataFrame,
    df_endc_distr_profile: pd.DataFrame,
    old_arfcn: int,
    new_arfcn: int,
    n77b_ssb: int,
    allowed_n77_ssb,
    allowed_n77_arfcn,
) -> pd.DataFrame:
    """
    Build a synthetic 'SummaryAudit' table with high-level checks:

      - N77 detection on NRCellDU and NRSectorCarrier.
      - NR/LTE nodes where specific ARFCNs (old_arfcn / new_arfcn) are defined.
      - NR/LTE nodes with ARFCNs not in {old_arfcn, new_arfcn}.
      - Cardinality limits per cell and per node.
      - EndcDistrProfile gUtranFreqRef values.

    Notes:
      - N77 cells are approximated as those with ARFCN/SSB text starting with '6'.
      - This function is best-effort and should not raise exceptions; any error is
        represented as a row in the resulting dataframe.
    """

    # old_arfcn = int(old_arfcn)
    # new_arfcn = int(new_arfcn)
    # n77b_ssb = int(n77b_ssb)

    allowed_n77_ssb_set = {int(v) for v in (allowed_n77_ssb or [])}
    allowed_n77_arfcn_set = {int(v) for v in (allowed_n77_arfcn or [])}

    rows: List[Dict[str, object]] = []

    def is_not_old_not_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq not in (old_arfcn, new_arfcn)

    def series_only_not_old_not_new(series) -> bool:
        return all(is_not_old_not_new(v) for v in series)

    def is_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == new_arfcn

    def is_old(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == old_arfcn
    
    def has_value(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq is not None

    def is_allowed_n77_ssb(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_set if freq is not None else False

    def is_allowed_n77_arfcn(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_set if freq is not None else False

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
                arfcn_col = resolve_column_case_insensitive(df_nr_freq,["arfcnValueNRDl", "NRFrequencyId", "nRFrequencyId"])

                if node_col and arfcn_col:
                    work = df_nr_freq[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    # Only consider N77 rows
                    n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                    if not n77_work.empty:
                        grouped = n77_work.groupby(node_col)[arfcn_col]

                        # NR Frequency Audit: ALL nodes (not only N77) with any non-empty ARFCN in NRFrequency
                        all_nodes_with_freq = sorted(df_nr_freq.loc[df_nr_freq[arfcn_col].map(has_value), node_col].astype(str).unique())
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with ARFCN defined in NRFrequency",
                            len(all_nodes_with_freq),
                            ", ".join(all_nodes_with_freq),
                        )

                        # NR Frequency Audit: NR nodes with the old ARFCN in NRFrequency
                        old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with the old ARFCN ({old_arfcn}) in NRFrequency",
                            len(old_nodes),
                            ", ".join(old_nodes),
                        )

                        # NR Frequency Audit: NR nodes with the new ARFCN in NRFrequency
                        new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFrequency",
                            f"NR nodes with the new ARFCN ({new_arfcn}) in NRFrequency",
                            len(new_nodes),
                            ", ".join(new_nodes),
                        )

                        # NR Frequency Inconsistencies: NR nodes with the ARFCN not in (old_freq, new_freq) in NRFrequency
                        not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFrequency",
                            f"NR nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFrequency",
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
                arfcn_col = resolve_column_case_insensitive(df_nr_freq_rel,["NRFreqRelationId"])

                if node_col and arfcn_col:
                    work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                    if not n77_work.empty:
                        grouped = n77_work.groupby(node_col)[arfcn_col]

                        # NR Frequency Audit: NR nodes with the old ARFCN in NRFreqRelation
                        old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFreqRelation",
                            f"NR nodes with the old ARFCN ({old_arfcn}) in NRFreqRelation",
                            len(old_nodes),
                            ", ".join(old_nodes),
                        )

                        # NR Frequency Audit: NR nodes with the new ARFCN in NRFreqRelation
                        new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                        add_row(
                            "NR Frequency Audit",
                            "NRFreqRelation",
                            f"NR nodes with the new ARFCN ({new_arfcn}) in NRFreqRelation",
                            len(new_nodes),
                            ", ".join(new_nodes),
                        )

                        # NR Frequency Inconsistencies: NR nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFreqRelation
                        not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRFreqRelation",
                            f"NR nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFreqRelation",
                            len(not_old_not_new_nodes),
                            ", ".join(not_old_not_new_nodes),
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
                sector_carrier_id_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["NRSectorCarrierId"])

                if node_col and arfcn_col:
                    # work = df_nr_sector_carrier[[node_col, arfcn_col]].copy()
                    work = df_nr_sector_carrier[[node_col, arfcn_col, sector_carrier_id_col]].copy()

                    work[node_col] = work[node_col].astype(str)

                    # N77 nodes = those having at least one ARFCN starting with "6"
                    mask_n77 = work[arfcn_col].map(is_n77_from_string)
                    n77_rows = work.loc[mask_n77].copy()
                    n77_rows["unique_id"] = (n77_rows[node_col].astype(str) + "-" + n77_rows[sector_carrier_id_col].astype(str))

                    # NR Frequency Audit: NR nodes with ARFCN starting with '6' in NRSectorCarrier
                    # n77_sector_carriers = sorted(set(n77_rows[node_col].astype(str)))
                    n77_sector_carriers = sorted(set(n77_rows["unique_id"]))

                    add_row(
                        "NR Frequency Audit",
                        "NRSectorCarrier",
                        "NR nodes with ARFCN starting with '6' in NRSectorCarrier",
                        len(n77_sector_carriers),
                        ", ".join(n77_sector_carriers),
                    )

                    # NR Frequency Inconsistencies: NR ARFCN not in allowed list
                    if allowed_n77_arfcn_set:
                        bad_rows = n77_rows.loc[~n77_rows[arfcn_col].map(is_allowed_n77_arfcn)]
                        bad_nodes = sorted(set(bad_rows[node_col].astype(str)))
                        extra = "; ".join(
                            f"{r[node_col]}: {r[arfcn_col]}-{r[sector_carrier_id_col]}"
                            for _, r in bad_rows.head(200).iterrows()
                        )
                        if len(bad_rows) > 200:
                            extra += " (truncated)"
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRSectorCarrier",
                            "NR nodes with ARFCN not in allowed list",
                            len(bad_nodes),
                            extra,
                        )
                    else:
                        add_row(
                            "NR Frequency Inconsistencies",
                            "NRSectorCarrier",
                            "NR nodes with ARFCN not in allowed list (no allowed list configured)",
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
                node_col = resolve_column_case_insensitive(df_nr_cell_du, ["NRCellDUId"])
                ssb_col = resolve_column_case_insensitive(df_nr_cell_du, ["ssbFrequency", "ssbFreq", "ssb"])
                if node_col and ssb_col:
                    work = df_nr_cell_du[[node_col, ssb_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    # NR Frequency Audit: NR nodes with SSB starting with '6' in NRCellDU
                    mask_n77 = work[ssb_col].map(is_n77_from_string)
                    n77_sector_carriers = sorted(set(work.loc[mask_n77, node_col].astype(str)))
                    add_row(
                        "NR Frequency Audit",
                        "NRCellDU",
                        "NR nodes with SSB starting with '6' in NRCellDU",
                        len(n77_sector_carriers),
                        ", ".join(n77_sector_carriers),
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

    # ----------------------------- LTE GUtranSyncSignalFrequency (OLD/NEW ARFCN) -----------------------------
    def process_gu_sync_signal_freq():
        try:
            if df_gu_sync_signal_freq is not None and not df_gu_sync_signal_freq.empty:
                node_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_gu_sync_signal_freq,["arfcn", "arfcnDL"])

                if node_col and arfcn_col:
                    work = df_gu_sync_signal_freq[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    # LTE nodes with any GUtranSyncSignalFrequency defined
                    all_nodes_with_freq = sorted(work.loc[work[arfcn_col].map(has_value), node_col].astype(str).unique())
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        "LTE nodes with GUtranSyncSignalFrequency defined:",
                        len(all_nodes_with_freq),
                        ", ".join(all_nodes_with_freq),
                    )

                    grouped = work.groupby(node_col)[arfcn_col]

                    # GUtran Frequency Audit: LTE nodes with the old ARFCN in GUtranSyncSignalFrequency
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the old ARFCN ({old_arfcn}) in GUtranSyncSignalFrequency",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # GUtran Frequency Audit: LTE nodes with the new ARFCN in GUtranSyncSignalFrequency
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the new ARFCN ({new_arfcn}) in GUtranSyncSignalFrequency",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # GUtran Frequency Inconsistences: LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranSyncSignalFrequency
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row(
                        "GUtran Frequency Inconsistences",
                        "GUtranSyncSignalFrequency",
                        f"LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranSyncSignalFrequency",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )
                else:
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranSyncSignalFrequency",
                        "GUtranSyncSignalFrequency table present but required columns missing",
                        "N/A",
                    )
            else:
                add_row(
                    "GUtran Frequency Audit",
                    "GUtranSyncSignalFrequency",
                    "GUtranSyncSignalFrequency table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "GUtran Frequency Audit",
                "GUtranSyncSignalFrequency",
                "Error while checking GUtranSyncSignalFrequency",
                f"ERROR: {ex}",
            )

    # ----------------------------- LTE GUtranFreqRelation (OLD/NEW ARFCN) -----------------------------
    def process_gu_freq_rel():
        try:
            if df_gu_freq_rel is not None and not df_gu_freq_rel.empty:
                node_col = resolve_column_case_insensitive(df_gu_freq_rel, ["NodeId"])
                arfcn_col = resolve_column_case_insensitive(df_gu_freq_rel,["GUtranFreqRelationId", "gUtranFreqRelationId"])

                if node_col and arfcn_col:
                    work = df_gu_freq_rel[[node_col, arfcn_col]].copy()
                    work[node_col] = work[node_col].astype(str)

                    grouped = work.groupby(node_col)[arfcn_col]

                    # GUtran Frequency Audit: LTE nodes with the old ARFCN in GUtranFreqRelation
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranFreqRelation",
                        f"LTE nodes with the old ARFCN ({old_arfcn}) in GUtranFreqRelation",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # GUtran Frequency Audit: LTE nodes with the new ARFCN in GUtranFreqRelation
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranFreqRelation",
                        f"LTE nodes with the new ARFCN ({new_arfcn}) in GUtranFreqRelation",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # GUtran Frequency Inconsistences: LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranFreqRelation
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row(
                        "GUtran Frequency Inconsistences",
                        "GUtranFreqRelation",
                        f"LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranFreqRelation",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )
                else:
                    add_row(
                        "GUtran Frequency Audit",
                        "GUtranFreqRelation",
                        "GUtranFreqRelation table present but ARFCN/NodeId missing",
                        "N/A",
                    )
            else:
                add_row(
                    "GUtran Frequency Audit",
                    "GUtranFreqRelation",
                    "GUtranFreqRelation table",
                    "Table not found or empty",
                )
        except Exception as ex:
            add_row(
                "GUtran Frequency Audit",
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
                    expected_old = str(old_arfcn)
                    expected_new = str(new_arfcn)
                    expected_n77b = str(n77b_ssb)

                    # Nodes with GUtranSyncSignalFrequency containing old_arfcn and n77b_ssb
                    mask_old_pair = freq_sets.map(
                        lambda s: (expected_old in s) and (expected_n77b in s)
                    )
                    old_nodes = sorted(
                        work.loc[mask_old_pair, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile Audit",
                        "EndcDistrProfile",
                        f"Nodes with GUtranSyncSignalFrequency containing {old_arfcn} and {n77b_ssb}",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )

                    # Nodes with GUtranSyncSignalFrequency containing new_arfcn and n77b_ssb
                    mask_new_pair = freq_sets.map(
                        lambda s: (expected_new in s) and (expected_n77b in s)
                    )
                    new_nodes = sorted(
                        work.loc[mask_new_pair, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile Audit",
                        "EndcDistrProfile",
                        f"Nodes with GUtranSyncSignalFrequency containing {new_arfcn} and {n77b_ssb}",
                        len(new_nodes),
                        ", ".join(new_nodes),
                    )

                    # Inconsistencies:
                    # - rows where neither old_arfcn nor new_arfcn is present
                    #   OR
                    # - rows where n77b_ssb is not present
                    mask_inconsistent = freq_sets.map(
                        lambda s: ((expected_old not in s and expected_new not in s) or (expected_n77b not in s))
                    )
                    bad_nodes = sorted(
                        work.loc[mask_inconsistent, node_col_edp].astype(str).unique()
                    )

                    add_row(
                        "EndcDistrProfile Inconsistencies",
                        "EndcDistrProfile",
                        f"Nodes with GUtranSyncSignalFrequency not containing ({old_arfcn} or {new_arfcn}) together with {n77b_ssb}",
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
                cell_col = resolve_column_case_insensitive(df_nr_freq_rel,["NRCellCUId", "NRCellId", "CellId"])
                if cell_col:
                    counts = df_nr_freq_rel[cell_col].value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0

                    at_limit_or_above = counts[counts >= 16]
                    over_limit = counts[counts > 16]

                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "Max NRFreqRelation per NR cell (limit 16)",
                        max_count,
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.head(50).items())
                        + (" (truncated)" if at_limit_or_above.size > 50 else ""),
                    )

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "Nodes with #NRFreqRelation per NR cell above limit (16)",
                        int(over_limit.size),
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in over_limit.head(50).items())
                        + (" (truncated)" if over_limit.size > 50 else ""),
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

                    at_limit_or_above = counts[counts >= 64]
                    over_limit = counts[counts > 64]

                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "Max NRFrequency definitions per node (limit 64)",
                        max_count,
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.head(50).items())
                        + (" (truncated)" if at_limit_or_above.size > 50 else ""),
                    )

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "Nodes with #NRFrequency definitions per node above limit (64)",
                        int(over_limit.size),
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in over_limit.head(50).items())
                        + (" (truncated)" if over_limit.size > 50 else ""),
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
                cell_col_gu = resolve_column_case_insensitive(df_gu_freq_rel,["EUtranCellFDDId", "EUtranCellId", "CellId", "GUCellId"])
                if cell_col_gu:
                    counts = df_gu_freq_rel[cell_col_gu].value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0

                    at_limit_or_above = counts[counts >= 16]
                    over_limit = counts[counts > 16]

                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "Max GUtranFreqRelation per LTE cell (limit 16)",
                        max_count,
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.head(50).items())
                        + (" (truncated)" if at_limit_or_above.size > 50 else ""),
                    )

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "Nodes with #GUtranFreqRelation per LTE cell above limit (16)",
                        int(over_limit.size),
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in over_limit.head(50).items())
                        + (" (truncated)" if over_limit.size > 50 else ""),
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
                node_col = resolve_column_case_insensitive(df_gu_sync_signal_freq,["NodeId"])
                if node_col:
                    counts = df_gu_sync_signal_freq[node_col].astype(str).value_counts(dropna=False)
                    max_count = int(counts.max()) if not counts.empty else 0

                    at_limit_or_above = counts[counts >= 24]
                    over_limit = counts[counts > 24]

                    add_row(
                        "Cardinality Audit",
                        "Cardinality",
                        "Max GUtranSyncSignalFrequency definitions per node (limit 24)",
                        max_count,
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in at_limit_or_above.head(50).items())
                        + (" (truncated)" if at_limit_or_above.size > 50 else ""),
                    )

                    add_row(
                        "Cardinality Inconsistencies",
                        "Cardinality",
                        "Nodes with #GUtranSyncSignalFrequency definitions per node above limit (24)",
                        int(over_limit.size),
                        "; ".join(f"{idx}: {cnt}" for idx, cnt in over_limit.head(50).items())
                        + (" (truncated)" if over_limit.size > 50 else ""),
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

                # NR Frequency Inconsistencies
                ("NR Frequency Inconsistencies", "NRFrequency"),
                ("NR Frequency Inconsistencies", "NRFreqRelation"),
                ("NR Frequency Inconsistencies", "NRSectorCarrier"),

                # GUtran Frequency Audit
                ("GUtran Frequency Audit", "GUtranSyncSignalFrequency"),
                ("GUtran Frequency Audit", "GUtranFreqRelation"),

                # GUtran Frequency Inconsistences
                ("GUtran Frequency Inconsistences", "GUtranSyncSignalFrequency"),
                ("GUtran Frequency Inconsistences", "GUtranFreqRelation"),

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