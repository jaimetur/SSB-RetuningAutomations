# -*- coding: utf-8 -*-

from typing import List, Dict
import pandas as pd

from src.modules.CommonMethods import resolve_column_case_insensitive, parse_int_frequency, is_n77_from_string


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

    old_arfcn = int(old_arfcn)
    new_arfcn = int(new_arfcn)

    allowed_n77_ssb_set = {int(v) for v in (allowed_n77_ssb or [])}
    allowed_n77_arfcn_set = {int(v) for v in (allowed_n77_arfcn or [])}

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

    def is_allowed_n77_ssb(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_set if freq is not None else False

    def is_allowed_n77_arfcn(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_set if freq is not None else False

    rows: List[Dict[str, object]] = []

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

    # ----------------------------- NRCellDU (N77 detection) -----------------------------
    try:
        if df_nr_cell_du is not None and not df_nr_cell_du.empty:
            node_col = resolve_column_case_insensitive(df_nr_cell_du, ["NodeId"])
            ssb_col = resolve_column_case_insensitive(
                df_nr_cell_du,
                ["ssbFrequency", "ssbFreq", "ssb"],
            )
            if node_col and ssb_col:
                work = df_nr_cell_du[[node_col, ssb_col]].copy()
                work[node_col] = work[node_col].astype(str)

                mask_n77 = work[ssb_col].map(is_n77_from_string)
                n77_nodes = sorted(set(work.loc[mask_n77, node_col].astype(str)))

                add_row(
                    "NR Frequency Audit",
                    "NRCellDU",
                    "N77 nodes (NRCellDU SSB starting with '6')",
                    len(n77_nodes),
                    ", ".join(n77_nodes),
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

    # ----------------------------- NRSectorCarrier (N77 + allowed ARFCN) -----------------------------
    try:
        if df_nr_sector_carrier is not None and not df_nr_sector_carrier.empty:
            node_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(
                df_nr_sector_carrier,
                ["arfcnDL", "arfcn", "arfcnValueNRDl"],
            )
            if node_col and arfcn_col:
                work = df_nr_sector_carrier[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str)

                # N77 nodes = those having at least one ARFCN starting with "6"
                mask_n77 = work[arfcn_col].map(is_n77_from_string)
                n77_rows = work.loc[mask_n77].copy()

                n77_nodes = sorted(set(n77_rows[node_col].astype(str)))
                add_row(
                    "NR Frequency Audit",
                    "NRSectorCarrier",
                    "N77 nodes with ARFCN starting with '6'",
                    len(n77_nodes),
                    ", ".join(n77_nodes),
                )

                # Inconsistencies: N77 ARFCN not in allowed list
                if allowed_n77_arfcn_set:
                    bad_rows = n77_rows.loc[
                        ~n77_rows[arfcn_col].map(is_allowed_n77_arfcn)
                    ]
                    bad_nodes = sorted(set(bad_rows[node_col].astype(str)))
                    extra = "; ".join(
                        f"{r[node_col]}: {r[arfcn_col]}"
                        for _, r in bad_rows.head(200).iterrows()
                    )
                    if len(bad_rows) > 200:
                        extra += " (truncated)"
                    add_row(
                        "NR Frequency Inconsistencies",
                        "NRSectorCarrier",
                        "N77 nodes with ARFCN not in allowed list",
                        len(bad_nodes),
                        extra,
                    )
                else:
                    add_row(
                        "NR Frequency Inconsistencies",
                        "NRSectorCarrier",
                        "N77 nodes with ARFCN not in allowed list (no allowed list configured)",
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

    # ----------------------------- NRFrequency (OLD/NEW ARFCN on N77 rows) -----------------------------
    try:
        if df_nr_freq is not None and not df_nr_freq.empty:
            node_col = resolve_column_case_insensitive(df_nr_freq, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(
                df_nr_freq,
                ["arfcnValueNRDl", "arfcn", "arfcnDL"],
            )

            if node_col and arfcn_col:
                work = df_nr_freq[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str)

                # Only consider N77 rows
                n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                if not n77_work.empty:
                    grouped = n77_work.groupby(node_col)[arfcn_col]

                    not_old_not_new_nodes = sorted(
                        str(node)
                        for node, series in grouped
                        if series_only_not_old_not_new(series)
                    )
                    new_nodes = sorted(
                        str(node)
                        for node, series in grouped
                        if any(is_new(v) for v in series)
                    )
                    old_nodes = sorted(
                        str(node)
                        for node, series in grouped
                        if any(is_old(v) for v in series)
                    )

                    add_row(
                        "NR Frequency Inconsistencies",
                        "NRFrequency",
                        f"N77 nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFrequency",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )
                    add_row(
                        "NR Frequency Audit",
                        "NRFrequency",
                        f"N77 nodes with the old ARFCN ({old_arfcn}) in NRFrequency",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )
                    add_row(
                        "NR Frequency Audit",
                        "NRFrequency",
                        f"N77 nodes with the new ARFCN ({new_arfcn}) in NRFrequency",
                        len(new_nodes),
                        ", ".join(new_nodes),
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

    # ----------------------------- NRFreqRelation (OLD/NEW ARFCN on N77 rows) -----------------------------
    try:
        if df_nr_freq_rel is not None and not df_nr_freq_rel.empty:
            node_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(
                df_nr_freq_rel,
                ["NRFreqRelationId"],
            )

            if node_col and arfcn_col:
                work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str)

                n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                if not n77_work.empty:
                    grouped = n77_work.groupby(node_col)[arfcn_col]

                    not_old_not_new_nodes = sorted(
                        str(node)
                        for node, series in grouped
                        if series_only_not_old_not_new(series)
                    )
                    new_nodes = sorted(
                        str(node)
                        for node, series in grouped
                        if any(is_new(v) for v in series)
                    )
                    old_nodes = sorted(
                        str(node)
                        for node, series in grouped
                        if any(is_old(v) for v in series)
                    )

                    add_row(
                        "NR Frequency Inconsistencies",
                        "NRFreqRelation",
                        f"N77 nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFreqRelation",
                        len(not_old_not_new_nodes),
                        ", ".join(not_old_not_new_nodes),
                    )
                    add_row(
                        "NR Frequency Audit",
                        "NRFreqRelation",
                        f"N77 nodes with the old ARFCN ({old_arfcn}) in NRFreqRelation",
                        len(old_nodes),
                        ", ".join(old_nodes),
                    )
                    add_row(
                        "NR Frequency Audit",
                        "NRFreqRelation",
                        f"N77 nodes with the new ARFCN ({new_arfcn}) in NRFreqRelation",
                        len(new_nodes),
                        ", ".join(new_nodes),
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

    # ----------------------------- LTE GUtranSyncSignalFrequency (OLD/NEW ARFCN) -----------------------------
    try:
        if df_gu_sync_signal_freq is not None and not df_gu_sync_signal_freq.empty:
            node_col = resolve_column_case_insensitive(df_gu_sync_signal_freq, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(
                df_gu_sync_signal_freq,
                ["arfcn", "arfcnDL"],
            )

            if node_col and arfcn_col:
                work = df_gu_sync_signal_freq[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str)

                grouped = work.groupby(node_col)[arfcn_col]

                not_old_not_new_nodes = sorted(
                    str(node)
                    for node, series in grouped
                    if series_only_not_old_not_new(series)
                )
                new_nodes = sorted(
                    str(node)
                    for node, series in grouped
                    if any(is_new(v) for v in series)
                )
                old_nodes = sorted(
                    str(node)
                    for node, series in grouped
                    if any(is_old(v) for v in series)
                )

                add_row(
                    "Gutran Frequency Inconsistences",
                    "GUtranSyncSignalFrequency",
                    f"LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranSyncSignalFrequency",
                    len(not_old_not_new_nodes),
                    ", ".join(not_old_not_new_nodes),
                )
                add_row(
                    "Gutran Frequency Audit",
                    "GUtranSyncSignalFrequency",
                    f"LTE nodes with the old ARFCN ({old_arfcn}) in GUtranSyncSignalFrequency",
                    len(old_nodes),
                    ", ".join(old_nodes),
                )
                add_row(
                    "Gutran Frequency Audit",
                    "GUtranSyncSignalFrequency",
                    f"LTE nodes with the new ARFCN ({new_arfcn}) in GUtranSyncSignalFrequency",
                    len(new_nodes),
                    ", ".join(new_nodes),
                )
            else:
                add_row(
                    "Gutran Frequency Audit",
                    "GUtranSyncSignalFrequency",
                    "GUtranSyncSignalFrequency table present but required columns missing",
                    "N/A",
                )
        else:
            add_row(
                "Gutran Frequency Audit",
                "GUtranSyncSignalFrequency",
                "GUtranSyncSignalFrequency table",
                "Table not found or empty",
            )
    except Exception as ex:
        add_row(
            "Gutran Frequency Audit",
            "GUtranSyncSignalFrequency",
            "Error while checking GUtranSyncSignalFrequency",
            f"ERROR: {ex}",
        )

    # ----------------------------- LTE GUtranFreqRelation (OLD/NEW ARFCN) -----------------------------
    try:
        if df_gu_freq_rel is not None and not df_gu_freq_rel.empty:
            node_col = resolve_column_case_insensitive(df_gu_freq_rel, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(
                df_gu_freq_rel,
                ["GUtranFreqRelationId", "gUtranFreqRelationId"],
            )

            if node_col and arfcn_col:
                work = df_gu_freq_rel[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str)

                grouped = work.groupby(node_col)[arfcn_col]

                not_old_not_new_nodes = sorted(
                    str(node)
                    for node, series in grouped
                    if series_only_not_old_not_new(series)
                )
                new_nodes = sorted(
                    str(node)
                    for node, series in grouped
                    if any(is_new(v) for v in series)
                )
                old_nodes = sorted(
                    str(node)
                    for node, series in grouped
                    if any(is_old(v) for v in series)
                )

                add_row(
                    "Gutran Frequency Inconsistences",
                    "GUtranFreqRelation",
                    f"LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranFreqRelation",
                    len(not_old_not_new_nodes),
                    ", ".join(not_old_not_new_nodes),
                )
                add_row(
                    "Gutran Frequency Audit",
                    "GUtranFreqRelation",
                    f"LTE nodes with the old ARFCN ({old_arfcn}) in GUtranFreqRelation",
                    len(old_nodes),
                    ", ".join(old_nodes),
                )
                add_row(
                    "Gutran Frequency Audit",
                    "GUtranFreqRelation",
                    f"LTE nodes with the new ARFCN ({new_arfcn}) in GUtranFreqRelation",
                    len(new_nodes),
                    ", ".join(new_nodes),
                )
            else:
                add_row(
                    "Gutran Frequency Audit",
                    "GUtranFreqRelation",
                    "GUtranFreqRelation table present but ARFCN/NodeId missing",
                    "N/A",
                )
        else:
            add_row(
                "Gutran Frequency Audit",
                "GUtranFreqRelation",
                "GUtranFreqRelation table",
                "Table not found or empty",
            )
    except Exception as ex:
        add_row(
            "Gutran Frequency Audit",
            "GUtranFreqRelation",
            "Error while checking GUtranFreqRelation",
            f"ERROR: {ex}",
        )

    # ----------------------------- CARDINALITY LIMITS -----------------------------
    # Max 16 NRFreqRelation per NR cell
    try:
        if df_nr_freq_rel is not None and not df_nr_freq_rel.empty:
            cell_col = resolve_column_case_insensitive(
                df_nr_freq_rel,
                ["NRCellCUId", "NRCellId", "CellId"],
            )
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

    # Max 16 GUtranFreqRelation per LTE cell
    try:
        if df_gu_freq_rel is not None and not df_gu_freq_rel.empty:
            cell_col_gu = resolve_column_case_insensitive(
                df_gu_freq_rel,
                ["EUtranCellFDDId", "EUtranCellId", "CellId", "GUCellId"],
            )
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
            node_col = resolve_column_case_insensitive(
                df_gu_sync_signal_freq,
                ["NodeId"],
            )
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

    # Max 64 NRFrequency per node
    try:
        if df_nr_freq is not None and not df_nr_freq.empty:
            node_col = resolve_column_case_insensitive(
                df_nr_freq,
                ["NodeId"],
            )
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

    # ----------------------------- EndcDistrProfile gUtranFreqRef -----------------------------
    try:
        if df_endc_distr_profile is not None and not df_endc_distr_profile.empty:
            node_col_edp = resolve_column_case_insensitive(
                df_endc_distr_profile,
                ["NodeId"],
            )
            ref_col = resolve_column_case_insensitive(
                df_endc_distr_profile,
                ["gUtranFreqRef"],
            )
            if node_col_edp and ref_col:
                work = df_endc_distr_profile[[node_col_edp, ref_col]].copy()
                work[node_col_edp] = work[node_col_edp].astype(str)
                work[ref_col] = work[ref_col].astype(str)

                def _normalize_ref(s: str) -> str:
                    return str(s).replace(" ", "").strip()

                expected_str = str(new_arfcn)

                bad_mask = ~work[ref_col].map(
                    lambda v: expected_str in _normalize_ref(v)
                )
                bad_rows = work.loc[bad_mask, [node_col_edp, ref_col]]

                add_row(
                    "EndcDistrProfile Audit",
                    "EndcDistrProfile",
                    f"EndcDistrProfile rows with gUtranFreqRef not containing {new_arfcn}",
                    len(bad_rows),
                    "; ".join(
                        f"{r[node_col_edp]}: {r[ref_col]}"
                        for _, r in bad_rows.head(50).iterrows()
                    )
                    + (" (truncated)" if len(bad_rows) > 50 else ""),
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
        desired_order = [
            # NR Frequency Audit
            ("NR Frequency Audit", "NRCellDU", "N77 nodes (NRCellDU SSB starting with '6')"),
            ("NR Frequency Audit", "NRSectorCarrier", "N77 nodes with ARFCN starting with '6'"),
            ("NR Frequency Audit", "NRFrequency",
             f"N77 nodes with the new ARFCN ({new_arfcn}) in NRFrequency"),
            ("NR Frequency Audit", "NRFrequency",
             f"N77 nodes with the old ARFCN ({old_arfcn}) in NRFrequency"),
            ("NR Frequency Audit", "NRFreqRelation",
             f"N77 nodes with the new ARFCN ({new_arfcn}) in NRFreqRelation"),
            ("NR Frequency Audit", "NRFreqRelation",
             f"N77 nodes with the old ARFCN ({old_arfcn}) in NRFreqRelation"),

            # NR Frequency Inconsistencies
            ("NR Frequency Inconsistencies", "NRSectorCarrier",
             "N77 nodes with ARFCN not in allowed list"),
            ("NR Frequency Inconsistencies", "NRFrequency",
             f"N77 nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFrequency"),
            ("NR Frequency Inconsistencies", "NRFreqRelation",
             f"N77 nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in NRFreqRelation"),

            # GUtran Frequency Audit
            ("Gutran Frequency Audit", "GUtranSyncSignalFrequency",
             f"LTE nodes with the new ARFCN ({new_arfcn}) in GUtranSyncSignalFrequency"),
            ("Gutran Frequency Audit", "GUtranSyncSignalFrequency",
             f"LTE nodes with the old ARFCN ({old_arfcn}) in GUtranSyncSignalFrequency"),
            ("Gutran Frequency Audit", "GUtranFreqRelation",
             f"LTE nodes with the new ARFCN ({new_arfcn}) in GUtranFreqRelation"),
            ("Gutran Frequency Audit", "GUtranFreqRelation",
             f"LTE nodes with the old ARFCN ({old_arfcn}) in GUtranFreqRelation"),

            # GUtran Frequency Inconsistences
            ("Gutran Frequency Inconsistences", "GUtranSyncSignalFrequency",
             f"LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranSyncSignalFrequency"),
            ("Gutran Frequency Inconsistences", "GUtranFreqRelation",
             f"LTE nodes with the ARFCN not in ({old_arfcn}, {new_arfcn}) in GUtranFreqRelation"),

            # Cardinality Audit
            ("Cardinality Audit", "Cardinality", "Max NRFreqRelation per NR cell (limit 16)"),
            ("Cardinality Audit", "Cardinality", "Max NRFrequency definitions per node (limit 64)"),
            ("Cardinality Audit", "Cardinality", "Max GUtranFreqRelation per LTE cell (limit 16)"),
            ("Cardinality Audit", "Cardinality", "Max GUtranSyncSignalFrequency definitions per node (limit 24)"),

            # Cardinality Inconsistencies
            ("Cardinality Inconsistencies", "Cardinality",
             "Nodes with #NRFreqRelation per NR cell above limit (16)"),
            ("Cardinality Inconsistencies", "Cardinality",
             "Nodes with #NRFrequency definitions per node above limit (64)"),
            ("Cardinality Inconsistencies", "Cardinality",
             "Nodes with #GUtranFreqRelation per LTE cell above limit (16)"),
            ("Cardinality Inconsistencies", "Cardinality",
             "Nodes with #GUtranSyncSignalFrequency definitions per node above limit (24)"),

            # EndcDistrProfile Audit
            ("EndcDistrProfile Audit", "EndcDistrProfile",
             f"EndcDistrProfile rows with gUtranFreqRef not containing {new_arfcn}"),
        ]

        # Map (Category, SubCategory, Metric) to an integer order
        order_map: Dict[tuple, int] = {
            key: idx for idx, key in enumerate(desired_order)
        }

        def _order_row(r: pd.Series) -> int:
            """Return the desired order index for each row (or a large value if not explicitly listed)."""
            key = (
                str(r.get("Category", "")),
                str(r.get("SubCategory", "")),
                str(r.get("Metric", "")),
            )
            return order_map.get(key, len(desired_order) + 100)

        df["__order__"] = df.apply(_order_row, axis=1)
        df = (
            df.sort_values("__order__", kind="mergesort")
            .drop(columns=["__order__"])
            .reset_index(drop=True)
        )

    return df


