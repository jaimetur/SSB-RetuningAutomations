# -*- coding: utf-8 -*-
import pandas as pd
from typing import Dict


from src.utils.utils_frequency import resolve_column_case_insensitive, extract_sync_frequencies, parse_int_frequency, is_n77_from_string


# ----------------------------- EndcDistrProfile gUtranFreqRef -----------------------------
def process_endc_distr_profile(df_endc_distr_profile, n77_ssb_pre, n77_ssb_post, n77b_ssb, add_row, nodes_pre=None, nodes_post=None):
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
def process_freq_prio_nr(df_freq_prio_nr, n77_ssb_pre, n77_ssb_post, add_row, nodes_pre=None, nodes_post=None):
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
                work["GNodeB_SSB_Source"] = work[freq_col].map(parse_int_frequency)

                # ------------------------------------------------------------------
                # New checks: old SSB (648672) vs new SSB (647328) in FreqPrioNR
                # ------------------------------------------------------------------
                old_ssb = n77_ssb_pre
                new_ssb = n77_ssb_post

                old_rows = work.loc[work["GNodeB_SSB_Source"] == old_ssb].copy()
                new_rows = work.loc[work["GNodeB_SSB_Source"] == new_ssb].copy()

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
                    cell_work["GNodeB_SSB_Source"] = cell_work[freq_col].map(parse_int_frequency)

                    cell_work = cell_work.loc[
                        cell_work["GNodeB_SSB_Source"].isin([old_ssb, new_ssb]) & (cell_work[cell_col] != "")
                        ].copy()

                    # Build mapping {(node, cell): {freq: row}}
                    pairs: Dict[tuple, Dict[int, pd.Series]] = {}
                    for _, row in cell_work.iterrows():
                        key = (row[node_col], row[cell_col])
                        freq_val = row["GNodeB_SSB_Source"]
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
                                if col in {node_col, cell_col, freq_col, "GNodeB_SSB_Source"}:
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
def process_cardinalities(df_nr_freq, add_row, df_nr_freq_rel, df_gu_sync_signal_freq, df_gu_freq_rel, nodes_pre=None, nodes_post=None):
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