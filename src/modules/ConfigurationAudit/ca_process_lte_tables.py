# -*- coding: utf-8 -*-
import pandas as pd

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency

# ----------------------------- LTE GUtranSyncSignalFrequency (OLD/NEW SSB + LowMidBand/mmWave) -----------------------------
def process_gu_sync_signal_freq(df_gu_sync_signal_freq, has_value, add_row, is_old, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new):
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

                    # LTE nodes with GUtranSyncSignalFrequency defined (from GUtranSyncSignalFrequency table)
                    all_nodes_with_freq = sorted(work.loc[work[arfcn_col].map(has_value), node_col].astype(str).unique())
                    add_row(
                        "GUtranSyncSignalFrequency",
                        "LTE Frequency Audit",
                        "LTE nodes with GUtranSyncSignalFrequency defined (from GUtranSyncSignalFrequency table)",
                        len(all_nodes_with_freq),
                        ", ".join(all_nodes_with_freq),
                    )

                    # nodes_with_lte_sync = sorted(valid_rows[node_col].astype(str).unique())
                    # mmwave_nodes: list[str] = []
                    # lowmid_nodes: list[str] = []
                    # mixed_nodes: list[str] = []
                    #
                    # for node, series in valid_rows.groupby(node_col)["_is_mmwave_"]:
                    #     has_mmwave = bool(series.any())
                    #     has_lowmid = bool((~series).any())
                    #     node_str = str(node)
                    #
                    #     if has_mmwave and not has_lowmid:
                    #         mmwave_nodes.append(node_str)
                    #     elif has_lowmid and not has_mmwave:
                    #         lowmid_nodes.append(node_str)
                    #     elif has_mmwave and has_lowmid:
                    #         mixed_nodes.append(node_str)
                    #
                    # # Summary rows for LTE LowMidBand / mmWave node types
                    # add_row(
                    #     "GUtranSyncSignalFrequency",
                    #     "LTE Frequency Audit",
                    #     "LTE LowMidBand Nodes (from GUtranSyncSignalFrequency table)",
                    #     len(lowmid_nodes),
                    #     ", ".join(sorted(lowmid_nodes)),
                    # )
                    # add_row(
                    #     "GUtranSyncSignalFrequency",
                    #     "LTE Frequency Audit",
                    #     "LTE mmWave Nodes (from GUtranSyncSignalFrequency table)",
                    #     len(mmwave_nodes),
                    #     ", ".join(sorted(mmwave_nodes)),
                    # )
                    #
                    # # Optional: nodes having both LowMidBand and mmWave SSBs
                    # if mixed_nodes:
                    #     add_row(
                    #         "GUtranSyncSignalFrequency",
                    #         "LTE Frequency Audit",
                    #         "LTE Nodes with both LowMidBand and mmWave GUtranSyncSignalFrequency SSBs (from GUtranSyncSignalFrequency table)",
                    #         len(mixed_nodes),
                    #         ", ".join(sorted(mixed_nodes)),
                    #     )

                # ------------------------------------------------------------------
                # Existing logic: old/new SSB checks on LTE side
                # ------------------------------------------------------------------
                grouped = work.groupby(node_col)[arfcn_col]

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
def process_gu_freq_rel(df_gu_freq_rel, is_old, add_row, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, param_mismatch_rows_gu):
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
                        f"LTE cells with mismatching params between GUtranFreqRelationId {expected_old_rel_id} and {expected_new_rel_id} (from GUtranFreqRelation table)",
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
def process_gu_cell_relation(df_gu_cell_rel, n77_ssb_pre, n77_ssb_post, add_row):
    try:
        if df_gu_cell_rel is not None and not df_gu_cell_rel.empty:
            node_col = resolve_column_case_insensitive(df_gu_cell_rel, ["NodeId"])
            freq_col = resolve_column_case_insensitive(df_gu_cell_rel, ["GUtranFreqRelationId"])

            if node_col and freq_col:
                work = df_gu_cell_rel[[node_col, freq_col]].copy()
                work[node_col] = work[node_col].astype(str).str.strip()

                # Extract numeric frequency from GUtranFreqRelationId (part before first '-')
                work["GNodeB_SSB_Source"] = work[freq_col].map(lambda v: parse_int_frequency(str(v).split("-", 1)[0]) if pd.notna(v) else None)

                count_old = int((work["GNodeB_SSB_Source"] == n77_ssb_pre).sum())
                count_new = int((work["GNodeB_SSB_Source"] == n77_ssb_post).sum())

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