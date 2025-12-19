# -*- coding: utf-8 -*-
import pandas as pd

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency, is_n77_from_string


# ----------------------------- NRCellDU (N77 detection + allowed SSB + LowMidBand/mmWave) -----------------------------
def process_nr_cell_du(df_nr_cell_du, add_row, allowed_n77_ssb_pre_set, allowed_n77_ssb_post_set):
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

                    add_row("NRCellDU", "NR Frequency Audit", "NR Nodes with ssbFrequency (from NRCellDU table)", len(nodes_with_nr_cells), ", ".join(nodes_with_nr_cells))
                    add_row("NRCellDU", "NR Frequency Audit", "NR LowMidBand Nodes (from NRCellDU table)", len(lowmid_nodes), ", ".join(sorted(lowmid_nodes)))
                    add_row("NRCellDU", "NR Frequency Audit", "NR mmWave Nodes (from NRCellDU table)", len(mmwave_nodes), ", ".join(sorted(mmwave_nodes)))

                    # Optional: nodes having both LowMidBand and mmWave cells
                    if mixed_nodes:
                        add_row("NRCellDU", "NR Frequency Audit", "NR Nodes with both LowMidBand and mmWave NR cells (from NRCellDU table)", len(mixed_nodes), ", ".join(sorted(mixed_nodes)))

                # ------------------------------------------------------------------
                # Existing N77 logic (kept as it was)
                # ------------------------------------------------------------------
                # N77 cells = those having at least one SSB in N77 band (646600-660000)
                mask_n77 = work[ssb_col].map(is_n77_from_string)
                n77_rows = work.loc[mask_n77].copy()

                if not n77_rows.empty:
                    # NR Frequency Audit: NR nodes with N77 SSB in band (646600-660000) (from NRCellDU table)
                    n77_nodes = sorted(n77_rows[node_col].astype(str).unique())
                    add_row("NRCellDU", "NR Frequency Audit", "NR nodes with N77 SSB in band (646600-660000) (from NRCellDU table)", len(n77_nodes), ", ".join(n77_nodes))

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
                        add_row("NRCellDU", "NR Frequency Audit", f"NR nodes with N77 SSB in Pre-Retune allowed list ({allowed_pre_str}) (from NRCellDU table)", len(pre_nodes), ", ".join(pre_nodes))

                    if allowed_n77_ssb_post_set:
                        grouped_n77 = n77_rows.groupby(node_col)[ssb_col]

                        def all_n77_ssb_in_post(series: pd.Series) -> bool:
                            freqs = series.map(parse_int_frequency)
                            freqs_valid = {f for f in freqs if f is not None}
                            # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_ssb_post_set
                            return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_ssb_post_set)

                        post_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_ssb_in_post(series))
                        allowed_post_str = ", ".join(str(v) for v in sorted(allowed_n77_ssb_post_set))
                        add_row("NRCellDU", "NR Frequency Audit", f"NR nodes with N77 SSB in Post-Retune allowed list ({allowed_post_str}) (from NRCellDU table)", len(post_nodes), ", ".join(post_nodes))

                    if allowed_n77_ssb_pre_set or allowed_n77_ssb_post_set:
                        allowed_union = set(allowed_n77_ssb_pre_set) | set(allowed_n77_ssb_post_set)

                        def _is_not_in_union_ssb(v: object) -> bool:
                            freq = parse_int_frequency(v)
                            return freq is not None and freq not in allowed_union

                        bad_rows = n77_rows.loc[n77_rows[ssb_col].map(_is_not_in_union_ssb)]

                        # Unique nodes with at least one SSB not in pre/post allowed lists
                        bad_nodes = sorted(bad_rows[node_col].astype(str).unique())

                        # Build a unique (NodeId, SSB) list to avoid duplicated lines in ExtraInfo
                        unique_pairs = sorted({(str(r[node_col]).strip(), str(r[ssb_col]).strip()) for _, r in bad_rows.iterrows()})
                        extra = "; ".join(f"{node}: {ssb}" for node, ssb in unique_pairs)

                        add_row("NRCellDU", "NR Frequency Inconsistencies", "NR nodes with N77 SSB not in Pre/Post Retune allowed lists (from NRCellDU table)", len(bad_nodes), extra)
                    else:
                        add_row("NRCellDU", "NR Frequency Inconsistencies", "NR nodes with N77 SSB not in Pre/Post Retune allowed lists (no pre/post allowed lists configured) (from NRCellDU table)", "N/A")
                else:
                    add_row("NRCellDU", "NR Frequency Audit", "NRCellDU table has no N77 rows", 0)
            else:
                add_row("NRCellDU", "NR Frequency Audit", "NRCellDU table present but required columns missing", "N/A")
        else:
            add_row("NRCellDU", "NR Frequency Audit", "NRCellDU table", "Table not found or empty")
    except Exception as ex:
        add_row("NRCellDU", "NR Frequency Audit", "Error while checking NRCellDU", f"ERROR: {ex}")


# ----------------------------- NRFrequency (OLD/NEW SSB on N77 rows) -----------------------------
def process_nr_freq(df_nr_freq, has_value, add_row, is_old, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new):
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
                    add_row("NRFrequency", "NR Frequency Audit", f"NR nodes with N77 SSB defined (from NRFrequency table)", len(all_nodes_with_freq), ", ".join(all_nodes_with_freq))

                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row("NRFrequency", "NR Frequency Audit", f"NR nodes with the old N77 SSB ({n77_ssb_pre}) (from NRFrequency table)", len(old_nodes), ", ".join(old_nodes))

                    # NR Frequency Audit: NR nodes with the new N77 SSB (from NRFrequency table)
                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row("NRFrequency", "NR Frequency Audit", f"NR nodes with the new N77 SSB ({n77_ssb_post}) (from NRFrequency table)", len(new_nodes), ", ".join(new_nodes))

                    # NEW: check nodes that have old_ssb and also new_ssb vs those missing the new_arfcn (from NRFrequency table)
                    old_set = set(old_nodes)
                    new_set = set(new_nodes)

                    nodes_old_and_new = sorted(old_set & new_set)
                    add_row("NRFrequency", "NR Frequency Audit", f"NR nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from NRFrequency table)", len(nodes_old_and_new), ", ".join(nodes_old_and_new))

                    nodes_old_without_new = sorted(old_set - new_set)
                    add_row("NRFrequency", "NR Frequency Audit", f"NR nodes with the old N77 SSB ({n77_ssb_pre}) but without the new N77 SSB ({n77_ssb_post}) (from NRFrequency table)", len(nodes_old_without_new), ", ".join(nodes_old_without_new))

                    # NR Frequency Inconsistencies: NR nodes with the N77 SSB not in (old_freq, new_freq) (from NRFrequency table)
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row("NRFrequency", "NR Frequency Inconsistencies", f"NR nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post}) (from NRFrequency table)", len(not_old_not_new_nodes), ", ".join(not_old_not_new_nodes))
                else:
                    add_row("NRFrequency", "NR Frequency Audit", "NRFrequency table has no N77 rows", 0)
            else:
                add_row("NRFrequency", "NR Frequency Audit", "NRFrequency table present but required columns missing", "N/A")
        else:
            add_row("NRFrequency", "NR Frequency Audit", "NRFrequency table", "Table not found or empty")
    except Exception as ex:
        add_row("NRFrequency", "NR Frequency Audit", "Error while checking NRFrequency", f"ERROR: {ex}")


# ----------------------------- NRFreqRelation (OLD/NEW SSB on NR rows) -----------------------------
def process_nr_freq_rel(df_nr_freq_rel, is_old, add_row, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, param_mismatch_rows_nr):
    try:
        if df_nr_freq_rel is not None and not df_nr_freq_rel.empty:
            node_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(df_nr_freq_rel, ["NRFreqRelationId"])
            gnb_col = resolve_column_case_insensitive(df_nr_freq_rel, ["GNBCUCPFunctionId"])

            if node_col and arfcn_col:
                work = df_nr_freq_rel[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str)

                n77_work = work.loc[work[arfcn_col].map(is_n77_from_string)].copy()

                if not n77_work.empty:
                    grouped = n77_work.groupby(node_col)[arfcn_col]

                    # NR Frequency Audit: NR nodes with the old N77 SSB (from NRFreqRelation table)
                    old_nodes = sorted(str(node) for node, series in grouped if any(is_old(v) for v in series))
                    add_row("NRFreqRelation", "NR Frequency Audit", f"NR nodes with the old N77 SSB ({n77_ssb_pre}) (from NRFreqRelation table)", len(old_nodes), ", ".join(old_nodes))

                    new_nodes = sorted(str(node) for node, series in grouped if any(is_new(v) for v in series))
                    add_row("NRFreqRelation", "NR Frequency Audit", f"NR nodes with the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)", len(new_nodes), ", ".join(new_nodes))

                    # NEW: node-level check old_ssb vs new_ssb presence
                    old_set = set(old_nodes)
                    new_set = set(new_nodes)

                    nodes_old_and_new = sorted(old_set & new_set)
                    add_row("NRFreqRelation", "NR Frequency Audit", f"NR nodes with both, the old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)", len(nodes_old_and_new), ", ".join(nodes_old_and_new))

                    nodes_old_without_new = sorted(old_set - new_set)
                    add_row("NRFreqRelation", "NR Frequency Audit", f"NR nodes with the old N77 SSB ({n77_ssb_pre}) but without the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)", len(nodes_old_without_new), ", ".join(nodes_old_without_new))

                    # NR Frequency Inconsistencies: NR nodes with the SSB not in ({old_ssb}, {new_ssb}) (from NRFreqRelation table)
                    not_old_not_new_nodes = sorted(str(node) for node, series in grouped if series_only_not_old_not_new(series))
                    add_row("NRFreqRelation", "NR Frequency Inconsistencies", f"NR nodes with the N77 SSB not in ({n77_ssb_pre}, {n77_ssb_post}) (from NRFreqRelation table)", len(not_old_not_new_nodes), ", ".join(not_old_not_new_nodes))

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
                    add_row("NRFreqRelation", "NR Frequency Inconsistencies", f"NR nodes with Auto-created NRFreqRelationId to new N77 SSB ({n77_ssb_post}) but not following VZ naming convention (e.g. with extra characters: 'auto_{n77_ssb_post}')", len(bad_pattern_nodes), ", ".join(bad_pattern_nodes))

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

                        add_row("NRFreqRelation", "NR Frequency Audit", f"NR cells with the old N77 SSB ({n77_ssb_pre}) and the new SSB ({n77_ssb_post}) (from NRFreqRelation table)", len(cells_both), ", ".join(cells_both))
                        add_row("NRFreqRelation", "NR Frequency Audit", f"NR cells with the old N77 SSB ({n77_ssb_pre}) but without new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)", len(cells_old_without_new), ", ".join(cells_old_without_new))

                        # Parameter equality check (ignoring ID/reference columns and helper columns)
                        cols_to_ignore = {arfcn_col, "_arfcn_int_"}
                        for name in full_n77.columns:
                            lname = str(name).lower()
                            if lname in {"nrfreqrelationid", "nrfrequencyref", "reservedby"}:
                                cols_to_ignore.add(name)

                        bad_cells_params = []

                        for cell_id in cells_both:
                            cell_rows = full_n77.loc[full_n77[cell_col].astype(str) == cell_id].copy()
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

                                    old_clean = (old_rel.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True))
                                    new_clean = (new_rel.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True))

                                    # Allign and sort rows
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
                                                param_mismatch_rows_nr.append({"Layer": "NR", "Table": "NRFreqRelation", "NodeId": node_val, "GNBCUCPFunctionId": gnb_val, "NRCellCUId": nrcell_val, "NRFreqRelationId": nrfreqrel_val, "Parameter": str(col_name), "OldSSB": n77_ssb_pre, "NewSSB": n77_ssb_post, "OldValue": "" if pd.isna(old_val) else str(old_val), "NewValue": "" if pd.isna(new_val) else str(new_val)})

                                        bad_cells_params.append(str(cell_id))
                                        break

                            else:
                                # Fallback: without NRCellRelationId, compare all block OLD vs NEW
                                old_clean = (old_rows.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True))
                                new_clean = (new_rows.drop(columns=list(cols_to_ignore), errors="ignore").drop_duplicates().reset_index(drop=True))

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
                                            param_mismatch_rows_nr.append({"Layer": "NR", "Table": "NRFreqRelation", "NodeId": node_val, "GNBCUCPFunctionId": gnb_val, "NRCellCUId": nrcell_val, "NRFreqRelationId": nrfreqrel_val, "Parameter": str(col_name), "OldSSB": n77_ssb_pre, "NewSSB": n77_ssb_post, "OldValue": "" if pd.isna(old_val) else str(old_val), "NewValue": "" if pd.isna(new_val) else str(new_val)})

                                    bad_cells_params.append(str(cell_id))

                        bad_cells_params = sorted(set(bad_cells_params))
                        add_row("NRFreqRelation", "NR Frequency Inconsistencies", f"NR cells with mismatching params between old N77 SSB ({n77_ssb_pre}) and the new N77 SSB ({n77_ssb_post}) (from NRFreqRelation table)", len(bad_cells_params), ", ".join(bad_cells_params))
                    else:
                        add_row("NRFreqRelation", "NR Frequency Audit", "NRFreqRelation cell-level check skipped (NRCellCUId/NRCellId/CellId missing)", "N/A")
                else:
                    add_row("NRFreqRelation", "NR Frequency Audit", "NRFreqRelation table has no N77 rows", 0)
            else:
                add_row("NRFreqRelation", "NR Frequency Audit", "NRFreqRelation table present but SSB/NodeId missing", "N/A")
        else:
            add_row("NRFreqRelation", "NR Frequency Audit", "NRFreqRelation table", "Table not found or empty")
    except Exception as ex:
        add_row("NRFreqRelation", "NR Frequency Audit", "Error while checking NRFreqRelation", f"ERROR: {ex}")


# ----------------------------- NRSectorCarrier (N77 + allowed ARCFN) -----------------------------
def process_nr_sector_carrier(df_nr_sector_carrier, add_row, allowed_n77_arfcn_pre_set, all_n77_arfcn_in_pre, allowed_n77_arfcn_post_set, all_n77_arfcn_in_post):
    try:
        if df_nr_sector_carrier is not None and not df_nr_sector_carrier.empty:
            node_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["NodeId"])
            arfcn_col = resolve_column_case_insensitive(df_nr_sector_carrier, ["arfcnDL"])

            if node_col and arfcn_col:
                work = df_nr_sector_carrier[[node_col, arfcn_col]].copy()
                work[node_col] = work[node_col].astype(str).str.strip()

                # N77 nodes = those having at least one ARCFN in N77 band (646600-660000)
                mask_n77 = work[arfcn_col].map(is_n77_from_string)
                n77_rows = work.loc[mask_n77].copy()

                # NR Frequency Audit: NR nodes with ARCFN in N77 band (646600-660000) (from NRSectorCarrier table)
                n77_nodes = sorted(n77_rows[node_col].astype(str).unique())
                add_row("NRSectorCarrier", "NR Frequency Audit", "NR nodes with N77 ARCFN in band (646600-660000) (from NRSectorCarrier table)", len(n77_nodes), ", ".join(n77_nodes))

                # NR nodes whose ALL N77 ARCFNs are in Pre-Retune allowed list (from NRSectorCarrier table)
                if allowed_n77_arfcn_pre_set:
                    grouped_n77 = n77_rows.groupby(node_col)[arfcn_col]
                    pre_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_arfcn_in_pre(series))
                    allowed_pre_str = ", ".join(str(v) for v in sorted(allowed_n77_arfcn_pre_set))
                    add_row("NRSectorCarrier", "NR Frequency Audit", f"NR nodes with N77 ARCFN in Pre-Retune allowed list ({allowed_pre_str}) (from NRSectorCarrier table)", len(pre_nodes), ", ".join(pre_nodes))

                # NR nodes whose ALL N77 ARCFNs are in Post-Retune allowed list (from NRSectorCarrier table)
                if allowed_n77_arfcn_post_set:
                    grouped_n77 = n77_rows.groupby(node_col)[arfcn_col]
                    post_nodes = sorted(str(node) for node, series in grouped_n77 if all_n77_arfcn_in_post(series))
                    allowed_post_str = ", ".join(str(v) for v in sorted(allowed_n77_arfcn_post_set))
                    add_row("NRSectorCarrier", "NR Frequency Audit", f"NR nodes with N77 ARCFN in Post-Retune allowed list ({allowed_post_str}) (from NRSectorCarrier table)", len(post_nodes), ", ".join(post_nodes))

                # NR Frequency Inconsistencies: NR ARCFN not in pre nor post allowed lists
                if allowed_n77_arfcn_pre_set or allowed_n77_arfcn_post_set:
                    allowed_union = set(allowed_n77_arfcn_pre_set) | set(allowed_n77_arfcn_post_set)

                    def _is_not_in_union(v: object) -> bool:
                        freq = parse_int_frequency(v)
                        return freq is not None and freq not in allowed_union

                    bad_rows = n77_rows.loc[n77_rows[arfcn_col].map(_is_not_in_union)]

                    # Unique nodes with at least one ARCFN not in pre/post allowed lists
                    bad_nodes = sorted(bad_rows[node_col].astype(str).unique())

                    # Build a unique (NodeId, ARCFN) list to avoid duplicated lines in ExtraInfo
                    unique_pairs = sorted({(str(r[node_col]).strip(), str(r[arfcn_col]).strip()) for _, r in bad_rows.iterrows()})
                    extra = "; ".join(f"{node}: {arfcn}" for node, arfcn in unique_pairs)

                    add_row("NRSectorCarrier", "NR Frequency Inconsistencies", "NR nodes with N77 ARCFN not in Pre/Post Retune allowed lists (from NRSectorCarrier table)", len(bad_nodes), extra)
                else:
                    add_row("NRSectorCarrier", "NR Frequency Inconsistencies", "NR nodes with N77 ARCFN not in Pre/Post Retune allowed lists (no pre/post allowed lists configured) (from NRSectorCarrier table)", "N/A")
            else:
                add_row("NRSectorCarrier", "NR Frequency Audit", "NRSectorCarrier table present but required columns missing", "N/A")
        else:
            add_row("NRSectorCarrier", "NR Frequency Audit", "NRSectorCarrier table", "Table not found or empty")
    except Exception as ex:
        add_row("NRSectorCarrier", "NR Frequency Audit", "Error while checking NRSectorCarrier", f"ERROR: {ex}")


# ------------------------------------- NRCellRelations --------------------------------------------
def process_nr_cell_relation(df_nr_cell_rel, _extract_freq_from_nrfreqrelationref, n77_ssb_pre, n77_ssb_post, add_row):
    try:
        if df_nr_cell_rel is not None and not df_nr_cell_rel.empty:
            node_col = resolve_column_case_insensitive(df_nr_cell_rel, ["NodeId"])
            freq_col = resolve_column_case_insensitive(df_nr_cell_rel, ["nRFreqRelationRef", "NRFreqRelationRef"])

            # # Helper embedded inside the main function
            # def _extract_freq_from_nrfreqrelationref_local(value: object) -> int | None:
            #     """Extract NRFreqRelation integer from NRCellRelation reference string."""
            #     return _extract_freq_from_nrfreqrelationref(value)

            if node_col and freq_col:
                work = df_nr_cell_rel[[node_col, freq_col]].copy()
                work[node_col] = work[node_col].astype(str).str.strip()
                work["_freq_int_"] = work[freq_col].map(_extract_freq_from_nrfreqrelationref)

                # Use your variables for pre/post frequencies
                old_ssb = n77_ssb_pre
                new_ssb = n77_ssb_post

                count_old = int((work["_freq_int_"] == old_ssb).sum())
                count_new = int((work["_freq_int_"] == new_ssb).sum())

                add_row("NRCellRelation", "NR Frequency Audit", f"NR cellRelations to old N77 SSB ({old_ssb}) (from NRCellRelation table)", count_old)
                add_row("NRCellRelation", "NR Frequency Audit", f"NR cellRelations to new N77 SSB ({new_ssb}) (from NRCellRelation table)", count_new)
            else:
                add_row("NRCellRelation", "NR Frequency Audit", "NRCellRelation table present but NodeId / nRFreqRelationRef column missing", "N/A")
        else:
            add_row("NRCellRelation", "NR Frequency Audit", "NRCellRelation table", "Table not found or empty")
    except Exception as ex:
        add_row("NRCellRelation", "NR Frequency Audit", "Error while checking NRCellRelation", f"ERROR: {ex}")
