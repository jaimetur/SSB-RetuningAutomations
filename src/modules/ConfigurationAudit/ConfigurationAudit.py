# -*- coding: utf-8 -*-

import os
from typing import List, Tuple, Optional, Dict
import pandas as pd

from src.utils.utils_io import find_log_files, read_text_file, to_long_path, pretty_path
from src.utils.utils_parsing import SUMMARY_RE, find_all_subnetwork_headers, extract_mo_from_subnetwork_line, parse_table_slice_from_subnetwork, parse_log_lines, find_subnetwork_header_index, extract_mo_name_from_previous_line, cap_rows
from src.utils.utils_excel import sanitize_sheet_name, unique_sheet_name, color_summary_tabs, apply_alternating_category_row_fills, style_headers_autofilter_and_autofit
from src.utils.utils_sorting import natural_logfile_key
from src.utils.utils_pivot import concat_or_empty, safe_pivot_count, safe_crosstab_count, apply_frequency_column_filter
from .ca_excel_summary import build_summary_audit
from .ca_ppt_summary import generate_ppt_summary


class ConfigurationAudit:
    """
    Generates an Excel in input_dir with one sheet per *.log / *.logs / *.txt file.
    (Functionality kept, extended with SummaryAudit sheet and PPT summary.)

    ARFCN-related parameters (N77, etc.) are now configurable via __init__:
      - new_arfcn            → main "new" NR / LTE ARFCN (e.g. 648672)
      - old_arfcn            → main "old" NR / LTE ARFCN (e.g. 647328)
      - allowed_n77_ssb      → allowed SSB values for N77 (e.g. {648672, 653952})
      - allowed_n77_arfcn    → allowed ARFCN values for N77 sectors
    """

    SUMMARY_RE = SUMMARY_RE  # keep class reference

    def __init__(
        self,
        n77_ssb_pre: int,
        n77_ssb_post: int,
        allowed_n77_ssb_pre: Optional[set[int]] = None,
        allowed_n77_arfcn_pre: Optional[set[int]] = None,
        allowed_n77_ssb_post: Optional[set[int]] = None,
        allowed_n77_arfcn_post: Optional[set[int]] = None,
        n77b_ssb_arfcn: Optional[int] = None
    ):
        """
        Initialize ConfigurationAudit with ARFCN-related parameters.

        All values are converted to integers/sets of integers internally to make checks robust.
        """
        # Core ARFCN values
        self.N77_SSB_PRE: int = int(n77_ssb_pre)
        self.N77_SSB_POST: int = int(n77_ssb_post)
        self.N77B_SSB: int = int(n77b_ssb_arfcn)

        # Allowed SSB (Pre) values for N77 cells (e.g. {648672, 653952})
        if allowed_n77_ssb_pre is None:
            self.ALLOWED_N77_SSB_PRE = set()
        else:
            self.ALLOWED_N77_SSB_PRE = {int(v) for v in allowed_n77_ssb_pre}

        # Allowed ARFCN (Pre) values for N77 sectors (e.g. {654652, 655324, 655984, 656656})
        if allowed_n77_arfcn_pre is None:
            self.ALLOWED_N77_ARFCN_PRE = set()
        else:
            self.ALLOWED_N77_ARFCN_PRE = {int(v) for v in allowed_n77_arfcn_pre}


        # Allowed SSB (Post) values for N77 cells (e.g. {648672, 653952})
        if allowed_n77_ssb_post is None:
            self.ALLOWED_N77_SSB_POST = set()
        else:
            self.ALLOWED_N77_SSB_POST = {int(v) for v in allowed_n77_ssb_post}

        # Allowed ARFCN (Post) values for N77 sectors (e.g. {654652, 655324, 655984, 656656})
        if allowed_n77_arfcn_post is None:
            self.ALLOWED_N77_ARFCN_POST = set()
        else:
            self.ALLOWED_N77_ARFCN_POST = {int(v) for v in allowed_n77_arfcn_post}

    # =====================================================================
    #                            PUBLIC API
    # =====================================================================
    def run(
            self,
            input_dir: str,
            module_name: Optional[str] = "",
            versioned_suffix: Optional[str] = None,
            tables_order: Optional[List[str]] = None,  # optional sheet ordering
            filter_frequencies: Optional[List[str]] = None,  # substrings to filter pivot columns
            output_dir: Optional[str] = None,  # <<< NEW: optional dedicated output folder
    ) -> str:
        """
        Main entry point: creates an Excel file with one sheet per detected table.
        Sheets are ordered according to TABLES_ORDER if provided; otherwise,
        they are sorted in a natural order by filename (Data_Collection.txt, Data_Collection(1).txt, ...).

        If 'filter_frequencies' is provided, the three added summary sheets will keep only
        those pivot *columns* whose header contains any of the provided substrings
        (case-insensitive). 'NodeId' and 'Total' are always kept.

        In addition, a 'SummaryAudit' sheet is created with high-level checks
        across the parsed tables, and a PowerPoint (.pptx) summary is generated
        with a textual bullet-style overview per category.
        """
        # --- Normalize filters ---
        freq_filters = [str(f).strip() for f in (filter_frequencies or []) if str(f).strip()]

        # --- Validate the input directory ---
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")

        # <<< NEW: decide the base output folder and ensure it exists >>>
        # If output_dir is provided, all generated files (Excel/PPT) will be written there.
        # Otherwise, legacy behavior is kept and files are created under input_dir.
        base_output_dir = output_dir or input_dir
        base_output_dir_long = to_long_path(base_output_dir)
        os.makedirs(base_output_dir_long, exist_ok=True)

        # --- Detect log/txt files ---
        log_files = find_log_files(input_dir)
        if not log_files:
            raise FileNotFoundError(f"No .log/.logs/.txt files found in: {input_dir}")

        # --- Natural sorting of files (handles '(1)', '(2)', '(10)', etc.) ---
        sorted_files = sorted(log_files, key=natural_logfile_key)
        file_rank: Dict[str, int] = {os.path.basename(p): i for i, p in enumerate(sorted_files)}

        # --- Build MO (table) ranking if TABLES_ORDER is provided ---
        mo_rank: Dict[str, int] = {}
        if tables_order:
            mo_rank = {name: i for i, name in enumerate(tables_order)}

        # --- Prepare Excel output path ---
        excel_path = os.path.join(base_output_dir_long, f"ConfigurationAudit_{versioned_suffix}.xlsx")
        excel_path_long = to_long_path(excel_path)

        table_entries: List[Dict[str, object]] = []

        # --- Keep a per-file index to preserve order of multiple tables inside same file ---
        per_file_table_idx: Dict[str, int] = {}

        # =====================================================================
        #                PHASE 1: Parse all log/txt files
        # =====================================================================
        for path in log_files:
            base_filename = os.path.basename(path)
            lines, encoding_used = read_text_file(path)

            header_indices = find_all_subnetwork_headers(lines)

            # Case 1: no 'SubNetwork' header found, fallback single-table mode
            if not header_indices:
                header_idx = find_subnetwork_header_index(lines, self.SUMMARY_RE)
                df, note = parse_log_lines(lines, self.SUMMARY_RE, forced_header_idx=header_idx)
                mo_name_prev = extract_mo_name_from_previous_line(lines, header_idx)

                if encoding_used:
                    note = (note + " | " if note else "") + f"encoding={encoding_used}"
                df, note = cap_rows(df, note)

                idx_in_file = per_file_table_idx.get(base_filename, 0)
                per_file_table_idx[base_filename] = idx_in_file + 1

                table_entries.append(
                    {
                        "df": df,
                        "sheet_candidate": mo_name_prev if mo_name_prev else os.path.splitext(base_filename)[0],
                        "log_file": base_filename,
                        "tables_in_log": 1,
                        "note": note or "",
                        "idx_in_file": idx_in_file,  # numeric index of this table inside the same file
                    }
                )
                continue

            # Case 2: multiple 'SubNetwork' headers found (multi-table log)
            tables_in_log = len(header_indices)
            header_indices.append(len(lines))  # add sentinel index

            for ix in range(tables_in_log):
                h = header_indices[ix]
                nxt = header_indices[ix + 1]
                mo_name_from_line = extract_mo_from_subnetwork_line(lines[h])
                desired_sheet = mo_name_from_line if mo_name_from_line else os.path.splitext(base_filename)[0]

                df = parse_table_slice_from_subnetwork(lines, h, nxt)
                note = "Slice parsed"
                if encoding_used:
                    note += f" | encoding={encoding_used}"
                df, note = cap_rows(df, note)

                idx_in_file = per_file_table_idx.get(base_filename, 0)
                per_file_table_idx[base_filename] = idx_in_file + 1

                table_entries.append(
                    {
                        "df": df,
                        "sheet_candidate": desired_sheet,
                        "log_file": base_filename,
                        "tables_in_log": tables_in_log,
                        "note": note or "",
                        "idx_in_file": idx_in_file,
                    }
                )

        # =====================================================================
        #                PHASE 2: Determine final sorting order
        # =====================================================================
        def entry_sort_key(entry: Dict[str, object]) -> Tuple[int, int, int]:
            """
            Final sorting key for Excel sheets:
              - If TABLES_ORDER exists → sort by table order first, then by file (natural), then by table index
              - Otherwise → sort only by file (natural) and table index
            """
            if tables_order:
                mo = str(entry["sheet_candidate"]).strip()
                mo_pos = mo_rank.get(mo, len(mo_rank) + 1)
                return (mo_pos, file_rank.get(entry["log_file"], 10 ** 9), int(entry["idx_in_file"]))
            else:
                return (file_rank.get(entry["log_file"], 10 ** 9), int(entry["idx_in_file"]), 0)

        table_entries.sort(key=entry_sort_key)

        # =====================================================================
        #                PHASE 3: Assign unique sheet names
        # =====================================================================
        used_sheet_names: set = {"Summary"}
        for entry in table_entries:
            base_name = sanitize_sheet_name(str(entry["sheet_candidate"]))
            final_sheet = unique_sheet_name(base_name, used_sheet_names)
            used_sheet_names.add(final_sheet)
            entry["final_sheet"] = final_sheet

        # =====================================================================
        #                PHASE 4: Build the Summary sheet
        # =====================================================================
        summary_rows: List[Dict[str, object]] = []
        for entry in table_entries:
            note = str(entry.get("note", ""))
            separator_str, encoding_str = "", ""

            # Split "Header=..., | encoding=..." into two separate columns
            if note:
                parts = [p.strip() for p in note.split("|")]
                for part in parts:
                    pl = part.lower()
                    if pl.startswith("header=") or "separated" in pl:
                        separator_str = part
                    elif pl.startswith("encoding="):
                        encoding_str = part.replace("encoding=", "")

            df: pd.DataFrame = entry["df"]
            summary_rows.append(
                {
                    "File": entry["log_file"],
                    "Sheet": entry["final_sheet"],
                    "Rows": int(len(df)),
                    "Columns": int(df.shape[1]),
                    "Separator": separator_str,
                    "Encoding": encoding_str,
                    "LogFile": entry["log_file"],
                    "LogPath": pretty_path(input_dir),
                    "TablesInLog": entry["tables_in_log"],
                }
            )

        # =====================================================================
        #        PHASE 4.1: Prepare pivot tables for extra summary sheets
        # =====================================================================
        # Local Helper to add columns LowMidBand/mmWave to Summary NR_CellDU
        def add_lowmid_mmwave_to_nr_celldu(pivot_df: pd.DataFrame) -> pd.DataFrame:
            """
            Add LowMidBand and mmWave columns to Summary NR_CellDU pivot.

            Logic:
              - Columns whose header (int) is in [2_000_000, 2_300_000] are mmWave SSBs.
              - Other numeric SSB columns are LowMidBand SSBs.
              - For each NodeId we count how many cells it has for each band.
              - Columns are inserted just before 'Total'.
            """
            if pivot_df is None or pivot_df.empty:
                return pivot_df

            cols = list(pivot_df.columns)
            base_cols = {"NodeId", "Total", "LowMidBand", "mmWave"}

            ssb_cols: list[str] = [c for c in cols if c not in base_cols]

            mmwave_cols: list[str] = []
            lowmid_cols: list[str] = []
            for col in ssb_cols:
                try:
                    ssb_val = int(str(col))
                except ValueError:
                    # Non-numeric headers are ignored
                    continue
                if 2_000_000 <= ssb_val <= 2_300_000:
                    mmwave_cols.append(col)
                else:
                    lowmid_cols.append(col)

            if lowmid_cols:
                lowmid_series = pivot_df[lowmid_cols].sum(axis=1).astype(int)
            else:
                lowmid_series = pd.Series(0, index=pivot_df.index, dtype=int)

            if mmwave_cols:
                mmwave_series = pivot_df[mmwave_cols].sum(axis=1).astype(int)
            else:
                mmwave_series = pd.Series(0, index=pivot_df.index, dtype=int)

            if "Total" in pivot_df.columns:
                total_idx = pivot_df.columns.get_loc("Total")
            else:
                total_idx = len(pivot_df.columns)

            pivot_df.insert(total_idx, "LowMidBand", lowmid_series)
            pivot_df.insert(total_idx + 1, "mmWave", mmwave_series)

            return pivot_df

        # Collect dataframes for the specific MOs we need
        mo_collectors: Dict[str, List[pd.DataFrame]] = {
            "NRFrequency": [],
            "NRFreqRelation": [],
            "NRSectorCarrier": [],
            "NRCellDU": [],
            "NRCellRelation": [],
            "GUtranSyncSignalFrequency": [],
            "GUtranFreqRelation": [],
            "GUtranCellRelation": [],
            "FreqPrioNR": [],
            "EndcDistrProfile": [],
        }
        for entry in table_entries:
            mo_name = str(entry.get("sheet_candidate", "")).strip()
            if mo_name in mo_collectors:
                df_mo = entry["df"]
                if isinstance(df_mo, pd.DataFrame) and not df_mo.empty:
                    mo_collectors[mo_name].append(df_mo)

        # ---- Build pivots ----
        # Pivot NRCellDU
        df_nr_cell_du = concat_or_empty(mo_collectors["NRCellDU"])
        pivot_nr_cells_du = safe_pivot_count(
            df=df_nr_cell_du,
            index_field="NodeId",
            columns_field="ssbFrequency",
            values_field="NRCellDUId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_cells_du = apply_frequency_column_filter(pivot_nr_cells_du, freq_filters)
        pivot_nr_cells_du = add_lowmid_mmwave_to_nr_celldu(pivot_nr_cells_du)

        # Pivot NRSectorCarrier
        df_nr_sector_carrier = concat_or_empty(mo_collectors["NRSectorCarrier"])
        pivot_nr_sector_carrier = safe_pivot_count(
            df=df_nr_sector_carrier,
            index_field="NodeId",
            columns_field="arfcnDL",
            values_field="NRSectorCarrierId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_sector_carrier = apply_frequency_column_filter(pivot_nr_sector_carrier, freq_filters)

        # Pivot NRFrequency
        df_nr_freq = concat_or_empty(mo_collectors["NRFrequency"])
        pivot_nr_freq = safe_pivot_count(
            df=df_nr_freq,
            index_field="NodeId",
            columns_field="arfcnValueNRDl",
            values_field="NRFrequencyId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_freq = apply_frequency_column_filter(pivot_nr_freq, freq_filters)

        # Pivot NRFreqRelation
        df_nr_freq_rel = concat_or_empty(mo_collectors["NRFreqRelation"])
        pivot_nr_freq_rel = safe_pivot_count(
            df=df_nr_freq_rel,
            index_field="NodeId",
            columns_field="NRFreqRelationId",
            values_field="NRCellCUId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_freq_rel = apply_frequency_column_filter(pivot_nr_freq_rel, freq_filters)

        # Pivot GUtranSyncSignalFrequency
        df_gu_sync_signal_freq = concat_or_empty(mo_collectors["GUtranSyncSignalFrequency"])
        pivot_gu_sync_signal_freq = safe_crosstab_count(
            df=df_gu_sync_signal_freq,
            index_field="NodeId",
            columns_field="arfcn",
            add_margins=True,
            margins_name="Total",
        )
        pivot_gu_sync_signal_freq = apply_frequency_column_filter(pivot_gu_sync_signal_freq, freq_filters)
        # pivot_gu_sync_signal_freq = add_lowmid_mmwave_to_nr_celldu(pivot_gu_sync_signal_freq)

        # Pivot GUtranFreqRelation
        df_gu_freq_rel = concat_or_empty(mo_collectors["GUtranFreqRelation"])
        pivot_gu_freq_rel = safe_crosstab_count(
            df=df_gu_freq_rel,
            index_field="NodeId",
            columns_field="GUtranFreqRelationId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_gu_sync_signal_freq = apply_frequency_column_filter(pivot_gu_sync_signal_freq, freq_filters)

        # Extra tables for audit logic
        df_nr_cell_rel = concat_or_empty(mo_collectors["NRCellRelation"])
        df_gu_cell_rel = concat_or_empty(mo_collectors["GUtranCellRelation"])
        df_freq_prio_nr = concat_or_empty(mo_collectors["FreqPrioNR"])
        df_endc_distr_profile = concat_or_empty(mo_collectors["EndcDistrProfile"])

        # =====================================================================
        #                PHASE 4.2: Build SummaryAudit
        # =====================================================================
        summary_audit_df, param_mismatch_nr_df, param_mismatch_gu_df = build_summary_audit(
            df_nr_cell_du=df_nr_cell_du,
            df_nr_freq=df_nr_freq,
            df_nr_freq_rel=df_nr_freq_rel,
            df_nr_cell_rel=df_nr_cell_rel,
            df_nr_sector_carrier=df_nr_sector_carrier,
            df_freq_prio_nr=df_freq_prio_nr,
            df_gu_sync_signal_freq=df_gu_sync_signal_freq,
            df_gu_freq_rel=df_gu_freq_rel,
            df_gu_cell_rel=df_gu_cell_rel,
            df_endc_distr_profile=df_endc_distr_profile,
            n77_ssb_pre=self.N77_SSB_PRE,
            n77_ssb_post=self.N77_SSB_POST,
            n77b_ssb=self.N77B_SSB,
            allowed_n77_ssb_pre=self.ALLOWED_N77_SSB_PRE,
            allowed_n77_arfcn_pre=self.ALLOWED_N77_ARFCN_PRE,
            allowed_n77_ssb_post=self.ALLOWED_N77_SSB_POST,
            allowed_n77_arfcn_post=self.ALLOWED_N77_ARFCN_POST,
        )

        # =====================================================================
        #                PHASE 5: Write the Excel file
        # =====================================================================
        with pd.ExcelWriter(excel_path_long, engine="openpyxl") as writer:
            # Write Summary first
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

            # SummaryAudit with high-level checks
            summary_audit_df.to_excel(writer, sheet_name="SummaryAudit", index=False)
            # Apply alternating background colors by Category for SummaryAudit sheet
            wb = writer.book
            ws_summary_audit = writer.sheets.get("SummaryAudit")
            if ws_summary_audit is not None:
                apply_alternating_category_row_fills(ws_summary_audit, category_header="Category")

            # Extra summary sheets
            pivot_nr_cells_du.to_excel(writer, sheet_name="Summary NR_CellDU", index=False)
            pivot_nr_sector_carrier.to_excel(writer, sheet_name="Summary NR_SectorCarrier", index=False)
            pivot_nr_freq.to_excel(writer, sheet_name="Summary NR_Frequency", index=False)
            pivot_nr_freq_rel.to_excel(writer, sheet_name="Summary NR_FreqRelation", index=False)
            pivot_gu_sync_signal_freq.to_excel(writer, sheet_name="Summary GU_SyncSignalFrequency", index=False)
            pivot_gu_freq_rel.to_excel(writer, sheet_name="Summary GU_FreqRelation", index=False)

            # New: separate NR / LTE param mismatching sheets
            if not param_mismatch_nr_df.empty:
                param_mismatch_nr_df.to_excel(writer, sheet_name="Summary NR Param Mismatching", index=False)

            if not param_mismatch_gu_df.empty:
                param_mismatch_gu_df.to_excel(writer, sheet_name="Summary LTE Param Mismatching", index=False)

            # Then write each table in the final determined order
            for entry in table_entries:
                entry["df"].to_excel(writer, sheet_name=entry["final_sheet"], index=False)

            # Color the 'Summary*' tabs in green
            color_summary_tabs(writer, prefix="Summary", rgb_hex="00B050")

            # Apply header color + auto-fit to all sheets
            style_headers_autofilter_and_autofit(writer, freeze_header=True, align="left")

        print(f"{module_name} Wrote Excel with {len(table_entries)} sheet(s) in: '{pretty_path(excel_path)}'")

        # =====================================================================
        #                PHASE 6: Generate PPT textual summary
        # =====================================================================
        try:
            ppt_path = generate_ppt_summary(summary_audit_df, excel_path, module_name)
            if ppt_path:
                print(f"{module_name} PPT summary generated in: '{pretty_path(ppt_path)}'")
        except Exception as ex:
            # Never fail the whole module just for PPT creation
            print(f"{module_name} [WARN] PPT summary generation failed: {ex}")

        return excel_path

