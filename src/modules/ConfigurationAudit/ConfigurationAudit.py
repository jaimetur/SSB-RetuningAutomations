# -*- coding: utf-8 -*-

import os
import time
import re
import shutil
import tempfile
from typing import List, Tuple, Optional, Dict
from openpyxl.styles import Font
import pandas as pd

from src.utils.utils_io import find_log_files, read_text_file, to_long_path, pretty_path
from src.utils.utils_parsing import SUMMARY_RE, find_all_subnetwork_headers, extract_mo_from_subnetwork_line, parse_table_slice_from_subnetwork, parse_log_lines, find_subnetwork_header_index, extract_mo_name_from_previous_line, cap_rows
from src.utils.utils_excel import sanitize_sheet_name, unique_sheet_name, color_summary_tabs, apply_alternating_category_row_fills, style_headers_autofilter_and_autofit
from src.utils.utils_sorting import natural_logfile_key
from src.utils.utils_pivot import safe_pivot_count, safe_crosstab_count, apply_frequency_column_filter
from src.utils.utils_dataframe import concat_or_empty
from src.utils.utils_datetime import log_phase_timer, format_duration_hms
from src.modules.Common.correction_commands_exporter import export_correction_cmd_texts, export_external_and_termpoint_commands
from .ca_summary_excel import build_summary_audit
from .ca_summary_ppt import generate_ppt_summary


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
            profiles_audit: bool = False,  # <<< NEW: enable Profiles audit logic
            show_phase_starts: bool = False,  # <<< NEW: show only START lines (no END lines)
            show_phase_timings: bool = True,  # <<< NEW: show timings as [INFO]
            slow_file_seconds_threshold: float = 10.0,  # <<< NEW: report per-file parsing when a file is slow
            slow_sheet_seconds_threshold: float = 10.0, # <<< NEW: report per-sheet when a sheet is slow
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

        Optional:
          - If profiles_audit=True, profiles tables will be collected and checked for old/new SSB replica consistency.
        """
        prefix = f"{module_name} " if module_name else ""

        _LEVEL_PREFIX_RE = re.compile(r"^\s*\[(INFO|DEBUG|WARNING|WARN|ERROR)\]\s*", re.IGNORECASE)

        def _ensure_level_prefix(message: str, level: str) -> str:
            """
            Ensure message starts with a single level prefix like '[INFO] '.
            If it already starts with any '[LEVEL]', keep it to avoid duplicates like '[INFO] [INFO] ...'.
            """
            msg = "" if message is None else str(message)
            if _LEVEL_PREFIX_RE.match(msg):
                return msg.strip()
            lvl = (level or "INFO").upper().strip()
            if lvl == "WARN":
                lvl = "WARNING"
            return f"[{lvl}] {msg}".strip()

        def _log(level: str, message: str) -> None:
            print(f"{prefix}{_ensure_level_prefix(message, level)}")

        def _log_info(message: str) -> None:
            _log("INFO", message)

        def _log_warn(message: str) -> None:
            _log("WARNING", message)

        def _make_temp_xlsx_path(final_xlsx_path: str) -> Tuple[str, str]:
            """
            Create a temp dir and a temp xlsx path. Prefer system temp to avoid OneDrive sync during write.
            Returns (tmp_dir, tmp_xlsx_path).
            """
            tmp_dir = tempfile.mkdtemp(prefix="SSB_RA_")
            tmp_name = os.path.basename(final_xlsx_path)
            tmp_xlsx = os.path.join(tmp_dir, tmp_name)
            return tmp_dir, tmp_xlsx

        def _move_into_place(tmp_path: str, final_path: str) -> None:
            """
            Move temp file into final destination.
            - Try atomic replace if possible.
            - Fallback to shutil.move (handles cross-device).
            """
            try:
                os.replace(tmp_path, final_path)
            except Exception:
                shutil.move(tmp_path, final_path)

        overall_start = time.perf_counter()

        with log_phase_timer("ConfigurationAudit", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
            # --- Normalize filters ---
            with log_phase_timer("PHASE 0.1: Normalize filters", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                freq_filters = [str(f).strip() for f in (filter_frequencies or []) if str(f).strip()]

            # --- Validate the input directory ---
            with log_phase_timer("PHASE 0.2: Validate input directory", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                if not os.path.isdir(input_dir):
                    raise NotADirectoryError(f"Invalid directory: {input_dir}")

            # <<< NEW: decide the base output folder and ensure it exists >>>
            # If output_dir is provided, all generated files (Excel/PPT) will be written there.
            # Otherwise, legacy behavior is kept and files are created under input_dir.
            with log_phase_timer("PHASE 0.3: Prepare output directory", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                base_output_dir = output_dir or input_dir
                base_output_dir_long = to_long_path(base_output_dir)
                os.makedirs(base_output_dir_long, exist_ok=True)

            # --- Detect log/txt files ---
            with log_phase_timer("PHASE 0.4: Detect log/txt files", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                log_files = find_log_files(input_dir)
                if not log_files:
                    _log_info(f"No log/txt files found in: '{pretty_path(input_dir)}'")
                    return ""

            # --- Natural sorting of files (handles '(1)', '(2)', '(10)', etc.) ---
            with log_phase_timer("PHASE 0.5: Sort files (natural order)", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                sorted_files = sorted(log_files, key=natural_logfile_key)
                file_rank: Dict[str, int] = {os.path.basename(p): i for i, p in enumerate(sorted_files)}

            # --- Build MO (table) ranking if TABLES_ORDER is provided ---
            with log_phase_timer("PHASE 0.6: Build MO rank (optional TABLES_ORDER)", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                mo_rank: Dict[str, int] = {}
                if tables_order:
                    mo_rank = {name: i for i, name in enumerate(tables_order)}

            # --- Prepare Excel output path ---
            # prefix = "ProfilesAudit" if profiles_audit else "ConfigurationAudit"
            prefix_name = "ConfigurationAudit"
            excel_path = os.path.join(base_output_dir_long, f"{prefix_name}_{versioned_suffix}.xlsx")
            excel_path_long = to_long_path(excel_path)

            table_entries: List[Dict[str, object]] = []

            # --- Keep a per-file index to preserve order of multiple tables inside same file ---
            per_file_table_idx: Dict[str, int] = {}

            # =====================================================================
            #                PHASE 1: Parse all log/txt files
            # =====================================================================
            with log_phase_timer("PHASE 1: Parse all log/txt files", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                for i, path in enumerate(log_files, start=1):
                    file_start = time.perf_counter()

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
                    else:
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

                    file_elapsed = time.perf_counter() - file_start
                    if show_phase_timings and file_elapsed >= float(slow_file_seconds_threshold):
                        _log_info(f"PHASE 1: Parse all log/txt files - Slow file parse {i}/{len(log_files)} (>{slow_file_seconds_threshold}s): '{base_filename}' took {file_elapsed:.3f}s")

            # =====================================================================
            #                PHASE 2: Determine final sorting order
            # =====================================================================
            with log_phase_timer("PHASE 2: Determine final sorting order", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
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
                    return (file_rank.get(entry["log_file"], 10 ** 9), int(entry["idx_in_file"]), 0)

                table_entries.sort(key=entry_sort_key)

            # =====================================================================
            #           PHASE 2.1: Merge repeated MO tables into one sheet
            # =====================================================================
            with log_phase_timer("PHASE 2.1: Merge repeated MO tables", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                merged_entries: List[Dict[str, object]] = []
                by_mo: Dict[str, Dict[str, object]] = {}

                for entry in table_entries:
                    mo_name = str(entry.get("sheet_candidate", "")).strip()
                    if not mo_name:
                        merged_entries.append(entry)
                        continue

                    if mo_name not in by_mo:
                        entry["log_files"] = [str(entry.get("log_file", "")).strip()]
                        entry["notes_list"] = [str(entry.get("note", "")).strip()] if str(entry.get("note", "")).strip() else []
                        by_mo[mo_name] = entry
                        merged_entries.append(entry)
                        continue

                    base = by_mo[mo_name]
                    base_df = base.get("df", pd.DataFrame())
                    new_df = entry.get("df", pd.DataFrame())

                    try:
                        base["df"] = pd.concat([base_df, new_df], ignore_index=True, sort=False)
                    except Exception:
                        base["df"] = base_df

                    lf = str(entry.get("log_file", "")).strip()
                    if lf:
                        base.setdefault("log_files", [])
                        base["log_files"].append(lf)

                    note = str(entry.get("note", "")).strip()
                    if note:
                        base.setdefault("notes_list", [])
                        if note not in base["notes_list"]:
                            base["notes_list"].append(note)

                    base["note"] = " | ".join([n for n in base.get("notes_list", []) if n])
                    base["tables_in_log"] = max(int(base.get("tables_in_log", 0)), int(entry.get("tables_in_log", 0)))

                table_entries = merged_entries


            # =====================================================================
            #                PHASE 3: Assign unique sheet names
            # =====================================================================
            with log_phase_timer("PHASE 3: Assign unique sheet names", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                used_sheet_names: set = {"Summary", "SummaryAudit"}

                for entry in table_entries:
                    base_name = sanitize_sheet_name(str(entry["sheet_candidate"]))
                    final_sheet = unique_sheet_name(base_name, used_sheet_names)
                    used_sheet_names.add(final_sheet)
                    entry["final_sheet"] = final_sheet

                candidate_to_final_sheet: Dict[str, str] = {
                    str(e.get("sheet_candidate", "")).strip(): str(e.get("final_sheet", "")).strip()
                    for e in table_entries
                    if str(e.get("sheet_candidate", "")).strip() and str(e.get("final_sheet", "")).strip()
                }

            # =====================================================================
            #                PHASE 4: Build the Summary sheet
            # =====================================================================
            with log_phase_timer("PHASE 4: Build Summary rows", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
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
                            # "File": entry["log_file"],
                            "File": ", ".join(entry.get("log_files", [entry["log_file"]])),
                            "Sheet": entry["final_sheet"],
                            "Rows": int(len(df)),
                            "Columns": int(df.shape[1]),
                            "Separator": separator_str,
                            "Encoding": encoding_str,
                            # "LogFile": entry["log_file"],
                            "LogFile": ", ".join(entry.get("log_files", [entry["log_file"]])),
                            "LogPath": pretty_path(input_dir),
                            "TablesInLog": entry["tables_in_log"],
                        }
                    )

            # =====================================================================
            #        PHASE 4.1: Prepare pivot tables for extra summary sheets
            # =====================================================================
            with log_phase_timer("PHASE 4.1: Prepare pivot tables", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
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

                    lowmid_series = pivot_df[lowmid_cols].sum(axis=1).astype(int) if lowmid_cols else pd.Series(0, index=pivot_df.index, dtype=int)
                    mmwave_series = pivot_df[mmwave_cols].sum(axis=1).astype(int) if mmwave_cols else pd.Series(0, index=pivot_df.index, dtype=int)

                    total_idx = pivot_df.columns.get_loc("Total") if "Total" in pivot_df.columns else len(pivot_df.columns)
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
                    # Consistency Checks Post Step2
                    "NRCellCU": [],
                    "EUtranFreqRelation": [],
                    "ExternalNRCellCU": [],
                    "ExternalGUtranCell": [],
                    "TermPointToGNodeB": [],
                    "TermPointToGNB": [],
                    "TermPointToENodeB": [],
                    # <<< NEW: Profiles tables collectors >>>
                    "McpcPCellNrFreqRelProfileUeCfg": [],
                    "McpcPCellProfileUeCfg": [],
                    "UlQualMcpcMeasCfg": [],
                    "McpcPSCellProfileUeCfg": [],
                    "McfbCellProfile": [],
                    "McfbCellProfileUeCfg": [],
                    "TrStSaCellProfile": [],
                    "TrStSaCellProfileUeCfg": [],
                    "TrStSaNrFreqRelProfileUeCfg": [],
                    "McpcPCellEUtranFreqRelProfile": [],
                    "McpcPCellEUtranFreqRelProfileUeCfg": [],
                    "UeMCEUtranFreqRelProfile": [],
                    "UeMCEUtranFreqRelProfileUeCfg": [],
                }
                for entry in table_entries:
                    mo_name = str(entry.get("sheet_candidate", "")).strip()
                    if mo_name in mo_collectors:
                        df_mo = entry["df"]
                        if isinstance(df_mo, pd.DataFrame) and not df_mo.empty:
                            mo_collectors[mo_name].append(df_mo)

                # ---- Build pivots ----
                df_nr_cell_du = concat_or_empty(mo_collectors["NRCellDU"])
                pivot_nr_cells_du = safe_pivot_count(df=df_nr_cell_du, index_field="NodeId", columns_field="ssbFrequency", values_field="NRCellDUId", add_margins=True, margins_name="Total")
                pivot_nr_cells_du = apply_frequency_column_filter(pivot_nr_cells_du, freq_filters)
                pivot_nr_cells_du = add_lowmid_mmwave_to_nr_celldu(pivot_nr_cells_du)

                df_nr_sector_carrier = concat_or_empty(mo_collectors["NRSectorCarrier"])
                pivot_nr_sector_carrier = safe_pivot_count(df=df_nr_sector_carrier, index_field="NodeId", columns_field="arfcnDL", values_field="NRSectorCarrierId", add_margins=True, margins_name="Total")
                pivot_nr_sector_carrier = apply_frequency_column_filter(pivot_nr_sector_carrier, freq_filters)

                df_nr_freq = concat_or_empty(mo_collectors["NRFrequency"])
                pivot_nr_freq = safe_pivot_count(df=df_nr_freq, index_field="NodeId", columns_field="arfcnValueNRDl", values_field="NRFrequencyId", add_margins=True, margins_name="Total")
                pivot_nr_freq = apply_frequency_column_filter(pivot_nr_freq, freq_filters)

                df_nr_freq_rel = concat_or_empty(mo_collectors["NRFreqRelation"])
                pivot_nr_freq_rel = safe_pivot_count(df=df_nr_freq_rel, index_field="NodeId", columns_field="NRFreqRelationId", values_field="NRCellCUId", add_margins=True, margins_name="Total")
                pivot_nr_freq_rel = apply_frequency_column_filter(pivot_nr_freq_rel, freq_filters)

                df_gu_sync_signal_freq = concat_or_empty(mo_collectors["GUtranSyncSignalFrequency"])
                pivot_gu_sync_signal_freq = safe_crosstab_count(df=df_gu_sync_signal_freq, index_field="NodeId", columns_field="arfcn", add_margins=True, margins_name="Total")
                pivot_gu_sync_signal_freq = apply_frequency_column_filter(pivot_gu_sync_signal_freq, freq_filters)

                df_gu_freq_rel = concat_or_empty(mo_collectors["GUtranFreqRelation"])
                pivot_gu_freq_rel = safe_crosstab_count(df=df_gu_freq_rel, index_field="NodeId", columns_field="GUtranFreqRelationId", add_margins=True, margins_name="Total")
                pivot_gu_freq_rel = apply_frequency_column_filter(pivot_gu_freq_rel, freq_filters)

                # Extra tables for audit logic
                df_nr_cell_rel = concat_or_empty(mo_collectors["NRCellRelation"])
                df_gu_cell_rel = concat_or_empty(mo_collectors["GUtranCellRelation"])
                df_freq_prio_nr = concat_or_empty(mo_collectors["FreqPrioNR"])
                df_endc_distr_profile = concat_or_empty(mo_collectors["EndcDistrProfile"])
                df_external_nr_cell_cu = concat_or_empty(mo_collectors["ExternalNRCellCU"])
                df_external_gutran_cell = concat_or_empty(mo_collectors["ExternalGUtranCell"])
                df_term_point_to_gnodeb = concat_or_empty(mo_collectors["TermPointToGNodeB"])
                df_term_point_to_gnb = concat_or_empty(mo_collectors["TermPointToGNB"])
                df_term_point_to_enodeb = concat_or_empty(mo_collectors["TermPointToENodeB"])

                # Extra tables for Consistency Checks Post Step2
                df_nr_cell_cu = concat_or_empty(mo_collectors["NRCellCU"])
                df_eutran_freq_rel = concat_or_empty(mo_collectors["EUtranFreqRelation"])

                # <<< NEW: Build profiles tables dict (only used when profiles_audit=True) >>>
                profile_table_names = [
                    "McpcPCellProfileUeCfg",
                    "McpcPCellNrFreqRelProfileUeCfg",
                    "UlQualMcpcMeasCfg",
                    "McpcPSCellProfileUeCfg",
                    "McfbCellProfile",
                    "McfbCellProfileUeCfg",
                    "McpcPCellEUtranFreqRelProfile",
                    "McpcPCellEUtranFreqRelProfileUeCfg",
                    "UeMCEUtranFreqRelProfile",
                    "UeMCEUtranFreqRelProfileUeCfg",
                    "TrStSaCellProfile",
                    "TrStSaCellProfileUeCfg",
                    "TrStSaNrFreqRelProfileUeCfg",
                ]
                profiles_tables: Dict[str, pd.DataFrame] = {}
                if profiles_audit:
                    for table_name in profile_table_names:
                        profiles_tables[table_name] = concat_or_empty(mo_collectors.get(table_name, []))

            # =====================================================================
            #                PHASE 4.2: Build SummaryAudit
            # =====================================================================
            with log_phase_timer("PHASE 4.2: Build SummaryAudit", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                summary_audit_df, param_mismatch_nr_df, param_mismatch_gu_df = build_summary_audit(
                    df_nr_cell_du=df_nr_cell_du,
                    df_nr_freq=df_nr_freq,
                    df_nr_freq_rel=df_nr_freq_rel,
                    df_nr_cell_rel=df_nr_cell_rel,
                    df_freq_prio_nr=df_freq_prio_nr,
                    df_gu_sync_signal_freq=df_gu_sync_signal_freq,
                    df_gu_freq_rel=df_gu_freq_rel,
                    df_gu_cell_rel=df_gu_cell_rel,
                    df_nr_sector_carrier=df_nr_sector_carrier,
                    df_endc_distr_profile=df_endc_distr_profile,
                    df_nr_cell_cu=df_nr_cell_cu,
                    df_eutran_freq_rel=df_eutran_freq_rel,
                    n77_ssb_pre=self.N77_SSB_PRE,
                    n77_ssb_post=self.N77_SSB_POST,
                    n77b_ssb=self.N77B_SSB,
                    allowed_n77_ssb_pre=self.ALLOWED_N77_SSB_PRE,
                    allowed_n77_arfcn_pre=self.ALLOWED_N77_ARFCN_PRE,
                    allowed_n77_ssb_post=self.ALLOWED_N77_SSB_POST,
                    allowed_n77_arfcn_post=self.ALLOWED_N77_ARFCN_POST,
                    df_external_nr_cell_cu=df_external_nr_cell_cu,
                    df_external_gutran_cell=df_external_gutran_cell,
                    df_term_point_to_gnodeb=df_term_point_to_gnodeb,
                    df_term_point_to_gnb=df_term_point_to_gnb,
                    df_term_point_to_enodeb=df_term_point_to_enodeb,
                    module_name=module_name,
                    profiles_tables=profiles_tables if profiles_audit else None,
                    profiles_audit=profiles_audit,
                )

            # ------------------------------------------------------------------
            # Re-inject modified audit tables back into table_entries
            # ------------------------------------------------------------------
            with log_phase_timer("PHASE 4.3: Re-inject modified tables", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                reinject_map = {
                    "NRCellDU": df_nr_cell_du,
                    "NRFrequency": df_nr_freq,
                    "NRFreqRelation": df_nr_freq_rel,
                    "NRCellRelation": df_nr_cell_rel,
                    "FreqPrioNR": df_freq_prio_nr,
                    "GUtranSyncSignalFrequency": df_gu_sync_signal_freq,
                    "GUFreqRelation": df_gu_freq_rel,
                    "GUtranCellRelation": df_gu_cell_rel,
                    "NRSectorCarrier": df_nr_sector_carrier,
                    "EndcDistrProfile": df_endc_distr_profile,
                    "NRCellCU": df_nr_cell_cu,
                    "EUtranFreqRelation": df_eutran_freq_rel,
                    "ExternalNRCellCU": df_external_nr_cell_cu,
                    "ExternalGUtranCell": df_external_gutran_cell,
                    "TermPointToGNodeB": df_term_point_to_gnodeb,
                    "TermPointToGNB": df_term_point_to_gnb,
                    "TermPointToENodeB": df_term_point_to_enodeb,
                }

                # IMPORTANT: Some MOs can appear multiple times across multiple logs/slices.
                # The aggregated dataframe (concat_or_empty) must be written only once, otherwise we duplicate full content in multiple sheets.
                reinjected_once: set[str] = set()

                for entry in table_entries:
                    sheet_name = str(entry.get("sheet_candidate", "")).strip()
                    entry["skip_write"] = False

                    if sheet_name in reinject_map:
                        if sheet_name in reinjected_once:
                            entry["skip_write"] = True
                            continue

                        entry["df"] = reinject_map[sheet_name]
                        reinjected_once.add(sheet_name)

            # =====================================================================
            #                PHASE 5: Write the Excel file
            # =====================================================================
            with log_phase_timer("PHASE 5: Write Excel", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                tmp_dir, tmp_excel_path = _make_temp_xlsx_path(excel_path_long)
                tmp_excel_path_long = to_long_path(tmp_excel_path)

                try:
                    # ------------------------------------------------------------------
                    # PHASE 5.0: Open ExcelWriter (measure open cost)
                    # ------------------------------------------------------------------
                    t_open0 = time.perf_counter()
                    _log_info(f"PHASE 5.0: ExcelWriter OPEN starting → tmp: '{pretty_path(tmp_excel_path)}'")

                    writer = None
                    writer_closed = False
                    try:
                        writer = pd.ExcelWriter(tmp_excel_path_long, engine="openpyxl")
                        t_open1 = time.perf_counter()
                        _log_info(f"PHASE 5.0: ExcelWriter OPEN done in {format_duration_hms(t_open1 - t_open0)} ({t_open1 - t_open0:.3f}s)")

                        # We emulate "with" to measure close precisely
                        # (so we can time writer.close() and/or writer.__exit__ behavior)
                        # ------------------------------------------------------------------
                        # PHASE 5.1: Write Summary + SummaryAudit + Param mismatch sheets
                        # ------------------------------------------------------------------
                        with log_phase_timer("PHASE 5.1: Write Summary + SummaryAudit", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                            # Write Summary first
                            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

                            # SummaryAudit with high-level checks
                            summary_audit_df.to_excel(writer, sheet_name="SummaryAudit", index=False)

                            # Apply alternating background colors by Category for SummaryAudit sheet
                            wb = writer.book
                            ws_summary_audit = writer.sheets.get("SummaryAudit")
                            if ws_summary_audit is not None:
                                apply_alternating_category_row_fills(ws_summary_audit, category_header="Category")

                            # New: separate NR / LTE param mismatching sheets
                            if not param_mismatch_nr_df.empty:
                                param_mismatch_nr_df.to_excel(writer, sheet_name="Summary NR Param Mismatching", index=False)

                            if not param_mismatch_gu_df.empty:
                                param_mismatch_gu_df.to_excel(writer, sheet_name="Summary LTE Param Mismatching", index=False)

                        # ------------------------------------------------------------------
                        # PHASE 5.2: Write pivot summary sheets
                        # ------------------------------------------------------------------
                        with log_phase_timer("PHASE 5.2: Write pivot summary sheets", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                            # Extra summary sheets
                            pivot_nr_cells_du.to_excel(writer, sheet_name="Summary NR_CellDU", index=False)
                            pivot_nr_sector_carrier.to_excel(writer, sheet_name="Summary NR_SectorCarrier", index=False)
                            pivot_nr_freq.to_excel(writer, sheet_name="Summary NR_Frequency", index=False)
                            pivot_nr_freq_rel.to_excel(writer, sheet_name="Summary NR_FreqRelation", index=False)
                            pivot_gu_sync_signal_freq.to_excel(writer, sheet_name="Summary GU_SyncSignalFrequency", index=False)
                            pivot_gu_freq_rel.to_excel(writer, sheet_name="Summary GU_FreqRelation", index=False)

                        # ------------------------------------------------------------------
                        # PHASE 5.3: Write parsed MO tables
                        # ------------------------------------------------------------------
                        with log_phase_timer("PHASE 5.3: Write parsed MO tables", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                            # Then write each table in the final determined order
                            written = 0
                            total_to_write = sum(1 for e in table_entries if not bool(e.get("skip_write", False)))

                            for entry in table_entries:
                                if bool(entry.get("skip_write", False)):
                                    continue

                                written += 1
                                sheet_start = time.perf_counter()
                                entry["df"].to_excel(writer, sheet_name=entry["final_sheet"], index=False)
                                sheet_elapsed = time.perf_counter() - sheet_start

                                if show_phase_timings and sheet_elapsed >= float(slow_sheet_seconds_threshold):
                                    _log_info(
                                        f"PHASE 5.3: Write parsed MO tables - Slow sheet write {written}/{total_to_write} "
                                        f"(>{slow_sheet_seconds_threshold}s): '{entry['final_sheet']}' ({entry.get('log_file', '')}) "
                                        f"took {sheet_elapsed:.3f}s"
                                    )

                        # ------------------------------------------------------------------
                        # PHASE 5.4: Style sheets (tabs, headers, autofit, hyperlinks)
                        # ------------------------------------------------------------------
                        with log_phase_timer("PHASE 5.4: Style sheets (tabs, headers, autofit, hyperlinks)", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                            # Color the 'Summary*' tabs in green
                            color_summary_tabs(writer, prefix="Summary", rgb_hex="00B050")

                            # Apply header color + auto-fit to all sheets
                            # Optimization: default autofit only scans the first N rows (handled inside style_headers_autofilter_and_autofit)
                            style_headers_autofilter_and_autofit(writer, freeze_header=True, align="left")

                            # ------------------------------------------------------------------
                            # Add hyperlinks from SummaryAudit.Category to corresponding sheets
                            # ------------------------------------------------------------------
                            ws_summary_audit = writer.sheets.get("SummaryAudit")
                            if ws_summary_audit is not None:
                                header = [cell.value for cell in ws_summary_audit[1]]
                                try:
                                    category_col_idx = header.index("Category") + 1
                                except ValueError:
                                    category_col_idx = None

                                if category_col_idx:
                                    for row in range(2, ws_summary_audit.max_row + 1):
                                        cell = ws_summary_audit.cell(row=row, column=category_col_idx)
                                        raw = str(cell.value).strip() if cell.value else ""
                                        target_sheet = raw

                                        # 1) If no exists a sheet with that name, try to resolve using the previous mapping
                                        if target_sheet and target_sheet not in writer.book.sheetnames:
                                            target_sheet = candidate_to_final_sheet.get(raw, raw)

                                        # 2) If exists a sheet with that name, create hyperlink
                                        if target_sheet and target_sheet in writer.book.sheetnames:
                                            cell.hyperlink = f"#{target_sheet}!A1"
                                            cell.font = Font(color="0563C1", underline="single")

                        # ------------------------------------------------------------------
                        # PHASE 5.5: CLOSE / FINALIZE workbook (THIS IS THE LIKELY 7 min)
                        # ------------------------------------------------------------------
                        _log_info("PHASE 5.5: ExcelWriter CLOSE starting (finalize workbook / write zip parts)...")
                        t_close0 = time.perf_counter()

                        # Optional: force a GC before close to reduce memory spikes / pauses
                        # (Leave enabled only for diagnosis; keep disabled if no benefit)
                        # import gc
                        # t_gc0 = time.perf_counter()
                        # gc.collect()
                        # _log_info(f"PHASE 5.X: gc.collect() took {time.perf_counter() - t_gc0:.3f}s")

                        writer.close()  # <-- this is where openpyxl can take minutes
                        writer_closed = True

                        t_close1 = time.perf_counter()

                        _log_info(f"PHASE 5.5: ExcelWriter CLOSE done in {format_duration_hms(t_close1 - t_close0)} ({t_close1 - t_close0:.3f}s)")

                    finally:
                        # Safety close (avoid leaked handles if something fails mid-write)
                        try:
                            if writer is not None and not writer_closed:
                                # If already closed, this is a no-op / may raise; ignore.
                                writer.close()
                        except Exception:
                            pass

                    # ----------------------------------------------------------------------
                    # PHASE 5.6: Inspect temp file size after close (helps diagnose)
                    # ----------------------------------------------------------------------
                    try:
                        tmp_size = os.path.getsize(tmp_excel_path_long)
                        _log_info(f"PHASE 5.6: Temp XLSX size: {tmp_size / (1024 * 1024):.2f} MB")
                    except Exception as ex:
                        _log_warn(f"PHASE 5.6: Could not read temp XLSX size: {ex}")

                    # ----------------------------------------------------------------------
                    # PHASE 5.7: Move Excel into final destination (prefer atomic replace)
                    # ----------------------------------------------------------------------
                    with log_phase_timer("PHASE 5.7: Move Excel into destination", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                        # Move into final destination (prefer atomic replace)
                        _move_into_place(tmp_excel_path_long, excel_path_long)

                finally:
                    shutil.rmtree(tmp_dir, ignore_errors=True)

            _log_info(f"Wrote Excel with {len(table_entries)} sheet(s) in: '{pretty_path(excel_path)}'")

            # =====================================================================
            #                PHASE 6: Export Correction Commands (ConfigurationAudit)
            # =====================================================================
            with log_phase_timer("PHASE 6: Export Correction Commands", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                correction_cmd_sources = {}
                if df_nr_cell_rel is not None and not df_nr_cell_rel.empty:
                    correction_cmd_sources["NR_disc"] = df_nr_cell_rel
                if df_gu_cell_rel is not None and not df_gu_cell_rel.empty:
                    correction_cmd_sources["GU_disc"] = df_gu_cell_rel
                if correction_cmd_sources:
                    export_correction_cmd_texts(base_output_dir_long, correction_cmd_sources, base_folder_name="Correction_Cmd")
                export_external_and_termpoint_commands(excel_path_long, base_output_dir_long, base_folder_name="Correction_Cmd")


            # =====================================================================
            #                PHASE 7: Generate PPT textual summary
            # =====================================================================
            with log_phase_timer("PHASE 7: Generate PPT summary", log_fn=_log_info, show_start=show_phase_starts, show_end=False, show_timing=show_phase_timings, line_prefix="", start_level="INFO", end_level="INFO", timing_level="INFO"):
                try:
                    ppt_path = generate_ppt_summary(summary_audit_df, excel_path, module_name)
                    if ppt_path:
                        _log_info(f"PPT summary generated in: '{pretty_path(ppt_path)}'")
                except Exception as ex:
                    # Never fail the whole module just for PPT creation
                    _log_warn(f"PPT summary generation failed: {ex}")

            overall_elapsed = time.perf_counter() - overall_start
            if show_phase_timings:
                _log_info(f"TOTAL ConfigurationAudit.run took {overall_elapsed:.3f}s")

        return excel_path
