# -*- coding: utf-8 -*-

import os
import re
from typing import List, Tuple, Optional, Dict
import pandas as pd

from src.modules.CommonMethods import (
    read_text_with_encoding,
    find_all_subnetwork_headers,
    extract_mo_from_subnetwork_line,
    parse_table_slice_from_subnetwork,
    SUMMARY_RE,
    sanitize_sheet_name,
    unique_sheet_name,
    natural_logfile_key,
)


class ConfigurationAudit:
    """
    Generates an Excel in input_dir with one sheet per *.log / *.logs / *.txt file.
    (Funcionalidad intacta.)
    """

    SUMMARY_RE = SUMMARY_RE  # keep class reference

    def __init__(self):
        pass

    # =====================================================================
    #                            PUBLIC API
    # =====================================================================
    def run(
        self,
        input_dir: str,
        module_name: Optional[str] = "",
        versioned_suffix: Optional[str] = None,
        tables_order: Optional[List[str]] = None,      # optional sheet ordering
        filter_frequencies: Optional[List[str]] = None # NEW: substrings to filter pivot columns
    ) -> str:
        """
        Main entry point: creates an Excel file with one sheet per detected table.
        Sheets are ordered according to TABLES_ORDER if provided; otherwise,
        they are sorted in a natural order by filename (Data_Collection.txt, Data_Collection(1).txt, ...).

        If 'filter_frequencies' is provided, the three added summary sheets will keep only
        those pivot *columns* whose header contains any of the provided substrings
        (case-insensitive). 'NodeId' and 'Total' are always kept.
        """
        # --- Normalize filters ---
        freq_filters = [str(f).strip() for f in (filter_frequencies or []) if str(f).strip()]

        # --- Validate the input directory ---
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")

        # --- Detect log/txt files ---
        log_files = self._find_log_files(input_dir)
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
        excel_path = os.path.join(input_dir, f"LogsCombined_{versioned_suffix}.xlsx")
        table_entries: List[Dict[str, object]] = []

        # --- Keep a per-file index to preserve order of multiple tables inside same file ---
        per_file_table_idx: Dict[str, int] = {}

        # =====================================================================
        #                PHASE 1: Parse all log/txt files
        # =====================================================================
        for path in log_files:
            base_filename = os.path.basename(path)
            lines, encoding_used = self._read_text_file(path)

            header_indices = self._find_all_subnetwork_headers(lines)

            # --- Case 1: No 'SubNetwork' header found, fallback single-table mode ---
            if not header_indices:
                header_idx = self._find_subnetwork_header_index(lines)
                mo_name_prev = self._extract_mo_name_from_previous_line(lines, header_idx)
                df, note = self._parse_log_lines(lines, forced_header_idx=header_idx)

                if encoding_used:
                    note = (note + " | " if note else "") + f"encoding={encoding_used}"
                df, note = self._cap_rows(df, note)

                idx_in_file = per_file_table_idx.get(base_filename, 0)
                per_file_table_idx[base_filename] = idx_in_file + 1

                table_entries.append({
                    "df": df,
                    "sheet_candidate": mo_name_prev if mo_name_prev else os.path.splitext(base_filename)[0],
                    "log_file": base_filename,
                    "tables_in_log": 1,
                    "note": note or "",
                    "idx_in_file": idx_in_file,  # numeric index of this table inside the same file
                })
                continue

            # --- Case 2: Multiple 'SubNetwork' headers found (multi-table log) ---
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
                df, note = self._cap_rows(df, note)

                idx_in_file = per_file_table_idx.get(base_filename, 0)
                per_file_table_idx[base_filename] = idx_in_file + 1

                table_entries.append({
                    "df": df,
                    "sheet_candidate": desired_sheet,
                    "log_file": base_filename,
                    "tables_in_log": tables_in_log,
                    "note": note or "",
                    "idx_in_file": idx_in_file,
                })

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
        used_sheet_names: set = set(["Summary"])
        for entry in table_entries:
            base_name = self._sanitize_sheet_name(str(entry["sheet_candidate"]))
            final_sheet = self._unique_sheet_name(base_name, used_sheet_names)
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
            summary_rows.append({
                "File": entry["log_file"],
                "Sheet": entry["final_sheet"],
                "Rows": int(len(df)),
                "Columns": int(df.shape[1]),
                "Separator": separator_str,
                "Encoding": encoding_str,
                "LogFile": entry["log_file"],
                "TablesInLog": entry["tables_in_log"],
            })

        # =====================================================================
        #        PHASE 4.1: Prepare pivot tables for extra summary sheets
        # =====================================================================
        # Collect dataframes for the specific MOs we need
        mo_collectors: Dict[str, List[pd.DataFrame]] = {
            "GUtranSyncSignalFrequency": [],
            "NRCellDU": [],
            "NRFrequency": [],
            "NRFreqRelation": [],
        }
        for entry in table_entries:
            mo_name = str(entry.get("sheet_candidate", "")).strip()
            if mo_name in mo_collectors:
                df_mo = entry["df"]
                if isinstance(df_mo, pd.DataFrame) and not df_mo.empty:
                    mo_collectors[mo_name].append(df_mo)

        # ---- Build pivots ----
        # Pivot GUtranSyncSignalFrequency
        df_gu_sync_signal_freq = self._concat_or_empty(mo_collectors["GUtranSyncSignalFrequency"])
        pivot_gu_sync_signal_freq = self._safe_crosstab_count(
            df=df_gu_sync_signal_freq,
            index_field="NodeId",
            columns_field="arfcn",
            add_margins=True,
            margins_name="Total",
        )
        pivot_gu_sync_signal_freq = self._apply_frequency_column_filter(pivot_gu_sync_signal_freq, freq_filters)

        # Pivot NRCellDU
        df_nr_cell_du = self._concat_or_empty(mo_collectors["NRCellDU"])
        pivot_nr_cells_du = self._safe_pivot_count(
            df=df_nr_cell_du,
            index_field="NodeId",
            columns_field="ssbFrequency",
            values_field="NRCellDUId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_cells_du = self._apply_frequency_column_filter(pivot_nr_cells_du, freq_filters)

        # Pivot NRFrequency
        df_nr_freq = self._concat_or_empty(mo_collectors["NRFrequency"])
        pivot_nr_freq = self._safe_pivot_count(
            df=df_nr_freq,
            index_field="NodeId",
            columns_field="arfcnValueNRDl",
            values_field="NRFrequencyId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_freq = self._apply_frequency_column_filter(pivot_nr_freq, freq_filters)

        # Pivot NRFreqRelation
        df_nr_freq_rel = self._concat_or_empty(mo_collectors["NRFreqRelation"])
        pivot_nr_freq_rel = self._safe_pivot_count(
            df=df_nr_freq_rel,
            index_field="NodeId",
            columns_field="NRFreqRelationId",
            values_field="NRCellCUId",
            add_margins=True,
            margins_name="Total",
        )
        pivot_nr_freq_rel = self._apply_frequency_column_filter(pivot_nr_freq_rel, freq_filters)

        # =====================================================================
        #                PHASE 5: Write the Excel file
        # =====================================================================
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            # Write Summary first
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)

            # Extra summary sheets
            pivot_gu_sync_signal_freq.to_excel(writer, sheet_name="Summary GU_SyncSignalFrequency", index=False)
            pivot_nr_cells_du.to_excel(writer, sheet_name="Summary NR_CelDU", index=False)
            pivot_nr_freq.to_excel(writer, sheet_name="Summary NR_Frequency", index=False)
            pivot_nr_freq_rel.to_excel(writer, sheet_name="Summary NR_FreqRelation", index=False)

            # Then write each table in the final determined order
            for entry in table_entries:
                entry["df"].to_excel(writer, sheet_name=entry["final_sheet"], index=False)

            # <<< NEW: color the 'Summary*' tabs in green >>>
            self._color_summary_tabs(writer, prefix="Summary", rgb_hex="00B050")

            # <<< NEW: enable filters (and freeze header row) on all sheets >>>
            self._enable_header_filters(writer, freeze_header=True)

        print(f"{module_name} Wrote Excel with {len(table_entries)} sheet(s) in: '{excel_path}'")
        return excel_path

    # =====================================================================
    #                        PRIVATE HELPERS (I/O)
    # =====================================================================
    def _find_log_files(self, folder: str) -> List[str]:
        files = []
        for name in os.listdir(folder):
            lower = name.lower()
            if lower.endswith((".log", ".logs", ".txt")):
                p = os.path.join(folder, name)
                if os.path.isfile(p):
                    files.append(p)
        files.sort()
        return files

    def _read_text_file(self, path: str) -> Tuple[List[str], Optional[str]]:
        return read_text_with_encoding(path)

    # =====================================================================
    #                        PRIVATE HELPERS (Parsing)
    # =====================================================================
    def _parse_log_lines(self, lines: List[str], forced_header_idx: Optional[int] = None) -> Tuple[pd.DataFrame, str]:
        valid = [ln for ln in lines if ln.strip() and not self.SUMMARY_RE.match(ln)]
        header_idx = forced_header_idx
        if header_idx is None:
            header_idx = self._fallback_header_index(valid, lines)
        if header_idx is None:
            return pd.DataFrame(), "No header detected"

        header_line = lines[header_idx].strip()
        any_tab = any("\t" in ln for ln in valid)
        data_sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid) else None)

        if header_line.startswith("SubNetwork"):
            header_cols = [c.strip() for c in header_line.split(",")]
        else:
            header_cols = [c.strip() for c in (header_line.split(data_sep) if data_sep else re.split(r"\s+", header_line.strip()))]
        header_cols = make_unique_columns(header_cols)

        rows: List[List[str]] = []
        for ln in lines[header_idx + 1:]:
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            parts = [p.strip() for p in (ln.split(data_sep) if data_sep else re.split(r"\s+", ln.strip()))]
            if len(parts) < len(header_cols):
                parts += [""] * (len(header_cols) - len(parts))
            elif len(parts) > len(header_cols):
                parts = parts[:len(header_cols)]
            rows.append(parts)

        df = pd.DataFrame(rows, columns=header_cols)
        df = df.replace({"nan": "", "NaN": "", "None": "", "NULL": ""}).dropna(how="all")
        for c in df.columns:
            df[c] = df[c].astype(str).str.strip()

        note = "Header=SubNetwork-comma" if header_line.startswith("SubNetwork") else (
            "Tab-separated" if data_sep == "\t" else ("Comma-separated" if data_sep == "," else "Whitespace-separated")
        )
        return df, note

    def _fallback_header_index(self, valid_lines: List[str], all_lines: List[str]) -> Optional[int]:
        any_tab = any("\t" in ln for ln in valid_lines)
        sep: Optional[str] = "\t" if any_tab else ("," if any("," in ln for ln in valid_lines) else None)
        for i, ln in enumerate(all_lines):
            if not ln.strip() or self.SUMMARY_RE.match(ln):
                continue
            if sep == "\t" and "\t" in ln:
                return i
            if sep == "," and "," in ln:
                return i
            if sep is None:
                return i
        return None

    @staticmethod
    def _find_subnetwork_header_index(lines: List[str]) -> Optional[int]:
        for i, ln in enumerate(lines):
            if ln.strip().startswith("SubNetwork"):
                return i
        return None

    @staticmethod
    def _extract_mo_name_from_previous_line(lines: List[str], header_idx: Optional[int]) -> Optional[str]:
        if header_idx is None or header_idx == 0:
            return None
        prev = lines[header_idx - 1].strip()
        if not prev:
            return None
        if "," in prev:
            last = prev.split(",")[-1].strip()
            return last or None
        toks = prev.split()
        return toks[-1].strip() if toks else None

    # =====================================================================
    #                        PRIVATE HELPERS (Sheets)
    # =====================================================================
    @staticmethod
    def _sanitize_sheet_name(name: str) -> str:
        return sanitize_sheet_name(name)

    @staticmethod
    def _unique_sheet_name(base: str, used: set) -> str:
        return unique_sheet_name(base, used)

    @staticmethod
    def _cap_rows(df: pd.DataFrame, note: str, max_rows_excel: int = 1_048_576) -> Tuple[pd.DataFrame, str]:
        if len(df) > max_rows_excel:
            df = df.iloc[:max_rows_excel, :].copy()
            note = (note + " | " if note else "") + f"Trimmed to {max_rows_excel} rows"
        return df, note

    @staticmethod
    def _find_all_subnetwork_headers(lines: List[str]) -> List[int]:
        return find_all_subnetwork_headers(lines)

    # =====================================================================
    #                     PRIVATE HELPERS (Pivots & Filters)
    # =====================================================================
    @staticmethod
    def _concat_or_empty(dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Return a single concatenated DataFrame or an empty one if none; align on common cols if needed."""
        if not dfs:
            return pd.DataFrame()
        try:
            return pd.concat(dfs, ignore_index=True)
        except Exception:
            common_cols = set.intersection(*(set(d.columns) for d in dfs)) if dfs else set()
            if not common_cols:
                return pd.DataFrame()
            dfs_aligned = [d[list(common_cols)].copy() for d in dfs]
            return pd.concat(dfs_aligned, ignore_index=True)

    def _safe_pivot_count(
            self,
            df: pd.DataFrame,
            index_field: str,
            columns_field: str,
            values_field: str,
            add_margins: bool = True,
            margins_name: str = "Total",
    ) -> pd.DataFrame:
        """
        Robust pivot builder that prevents 'Grouper for ... not 1-dimensional' errors.

        Fixes cases where:
          - multiple 'NodeId' columns exist (with spaces or suffixes)
          - index name collides with column name
          - MultiIndex columns appear (e.g., from 2-line headers)
        """

        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame({"Info": ["Table not found or empty"]})

        # --- 1) Always flatten MultiIndex (columns and index) ---
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(c).strip() for c in tup if str(c).strip()]) for tup in df.columns]
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()

        # --- 2) Reset index to avoid index/column name collisions ---
        work = df.reset_index(drop=True).copy()

        # --- 3) Normalize and deduplicate columns case-insensitively ---
        work.columns = pd.Index([str(c).strip() for c in work.columns])
        seen_lower = set()
        unique_cols = []
        for c in work.columns:
            cl = c.lower()
            if cl in seen_lower:
                continue
            seen_lower.add(cl)
            unique_cols.append(c)
        work = work[unique_cols]

        # --- 4) Case-insensitive resolver (accepts suffixes like NodeId_1) ---
        def _resolve(name: str) -> Optional[str]:
            nl = name.lower()
            for c in work.columns:
                if c.lower() == nl or c.lower().startswith(nl + "_"):
                    return c
            return None

        idx_col = _resolve(index_field)
        col_col = _resolve(columns_field)
        val_col = _resolve(values_field)

        if not all([idx_col, col_col, val_col]):
            missing = [n for n, v in [(index_field, idx_col), (columns_field, col_col), (values_field, val_col)] if v is None]
            return pd.DataFrame({
                "Info": [f"Required columns missing: {', '.join(missing)}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            })

        # --- 5) Sanitize data ---
        for col in {idx_col, col_col, val_col}:
            work[col] = work[col].astype(str).str.strip()

        try:
            piv = pd.pivot_table(
                work,
                index=idx_col,
                columns=col_col,
                values=val_col,
                aggfunc="count",
                fill_value=0,
                margins=add_margins,
                margins_name=margins_name,
            ).reset_index()

            # Flatten again in case margins cause MultiIndex
            if isinstance(piv.columns, pd.MultiIndex):
                piv.columns = [" ".join([str(x) for x in tup if str(x)]).strip() for tup in piv.columns]

            return piv

        except Exception as ex:
            return pd.DataFrame({
                "Error": [f"Pivot build failed: {ex}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            })

    def _safe_crosstab_count(
            self,
            df: pd.DataFrame,
            index_field: str,
            columns_field: str,
            add_margins: bool = True,
            margins_name: str = "Total",
    ) -> pd.DataFrame:
        """
        Build a frequency table with pd.crosstab (no 'values' needed).
        This avoids the 'not 1-dimensional' grouper error when index==values or
        when subtle duplicate headers exist.
        """
        import unicodedata
        import re

        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame({"Info": ["Table not found or empty"]})

        work = df.copy()
        if isinstance(work.columns, pd.MultiIndex):
            work.columns = ["_".join([str(c) for c in tup if str(c)]).strip() for tup in work.columns]
        if isinstance(work.index, pd.MultiIndex):
            work = work.reset_index()
        work = work.reset_index(drop=True)

        def _norm_header(s: str) -> str:
            s = "" if s is None else str(s)
            s = unicodedata.normalize("NFKC", s).replace("\ufeff", "").replace("\u200b", "").replace("\xa0", " ")
            s = re.sub(r"\s+", " ", s.strip())
            return s

        work.columns = pd.Index([_norm_header(c) for c in work.columns])

        def _canon(s: str) -> str:
            s = s.lower().replace(" ", "").replace("_", "").replace("-", "")
            return s

        # Deduplicate columns by canonical key
        seen = set()
        keep = []
        for c in work.columns:
            k = _canon(c)
            if k in seen:
                continue
            seen.add(k)
            keep.append(c)
        work = work[keep]

        # Resolver by canonical key
        def _resolve(name: str) -> Optional[str]:
            target = _canon(_norm_header(name))
            for c in work.columns:
                if _canon(c) == target:
                    return c
            for c in work.columns:
                if _canon(c).startswith(target):
                    return c
            return None

        idx_col = _resolve(index_field)
        col_col = _resolve(columns_field)
        if not idx_col or not col_col:
            missing = [n for n, v in [(index_field, idx_col), (columns_field, col_col)] if v is None]
            return pd.DataFrame({
                "Info": [f"Required columns missing: {', '.join(missing)}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            })

        # Clean data
        work[idx_col] = work[idx_col].astype(str).map(_norm_header)
        work[col_col] = work[col_col].astype(str).map(_norm_header)

        try:
            ct = pd.crosstab(
                index=work[idx_col],
                columns=work[col_col],
                dropna=False,
            ).reset_index()

            # Add margins (row totals and overall total)
            if add_margins:
                ct["Total"] = ct.drop(columns=[idx_col]).sum(axis=1)
                total_row = {idx_col: "Total"}
                for c in ct.columns:
                    if c != idx_col:
                        total_row[c] = ct[c].sum()
                ct = pd.concat([ct, pd.DataFrame([total_row])], ignore_index=True)

            return ct
        except Exception as ex:
            return pd.DataFrame({
                "Error": [f"Crosstab build failed: {ex}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            })

    @staticmethod
    def _apply_frequency_column_filter(piv: pd.DataFrame, filters: List[str]) -> pd.DataFrame:
        """
        Keep only the first (index) column, 'Total' (if present), and columns whose
        header contains any of the provided substrings (case-insensitive).
        If filters is empty/None, returns the pivot unchanged.
        """
        if not isinstance(piv, pd.DataFrame) or piv.empty or not filters:
            return piv

        # Normalize column names
        cols = [str(c) for c in piv.columns.tolist()]
        keep = []

        # First column (index after reset_index, e.g., 'NodeId')
        if cols:
            keep.append(cols[0])

        # Case-insensitive filtering
        fl = [f.lower() for f in filters if f]
        for c in cols[1:]:
            lc = c.lower()
            if c == "Total" or lc == "total":
                keep.append(c)
                continue
            if any(f in lc for f in fl):
                keep.append(c)

        # Ensure 'Total' is preserved if nothing else matched
        if len(keep) <= 1 and "Total" in cols and "Total" not in keep:
            keep.append("Total")

        try:
            return piv.loc[:, keep]
        except Exception:
            # Fallback: do not filter if selection fails for any reason
            return piv

    def _color_summary_tabs(self, writer, prefix: str = "Summary", rgb_hex: str = "00B050") -> None:
        """
        Set tab color for every worksheet whose name starts with `prefix`.
        Works with openpyxl-backed ExcelWriter.
        - rgb_hex: 6-hex RGB (e.g., '00B050' = green).
        """
        try:
            wb = writer.book  # openpyxl Workbook
            for ws in wb.worksheets:
                if ws.title.startswith(prefix):
                    # Set tab color (expects hex without '#')
                    ws.sheet_properties.tabColor = rgb_hex
        except Exception:
            # Hard-fail safe: never break file writing just for coloring tabs
            pass

    def _enable_header_filters(self, writer, freeze_header: bool = True) -> None:
        """
        Enable Excel AutoFilter on every worksheet for the used range.
        Optionally freeze the header row (row 1) so data scrolls under it.
        """
        try:
            wb = writer.book  # openpyxl Workbook
            for ws in wb.worksheets:
                # Skip empty sheets safely
                if ws.max_row < 1 or ws.max_column < 1:
                    continue

                # Define used range for the filter, from A1 to last used cell
                top_left = ws.cell(row=1, column=1).coordinate
                bottom_right = ws.cell(row=ws.max_row, column=ws.max_column).coordinate
                ws.auto_filter.ref = f"{top_left}:{bottom_right}"

                # Optionally freeze header row
                if freeze_header and ws.max_row >= 2:
                    ws.freeze_panes = "A2"
        except Exception:
            # Never fail the export just for filters
            pass


# --------- kept local to preserve current behavior (module-level helper) ----
def make_unique_columns(cols: List[str]) -> List[str]:
    """
    Ensure column names are unique by appending a numeric suffix when needed.
    """
    seen: Dict[str, int] = {}
    unique = []
    for c in cols:
        base = c or "Col"
        if base not in seen:
            seen[base] = 0
            unique.append(base)
        else:
            seen[base] += 1
            unique.append(f"{base}_{seen[base]}")
    return unique
