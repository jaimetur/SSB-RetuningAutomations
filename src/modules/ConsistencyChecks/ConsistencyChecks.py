# -*- coding: utf-8 -*-

import os
import re
from typing import Dict, Optional, List

import pandas as pd

from src.utils.utils_dataframe import select_latest_by_date, normalize_df, make_index_by_keys
from src.utils.utils_datetime import extract_date
from src.utils.utils_excel import color_summary_tabs, style_headers_autofilter_and_autofit
from src.utils.utils_frequency import detect_freq_column, detect_key_columns, extract_gu_freq_base, extract_nr_freq_base, enforce_gu_columns, enforce_nr_columns
from src.utils.utils_io import read_text_lines, to_long_path
from src.utils.utils_parsing import find_all_subnetwork_headers, extract_mo_from_subnetwork_line, parse_table_slice_from_subnetwork


class ConsistencyChecks:
    """
    Loads and compares GU/NR relation tables before (Pre) and after (Post) a refarming process.
    (Se mantiene la funcionalidad exacta.)
    """
    PRE_TOKENS = ("pre", "step0")
    POST_TOKENS = ("post", "step3")
    DATE_RE = re.compile(r"(?P<date>(19|20)\d{6})")  # yyyymmdd
    SUMMARY_RE = re.compile(r"^\s*\d+\s+instance\(s\)\s*$", re.IGNORECASE)

    def __init__(self, n77_ssb_pre: Optional[str] = None, n77_ssb_post: Optional[str] = None) -> None:
        # NEW: store N77 SSB frequencies for Pre and Post
        self.n77_ssb_pre: Optional[str] = n77_ssb_pre
        self.n77_ssb_post: Optional[str] = n77_ssb_post

        self.tables: Dict[str, pd.DataFrame] = {}
        # NEW: flags to signal whether at least one Pre/Post folder was found
        self.pre_folder_found: bool = False
        self.post_folder_found: bool = False
        # NEW: keep only per-table/per-side source file paths to be used exclusively in Summary (do not store them in DataFrames)
        self._source_paths: Dict[str, Dict[str, List[tuple]]] = {
            "GUtranCellRelation": {"Pre": [], "Post": []},
            "NRCellRelation": {"Pre": [], "Post": []},
        }

    # --------- folder helpers ---------
    @staticmethod
    def _detect_prepost(folder_name: str) -> Optional[str]:
        name = folder_name.lower()
        if any(tok in name for tok in ConsistencyChecks.PRE_TOKENS):
            return "Pre"
        if any(tok in name for tok in ConsistencyChecks.POST_TOKENS):
            return "Post"
        return None

    @staticmethod
    def _insert_front_columns(df: pd.DataFrame, prepost: str, date_str: Optional[str]) -> pd.DataFrame:
        df = df.copy()
        df.insert(0, "Pre/Post", prepost)
        df.insert(1, "Date", date_str if date_str else "")
        return df

    @staticmethod
    def _table_key_name(table_base: str) -> str:
        return table_base.strip()

    # ----------------------------- LOADING ----------------------------- #
    def loadPrePost(self, input_dir_or_pre: str, post_dir: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Load Pre/Post either from:
          - Legacy mode: a single parent folder that contains 'Pre' and 'Post' subfolders (post_dir=None), or
          - Dual-input mode: two explicit folders (pre_dir, post_dir) passed in.

        Returns a dict with concatenated GU/NR DataFrames in self.tables.
        """
        if post_dir is None:
            # ===== Legacy single-folder mode (original behavior) =====
            input_dir = input_dir_or_pre
            if not os.path.isdir(input_dir):
                raise NotADirectoryError(f"Invalid directory: {input_dir}")

            collected: Dict[str, List[pd.DataFrame]] = {"GUtranCellRelation": [], "NRCellRelation": []}
            self.pre_folder_found = False
            self.post_folder_found = False

            for entry in os.scandir(input_dir):
                if not entry.is_dir():
                    continue
                prepost = self._detect_prepost(entry.name)
                if not prepost:
                    continue
                if prepost == "Pre":
                    self.pre_folder_found = True
                elif prepost == "Post":
                    self.post_folder_found = True

                date_str = extract_date(entry.name)

                for fname in os.listdir(entry.path):
                    lower = fname.lower()
                    if not (lower.endswith(".log") or lower.endswith(".txt")):
                        continue
                    fpath = os.path.join(entry.path, fname)
                    if not os.path.isfile(fpath):
                        continue

                    lines = read_text_lines(fpath)
                    if not lines:
                        continue

                    headers = find_all_subnetwork_headers(lines)
                    if not headers:
                        continue
                    headers.append(len(lines))

                    for i in range(len(headers) - 1):
                        h, nxt = headers[i], headers[i + 1]
                        mo = extract_mo_from_subnetwork_line(lines[h])
                        if mo not in ("GUtranCellRelation", "NRCellRelation"):
                            continue

                        df = parse_table_slice_from_subnetwork(lines, h, nxt)
                        if df is None or df.empty:
                            continue

                        df = self._insert_front_columns(df, prepost, date_str)
                        # NEW: store file path only for Summary; do not persist it inside DataFrames
                        self._source_paths.setdefault(mo, {}).setdefault(prepost, []).append((date_str or "", fpath))
                        collected[mo].append(df)

            self.tables = {}
            for base, chunks in collected.items():
                if chunks:
                    self.tables[self._table_key_name(base)] = pd.concat(chunks, ignore_index=True)

            if not self.pre_folder_found:
                print(f"[INFO] 'Pre' folder not found under: {input_dir}. Returning to GUI.")
            if not self.post_folder_found:
                print(f"[INFO] 'Post' folder not found under: {input_dir}. Returning to GUI.")
            if not self.tables:
                print(f"[WARNING] No GU/NR tables were loaded from: {input_dir}.")

            return self.tables

        else:
            # ===== Dual-input mode: explicit PRE/POST folders =====
            pre_dir = input_dir_or_pre
            if not os.path.isdir(pre_dir):
                raise NotADirectoryError(f"Invalid PRE directory: {pre_dir}")
            if not os.path.isdir(post_dir):
                raise NotADirectoryError(f"Invalid POST directory: {post_dir}")

            collected: Dict[str, List[pd.DataFrame]] = {"GUtranCellRelation": [], "NRCellRelation": []}
            self.pre_folder_found = True
            self.post_folder_found = True

            def _collect_from(dir_path: str, prepost: str):
                date_str = extract_date(os.path.basename(dir_path))
                for fname in os.listdir(dir_path):
                    lower = fname.lower()
                    if not (lower.endswith(".log") or lower.endswith(".txt")):
                        continue
                    fpath = os.path.join(dir_path, fname)
                    if not os.path.isfile(fpath):
                        continue

                    lines = read_text_lines(fpath)
                    if not lines:
                        continue

                    headers = find_all_subnetwork_headers(lines)
                    if not headers:
                        continue
                    headers.append(len(lines))

                    for i in range(len(headers) - 1):
                        h, nxt = headers[i], headers[i + 1]
                        mo = extract_mo_from_subnetwork_line(lines[h])
                        if mo not in ("GUtranCellRelation", "NRCellRelation"):
                            continue

                        df = parse_table_slice_from_subnetwork(lines, h, nxt)
                        if df is None or df.empty:
                            continue

                        df = self._insert_front_columns(df, prepost, date_str)
                        # NEW: store file path only for Summary; do not persist it inside DataFrames
                        self._source_paths.setdefault(mo, {}).setdefault(prepost, []).append((date_str or "", fpath))
                        collected[mo].append(df)

            _collect_from(pre_dir, "Pre")
            _collect_from(post_dir, "Post")

            self.tables = {}
            for base, chunks in collected.items():
                if chunks:
                    self.tables[self._table_key_name(base)] = pd.concat(chunks, ignore_index=True)

            if not self.tables:
                print(f"[WARNING] No GU/NR tables were loaded from: {pre_dir} and {post_dir}.")

            return self.tables

    # ----------------------------- COMPARISON ----------------------------- #
    def comparePrePost(self, freq_before: str, freq_after: str, module_name: Optional[str] = "") -> Dict[str, Dict[str, pd.DataFrame]]:
        if not self.tables:
            # Soft fail: do not raise, just inform and return empty results
            print(f"{module_name} [WARNING] No tables loaded. Skipping comparison (likely missing Pre/Post folders).")
            return {}

        results: Dict[str, Dict[str, pd.DataFrame]] = {}

        for table_name, df_all in self.tables.items():
            if df_all.empty:
                continue

            freq_col = detect_freq_column(table_name, list(df_all.columns))
            if not freq_col:
                print(f"{module_name} [WARNING] No frequency column detected in {table_name}. Adjust mapping if needed.")
                continue

            key_cols = detect_key_columns(table_name, list(df_all.columns), freq_col)
            if not key_cols:
                print(f"{module_name} [WARNING] No key column detected in {table_name}.")
                continue

            # Forzar claves estables si existen
            if table_name == "GUtranCellRelation":
                forced = [c for c in ["NodeId", "EUtranCellFDDId", "GUtranCellRelationId"] if c in df_all.columns]
                if forced:
                    key_cols = forced
            elif table_name == "NRCellRelation":
                forced = [c for c in ["NodeId", "NRCellCUId", "NRCellRelationId"] if c in df_all.columns]
                if forced:
                    key_cols = forced

            pre_df_full = select_latest_by_date(df_all, "Pre")
            post_df_full = select_latest_by_date(df_all, "Post")

            if pre_df_full.empty and post_df_full.empty:
                continue

            # Pick representative source file for Summary from internal paths (no DF column)
            def _pick_src(tbl: str, side: str, target_date: str) -> str:
                pool = self._source_paths.get(tbl, {}).get(side, [])
                # Prefer exact date match (latest set), else first available
                for d, p in pool:
                    if d == target_date:
                        return p
                return pool[0][1] if pool else ""

            pre_date = pre_df_full["Date"].max() if not pre_df_full.empty and "Date" in pre_df_full.columns else ""
            post_date = post_df_full["Date"].max() if not post_df_full.empty and "Date" in post_df_full.columns else ""
            pre_source_file = _pick_src(table_name, "Pre", pre_date)
            post_source_file = _pick_src(table_name, "Post", post_date)

            pre_norm = normalize_df(pre_df_full)
            post_norm = normalize_df(post_df_full)

            pre_idx = make_index_by_keys(pre_norm, key_cols)
            post_idx = make_index_by_keys(post_norm, key_cols)

            pre_keys, post_keys = set(pre_idx.index), set(post_idx.index)
            common_idx = sorted(pre_keys & post_keys)

            new_in_post = post_idx.loc[sorted(post_keys - pre_keys)].copy()
            missing_in_post = pre_idx.loc[sorted(pre_keys - post_keys)].copy()

            pre_common = pre_idx.loc[common_idx]
            post_common = post_idx.loc[common_idx]

            def slim(df: pd.DataFrame, keep_cols: List[str]) -> pd.DataFrame:
                cols = ["Pre/Post", "Date"] + list(dict.fromkeys(keep_cols))
                cols = [c for c in cols if c in df.columns]
                return df[cols].copy()

            pre_slim = slim(pre_common, key_cols + [freq_col])
            post_slim = slim(post_common, key_cols + [freq_col])

            # Freq base
            if table_name == "NRCellRelation":
                pre_freq_base = extract_nr_freq_base(pre_slim.get(freq_col, pd.Series("", index=pre_slim.index)))
                post_freq_base = extract_nr_freq_base(post_slim.get(freq_col, pd.Series("", index=post_slim.index)))
            else:
                pre_freq_base = extract_gu_freq_base(pre_slim.get(freq_col, pd.Series("", index=pre_slim.index)))
                post_freq_base = extract_gu_freq_base(post_slim.get(freq_col, pd.Series("", index=post_slim.index)))

            fb, fa = str(freq_before).strip(), str(freq_after).strip()
            pre_has_before = (pre_freq_base == fb)
            pre_has_after = (pre_freq_base == fa)
            post_is_after = (post_freq_base == fa)
            freq_rule_mask = (pre_has_before & (~post_is_after)) | (pre_has_after & (~post_is_after))

            exclude_cols = {"Pre/Post", "Date", freq_col} | set(key_cols)
            shared_cols = [c for c in pre_common.columns if c in post_common.columns and c not in exclude_cols]

            any_diff_mask = pd.Series(False, index=pre_common.index)
            diff_cols_per_row = {k: [] for k in pre_common.index}
            for c in shared_cols:
                diffs = (pre_common[c] != post_common[c]).reindex(pre_common.index, fill_value=False)
                any_diff_mask = any_diff_mask | diffs
                for k in pre_common.index[diffs]:
                    diff_cols_per_row[k].append(c)

            combined_mask = (freq_rule_mask | any_diff_mask).reindex(pre_common.index, fill_value=False)
            discrepancy_keys = [k for k, m in zip(pre_common.index, combined_mask) if m and k in set(common_idx)]

            # Build discrepancies
            def desired_key_order(tbl: str) -> list:
                if tbl == "GUtranCellRelation":
                    return ["NodeId", "EUtranCellFDDId", "GUtranCellRelationId"]
                if tbl == "NRCellRelation":
                    return ["NodeId", "NRCellCUId", "NRCellRelationId"]
                return []

            def reorder_cols(df: pd.DataFrame, tbl: str) -> pd.DataFrame:
                if df is None or df.empty:
                    return df
                front = ["Date_Pre", "Date_Post", "Freq_Pre", "Freq_Post"]
                keys = [c for c in desired_key_order(tbl) if c in df.columns]
                seen = set(front + keys)
                rest = [c for c in df.columns if c not in seen]
                return df[[*(c for c in front if c in df.columns), *keys, *rest]]

            rows = []
            for k in discrepancy_keys:
                row = {}
                for c in key_cols:
                    row[c] = pre_common.loc[k, c] if c in pre_common.columns else ""
                row["Date_Pre"] = pre_slim.loc[k, "Date"] if "Date" in pre_slim.columns else ""
                row["Date_Post"] = post_slim.loc[k, "Date"] if "Date" in post_slim.columns else ""
                row["Freq_Pre"] = pre_slim.get(freq_col, pd.Series("", index=pre_slim.index)).loc[k] if k in pre_slim.index else ""
                row["Freq_Post"] = post_slim.get(freq_col, pd.Series("", index=post_slim.index)).loc[k] if k in post_slim.index else ""

                required_cols = (
                    ["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]
                    if table_name == "GUtranCellRelation" else
                    ["NodeId", "NRCellCUId", "NRCellRelationId"]
                )
                for rc in required_cols:
                    val = ""
                    if rc in post_common.columns:
                        val = post_common.loc[k, rc]
                    elif rc in pre_common.columns:
                        val = pre_common.loc[k, rc]
                    row[rc] = val

                difflist = diff_cols_per_row.get(k, [])
                row["DiffColumns"] = ", ".join(sorted(difflist))
                for c in difflist:
                    row[f"{c}_Pre"] = pre_common.loc[k, c]
                    row[f"{c}_Post"] = post_common.loc[k, c]

                rows.append(row)

            discrepancies = pd.DataFrame(rows)
            discrepancies = reorder_cols(discrepancies, table_name)

            if not new_in_post.empty:
                for col in new_in_post.columns:
                    new_in_post[col] = new_in_post[col].astype(str)
            if not missing_in_post.empty:
                for col in missing_in_post.columns:
                    missing_in_post[col] = missing_in_post[col].astype(str)

            # --- minimal replacement for new/missing frequency pairing ---
            def with_freq_pair(df_src: pd.DataFrame, tbl: str, kind: str) -> pd.DataFrame:
                """
                Build a light table for pair counting:
                  - Compute base frequency from freq_col
                  - For 'new': set Freq_Pre="" and Freq_Post=base
                  - For 'missing': set Freq_Pre=base and Freq_Post=""
                  - Drop only meta columns ('Pre/Post', 'Date'); keep freq_col if present
                """
                if df_src is None or df_src.empty:
                    return df_src

                df_tmp = df_src.copy()

                # Ensure string dtype for safe operations
                for col in df_tmp.columns:
                    df_tmp[col] = df_tmp[col].astype(str)

                # Compute base frequency using the proper extractor
                if tbl == "NRCellRelation":
                    base = extract_nr_freq_base(df_tmp.get(freq_col, pd.Series("", index=df_tmp.index)))
                else:
                    base = extract_gu_freq_base(df_tmp.get(freq_col, pd.Series("", index=df_tmp.index)))

                # Assign Freq_Pre/Freq_Post according to kind
                if kind == "new":
                    # New in Post: Pre side must be empty, Post side carries the base
                    df_tmp["Freq_Pre"] = ""
                    df_tmp["Freq_Post"] = base
                elif kind == "missing":
                    # Missing in Post: Pre side carries the base, Post side must be empty
                    df_tmp["Freq_Pre"] = base
                    df_tmp["Freq_Post"] = ""
                else:
                    # Fallback (should not happen)
                    df_tmp["Freq_Pre"] = ""
                    df_tmp["Freq_Post"] = ""

                # Drop only meta columns; keep freq_col for reference (harmless)
                df_tmp = df_tmp.drop(columns=[c for c in ["Pre/Post", "Date"] if c in df_tmp.columns], errors="ignore")
                return df_tmp

            # Build cleaned tables for pair counting
            new_in_post_clean = with_freq_pair(new_in_post, table_name, kind="new")
            missing_in_post_clean = with_freq_pair(missing_in_post, table_name, kind="missing")

            # Pair stats
            pair_stats = pd.DataFrame({
                "Freq_Pre": pre_freq_base.reindex(pre_common.index).fillna("").replace("", "<empty>"),
                "Freq_Post": post_freq_base.reindex(pre_common.index).fillna("").replace("", "<empty>"),
                "ParamDiff": any_diff_mask.reindex(pre_common.index).astype(bool),
                "FreqDiff": freq_rule_mask.reindex(pre_common.index).astype(bool),
            }, index=pre_common.index)

            # all_relations (merge último PRE/POST, manteniendo Freq_Pre/Freq_Post)
            pre_latest = pre_norm.copy()
            post_latest = post_norm.copy()
            if table_name == "NRCellRelation":
                pre_fb = extract_nr_freq_base(pre_latest.get(freq_col, pd.Series("", index=pre_latest.index)))
                post_fb = extract_nr_freq_base(post_latest.get(freq_col, pd.Series("", index=post_latest.index)))
            else:
                pre_fb = extract_gu_freq_base(pre_latest.get(freq_col, pd.Series("", index=pre_latest.index)))
                post_fb = extract_gu_freq_base(post_latest.get(freq_col, pd.Series("", index=post_latest.index)))
            pre_latest = pre_latest.assign(Freq_Pre=pre_fb.replace("", "<empty>"))
            post_latest = post_latest.assign(Freq_Post=post_fb.replace("", "<empty>"))

            def keys_first(df: pd.DataFrame) -> pd.DataFrame:
                if df is None or df.empty:
                    return df
                ko = [c for c in key_cols if c in df.columns]
                rest = [c for c in df.columns if c not in ko]
                return df[ko + rest]

            pre_keep = keys_first(pre_latest.drop(columns=["Pre/Post", "Date"], errors="ignore"))
            post_keep = keys_first(post_latest.drop(columns=["Pre/Post", "Date"], errors="ignore"))

            merged_all = pd.merge(pre_keep, post_keep, on=key_cols, how="outer", suffixes=("_PreSide", "_PostSide"))
            all_relations = merged_all[key_cols].copy()
            all_relations["Freq_Pre"] = merged_all.get("Freq_Pre", "")
            all_relations["Freq_Post"] = merged_all.get("Freq_Post", "")

            for col in set(pre_keep.columns) | set(post_keep.columns):
                if col in key_cols or col in ("Freq_Pre", "Freq_Post"):
                    continue
                pre_col = f"{col}_PreSide"
                post_col = f"{col}_PostSide"
                if post_col in merged_all.columns:
                    all_relations[col] = merged_all[post_col].where(merged_all[post_col].astype(str) != "",
                                                                   merged_all[pre_col] if pre_col in merged_all.columns else "")
                elif pre_col in merged_all.columns:
                    all_relations[col] = merged_all[pre_col]
                elif col in merged_all.columns:
                    all_relations[col] = merged_all[col]

            results[table_name] = {
                "discrepancies": discrepancies.reset_index(drop=True),
                "new_in_post": new_in_post_clean.reset_index(drop=True),
                "missing_in_post": missing_in_post_clean.reset_index(drop=True),
                "pair_stats": pair_stats.reset_index(drop=True),
                "all_relations": all_relations.reset_index(drop=True),
                "meta": {
                    "key_cols": key_cols,
                    "freq_col": freq_col,
                    "pre_rows": int(pre_df_full.shape[0]),
                    "post_rows": int(post_df_full.shape[0]),
                    "pre_source_file": pre_source_file,  # NEW
                    "post_source_file": post_source_file,  # NEW
                },
            }

            print(f"\n{module_name} === {table_name} ===")
            print(f"{module_name} Key: {key_cols} | Freq column: {freq_col}")
            print(f"{module_name} - Discrepancies: {len(discrepancies)}")
            print(f"{module_name} - New Relations in Post: {len(new_in_post_clean)}")
            print(f"{module_name} - Missing Relations in Post: {len(missing_in_post_clean)}")

        return results

    # ----------------------------- HELPERS FOR OUTPUT ----------------------------- #
    def add_correction_command_gu_new(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add 'Correction_Cmd' column for GU_new sheet, using existing row fields.

        Format example:
          647328\tdel EUtranCellFDD=<EUtranCellFDDId>,GUtranFreqRelation=<GUtranFreqRelationId>,GUtranCellRelation=<GUtranCellRelationId>
        If any of the required fields is missing/empty, an empty string is used.
        """
        if df is None or df.empty:
            df = df.copy()
            df["Correction_Cmd"] = ""
            return df

        df = df.copy()

        def build_command(row: pd.Series) -> str:
            # Prefer Freq_Post for GU_new rows, fall back to Freq_Pre if needed
            freq_val = str(row.get("Freq_Post") or row.get("Freq_Pre") or "").strip()
            eu_cell = str(row.get("EUtranCellFDDId") or "").strip()
            freq_rel = str(row.get("GUtranFreqRelationId") or "").strip()
            cell_rel = str(row.get("GUtranCellRelationId") or "").strip()

            if not (freq_val and eu_cell and freq_rel and cell_rel):
                return ""

            return f"del EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}"

        df["Correction_Cmd"] = df.apply(build_command, axis=1)
        return df

    def add_correction_command_nr_new(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add 'Correction_Cmd' column for NR_new sheet, using existing row fields.

        Format example:
          del NRCellCU=<NRCellCUId>,NRCellRelation=<NRCellRelationId>
        If any of the required fields is missing/empty, an empty string is used.
        """
        if df is None or df.empty:
            df = df.copy()
            df["Correction_Cmd"] = ""
            return df

        df = df.copy()

        def build_command(row: pd.Series) -> str:
            nr_cell_cu = str(row.get("NRCellCUId") or "").strip()
            nr_cell_rel = str(row.get("NRCellRelationId") or "").strip()

            if not (nr_cell_cu and nr_cell_rel):
                return ""

            return f"del NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}"

        df["Correction_Cmd"] = df.apply(build_command, axis=1)
        return df

    def add_correction_command_gu_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add 'Correction_Cmd' column for GU_missing sheet, building a multiline correction script.

        All placeholders are taken from the row itself (no external parameters).
        """
        if df is None or df.empty:
            df = df.copy()
            df["Correction_Cmd"] = ""
            return df

        df = df.copy()

        def build_command(row: pd.Series) -> str:
            enb_func = str(row.get("ENodeBFunctionId") or "").strip()
            eu_cell = str(row.get("EUtranCellFDDId") or "").strip()
            freq_rel = str(row.get("GUtranFreqRelationId") or "").strip()
            cell_rel = str(row.get("GUtranCellRelationId") or "").strip()
            neighbor_ref = str(row.get("neighborCellRef") or "").strip()
            is_endc = str(row.get("isEndcAllowed") or "").strip()
            is_ho = str(row.get("isHoAllowed") or "").strip()
            is_remove = str(row.get("isRemoveAllowed") or "").strip()
            is_voice_ho = str(row.get("isVoiceHoAllowed") or "").strip()
            user_label = str(row.get("userLabel") or "").strip()
            coverage = str(row.get("coverageIndicator") or "").strip()

            # Overwrite GUtranFreqRelationId to a hardcoded value (new SSB) only when old SSB (648672) is found
            if self.n77_ssb_pre and freq_rel.startswith(self.n77_ssb_pre):
                freq_rel = f"{self.n77_ssb_post}-30-20-0-1" if self.n77_ssb_post else freq_rel

            if not user_label:
                # Safe default label if none is provided in the row
                user_label = "SSBretune"

            # If core identifiers are missing, do not generate the command
            if not (enb_func and eu_cell and freq_rel and cell_rel):
                return ""

            # NEW: keep only GUtraNetwork / ExternalGNodeBFunction / ExternalGUtranCell part
            clean_neighbor_ref = neighbor_ref
            if "GUtraNetwork=" in neighbor_ref:
                pos = neighbor_ref.find("GUtraNetwork=")
                clean_neighbor_ref = neighbor_ref[pos:]

            parts = [
                f"crn ENodeBFunction={enb_func},EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}",
                f"neighborCellRef {clean_neighbor_ref}" if clean_neighbor_ref else "",
                f"isEndcAllowed {is_endc}" if is_endc else "",
                f"isHoAllowed {is_ho}" if is_ho else "",
                f"isRemoveAllowed {is_remove}" if is_remove else "",
                f"isVoiceHoAllowed {is_voice_ho}" if is_voice_ho else "",
                f"userlabel {user_label}",
                "end",
                f"set EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel} coverageIndicator {coverage}" if coverage else f"set EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}"
            ]

            # Keep non-empty lines only, preserving the line breaks
            lines = [p for p in parts if p]
            return "\n".join(lines)

        df["Correction_Cmd"] = df.apply(build_command, axis=1)
        return df

    def add_correction_command_nr_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add 'Correction_Cmd' column for NR_missing sheet, building a multiline correction script.

        All placeholders are taken from the row itself (no external parameters).
        """
        if df is None or df.empty:
            df = df.copy()
            df["Correction_Cmd"] = ""
            return df

        df = df.copy()

        def build_command(row: pd.Series) -> str:
            nr_cell_cu = str(row.get("NRCellCUId") or "").strip()
            nr_cell_rel = str(row.get("NRCellRelationId") or "").strip()
            coverage = str(row.get("coverageIndicator") or "").strip()
            is_ho = str(row.get("isHoAllowed") or "").strip()
            is_remove = str(row.get("isRemoveAllowed") or "").strip()
            s_cell_candidate = str(row.get("sCellCandidate") or "").strip()
            nrcell_ref = str(row.get("nRCellRef") or "").strip()
            nrfreq_ref = str(row.get("nRFreqRelationRef") or "").strip()

            # If core identifiers are missing, do not generate the command
            if not (nr_cell_cu and nr_cell_rel):
                return ""

            # --------- nRCellRef cleanup: keep everything from GNBCUCPFunction= ---------
            clean_nrcell_ref = ""
            if "GNBCUCPFunction=" in nrcell_ref:
                clean_nrcell_ref = nrcell_ref[nrcell_ref.find("GNBCUCPFunction="):]

            # --------- nRFreqRelationRef cleanup ---------
            clean_nrfreq_ref = ""
            if "GNBCUCPFunction=" in nrfreq_ref:
                sub = nrfreq_ref[nrfreq_ref.find("GNBCUCPFunction="):]
                gnb_part = sub.split(",", 1)[0]
                gnb_val = gnb_part.split("=", 1)[1] if "=" in gnb_part else ""

                m_nr_cell = re.search(r"NRCellCU=([^,]+)", sub)
                m_freq = re.search(r"NRFreqRelation=([^,]+)", sub)
                nr_cell_for_freq = m_nr_cell.group(1) if m_nr_cell else ""
                freq_id = m_freq.group(1) if m_freq else ""

                # NEW: replace old SSB (Pre) with Post SSB using class attributes
                if freq_id == self.n77_ssb_pre:
                    freq_id = self.n77_ssb_post

                if gnb_val and nr_cell_for_freq and freq_id:
                    clean_nrfreq_ref = f"GNBCUCPFunction={gnb_val},NRCellCU={nr_cell_for_freq},NRFreqRelation={freq_id}"

            parts = [
                f"crn NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}",
                f"nRCellRef {clean_nrcell_ref}" if clean_nrcell_ref else "",
                f"nRFreqRelationRef {clean_nrfreq_ref}" if clean_nrfreq_ref else "",
                f"isHoAllowed {is_ho}" if is_ho else "",
                f"isRemoveAllowed {is_remove}" if is_remove else "",
                "end",
                f"set NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel} coverageIndicator {coverage}" if coverage else f"set NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}",
                f"set NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel} sCellCandidate {s_cell_candidate}" if s_cell_candidate else ""
            ]

            lines = [p for p in parts if p]
            return "\n".join(lines)

        df["Correction_Cmd"] = df.apply(build_command, axis=1)
        return df

    # ----------------------------- CORRECTION COMMNADS TO TXT ----------------------------- #
    def export_correction_cmd_texts(self, output_dir: str, dfs_by_category: Dict[str, pd.DataFrame]) -> int:
        """
        Export Correction_Cmd values to text files grouped by NodeId and category.

        For each category (e.g. GU_missing, NR_new), one file per NodeId is created in:
          <output_dir>/Correction_Cmd/<NodeId>_<Category>.txt

        Each file contains all non-empty Correction_Cmd blocks for that NodeId and category,
        separated by a blank line.
        """
        base_dir = os.path.join(output_dir, "Correction_Cmd")
        os.makedirs(base_dir, exist_ok=True)

        total_files = 0  # Counter for generated command files

        for category, df in dfs_by_category.items():
            if df is None or df.empty:
                continue
            if "NodeId" not in df.columns or "Correction_Cmd" not in df.columns:
                continue

            # Ensure string types to avoid issues when grouping/writing
            work = df.copy()
            work["NodeId"] = work["NodeId"].astype(str).str.strip()
            work["Correction_Cmd"] = work["Correction_Cmd"].astype(str)

            for node_id, group in work.groupby("NodeId"):
                node_str = str(node_id).strip()
                if not node_str:
                    continue

                cmds = [cmd for cmd in group["Correction_Cmd"] if cmd.strip()]
                if not cmds:
                    continue

                file_name = f"{node_str}_{category}.txt"
                file_path = os.path.join(base_dir, file_name)

                # Convert path to long-path form on Windows to avoid MAX_PATH issues
                file_path_long = to_long_path(file_path)

                # One command block per entry, separated by a blank line
                with open(file_path_long, "w", encoding="utf-8") as f:
                    f.write("\n\n".join(cmds))

                total_files += 1

        return total_files

    # ----------------------------- OUTPUT TO EXCEL ----------------------------- #
    def save_outputs_excel(self, output_dir: str, results: Optional[Dict[str, Dict[str, pd.DataFrame]]] = None, versioned_suffix: Optional[str] = None) -> None:
        import os
        os.makedirs(output_dir, exist_ok=True)
        suf = f"_{versioned_suffix}" if versioned_suffix else ""

        # Path for output files
        excel_all = os.path.join(output_dir, f"CellRelation{suf}.xlsx")
        excel_disc = os.path.join(output_dir, f"ConsistencyChecks_CellRelation{suf}.xlsx")

        # Convert paths to long-path form on Windows to avoid MAX_PATH issues
        excel_all_long = to_long_path(excel_all)
        excel_disc_long = to_long_path(excel_disc)

        with pd.ExcelWriter(excel_all_long, engine="openpyxl") as writer:
            if "GUtranCellRelation" in self.tables:
                self.tables["GUtranCellRelation"].to_excel(writer, sheet_name="GU_all", index=False)
            if "NRCellRelation" in self.tables:
                self.tables["NRCellRelation"].to_excel(writer, sheet_name="NR_all", index=False)

        with pd.ExcelWriter(excel_disc_long, engine="openpyxl") as writer:
            # Summary
            summary_rows = []
            if results:
                for name, bucket in results.items():
                    meta = bucket.get("meta", {})
                    pair_stats = bucket.get("pair_stats", pd.DataFrame())
                    params_disc = int(pair_stats["ParamDiff"].sum()) if not pair_stats.empty else 0
                    freq_disc = int(pair_stats["FreqDiff"].sum()) if not pair_stats.empty else 0
                    summary_rows.append({
                        "Table": name,
                        "KeyColumns": ", ".join(meta.get("key_cols", [])),
                        "FreqColumn": meta.get("freq_col", "N/A"),
                        "Relations_Pre": meta.get("pre_rows", 0),
                        "Relations_Post": meta.get("post_rows", 0),
                        "Parameters_Discrepancies": params_disc,
                        "Frequency_Discrepancies": freq_disc,
                        "New_Relations": len(bucket.get("new_in_post", pd.DataFrame())),
                        "Missing_Relations": len(bucket.get("missing_in_post", pd.DataFrame())),
                        "SourceFile_Pre": meta.get("pre_source_file", ""),  # NEW (at the end)
                        "SourceFile_Post": meta.get("post_source_file", ""),  # NEW (at the end)
                    })

            summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
                columns=[
                    "Table", "KeyColumns", "FreqColumn", "Relations_Pre", "Relations_Post",
                    "Parameters_Discrepancies", "Frequency_Discrepancies", "New_Relations", "Missing_Relations",
                    "SourceFile_Pre", "SourceFile_Post"  # NEW
                ]
            )

            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # Summary_Detailed
            detailed_rows = []
            if results:
                for name, bucket in results.items():
                    meta = bucket.get("meta", {})
                    pair_stats = bucket.get("pair_stats", pd.DataFrame())
                    new_df = bucket.get("new_in_post", pd.DataFrame())
                    miss_df = bucket.get("missing_in_post", pd.DataFrame())

                    def count_side(side: str) -> Dict[str, int]:
                        tbl = select_latest_by_date(self.tables.get(name, pd.DataFrame()), side)
                        if tbl is None or tbl.empty:
                            return {}
                        if name == "NRCellRelation":
                            col = next((c for c in tbl.columns if c.lower() == "nrfreqrelationref"), None)
                            if col:
                                ser = tbl[col].astype(str).str.extract(r"NRFreqRelation\s*=\s*(\d+)", expand=False).fillna("")
                            else:
                                ser = (tbl[meta.get("freq_col")].astype(str).str.split("-", n=1).str[0]
                                       if meta.get("freq_col") in tbl.columns else pd.Series("", index=tbl.index))
                        else:
                            ser = (tbl[meta.get("freq_col")].astype(str).str.split("-", n=1).str[0]
                                   if meta.get("freq_col") in tbl.columns else pd.Series("", index=tbl.index))
                        return ser.fillna("").replace("", "<empty>").value_counts().to_dict()

                    pre_counts = count_side("Pre")
                    post_counts = count_side("Post")

                    if not pair_stats.empty:
                        grp = pair_stats.groupby(["Freq_Pre", "Freq_Post"], dropna=False)
                        params_by_pair = grp["ParamDiff"].sum().astype(int).to_dict()
                        freq_by_pair = grp["FreqDiff"].sum().astype(int).to_dict()
                        pairs_present = set(grp.size().index.tolist())
                    else:
                        params_by_pair, freq_by_pair, pairs_present = {}, {}, set()

                    def pair_counts(df_pairs: pd.DataFrame) -> Dict[tuple, int]:
                        if df_pairs is None or df_pairs.empty:
                            return {}
                        df_pairs = df_pairs.copy()
                        for col in ("Freq_Pre", "Freq_Post"):
                            if col not in df_pairs.columns:
                                df_pairs[col] = "<empty>"
                            df_pairs[col] = df_pairs[col].fillna("").replace("", "<empty>")
                        return df_pairs.groupby(["Freq_Pre", "Freq_Post"]).size().astype(int).to_dict()

                    new_by_pair = pair_counts(new_df)
                    miss_by_pair = pair_counts(miss_df)

                    neutral_pairs = {(f, f) for f in (set(pre_counts.keys()) | set(post_counts.keys()))}
                    all_pairs = set(params_by_pair) | set(freq_by_pair) | set(new_by_pair) | set(miss_by_pair) | neutral_pairs | pairs_present

                    for (fpre, fpost) in sorted(all_pairs, key=lambda t: (t[0], t[1])):
                        detailed_rows.append({
                            "Table": name,
                            "KeyColumns": ", ".join(meta.get("key_cols", [])),
                            "FreqColumn": meta.get("freq_col", "N/A"),
                            "Freq_Pre": fpre,
                            "Freq_Post": fpost,
                            "Relations_Pre": int(pre_counts.get(fpre, 0)),
                            "Relations_Post": int(post_counts.get(fpost, 0)),
                            "Parameters_Discrepancies": int(params_by_pair.get((fpre, fpost), 0)),
                            "Freq_Discrepancies": int(freq_by_pair.get((fpre, fpost), 0)),
                            "New_Relations": int(new_by_pair.get((fpre, fpost), 0)),
                            "Missing_Relations": int(miss_by_pair.get((fpre, fpost), 0)),
                        })

            detailed_df = pd.DataFrame(detailed_rows) if detailed_rows else pd.DataFrame(
                columns=[
                    "Table", "KeyColumns", "FreqColumn", "Freq_Pre", "Freq_Post",
                    "Relations_Pre", "Relations_Post", "Parameters_Discrepancies", "Freq_Discrepancies",
                    "New_Relations", "Missing_Relations"
                ]
            )

            detailed_df.to_excel(writer, sheet_name="Summary_Detailed", index=False)

            # Collect all dataframes that contain Correction_Cmd to export them later
            correction_cmd_sources: Dict[str, pd.DataFrame] = {}

            # GU sheets
            if results and "GUtranCellRelation" in results:
                b = results["GUtranCellRelation"]
                gu_disc_df = enforce_gu_columns(b.get("discrepancies"))
                gu_missing_df = enforce_gu_columns(b.get("missing_in_post"))
                gu_new_df = enforce_gu_columns(b.get("new_in_post"))
                # NEW: add correction commands
                gu_new_df = self.add_correction_command_gu_new(gu_new_df)
                gu_missing_df = self.add_correction_command_gu_missing(gu_missing_df)
                # NEW: register GU dataframes with Correction_Cmd for text export
                correction_cmd_sources["GU_missing"] = gu_missing_df
                correction_cmd_sources["GU_new"] = gu_new_df

                gu_disc_df.to_excel(writer, sheet_name="GU_disc", index=False)
                gu_missing_df.to_excel(writer, sheet_name="GU_missing", index=False)
                gu_new_df.to_excel(writer, sheet_name="GU_new", index=False)
                b.get("all_relations", pd.DataFrame()).to_excel(writer, sheet_name="GU_relations", index=False)
            else:
                enforce_gu_columns(pd.DataFrame()).to_excel(writer, sheet_name="GU_disc", index=False)
                empty_gu_missing_df = self.add_correction_command_gu_missing(enforce_gu_columns(pd.DataFrame()))
                empty_gu_new_df = self.add_correction_command_gu_new(enforce_gu_columns(pd.DataFrame()))
                empty_gu_missing_df.to_excel(writer, sheet_name="GU_missing", index=False)
                empty_gu_new_df.to_excel(writer, sheet_name="GU_new", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="GU_relations", index=False)

            # NR sheets
            if results and "NRCellRelation" in results:
                b = results["NRCellRelation"]
                nr_disc_df = enforce_nr_columns(b.get("discrepancies"))
                nr_missing_df = enforce_nr_columns(b.get("missing_in_post"))
                nr_new_df = enforce_nr_columns(b.get("new_in_post"))
                # NEW: add correction commands
                nr_new_df = self.add_correction_command_nr_new(nr_new_df)
                nr_missing_df = self.add_correction_command_nr_missing(nr_missing_df)
                # NEW: register NR dataframes with Correction_Cmd for text export
                correction_cmd_sources["NR_missing"] = nr_missing_df
                correction_cmd_sources["NR_new"] = nr_new_df

                nr_disc_df.to_excel(writer, sheet_name="NR_disc", index=False)
                nr_missing_df.to_excel(writer, sheet_name="NR_missing", index=False)
                nr_new_df.to_excel(writer, sheet_name="NR_new", index=False)
                b.get("all_relations", pd.DataFrame()).to_excel(writer, sheet_name="NR_relations", index=False)
            else:
                enforce_nr_columns(pd.DataFrame()).to_excel(writer, sheet_name="NR_disc", index=False)
                empty_nr_missing_df = self.add_correction_command_nr_missing(enforce_nr_columns(pd.DataFrame()))
                empty_nr_new_df = self.add_correction_command_nr_new(enforce_nr_columns(pd.DataFrame()))
                empty_nr_missing_df.to_excel(writer, sheet_name="NR_missing", index=False)
                empty_nr_new_df.to_excel(writer, sheet_name="NR_new", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="NR_relations", index=False)

            # NEW: export all Correction_Cmd blocks to per-node text files
            if correction_cmd_sources:
                cmd_files = self.export_correction_cmd_texts(output_dir, correction_cmd_sources)
                print(f"\n[Consistency Checks (Pre/Post Comparison)] Generated {cmd_files} Correction_Cmd text files in: '{os.path.join(output_dir, 'Correction_Cmd')}'")

            # -------------------------------------------------------------------
            #  APPLY HEADER STYLING + AUTO-FIT COLUMNS FOR ALL SHEETS
            # -------------------------------------------------------------------

            # Keep: color the 'Summary*' tabs in green
            color_summary_tabs(writer, prefix="Summary", rgb_hex="00B050")

            # New: apply header style + autofit columns (replaces the manual loop)
            style_headers_autofilter_and_autofit(writer, freeze_header=True, align="left")


