import os
import re
from datetime import datetime
from typing import Dict, Optional, List

import pandas as pd


class PrePostRelations:
    """
    Loads and compares GU/NR relation tables before (Pre) and after (Post) a refarming process.

    Methods:
    --------
    - loadPrePost(input_dir):
        * Searches subfolders whose names contain 'Pre'/'Step0' (Pre) or 'Post'/'Step3' (Post).
        * Extracts a 'yyyymmdd' date from the folder name (if any) and adds it as column 'Date'.
        * Reads GUtranCellRelation.log and NRCellRelation.log if found.
        * Inserts columns:
            - 'Pre/Post' (first column)
            - 'Date'
        * Cleans up empty rows and summary rows like 'N instance(s)'.

    - comparePrePost(freq_before, freq_after):
        * For each table, selects the most recent Pre and Post (by 'Date' if present).
        * Determines the key column(s) to match objects.
        * Detects the frequency column.
        * Reports discrepancies, new in Post, and missing in Post.

    - save_outputs(output_dir, results):
        * Writes loaded tables and (if any) comparison outputs into CSV files
          under the specified output_dir.
    """

    PRE_TOKENS = ("pre", "step0")
    POST_TOKENS = ("post", "step3")
    DATE_RE = re.compile(r"(?P<date>(19|20)\d{6})")  # yyyymmdd
    SUMMARY_RE = re.compile(r"^\s*\d+\s+instance\(s\)\s*$", re.IGNORECASE)

    def __init__(self) -> None:
        self.tables: Dict[str, pd.DataFrame] = {}

    # ----------------------------- UTILITIES ----------------------------- #

    @staticmethod
    def _detect_prepost(folder_name: str) -> Optional[str]:
        name = folder_name.lower()
        if any(tok in name for tok in PrePostRelations.PRE_TOKENS):
            return "Pre"
        if any(tok in name for tok in PrePostRelations.POST_TOKENS):
            return "Post"
        return None

    @staticmethod
    def _extract_date(folder_name: str) -> Optional[str]:
        match = PrePostRelations.DATE_RE.search(folder_name)
        if not match:
            return None
        s = match.group("date")
        try:
            datetime.strptime(s, "%Y%m%d")
            return s
        except ValueError:
            return None

    @staticmethod
    def _read_relation_log(path: str) -> Optional[pd.DataFrame]:
        """
        Reads a relation log file (.log) with tab-separated columns.
        Removes empty rows and summary rows like 'N instance(s)'.
        """
        if not os.path.isfile(path):
            return None

        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = [ln.rstrip("\n") for ln in f]

        # Detect header row: first line with tabs that is not a summary
        header_idx = None
        header_cols: List[str] = []
        for idx, ln in enumerate(lines):
            if "\t" in ln and not ln.strip().endswith("instance(s)"):
                header_idx = idx
                header_cols = [c.strip() for c in ln.split("\t")]
                break

        if header_idx is None or not header_cols:
            return None

        rows = []
        for ln in lines[header_idx + 1 :]:
            if not ln.strip():
                continue
            if PrePostRelations.SUMMARY_RE.match(ln):
                continue
            if "\t" not in ln:
                continue
            parts = [c.strip() for c in ln.split("\t")]
            if len(parts) < len(header_cols):
                parts += [None] * (len(header_cols) - len(parts))
            elif len(parts) > len(header_cols):
                parts = parts[: len(header_cols)]
            rows.append(parts)

        if not rows:
            return pd.DataFrame(columns=header_cols)

        df = pd.DataFrame(rows, columns=header_cols)
        df = df.dropna(how="all")
        for c in df.columns:
            df[c] = df[c].astype(str)
        return df

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

    def loadPrePost(self, input_dir: str) -> Dict[str, pd.DataFrame]:
        """
        Scans subfolders containing 'Pre'/'Step0' or 'Post'/'Step3',
        loads GUtranCellRelation.log and NRCellRelation.log if found,
        and creates DataFrames with columns 'Pre/Post' and 'Date'.
        """
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"Invalid directory: {input_dir}")

        collected: Dict[str, List[pd.DataFrame]] = {
            "GUtranCellRelation": [],
            "NRCellRelation": [],
        }

        for entry in os.scandir(input_dir):
            if not entry.is_dir():
                continue

            prepost = self._detect_prepost(entry.name)
            if not prepost:
                continue

            date_str = self._extract_date(entry.name)

            gu_path = os.path.join(entry.path, "GUtranCellRelation.log")
            nr_path = os.path.join(entry.path, "NRCellRelation.log")

            df_gu = self._read_relation_log(gu_path)
            if df_gu is not None:
                df_gu = self._insert_front_columns(df_gu, prepost, date_str)
                collected["GUtranCellRelation"].append(df_gu)

            df_nr = self._read_relation_log(nr_path)
            if df_nr is not None:
                df_nr = self._insert_front_columns(df_nr, prepost, date_str)
                collected["NRCellRelation"].append(df_nr)

        self.tables = {}
        for base, chunks in collected.items():
            if chunks:
                self.tables[self._table_key_name(base)] = pd.concat(chunks, ignore_index=True)

        return self.tables

    # ----------------------------- COMPARISON ----------------------------- #

    @staticmethod
    def _select_latest(df: pd.DataFrame, prepost_value: str) -> pd.DataFrame:
        """Return the latest Pre or Post subset by 'Date' (if present)."""
        subset = df[df["Pre/Post"].str.lower() == prepost_value.lower()]
        if subset.empty:
            return subset
        if "Date" not in subset.columns or (subset["Date"].astype(str).str.len() == 0).all():
            return subset
        try:
            subset = subset.copy()
            subset["__Date_dt"] = pd.to_datetime(subset["Date"], format="%Y%m%d", errors="coerce")
            max_date = subset["__Date_dt"].max()
            subset = subset[subset["__Date_dt"] == max_date].drop(columns="__Date_dt")
            return subset
        except Exception:
            return subset

    @staticmethod
    def _detect_freq_column(table_name: str, columns: List[str]) -> Optional[str]:
        if table_name == "GUtranCellRelation":
            if "GUtranFreqRelationId" in columns:
                return "GUtranFreqRelationId"
        for c in columns:
            lc = c.lower()
            if "freqrelation" in lc or "freq" in lc:
                return c
        return None

    @staticmethod
    def _detect_key_column(table_name: str, columns: List[str], freq_col: Optional[str]) -> List[str]:
        preferred = {
            "GUtranCellRelation": ["GUtranCellRelationId", "neighborCellRef"],
            "NRCellRelation": ["NRCellRelationId", "neighborCellRef"],
        }
        for cand in preferred.get(table_name, []):
            if cand in columns:
                return [cand]
        id_like = [c for c in columns if c.lower().endswith("id")]
        if freq_col and freq_col in id_like:
            id_like.remove(freq_col)
        id_like = [c for c in id_like if c not in ("Pre/Post", "Date")]
        if id_like:
            return id_like[:2]
        if "neighborCellRef" in columns:
            return ["neighborCellRef"]
        remaining = [c for c in columns if c not in ("Pre/Post", "Date")]
        return remaining[:1] if remaining else []

    @staticmethod
    def _rows_with_freq(df: pd.DataFrame, key_cols: List[str], freq_col: str) -> pd.DataFrame:
        cols = ["Pre/Post", "Date"] + key_cols + [freq_col]
        cols = [c for c in cols if c in df.columns]
        out = df[cols].copy()
        for c in out.columns:
            out[c] = out[c].astype(str)
        return out

    def comparePrePost(self, freq_before: str, freq_after: str, module_name: Optional[str] = "") -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Compare for each loaded table (GU/NR):
          - Discrepancies:
              * Frequency rule: if in Pre freq == freq_before, then in Post freq must be freq_after.
                Otherwise, it's a discrepancy (kept freq_before or changed to any other unexpected freq).
              * Any other attribute difference between Pre and Post for common keys (excluding 'Pre/Post' and 'Date').
          - New in Post.
          - Missing in Post.
        Returns a dictionary by table with DataFrames of results.
        """
        if not self.tables:
            raise RuntimeError("No tables loaded. Run loadPrePost() first.")

        results: Dict[str, Dict[str, pd.DataFrame]] = {}

        for table_name, df_all in self.tables.items():
            if df_all.empty:
                continue

            freq_col = self._detect_freq_column(table_name, list(df_all.columns))
            if not freq_col:
                print(f"{module_name} [WARNING] No frequency column detected in {table_name}. Adjust mapping if needed.")
                continue

            key_cols = self._detect_key_column(table_name, list(df_all.columns), freq_col)
            if not key_cols:
                print(f"{module_name} [WARNING] No key column detected in {table_name}.")
                continue

            # Force stable keys per table (as requested)
            if table_name == "GUtranCellRelation":
                forced_keys = [c for c in ["NodeId", "EUtranCellFDDId", "GUtranCellRelationId"] if c in df_all.columns]
                if forced_keys:
                    key_cols = forced_keys
            elif table_name == "NRCellRelation":
                forced_keys = [c for c in ["NodeId", "NRCellCUId", "NRCellRelationId"] if c in df_all.columns]
                if forced_keys:
                    key_cols = forced_keys

            # Latest Pre/Post
            pre_df_full = self._select_latest(df_all, "Pre")
            post_df_full = self._select_latest(df_all, "Post")
            if pre_df_full.empty and post_df_full.empty:
                continue

            # Normalize
            def _normalize(df: pd.DataFrame) -> pd.DataFrame:
                out = df.copy()
                for c in out.columns:
                    # NEW/CHANGED: hard-normalize textual nulls → "", strip spaces
                    out[c] = (
                        out[c]
                        .astype(str)
                        .str.strip()
                        .replace({"nan": "", "NaN": "", "None": "", "none": "", "NULL": "", "null": ""})
                    )
                return out

            pre_norm = _normalize(pre_df_full)
            post_norm = _normalize(post_df_full)

            # Index by join key and collapse dups (keep last)
            def make_index_full(dfx: pd.DataFrame) -> pd.DataFrame:
                dfx = dfx.copy()
                for c in key_cols:
                    if c not in dfx.columns:
                        dfx[c] = ""
                dfx["_join_key"] = dfx[key_cols].agg("||".join, axis=1)
                dfx = dfx.set_index("_join_key", drop=True)
                if dfx.index.has_duplicates:
                    dfx = dfx[~dfx.index.duplicated(keep="last")]
                return dfx

            pre_idx_full = make_index_full(pre_norm)
            post_idx_full = make_index_full(post_norm)

            pre_keys = set(pre_idx_full.index)
            post_keys = set(post_idx_full.index)
            common_idx = sorted(pre_keys & post_keys)

            new_in_post = post_idx_full.loc[sorted(post_keys - pre_keys)].copy()
            missing_in_post = pre_idx_full.loc[sorted(pre_keys - post_keys)].copy()

            pre_common_full = pre_idx_full.loc[common_idx]
            post_common_full = post_idx_full.loc[common_idx]

            # Slim views (context)
            def slim(df: pd.DataFrame, keep_cols: List[str]) -> pd.DataFrame:
                cols = ["Pre/Post", "Date"] + list(dict.fromkeys(keep_cols))
                cols = [c for c in cols if c in df.columns]
                return df[cols].copy()

            pre_slim = slim(pre_common_full, key_cols + [freq_col])
            post_slim = slim(post_common_full, key_cols + [freq_col])

            # Frequency extraction helpers (robust)
            def _base_series(s: pd.Series) -> pd.Series:
                return s.astype(str).str.split("-", n=1).str[0].fillna("").str.strip()

            def _extract_gu_freq(s: pd.Series) -> pd.Series:
                # Prefer base before '-' ; fallback to first numeric group
                base = _base_series(s)
                fallback = s.astype(str).str.extract(r"(\d+)", expand=False)
                return base.where(base != "", fallback).fillna("").astype(str)

            def _extract_nr_freq(s: pd.Series) -> pd.Series:
                # NR: number after 'NRFreqRelation=' ; fallback to base
                got = s.astype(str).str.extract(r"NRFreqRelation\s*=\s*(\d+)", expand=False)
                return got.fillna(_base_series(s)).fillna("").astype(str)

            if table_name == "NRCellRelation":
                pre_freq_base = _extract_nr_freq(pre_slim[freq_col] if freq_col in pre_slim.columns else pd.Series("", index=pre_slim.index))
                post_freq_base = _extract_nr_freq(post_slim[freq_col] if freq_col in post_slim.columns else pd.Series("", index=post_slim.index))
            else:
                pre_freq_base = _extract_gu_freq(pre_slim[freq_col] if freq_col in pre_slim.columns else pd.Series("", index=pre_slim.index))
                post_freq_base = _extract_gu_freq(post_slim[freq_col] if freq_col in post_slim.columns else pd.Series("", index=post_slim.index))

            fb = str(freq_before).strip()
            fa = str(freq_after).strip()

            # Frequency rule (includes: if Pre already == fa, Post must remain == fa)
            pre_has_before = (pre_freq_base == fb)
            pre_has_after = (pre_freq_base == fa)
            post_is_after = (post_freq_base == fa)

            freq_rule_mask = (pre_has_before & (~post_is_after)) | (pre_has_after & (~post_is_after))

            # Exclude meta + keys + frequency column from generic comparison
            exclude_cols = {"Pre/Post", "Date", freq_col} | set(key_cols)
            shared_cols = [
                c for c in pre_common_full.columns
                if c in post_common_full.columns and c not in exclude_cols
            ]

            any_diff_mask = pd.Series(False, index=pre_common_full.index)
            diff_cols_per_row = {k: [] for k in pre_common_full.index}
            for c in shared_cols:
                diffs = (pre_common_full[c] != post_common_full[c]).reindex(pre_common_full.index, fill_value=False)
                any_diff_mask = any_diff_mask | diffs
                for k in pre_common_full.index[diffs]:
                    diff_cols_per_row[k].append(c)

            combined_mask = (freq_rule_mask | any_diff_mask).reindex(pre_common_full.index, fill_value=False)

            # Discrepancies strictly limited to common keys (defensive)
            common_idx_set = set(common_idx)
            discrepancy_keys = [k for k, m in zip(pre_common_full.index, combined_mask) if m and k in common_idx_set]

            # Required columns per table (to populate from Post if available)
            if table_name == "GUtranCellRelation":
                required_cols = ["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]
            elif table_name == "NRCellRelation":
                required_cols = ["NodeId", "NRCellCUId", "NRCellRelationId"]
            else:
                required_cols = []

            # Helpers to reorder columns placing Date/Freq first, then keys, then the rest
            def _desired_key_order(tbl: str) -> list:
                if tbl == "GUtranCellRelation":
                    return ["NodeId", "EUtranCellFDDId", "GUtranCellRelationId"]
                if tbl == "NRCellRelation":
                    return ["NodeId", "NRCellCUId", "NRCellRelationId"]
                return []

            def _reorder_cols(df: pd.DataFrame, tbl: str) -> pd.DataFrame:
                if df is None or df.empty:
                    return df
                front = ["Date_Pre", "Date_Post", "Freq_Pre", "Freq_Post"]
                keys = [c for c in _desired_key_order(tbl) if c in df.columns]
                seen = set(front + keys)
                rest = [c for c in df.columns if c not in seen]
                return df[[*(c for c in front if c in df.columns), *keys, *rest]]

            # Build discrepancies rows
            rows = []
            for k in discrepancy_keys:
                row = {}
                # Key columns
                for c in key_cols:
                    row[c] = pre_common_full.loc[k, c] if c in pre_common_full.columns else ""

                # Dates (context only)
                row["Date_Pre"] = pre_slim.loc[k, "Date"] if "Date" in pre_slim.columns else ""
                row["Date_Post"] = post_slim.loc[k, "Date"] if "Date" in post_slim.columns else ""

                # Freqs (original text) for context
                row["Freq_Pre"] = pre_slim[freq_col].loc[k] if freq_col in pre_slim.columns else ""
                row["Freq_Post"] = post_slim[freq_col].loc[k] if freq_col in post_slim.columns else ""

                # Populate required columns from POST (fallback to PRE if missing)
                for rc in required_cols:
                    val = ""
                    if rc in post_common_full.columns:
                        val = post_common_full.loc[k, rc]
                    elif rc in pre_common_full.columns:
                        val = pre_common_full.loc[k, rc]
                    row[rc] = val

                # Which columns differ (excluding 'Pre/Post' and 'Date' by design)
                difflist = diff_cols_per_row.get(k, [])
                row["DiffColumns"] = ", ".join(sorted(difflist))
                # Side-by-side values for differing columns
                for c in difflist:
                    row[f"{c}_Pre"] = pre_common_full.loc[k, c]
                    row[f"{c}_Post"] = post_common_full.loc[k, c]

                rows.append(row)

            discrepancies = pd.DataFrame(rows)
            discrepancies = _reorder_cols(discrepancies, table_name)

            # Ensure string dtype in new/missing for stability
            for dfc in (new_in_post, missing_in_post):
                for col in dfc.columns:
                    dfc[col] = dfc[col].astype(str)

            # Clean new/missing tables by removing meta/freq/date columns (mantain only data cols)
            if not new_in_post.empty:
                cols_to_remove = {"Date_Pre", "Date_Post", "Freq_Pre", "Freq_Post", "Pre/Post", "Date"}
                new_in_post = new_in_post.drop(columns=[c for c in cols_to_remove if c in new_in_post.columns], errors="ignore")
            if not missing_in_post.empty:
                cols_to_remove = {"Date_Pre", "Date_Post", "Freq_Pre", "Freq_Post", "Pre/Post", "Date"}
                missing_in_post = missing_in_post.drop(columns=[c for c in cols_to_remove if c in missing_in_post.columns], errors="ignore")

            # ---------- NEW/CHANGED: build pair stats for Summary_Detailed ----------
            # Pair table (common keys) with freq bases and flags
            pair_stats = pd.DataFrame({
                "Freq_Pre": pre_freq_base.reindex(pre_common_full.index).fillna("").replace("", "<empty>"),
                "Freq_Post": post_freq_base.reindex(pre_common_full.index).fillna("").replace("", "<empty>"),
                "ParamDiff": any_diff_mask.reindex(pre_common_full.index).astype(bool),
                "FreqDiff": freq_rule_mask.reindex(pre_common_full.index).astype(bool),
            }, index=pre_common_full.index)

            # New/Missing annotate pair with empty/opposite side
            def _extract_side_freq_base(df_side: pd.DataFrame) -> pd.Series:
                if df_side is None or df_side.empty:
                    return pd.Series([], dtype=str)
                if table_name == "NRCellRelation":
                    ser = _extract_nr_freq(df_side[freq_col]) if (freq_col in df_side.columns) else pd.Series([""] * len(df_side), index=df_side.index)
                else:
                    ser = _extract_gu_freq(df_side[freq_col]) if (freq_col in df_side.columns) else pd.Series([""] * len(df_side), index=df_side.index)
                return ser.fillna("").replace("", "<empty>")

            if not new_in_post.empty:
                new_in_post = new_in_post.copy()
                new_in_post["Freq_Pre"] = "<empty>"
                new_in_post["Freq_Post"] = _extract_side_freq_base(post_idx_full.loc[new_in_post.index]) if len(new_in_post.index) else "<empty>"
            if not missing_in_post.empty:
                missing_in_post = missing_in_post.copy()
                missing_in_post["Freq_Pre"] = _extract_side_freq_base(pre_idx_full.loc[missing_in_post.index]) if len(missing_in_post.index) else "<empty>"
                missing_in_post["Freq_Post"] = "<empty>"

            # ---------- NEW/CHANGED: merged view of all relations for extra sheets ----------
            # Merge latest PRE/POST by keys to produce (keys + Freq_Pre + Freq_Post + rest)
            def _keys_first(df: pd.DataFrame) -> pd.DataFrame:
                if df is None or df.empty:
                    return df
                keys_order = [c for c in key_cols if c in df.columns]
                rest = [c for c in df.columns if c not in keys_order]
                return df[keys_order + rest]

            pre_latest = pre_norm.copy()
            post_latest = post_norm.copy()
            # Prepare helper freq base columns in these copies
            if table_name == "NRCellRelation":
                pre_fb = _extract_nr_freq(pre_latest[freq_col]) if (freq_col in pre_latest.columns) else pd.Series("", index=pre_latest.index)
                post_fb = _extract_nr_freq(post_latest[freq_col]) if (freq_col in post_latest.columns) else pd.Series("", index=post_latest.index)
            else:
                pre_fb = _extract_gu_freq(pre_latest[freq_col]) if (freq_col in pre_latest.columns) else pd.Series("", index=pre_latest.index)
                post_fb = _extract_gu_freq(post_latest[freq_col]) if (freq_col in post_latest.columns) else pd.Series("", index=post_latest.index)
            pre_latest = pre_latest.assign(Freq_Pre=pre_fb.replace("", "<empty>"))
            post_latest = post_latest.assign(Freq_Post=post_fb.replace("", "<empty>"))

            # left/right reduce to keys + freq + all other (prefer POST values when present)
            pre_keep = _keys_first(pre_latest.drop(columns=["Pre/Post", "Date"], errors="ignore"))
            post_keep = _keys_first(post_latest.drop(columns=["Pre/Post", "Date"], errors="ignore"))

            merged_all = pd.merge(
                pre_keep, post_keep,
                on=key_cols, how="outer", suffixes=("_PreSide", "_PostSide")
            )

            # Build clean columns: keep single Freq_Pre/Freq_Post, and then append "best effort" other fields (prefer PostSide)
            # Start with keys
            all_relations = merged_all[key_cols].copy()
            # add Freq_Pre and Freq_Post (second-chance fill from side columns if needed)
            all_relations["Freq_Pre"] = merged_all.get("Freq_Pre", "").replace("", "<empty>")
            all_relations["Freq_Post"] = merged_all.get("Freq_Post", "").replace("", "<empty>")
            if (all_relations["Freq_Pre"] == "<empty>").any():
                # intenta recuperar desde columnas de frecuencia con sufijos, case-insensitive
                side_cols = {c.lower(): c for c in merged_all.columns}
                # nombre de frecuencia original (case-insensitive)
                freq_lc = freq_col.lower()
                pre_side = side_cols.get(f"{freq_lc}_preside")
                if pre_side and (all_relations["Freq_Pre"] == "<empty>").any():
                    if table_name == "NRCellRelation":
                        fill = _extract_nr_freq(merged_all[pre_side])
                    else:
                        fill = _extract_gu_freq(merged_all[pre_side])
                    all_relations.loc[all_relations["Freq_Pre"] == "<empty>", "Freq_Pre"] = fill.loc[all_relations["Freq_Pre"] == "<empty>"].replace("", "<empty>")
            if (all_relations["Freq_Post"] == "<empty>").any():
                side_cols = {c.lower(): c for c in merged_all.columns}
                freq_lc = freq_col.lower()
                post_side = side_cols.get(f"{freq_lc}_postside")
                if post_side and (all_relations["Freq_Post"] == "<empty>").any():
                    if table_name == "NRCellRelation":
                        fill = _extract_nr_freq(merged_all[post_side])
                    else:
                        fill = _extract_gu_freq(merged_all[post_side])
                    all_relations.loc[all_relations["Freq_Post"] == "<empty>", "Freq_Post"] = fill.loc[all_relations["Freq_Post"] == "<empty>"].replace("", "<empty>")

            # append the rest (take PostSide if exists else PreSide)
            for col in set(pre_keep.columns) | set(post_keep.columns):
                if col in key_cols or col in ("Freq_Pre", "Freq_Post"):
                    continue
                pre_col = f"{col}_PreSide" if f"{col}_PreSide" in merged_all.columns else None
                post_col = f"{col}_PostSide" if f"{col}_PostSide" in merged_all.columns else None
                if post_col and post_col in merged_all.columns:
                    all_relations[col] = merged_all[post_col].where(merged_all[post_col].astype(str) != "", merged_all[pre_col] if pre_col else "")
                elif pre_col and pre_col in merged_all.columns:
                    all_relations[col] = merged_all[pre_col]
                elif col in merged_all.columns:
                    all_relations[col] = merged_all[col]

            # ---------- pack results ----------
            results[table_name] = {
                "discrepancies": discrepancies.reset_index(drop=True),
                "new_in_post": new_in_post.reset_index(drop=True),
                "missing_in_post": missing_in_post.reset_index(drop=True),
                "pair_stats": pair_stats.reset_index(drop=True),  # NEW/CHANGED: common keys with (Freq_Pre, Freq_Post, flags)
                "all_relations": all_relations.reset_index(drop=True),  # NEW/CHANGED: merged full relations (keys + Freq_* + rest)
                "meta": {
                    "key_cols": key_cols,
                    "freq_col": freq_col,
                    "pre_rows": int(pre_df_full.shape[0]),
                    "post_rows": int(post_df_full.shape[0]),
                },
            }

            print(f"\n{module_name} === {table_name} ===")
            print(f"{module_name} Key: {key_cols} | Freq column: {freq_col}")
            print(f"{module_name} - Discrepancies: {len(discrepancies)}")
            print(f"{module_name} - New in Post: {len(new_in_post)}")
            print(f"{module_name} - Missing in Post: {len(missing_in_post)}")

        # <-- IMPORTANT: return out of loop, after process GU and NR
        return results

    # ----------------------------- OUTPUT TO EXCEL (short sheet names + enforced columns) ----------------------------- #
    def save_outputs_excel(
            self,
            output_dir: str,
            results: Optional[Dict[str, Dict[str, pd.DataFrame]]] = None,
            versioned_suffix: Optional[str] = None,
    ) -> None:
        """
        Writes two Excel files in output_dir with short sheet names:

          CellRelation.xlsx
            - GU_all
            - NR_all

          CellRelationConsistencyChecks.xlsx
            - Summary
            - Summary_Detailed (NEW) -> same as Summary but split by Freq_Pre / Freq_Post
            - GU_disc
            - GU_missing
            - GU_new
            - GU_relations (NEW)
            - NR_disc
            - NR_missing
            - NR_new
            - NR_relations (NEW)

        For GU discrepancy/new/missing sheets, enforce columns:
          NodeId, EUtranCellFDDId, GUtranFreqRelationId, GUtranCellRelationId

        For NR discrepancy/new/missing sheets, enforce columns:
          NodeId, NRCellCUId, NRCellRelationId
        """
        import os
        os.makedirs(output_dir, exist_ok=True)

        suf = f"_{versioned_suffix}" if versioned_suffix else ""

        # ---------- Excel 1: all data ----------
        excel_all = os.path.join(output_dir, f"CellRelation{suf}.xlsx")
        with pd.ExcelWriter(excel_all, engine="openpyxl") as writer:
            if "GUtranCellRelation" in self.tables:
                self.tables["GUtranCellRelation"].to_excel(writer, sheet_name="GU_all", index=False)
            if "NRCellRelation" in self.tables:
                self.tables["NRCellRelation"].to_excel(writer, sheet_name="NR_all", index=False)

        # ---------- Excel 2: discrepancies + summary ----------
        excel_disc = os.path.join(output_dir, f"CellRelationConsistencyChecks{suf}.xlsx")
        with pd.ExcelWriter(excel_disc, engine="openpyxl") as writer:

            # ---------- Summary (renamed columns and extra frequency discrepancies) ----------
            summary_rows = []
            if results:
                for name, bucket in results.items():
                    meta = bucket.get("meta", {})
                    pair_stats = bucket.get("pair_stats", pd.DataFrame())
                    # Parameters_Discrepancies = any ParamDiff (not freq)
                    params_disc = int(pair_stats["ParamDiff"].sum()) if not pair_stats.empty else 0
                    # Frequency_Discrepancies = any FreqDiff
                    freq_disc = int(pair_stats["FreqDiff"].sum()) if not pair_stats.empty else 0
                    summary_rows.append({
                        "Table": name,
                        "KeyColumns": ", ".join(meta.get("key_cols", [])),
                        "FreqColumn": meta.get("freq_col", "N/A"),
                        # NEW names
                        "Relations_Pre": meta.get("pre_rows", 0),
                        "Relations_Post": meta.get("post_rows", 0),
                        "Parameters_Discrepancies": params_disc,
                        "Frequency_Discrepancies": freq_disc,
                        "New_Relations": len(bucket.get("new_in_post", pd.DataFrame())),
                        "Missing_Relations": len(bucket.get("missing_in_post", pd.DataFrame())),
                    })
            summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(
                columns=[
                    "Table", "KeyColumns", "FreqColumn", "Relations_Pre", "Relations_Post",
                    "Parameters_Discrepancies", "Frequency_Discrepancies", "New_Relations", "Missing_Relations"
                ]
            )
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # ---------- Summary_Detailed by (Freq_Pre, Freq_Post) ----------
            detailed_rows = []
            if results:
                for name, bucket in results.items():
                    meta = bucket.get("meta", {})
                    pair_stats = bucket.get("pair_stats", pd.DataFrame())
                    new_df = bucket.get("new_in_post", pd.DataFrame())
                    miss_df = bucket.get("missing_in_post", pd.DataFrame())

                    # Relations_Pre / Relations_Post per individual frequency
                    def _count_side(side: str) -> Dict[str, int]:
                        tbl = self._select_latest(self.tables.get(name, pd.DataFrame()), side)
                        if tbl is None or tbl.empty:
                            return {}
                        # Try to extract frequency column properly
                        if name == "NRCellRelation":
                            # NR: extract number after 'NRFreqRelation='
                            col = None
                            for c in tbl.columns:
                                if c.lower() == "nrfreqrelationref":
                                    col = c
                                    break
                            if col:
                                ser = tbl[col].astype(str).str.extract(r"NRFreqRelation\s*=\s*(\d+)", expand=False)
                                ser = ser.fillna("")
                            else:
                                # Fallback: extract digits only from frequency column
                                ser = (
                                    tbl[meta.get("freq_col")].astype(str).str.split("-", n=1).str[0]
                                    if meta.get("freq_col") in tbl.columns else pd.Series("", index=tbl.index)
                                )
                        else:
                            # GU: extract digits only from GUtranFreqRelationId
                            ser = (
                                tbl[meta.get("freq_col")].astype(str).str.split("-", n=1).str[0]
                                if meta.get("freq_col") in tbl.columns else pd.Series("", index=tbl.index)
                            )
                        return ser.fillna("").replace("", "<empty>").value_counts().to_dict()

                    pre_counts = _count_side("Pre")
                    post_counts = _count_side("Post")

                    # Parameters / Frequency discrepancies by (Freq_Pre, Freq_Post) for common keys
                    if not pair_stats.empty:
                        grp = pair_stats.groupby(["Freq_Pre", "Freq_Post"], dropna=False)
                        params_by_pair = grp["ParamDiff"].sum().astype(int).to_dict()
                        freq_by_pair = grp["FreqDiff"].sum().astype(int).to_dict()
                        pairs_present = set(grp.size().index.tolist())
                    else:
                        params_by_pair, freq_by_pair, pairs_present = {}, {}, set()

                    # Count New / Missing relations by (Freq_Pre, Freq_Post)
                    def _pair_counts(df_pairs: pd.DataFrame) -> Dict[tuple, int]:
                        if df_pairs is None or df_pairs.empty:
                            return {}
                        df_pairs = df_pairs.copy()
                        if "Freq_Pre" not in df_pairs.columns:
                            df_pairs["Freq_Pre"] = "<empty>"
                        if "Freq_Post" not in df_pairs.columns:
                            df_pairs["Freq_Post"] = "<empty>"
                        df_pairs["Freq_Pre"] = df_pairs["Freq_Pre"].fillna("").replace("", "<empty>")
                        df_pairs["Freq_Post"] = df_pairs["Freq_Post"].fillna("").replace("", "<empty>")
                        return df_pairs.groupby(["Freq_Pre", "Freq_Post"]).size().astype(int).to_dict()

                    new_by_pair = _pair_counts(new_df)
                    miss_by_pair = _pair_counts(miss_df)

                    # Build the full set of frequency pairs to report:
                    #   - Include all pairs found in discrepancies/new/missing
                    #   - Include neutral pairs (f,f) for every frequency seen in Pre or Post
                    neutral_pairs = {(f, f) for f in (set(pre_counts.keys()) | set(post_counts.keys()))}
                    all_pairs = set(params_by_pair) | set(freq_by_pair) | set(new_by_pair) | set(miss_by_pair) | neutral_pairs | pairs_present

                    for (fpre, fpost) in sorted(all_pairs, key=lambda t: (t[0], t[1])):
                        detailed_rows.append({
                            "Table": name,
                            "KeyColumns": ", ".join(meta.get("key_cols", [])),
                            "FreqColumn": meta.get("freq_col", "N/A"),
                            "Freq_Pre": fpre,
                            "Freq_Post": fpost,
                            # Relations per side (not per pair)
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

            # Helpers: enforce required columns per tech
            def enforce_gu_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
                cols_required = ["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]
                if not isinstance(df, pd.DataFrame) or df.empty:
                    return pd.DataFrame(columns=cols_required)
                out = df.copy()
                for c in cols_required:
                    if c not in out.columns:
                        out[c] = ""
                other = [c for c in out.columns if c not in cols_required]
                return out[cols_required + other]

            def enforce_nr_columns(df: Optional[pd.DataFrame]) -> pd.DataFrame:
                cols_required = ["NodeId", "NRCellCUId", "NRCellRelationId"]
                if not isinstance(df, pd.DataFrame) or df.empty:
                    return pd.DataFrame(columns=cols_required)
                out = df.copy()
                for c in cols_required:
                    if c not in out.columns:
                        out[c] = ""
                other = [c for c in out.columns if c not in cols_required]
                return out[cols_required + other]

            # Write GU sheets
            if results and "GUtranCellRelation" in results:
                bucket = results["GUtranCellRelation"]
                enforce_gu_columns(bucket.get("discrepancies")).to_excel(writer, sheet_name="GU_disc", index=False)
                enforce_gu_columns(bucket.get("missing_in_post")).to_excel(writer, sheet_name="GU_missing", index=False)
                enforce_gu_columns(bucket.get("new_in_post")).to_excel(writer, sheet_name="GU_new", index=False)
                # NEW: all relations merged
                bucket.get("all_relations", pd.DataFrame()).to_excel(writer, sheet_name="GU_relations", index=False)
            else:
                pd.DataFrame(columns=["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]).to_excel(writer, sheet_name="GU_disc", index=False)
                pd.DataFrame(columns=["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]).to_excel(writer, sheet_name="GU_missing", index=False)
                pd.DataFrame(columns=["NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"]).to_excel(writer, sheet_name="GU_new", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="GU_relations", index=False)

            # Write NR sheets
            if results and "NRCellRelation" in results:
                bucket = results["NRCellRelation"]
                enforce_nr_columns(bucket.get("discrepancies")).to_excel(writer, sheet_name="NR_disc", index=False)
                enforce_nr_columns(bucket.get("missing_in_post")).to_excel(writer, sheet_name="NR_missing", index=False)
                enforce_nr_columns(bucket.get("new_in_post")).to_excel(writer, sheet_name="NR_new", index=False)
                # NEW: all relations merged
                bucket.get("all_relations", pd.DataFrame()).to_excel(writer, sheet_name="NR_relations", index=False)
            else:
                pd.DataFrame(columns=["NodeId", "NRCellCUId", "NRCellRelationId"]).to_excel(writer, sheet_name="NR_disc", index=False)
                pd.DataFrame(columns=["NodeId", "NRCellCUId", "NRCellRelationId"]).to_excel(writer, sheet_name="NR_missing", index=False)
                pd.DataFrame(columns=["NodeId", "NRCellCUId", "NRCellRelationId"]).to_excel(writer, sheet_name="NR_new", index=False)
                pd.DataFrame().to_excel(writer, sheet_name="NR_relations", index=False)


