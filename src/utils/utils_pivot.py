# -*- coding: utf-8 -*-

import re
import unicodedata
from typing import List, Optional

import pandas as pd


def concat_or_empty(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Return a single concatenated DataFrame or an empty one if none;
    align on common columns when required.
    """
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


def safe_pivot_count(
    df: pd.DataFrame,
    index_field: str,
    columns_field: str,
    values_field: str,
    add_margins: bool = True,
    margins_name: str = "Total",
) -> pd.DataFrame:
    """
    Robust pivot builder that prevents 'Grouper for ... not 1-dimensional' errors.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame({"Info": ["Table not found or empty"]})

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(c).strip() for c in tup if str(c).strip()])
            for tup in df.columns
        ]
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()

    work = df.reset_index(drop=True).copy()
    work.columns = pd.Index([str(c).strip() for c in work.columns])

    # Remove duplicate columns (case-insensitive)
    seen_lower = set()
    unique_cols = []
    for c in work.columns:
        cl = c.lower()
        if cl in seen_lower:
            continue
        seen_lower.add(cl)
        unique_cols.append(c)
    work = work[unique_cols]

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
        missing = [
            n
            for n, v in [
                (index_field, idx_col),
                (columns_field, col_col),
                (values_field, val_col),
            ]
            if v is None
        ]
        return pd.DataFrame(
            {
                "Info": [f"Required columns missing: {', '.join(missing)}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            }
        )

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

        if isinstance(piv.columns, pd.MultiIndex):
            piv.columns = [
                " ".join([str(x) for x in tup if str(x)]).strip()
                for tup in piv.columns
            ]

        return piv

    except Exception as ex:
        return pd.DataFrame(
            {
                "Error": [f"Pivot build failed: {ex}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            }
        )


def safe_crosstab_count(
    df: pd.DataFrame,
    index_field: str,
    columns_field: str,
    add_margins: bool = True,
    margins_name: str = "Total",
) -> pd.DataFrame:
    """
    Build a frequency table with pd.crosstab (no 'values' column required).
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame({"Info": ["Table not found or empty"]})

    work = df.copy()
    if isinstance(work.columns, pd.MultiIndex):
        work.columns = [
            "_".join([str(c) for c in tup if str(c)]).strip()
            for tup in work.columns
        ]
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

    seen = set()
    keep = []
    for c in work.columns:
        k = _canon(c)
        if k in seen:
            continue
        seen.add(k)
        keep.append(c)
    work = work[keep]

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
        missing = [
            n
            for n, v in [(index_field, idx_col), (columns_field, col_col)]
            if v is None
        ]
        return pd.DataFrame(
            {
                "Info": [f"Required columns missing: {', '.join(missing)}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            }
        )

    work[idx_col] = work[idx_col].astype(str).map(_norm_header)
    work[col_col] = work[col_col].astype(str).map(_norm_header)

    try:
        ct = pd.crosstab(
            index=work[idx_col],
            columns=work[col_col],
            dropna=False,
        ).reset_index()

        if add_margins:
            ct["Total"] = ct.drop(columns=[idx_col]).sum(axis=1)
            total_row = {idx_col: "Total"}
            for c in ct.columns:
                if c != idx_col:
                    total_row[c] = ct[c].sum()
            ct = pd.concat([ct, pd.DataFrame([total_row])], ignore_index=True)

        return ct
    except Exception as ex:
        return pd.DataFrame(
            {
                "Error": [f"Crosstab build failed: {ex}"],
                "PresentColumns": [", ".join(work.columns.tolist())],
            }
        )


def apply_frequency_column_filter(piv: pd.DataFrame, filters: List[str]) -> pd.DataFrame:
    """
    Keep only the first (index) column, 'Total' (if present), and columns whose
    header contains any of the provided substrings (case-insensitive).
    """
    if not isinstance(piv, pd.DataFrame) or piv.empty or not filters:
        return piv

    cols = [str(c) for c in piv.columns.tolist()]
    keep: List[str] = []

    if cols:
        keep.append(cols[0])

    fl = [f.lower() for f in filters if f]
    for c in cols[1:]:
        lc = c.lower()
        if c == "Total" or lc == "total":
            keep.append(c)
            continue
        if any(f in lc for f in fl):
            keep.append(c)

    if len(keep) <= 1 and "Total" in cols and "Total" not in keep:
        keep.append("Total")

    try:
        return piv.loc[:, keep]
    except Exception:
        return piv
