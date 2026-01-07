# -*- coding: utf-8 -*-

"""
ProfilesAudit

This module contains two independent Profile-related audit blocks that write results to SummaryAudit
via the provided `add_row(category, subcategory, metric, value, extra="")` callback.

A) Profiles tables audit (replica + param equality)
   Entry: `process_profiles_tables(dfs_by_table, add_row, n77_ssb_pre, n77_ssb_post)`
   For a curated list of "profiles tables" (e.g. McpcPCellProfileUeCfg, UeMCEUtranFreqRelProfileUeCfg, ...):
     - Detect rows referencing the old SSB (pre) and verify a corresponding "replica" row exists for the new SSB (post).
     - When the replica exists, verify all other parameters match (ignoring `reservedBy`).
     - For *UeCfg tables*, pairing is done by (NodeId, <XxxUeCfgId>) so that only the ProfileId changes old->new.
     - Special case: for McpcPCellNrFreqRelProfileUeCfg:
         * The missing-replica check for "xxxx_<SSB>" profile ids is ALWAYS executed for ALL nodes.
         * The generic "Profiles with old N77 SSB (...) but not new N77 SSB (...)" inconsistency row is NOT emitted
           (it is replaced by the "xxxx_<SSB>" check).
         * The discrepancies row (params differ) is still emitted.

   Output:
     - One "Profiles Inconsistencies" row per table: missing new replica(s)
     - One "Profiles Discrepancies" row per table: replica exists but parameters differ
     - For McpcPCellNrFreqRelProfileUeCfg: the inconsistencies row is the "xxxx_<SSB>" rule (always for all nodes).

B) Post Step2 cleanup checks (node-scoped)
   Entry: `cc_post_step2(df_nr_cell_cu, df_eutran_freq_rel, df_mcpc_pcell_nr_freq_rel_profile_uecfg, add_row, n77_ssb_pre, n77_ssb_post, nodes_post=None)`
   These checks are scoped ONLY by `nodes_post` (NodeId in nodes_post):
     1) NRCellCU: detect nodes whose ref parameters still contain old SSB token (pre)
     2) EUtranFreqRelation: detect nodes whose ref parameters still contain old SSB token (pre)
     3) McpcPCellNrFreqRelProfileUeCfg: within each (NodeId, UeCfgId) group, ensure any <pre>_xxxx profile id
        has the corresponding <post>_xxxx clone.

Notes:
- No filtering by nRFrequencyRef is performed anywhere in this module.
- If nodes_post is empty, cc_post_step2 writes "0" with an explanatory ExtraInfo for all its metrics.
- Helper functions are either module-level (shared) or nested where they are only used locally.
"""

import re
from typing import Optional, Set, Iterable, List, Tuple, Dict

import pandas as pd

from src.utils.utils_frequency import resolve_column_case_insensitive, parse_int_frequency


# =====================================================================
#                           PUBLIC ENTRYPOINTS
# =====================================================================

def process_profiles_tables(dfs_by_table, add_row, n77_ssb_pre, n77_ssb_post) -> None:
    """
    Profiles tables audit (replica + param equality), including UeCfg pairing by (NodeId, UeCfgId) and a special suffix-style
    check for McpcPCellNrFreqRelProfileUeCfg executed for ALL nodes (not scoped by nodes_pre).
    """

    def _resolve_uecfg_id_col_for_profile_id(df: pd.DataFrame, profile_id_col_name: str) -> Optional[str]:
        # Given a ProfileId column name, infer its corresponding UeCfgId column name.
        if not profile_id_col_name:
            return None
        if profile_id_col_name.lower().endswith("id"):
            expected = f"{profile_id_col_name[:-2]}UeCfgId"
        else:
            expected = f"{profile_id_col_name}UeCfgId"
        return resolve_column_case_insensitive(df, [expected])

    def _replace_int_token(text: str, old_number: int, new_number: int) -> str:
        # Replace an integer token in a string, only when not surrounded by other digits.
        s = "" if text is None else str(text)
        pattern = rf"(?<!\d){re.escape(str(old_number))}(?!\d)"
        return re.sub(pattern, str(new_number), s)

    def _normalize_value(value: object) -> Optional[str]:
        # Normalize values to string for robust comparisons across mixed dtypes.
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, (int,)):
            return str(int(value))
        if isinstance(value, (float,)):
            if value.is_integer():
                return str(int(value))
            return str(value).strip()
        return str(value).strip()

    def _normalize_row_for_compare(row: pd.Series, compare_cols: List[str]) -> Dict[str, Optional[str]]:
        normalized: Dict[str, Optional[str]] = {}
        for c in compare_cols:
            normalized[c] = _normalize_value(row.get(c))
        return normalized

    def _best_diff_columns(pre_norm: Dict[str, Optional[str]], post_candidates: List[Dict[str, Optional[str]]], compare_cols: List[str]) -> Set[str]:
        # Find a "best" set of differing columns among candidate replicas (minimal diffs).
        best: Set[str] = set(compare_cols)
        best_len = len(best)
        for cand in post_candidates:
            diffs: Set[str] = set()
            for c in compare_cols:
                if pre_norm.get(c) != cand.get(c):
                    diffs.add(str(c))
            if len(diffs) < best_len:
                best = diffs
                best_len = len(diffs)
                if best_len == 0:
                    break
        return best

    def _format_discrepancy_extrainfo(discrepancy_nodes_to_cols: Dict[str, Set[str]]) -> str:
        if not discrepancy_nodes_to_cols:
            return ""
        parts: List[str] = []
        for node in sorted(discrepancy_nodes_to_cols.keys()):
            cols = sorted(discrepancy_nodes_to_cols.get(node, set()))
            cols_str = ", ".join(cols) if cols else ""
            parts.append(f"{node} ({cols_str})" if cols_str else f"{node} (UnknownColumn)")
        return ", ".join(parts)

    def _add_missing_suffix_profiles_check(work: pd.DataFrame, node_col: str, profile_id_col: str, uecfg_col: str, add_row_fn, ssb_pre_int_local: int, ssb_post_int_local: int, metric_text: str) -> None:
        """
        Special check for McpcPCellNrFreqRelProfileUeCfg:
          - For each (NodeId, UeCfgId) group, if any profile id ends with _<preSSB>,
            ensure the corresponding _<postSSB> clone exists in the same group.
          - This check is executed for ALL nodes (no nodes_pre scoping).
        """

        def _suffix_ssb(value: object) -> Optional[int]:
            s = "" if value is None else str(value).strip()
            if "_" not in s:
                return None
            token = s.split("_")[-1].strip()
            if not token.isdigit():
                return None
            try:
                return int(token)
            except Exception:
                return None

        def _replace_suffix_ssb(profile_id: str, old_ssb: int, new_ssb: int) -> str:
            s = "" if profile_id is None else str(profile_id).strip()
            if "_" not in s:
                return s
            parts = s.split("_")
            if parts and parts[-1].strip().isdigit() and int(parts[-1].strip()) == int(old_ssb):
                parts[-1] = str(new_ssb)
            return "_".join(parts)

        bad_nodes: Set[str] = set()
        if work is None or work.empty:
            add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_text, 0, "")
            return

        work2 = work[[node_col, uecfg_col, profile_id_col]].copy()
        work2[node_col] = work2[node_col].astype(str).str.strip()
        work2[uecfg_col] = work2[uecfg_col].astype(str).str.strip()
        work2[profile_id_col] = work2[profile_id_col].astype(str).str.strip()

        for (node, uecfg), grp in work2.groupby([node_col, uecfg_col], dropna=False):
            pids = set(grp[profile_id_col].dropna().astype(str).str.strip().tolist())
            for pid in pids:
                if _suffix_ssb(pid) != ssb_pre_int_local:
                    continue
                expected = _replace_suffix_ssb(pid, ssb_pre_int_local, ssb_post_int_local)
                if expected not in pids:
                    node_str = str(node).strip()
                    if node_str:
                        bad_nodes.add(node_str)
                    break

        add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_text, len(bad_nodes), ", ".join(sorted(bad_nodes)))

    def _process_profiles_table_uecfg(work: pd.DataFrame, table_name_local: str, node_col: str, profile_id_col: str, uecfg_col: str, reserved_col: Optional[str], add_row_fn, ssb_pre_int_local: int, ssb_post_int_local: int, metric_missing: str, metric_discr: str, skip_inconsistencies: bool = False) -> None:
        """
        Audit a UeCfg-based table by pairing rows using (NodeId, UeCfgId) and switching only the ProfileId token old->new.
        """
        exclude = {profile_id_col}
        if reserved_col:
            exclude.add(reserved_col)
        compare_cols = [c for c in work.columns if c not in exclude]

        def _is_old_only(v: object) -> bool:
            s = "" if v is None else str(v)
            return _contains_int_token(s, ssb_pre_int_local) and not _contains_int_token(s, ssb_post_int_local)

        def _is_new_only(v: object) -> bool:
            s = "" if v is None else str(v)
            return _contains_int_token(s, ssb_post_int_local) and not _contains_int_token(s, ssb_pre_int_local)

        pre_rows = work.loc[work[profile_id_col].map(_is_old_only)].copy()
        post_rows = work.loc[work[profile_id_col].map(_is_new_only)].copy()

        if pre_rows.empty:
            if not skip_inconsistencies:
                add_row_fn(table_name_local, "Profiles Inconsistencies", metric_missing, 0, "")
            add_row_fn(table_name_local, "Profiles Discrepancies", metric_discr, 0, "")
            return

        # Build fast indices for post rows
        post_exact: Set[Tuple[Tuple[str, str], str, Tuple[Optional[str], ...]]] = set()
        post_by_key_and_profile: Dict[Tuple[str, str], Dict[str, List[Dict[str, Optional[str]]]]] = {}

        for _, r in post_rows.iterrows():
            key = (str(r.get(node_col, "")).strip(), str(r.get(uecfg_col, "")).strip())
            pid = str(r.get(profile_id_col, "")).strip()
            norm = _normalize_row_for_compare(r, compare_cols)
            sig = tuple(norm.get(c) for c in compare_cols)
            post_exact.add((key, pid, sig))
            post_by_key_and_profile.setdefault(key, {}).setdefault(pid, []).append(norm)

        missing_count = 0
        discrepancy_count = 0
        missing_nodes: Set[str] = set()
        discrepancy_nodes_to_cols: Dict[str, Set[str]] = {}

        for _, pre in pre_rows.iterrows():
            key = (str(pre.get(node_col, "")).strip(), str(pre.get(uecfg_col, "")).strip())
            pre_pid = str(pre.get(profile_id_col, "")).strip()
            expected_pid = _replace_int_token(pre_pid, ssb_pre_int_local, ssb_post_int_local)
            pre_norm = _normalize_row_for_compare(pre, compare_cols)
            sig = tuple(pre_norm.get(c) for c in compare_cols)

            # Perfect match
            if (key, expected_pid, sig) in post_exact:
                continue

            node_val = key[0]
            candidates = post_by_key_and_profile.get(key, {}).get(expected_pid, [])
            if not candidates:
                missing_count += 1
                if node_val:
                    missing_nodes.add(node_val)
                continue

            discrepancy_count += 1
            diff_cols = _best_diff_columns(pre_norm, candidates, compare_cols)
            if node_val:
                discrepancy_nodes_to_cols.setdefault(node_val, set()).update(diff_cols)

        if not skip_inconsistencies:
            add_row_fn(table_name_local, "Profiles Inconsistencies", metric_missing, missing_count, ", ".join(sorted(missing_nodes)))
        add_row_fn(table_name_local, "Profiles Discrepancies", metric_discr, discrepancy_count, _format_discrepancy_extrainfo(discrepancy_nodes_to_cols))

    def _process_profiles_table_non_uecfg(work: pd.DataFrame, table_name_local: str, node_col: str, moid_col: str, reserved_col: Optional[str], add_row_fn, ssb_pre_int_local: int, ssb_post_int_local: int, metric_missing: str, metric_discr: str, skip_inconsistencies: bool = False) -> None:
        """
        Audit a non-UeCfg table by pairing rows using MOid replacement old->new (string token replacement).
        """
        exclude = {moid_col}
        if reserved_col:
            exclude.add(reserved_col)
        compare_cols = [c for c in work.columns if c not in exclude]

        pre_rows = work.loc[work[moid_col].map(lambda v: _contains_int_token(str(v), ssb_pre_int_local))].copy()
        post_rows = work.loc[work[moid_col].map(lambda v: _contains_int_token(str(v), ssb_post_int_local))].copy()

        if pre_rows.empty:
            if not skip_inconsistencies:
                add_row_fn(table_name_local, "Profiles Inconsistencies", metric_missing, 0, "")
            add_row_fn(table_name_local, "Profiles Discrepancies", metric_discr, 0, "")
            return

        post_index_exact: Set[Tuple[str, Tuple[Optional[str], ...]]] = set()
        post_by_moid: Dict[str, List[Dict[str, Optional[str]]]] = {}

        for _, r in post_rows.iterrows():
            moid_val = str(r[moid_col]).strip()
            normalized = _normalize_row_for_compare(r, compare_cols)
            post_index_exact.add((moid_val, tuple(normalized.get(c) for c in compare_cols)))
            post_by_moid.setdefault(moid_val, []).append(normalized)

        missing_count = 0
        discrepancy_count = 0
        missing_nodes: Set[str] = set()
        discrepancy_nodes_to_cols: Dict[str, Set[str]] = {}

        for _, pre in pre_rows.iterrows():
            expected_post_moid = _replace_int_token(str(pre[moid_col]).strip(), ssb_pre_int_local, ssb_post_int_local)
            pre_norm = _normalize_row_for_compare(pre, compare_cols)
            exact_key = (expected_post_moid, tuple(pre_norm.get(c) for c in compare_cols))

            if exact_key in post_index_exact:
                continue

            node_val = str(pre.get(node_col, "")).strip()
            candidates = post_by_moid.get(expected_post_moid, [])
            if not candidates:
                missing_count += 1
                if node_val:
                    missing_nodes.add(node_val)
                continue

            discrepancy_count += 1
            diff_cols = _best_diff_columns(pre_norm, candidates, compare_cols)
            if node_val:
                discrepancy_nodes_to_cols.setdefault(node_val, set()).update(diff_cols)

        if not skip_inconsistencies:
            add_row_fn(table_name_local, "Profiles Inconsistencies", metric_missing, missing_count, ", ".join(sorted(missing_nodes)))
        add_row_fn(table_name_local, "Profiles Discrepancies", metric_discr, discrepancy_count, _format_discrepancy_extrainfo(discrepancy_nodes_to_cols))

    def _process_single_profiles_table(df, table_name: str, moid_col_name: str, add_row_fn, ssb_pre_int_local: Optional[int], ssb_post_int_local: Optional[int]) -> None:
        metric_missing = f"Profiles with old N77 SSB ({ssb_pre_int_local}) but not new N77 SSB ({ssb_post_int_local}) (from {table_name})"
        metric_discr = f"Profiles with old N77 SSB ({ssb_pre_int_local}) and new N77 SSB ({ssb_post_int_local}) but with param discrepancies (from {table_name})"
        metric_missing_suffix = f"Profiles with old N77 SSB (xxxx_{ssb_pre_int_local}) but not new N77 SSB (xxxx_{ssb_post_int_local}) (from McpcPCellNrFreqRelProfileUeCfg)"

        try:
            if df is None or df.empty:
                add_row_fn(table_name, "Profiles Inconsistencies", metric_missing, "Table not found or empty", "")
                add_row_fn(table_name, "Profiles Discrepancies", metric_discr, "Table not found or empty", "")
                if table_name == "McpcPCellNrFreqRelProfileUeCfg":
                    add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_missing_suffix, "Table not found or empty", "")
                return

            if ssb_pre_int_local is None or ssb_post_int_local is None:
                add_row_fn(table_name, "Profiles Inconsistencies", metric_missing, 0, f"ERROR: Invalid SSB values ({ssb_pre_int_local}, {ssb_post_int_local})")
                add_row_fn(table_name, "Profiles Discrepancies", metric_discr, 0, f"ERROR: Invalid SSB values ({ssb_pre_int_local}, {ssb_post_int_local})")
                if table_name == "McpcPCellNrFreqRelProfileUeCfg":
                    add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_missing_suffix, 0, f"ERROR: Invalid SSB values ({ssb_pre_int_local}, {ssb_post_int_local})")
                return

            node_col = resolve_column_case_insensitive(df, ["NodeId"])
            moid_col = resolve_column_case_insensitive(df, [moid_col_name])
            reserved_col = resolve_column_case_insensitive(df, ["reservedBy", "ReservedBy"])

            if not node_col or not moid_col:
                add_row_fn(table_name, "Profiles Inconsistencies", metric_missing, "N/A", "NodeId / MOid column missing")
                add_row_fn(table_name, "Profiles Discrepancies", metric_discr, "N/A", "NodeId / MOid column missing")
                if table_name == "McpcPCellNrFreqRelProfileUeCfg":
                    add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_missing_suffix, "N/A", "NodeId / MOid column missing")
                return

            work = df.copy()
            work[node_col] = work[node_col].astype(str).str.strip()
            work[moid_col] = work[moid_col].astype(str).str.strip()

            uecfg_col = _resolve_uecfg_id_col_for_profile_id(work, moid_col_name)
            if uecfg_col:
                work[uecfg_col] = work[uecfg_col].astype(str).str.strip()

                # Special case: McpcPCellNrFreqRelProfileUeCfg
                # - Do NOT emit the generic "Profiles with old SSB but not new SSB" inconsistency row.
                # - Emit discrepancies row as usual.
                # - Always emit the suffix-style "xxxx_<SSB>" clone-missing inconsistency row for ALL nodes.
                if table_name == "McpcPCellNrFreqRelProfileUeCfg":
                    _process_profiles_table_uecfg(work=work, table_name_local=table_name, node_col=node_col, profile_id_col=moid_col, uecfg_col=uecfg_col, reserved_col=reserved_col, add_row_fn=add_row_fn, ssb_pre_int_local=ssb_pre_int_local, ssb_post_int_local=ssb_post_int_local, metric_missing=metric_missing, metric_discr=metric_discr, skip_inconsistencies=True)  # <<< replace generic row with suffix check
                    _add_missing_suffix_profiles_check(work=work.copy(), node_col=node_col, profile_id_col=moid_col, uecfg_col=uecfg_col, add_row_fn=add_row_fn, ssb_pre_int_local=ssb_pre_int_local, ssb_post_int_local=ssb_post_int_local, metric_text=metric_missing_suffix)
                    return

                # Default UeCfg logic for other tables
                _process_profiles_table_uecfg(work=work, table_name_local=table_name, node_col=node_col, profile_id_col=moid_col, uecfg_col=uecfg_col, reserved_col=reserved_col, add_row_fn=add_row_fn, ssb_pre_int_local=ssb_pre_int_local, ssb_post_int_local=ssb_post_int_local, metric_missing=metric_missing, metric_discr=metric_discr, skip_inconsistencies=False)
                return

            # Non-UeCfg tables
            skip_incons = (table_name == "McpcPCellNrFreqRelProfileUeCfg")
            _process_profiles_table_non_uecfg(work=work, table_name_local=table_name, node_col=node_col, moid_col=moid_col, reserved_col=reserved_col, add_row_fn=add_row_fn, ssb_pre_int_local=ssb_pre_int_local, ssb_post_int_local=ssb_post_int_local, metric_missing=metric_missing, metric_discr=metric_discr, skip_inconsistencies=skip_incons)

            # If this table appears without UeCfgId for any reason, still emit the suffix check row.
            if table_name == "McpcPCellNrFreqRelProfileUeCfg":
                # Best effort: attempt to resolve a UeCfg column and run the suffix check if possible.
                uecfg_guess = resolve_column_case_insensitive(work, ["McpcPCellNrFreqRelProfileUeCfgId"])
                if uecfg_guess:
                    work[uecfg_guess] = work[uecfg_guess].astype(str).str.strip()
                    _add_missing_suffix_profiles_check(work=work.copy(), node_col=node_col, profile_id_col=moid_col, uecfg_col=uecfg_guess, add_row_fn=add_row_fn, ssb_pre_int_local=ssb_pre_int_local, ssb_post_int_local=ssb_post_int_local, metric_text=metric_missing_suffix)
                else:
                    add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_missing_suffix, "N/A", "Missing UeCfgId column for suffix check")

        except Exception as ex:
            add_row_fn(table_name, "Profiles Inconsistencies", metric_missing, 0, f"ERROR: {ex}")
            add_row_fn(table_name, "Profiles Discrepancies", metric_discr, 0, f"ERROR: {ex}")
            if table_name == "McpcPCellNrFreqRelProfileUeCfg":
                add_row_fn("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_missing_suffix, 0, f"ERROR: {ex}")

    profile_tables: List[Tuple[str, str]] = [
        ("McpcPCellNrFreqRelProfileUeCfg", "McpcPCellNrFreqRelProfileId"),
        ("McpcPCellProfileUeCfg", "McpcPCellProfileId"),
        ("UlQualMcpcMeasCfg", "UlQualMcpcMeasCfgId"),
        ("McpcPSCellProfileUeCfg", "McpcPSCellProfileId"),
        ("McfbCellProfile", "McfbCellProfileId"),
        ("McfbCellProfileUeCfg", "McfbCellProfileId"),
        ("TrStSaCellProfile", "TrStSaCellProfileId"),
        ("TrStSaCellProfileUeCfg", "TrStSaCellProfileId"),
        ("McpcPCellEUtranFreqRelProfile", "McpcPCellEUtranFreqRelProfileId"),
        ("McpcPCellEUtranFreqRelProfileUeCfg", "McpcPCellEUtranFreqRelProfileUeCfgId"),
        ("UeMCEUtranFreqRelProfile", "UeMCEUtranFreqRelProfileId"),
        ("UeMCEUtranFreqRelProfileUeCfg", "UeMCEUtranFreqRelProfileUeCfgId"),
    ]

    ssb_pre_int = _safe_parse_int(n77_ssb_pre)
    ssb_post_int = _safe_parse_int(n77_ssb_post)

    for table_name, moid_col_name in profile_tables:
        _process_single_profiles_table(dfs_by_table.get(table_name), table_name, moid_col_name, add_row, ssb_pre_int, ssb_post_int)


def cc_post_step2(df_nr_cell_cu: Optional[pd.DataFrame], df_eutran_freq_rel: Optional[pd.DataFrame], df_mcpc_pcell_nr_freq_rel_profile_uecfg: Optional[pd.DataFrame], add_row, n77_ssb_pre: object, n77_ssb_post: object, nodes_post: Optional[Iterable[object]] = None) -> None:
    """
    Post Step2 cleanup checks (scoped ONLY by nodes_post) that flag nodes still referencing old SSB (pre) in ref parameters,
    plus a prefix-style clone existence check for McpcPCellNrFreqRelProfileUeCfg.
    """

    def _check_nrcellcu_profile_refs(df_local: Optional[pd.DataFrame], ssb_pre_local: int, nodes_post_local: Set[str], metric_text: str) -> None:
        category = "NRCellCU"
        subcategory = "Profiles Inconsistencies"

        if df_local is None or df_local.empty:
            add_row(category, subcategory, metric_text, "Table not found or empty", "")
            return

        node_col = resolve_column_case_insensitive(df_local, ["NodeId"])
        if not node_col:
            add_row(category, subcategory, metric_text, "N/A", "Missing NodeId column in NRCellCU")
            return

        ref_cols_candidates: List[List[str]] = [
            ["mcpcPCellProfileRef"],
            ["mcpcPSCellProfileRef", "mcpcPsCellProfileRef"],
            ["mcfbCellProfileRef", "McfbCellProfileRef"],
            ["trStSaCellProfileRef", "TrStSaCellProfileRef"],
        ]
        ref_cols: List[str] = []
        for cands in ref_cols_candidates:
            c = resolve_column_case_insensitive(df_local, cands)
            if c:
                ref_cols.append(c)

        if not ref_cols:
            add_row(category, subcategory, metric_text, "N/A", "Missing profile ref columns in NRCellCU")
            return

        work = df_local.copy()
        work[node_col] = _normalize_node_series(work[node_col])
        work = work.loc[work[node_col].isin(nodes_post_local)].copy()
        if work.empty:
            add_row(category, subcategory, metric_text, 0, "")
            return

        bad_nodes: Set[str] = set()
        for _, r in work.iterrows():
            node = str(r.get(node_col, "")).strip()
            for c in ref_cols:
                if c in work.columns and _contains_int_token(r.get(c), ssb_pre_local):
                    if node:
                        bad_nodes.add(node)
                    break

        add_row(category, subcategory, metric_text, len(bad_nodes), _format_nodes(bad_nodes))

    def _check_eutranfreqrelation_profile_refs(df_local: Optional[pd.DataFrame], ssb_pre_local: int, nodes_post_local: Set[str], metric_text: str) -> None:
        category = "EUtranFreqRelation"
        subcategory = "Profiles Inconsistencies"

        if df_local is None or df_local.empty:
            add_row(category, subcategory, metric_text, "Table not found or empty", "")
            return

        node_col = resolve_column_case_insensitive(df_local, ["NodeId"])
        if not node_col:
            add_row(category, subcategory, metric_text, "N/A", "Missing NodeId in EUtranFreqRelation")
            return

        col1 = resolve_column_case_insensitive(df_local, ["mcpcPCellEUtranFreqRelProfileRef"])
        col2 = resolve_column_case_insensitive(df_local, ["UeMCEUtranFreqRelProfile", "ueMCEUtranFreqRelProfile"])
        ref_cols: List[str] = []
        if col1:
            ref_cols.append(col1)
        if col2:
            ref_cols.append(col2)

        if not ref_cols:
            add_row(category, subcategory, metric_text, "N/A", "Missing required profile ref columns in EUtranFreqRelation")
            return

        work = df_local.copy()
        work[node_col] = _normalize_node_series(work[node_col])
        work = work.loc[work[node_col].isin(nodes_post_local)].copy()
        if work.empty:
            add_row(category, subcategory, metric_text, 0, "")
            return

        bad_nodes: Set[str] = set()
        for _, r in work.iterrows():
            node = str(r.get(node_col, "")).strip()
            for c in ref_cols:
                if c in work.columns and _contains_int_token(r.get(c), ssb_pre_local):
                    if node:
                        bad_nodes.add(node)
                    break

        add_row(category, subcategory, metric_text, len(bad_nodes), _format_nodes(bad_nodes))

    def _check_mcpc_pcell_nr_freq_rel_profile_uecfg_prefix_profiles(df_local: Optional[pd.DataFrame], ssb_pre_local: int, ssb_post_local: int, nodes_post_local: Set[str], metric_text: str) -> None:
        category = "McpcPCellNrFreqRelProfileUeCfg"
        subcategory = "Profiles Inconsistencies"

        if df_local is None or df_local.empty:
            add_row(category, subcategory, metric_text, "Table not found or empty", "")
            return

        node_col = resolve_column_case_insensitive(df_local, ["NodeId"])
        pid_col = resolve_column_case_insensitive(df_local, ["McpcPCellNrFreqRelProfileId"])
        uecfg_col = resolve_column_case_insensitive(df_local, ["McpcPCellNrFreqRelProfileUeCfgId"])
        if not node_col or not pid_col:
            add_row(category, subcategory, metric_text, "N/A", "Missing NodeId / McpcPCellNrFreqRelProfileId")
            return

        work = df_local.copy()
        work[node_col] = _normalize_node_series(work[node_col])
        work = work.loc[work[node_col].isin(nodes_post_local)].copy()
        if work.empty:
            add_row(category, subcategory, metric_text, 0, "")
            return

        def _prefix_ssb(value: object) -> Optional[int]:
            s = "" if value is None else str(value).strip()
            if "_" not in s:
                return None
            token = s.split("_")[0].strip()
            if not token.isdigit():
                return None
            try:
                return int(token)
            except Exception:
                return None

        def _replace_prefix_ssb(profile_id: str, old_ssb: int, new_ssb: int) -> str:
            s = "" if profile_id is None else str(profile_id).strip()
            if "_" not in s:
                return s
            parts = s.split("_")
            if parts and parts[0].strip().isdigit() and int(parts[0].strip()) == int(old_ssb):
                parts[0] = str(new_ssb)
            return "_".join(parts)

        bad_nodes: Set[str] = set()
        work[pid_col] = work[pid_col].astype(str).str.strip()

        if uecfg_col and uecfg_col in work.columns:
            work[uecfg_col] = work[uecfg_col].astype(str).str.strip()
            for (node, uecfg), grp in work.groupby([node_col, uecfg_col], dropna=False):
                pids = set(grp[pid_col].dropna().astype(str).str.strip().tolist())
                for pid in pids:
                    if _prefix_ssb(pid) != ssb_pre_local:
                        continue
                    expected = _replace_prefix_ssb(pid, ssb_pre_local, ssb_post_local)
                    if expected not in pids:
                        node_str = str(node).strip()
                        if node_str:
                            bad_nodes.add(node_str)
                        break
        else:
            for node, grp in work.groupby(node_col, dropna=False):
                pids = set(grp[pid_col].dropna().astype(str).str.strip().tolist())
                for pid in pids:
                    if _prefix_ssb(pid) != ssb_pre_local:
                        continue
                    expected = _replace_prefix_ssb(pid, ssb_pre_local, ssb_post_local)
                    if expected not in pids:
                        node_str = str(node).strip()
                        if node_str:
                            bad_nodes.add(node_str)
                        break

        add_row(category, subcategory, metric_text, len(bad_nodes), _format_nodes(bad_nodes))

    ssb_pre = _safe_parse_int(n77_ssb_pre)
    ssb_post = _safe_parse_int(n77_ssb_post)

    nodes_post_set: Set[str] = set()
    if nodes_post:
        for n in nodes_post:
            s = "" if n is None else str(n).strip()
            if s:
                nodes_post_set.add(s)

    metric_nrcellcu = f"NR nodes with the new N77 SSB ({ssb_post}) and NRCellCU Ref parameters to Profiles with the old SSB name (from NRCellCU table)"
    metric_eutran = f"NR nodes with the new N77 SSB ({ssb_post}) and EUtranFreqRelation Ref parameters to Profiles with the old SSB name (from EUtranFreqRelation table)"
    metric_profile_uecfg = f"NR Nodes with the new N77 SSB ({ssb_post}) and Profiles with old N77 SSB ({ssb_pre}_xxxx) but not new N77 SSB ({ssb_post}_xxxx) (from McpcPCellNrFreqRelProfileUeCfg)"

    if ssb_pre is None or ssb_post is None:
        add_row("NRCellCU", "Profiles Inconsistencies", metric_nrcellcu, 0, f"Invalid SSB values ({ssb_pre}, {ssb_post})")
        add_row("EUtranFreqRelation", "Profiles Inconsistencies", metric_eutran, 0, f"Invalid SSB values ({ssb_pre}, {ssb_post})")
        add_row("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_profile_uecfg, 0, f"Invalid SSB values ({ssb_pre}, {ssb_post})")
        return

    if not nodes_post_set:
        add_row("NRCellCU", "Profiles Inconsistencies", metric_nrcellcu, 0, "nodes_post is empty (checks are scoped ONLY by nodes_post)")
        add_row("EUtranFreqRelation", "Profiles Inconsistencies", metric_eutran, 0, "nodes_post is empty (checks are scoped ONLY by nodes_post)")
        add_row("McpcPCellNrFreqRelProfileUeCfg", "Profiles Inconsistencies", metric_profile_uecfg, 0, "nodes_post is empty (checks are scoped ONLY by nodes_post)")
        return

    _check_nrcellcu_profile_refs(df_nr_cell_cu, ssb_pre, nodes_post_set, metric_nrcellcu)
    _check_eutranfreqrelation_profile_refs(df_eutran_freq_rel, ssb_pre, nodes_post_set, metric_eutran)
    _check_mcpc_pcell_nr_freq_rel_profile_uecfg_prefix_profiles(df_mcpc_pcell_nr_freq_rel_profile_uecfg, ssb_pre, ssb_post, nodes_post_set, metric_profile_uecfg)


# =====================================================================
#                           MODULE HELPERS
# =====================================================================

def _safe_parse_int(value: object) -> Optional[int]:
    """
    Convert an input value (int/str/etc) to an int using parse_int_frequency when possible.
    """
    parsed = parse_int_frequency(value)
    if parsed is not None:
        return int(parsed)
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _contains_int_token(text: object, number: int) -> bool:
    """
    Return True if `text` contains the exact integer token `number` not surrounded by other digits.
    """
    if text is None:
        return False
    s = str(text)
    pattern = rf"(?<!\d){re.escape(str(number))}(?!\d)"
    return re.search(pattern, s) is not None


def _format_nodes(nodes: Set[str]) -> str:
    return ", ".join(sorted(nodes))


def _normalize_node_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()
