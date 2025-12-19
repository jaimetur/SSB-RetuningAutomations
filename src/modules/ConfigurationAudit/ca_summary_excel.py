# -*- coding: utf-8 -*-

"""
Summary Audit Orchestrator.

This module coordinates:
  - NR audit logic
  - LTE audit logic
  - External / TermPoint enrichment
  - Other functions to build Excel Summary Table

IMPORTANT:
- This function explicitly RETURNS modified audit tables
  to avoid relying on side-effects.
"""

import pandas as pd
from typing import List, Dict

from src.modules.ConfigurationAudit.ca_process_others_tables import process_endc_distr_profile, process_freq_prio_nr, process_cardinalities
from src.modules.ConfigurationAudit.ca_process_external_termpoint_tables import process_external_nr_cell_cu, process_external_gutran_cell, process_termpoint_to_gnodeb, process_termpoint_to_gnb, process_term_point_to_enodeb
from src.modules.ConfigurationAudit.ca_process_lte_tables import process_gu_sync_signal_freq, process_gu_freq_rel, process_gu_cell_relation
from src.modules.ConfigurationAudit.ca_process_tables_nr import process_nr_cell_du, process_nr_freq, process_nr_freq_rel, process_nr_sector_carrier, process_nr_cell_relation
from src.modules.Common.common_functions import load_nodes_names_and_id_from_summary_audit
from src.utils.utils_frequency import parse_int_frequency


# =====================================================================
#                        SUMMARY AUDIT BUILDER
# =====================================================================

def build_summary_audit(
        df_nr_cell_du: pd.DataFrame,
        df_nr_freq: pd.DataFrame,
        df_nr_freq_rel: pd.DataFrame,
        df_nr_cell_rel: pd.DataFrame,
        df_freq_prio_nr: pd.DataFrame,
        df_gu_sync_signal_freq: pd.DataFrame,
        df_gu_freq_rel: pd.DataFrame,
        df_gu_cell_rel: pd.DataFrame,
        df_nr_sector_carrier: pd.DataFrame,
        df_endc_distr_profile: pd.DataFrame,
        n77_ssb_pre: int,
        n77_ssb_post: int,
        n77b_ssb: int,
        allowed_n77_ssb_pre,
        allowed_n77_arfcn_pre,
        allowed_n77_ssb_post,
        allowed_n77_arfcn_post,
        df_external_nr_cell_cu,
        df_external_gutran_cell,
        df_term_point_to_gnodeb,
        df_term_point_to_gnb,
        df_term_point_to_enodeb,
        module_name,

) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build a synthetic 'SummaryAudit' table with high-level checks:

      - N77 detection on NRCellDU and NRSectorCarrier.
      - NR/LTE nodes where specific SSBs (old_arfcn / new_arfcn) are defined.
      - NR/LTE nodes with SSBs not in {old_arfcn, new_arfcn}.
      - Cardinality limits per cell and per node.
      - EndcDistrProfile gUtranFreqRef values.

    Notes:
      - N77 cells are those with SSB/SSB in range [646600-660000].
      - This function is best-effort and should not raise exceptions; any error is
        represented as a row in the resulting dataframe.
    """

    allowed_n77_ssb_pre_set = {int(v) for v in (allowed_n77_ssb_pre or [])}
    allowed_n77_arfcn_pre_set = {int(v) for v in (allowed_n77_arfcn_pre or [])}
    allowed_n77_ssb_post_set = {int(v) for v in (allowed_n77_ssb_post or [])}
    allowed_n77_arfcn_post_set = {int(v) for v in (allowed_n77_arfcn_post or [])}

    rows: List[Dict[str, object]] = []

    # Detailed parameter mismatching rows to build Excel sheets "Summary NR Param Missmatching" and
    # "Summary LTE Param Missmatching"
    param_mismatch_rows_nr: List[Dict[str, object]] = []
    param_mismatch_rows_gu: List[Dict[str, object]] = []

    param_mismatch_columns_nr = [
        "Layer",  # "NR"
        "Table",  # "NRFreqRelation"
        "NodeId",
        "GNBCUCPFunctionId",
        "NRCellCUId",
        "NRFreqRelationId",
        "Parameter",
        "OldSSB",
        "NewSSB",
        "OldValue",
        "NewValue",
    ]

    param_mismatch_columns_gu = [
        "Layer",  # "LTE"
        "Table",  # "GUtranFreqRelation"
        "NodeId",
        "EUtranCellId",  # generic LTE cell id
        "GUtranFreqRelationId",
        "Parameter",
        "OldSSB",
        "NewSSB",
        "OldValue",
        "NewValue",
    ]

    # -------------------------------------  HELPERS (Embedded, minimal impact) -------------------------------------
    def is_not_old_not_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq not in (n77_ssb_pre, n77_ssb_post)

    def series_only_not_old_not_new(series) -> bool:
        return all(is_not_old_not_new(v) for v in series)

    def is_new(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == n77_ssb_post

    def is_old(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq == n77_ssb_pre

    def has_value(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq is not None

    def is_n77_ssb_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_pre_set if freq is not None else False

    def is_n77_ssb_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_ssb_post_set if freq is not None else False

    def is_n77_arfcn_pre_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_pre_set if freq is not None else False

    def is_n77_arfcn_post_allowed(v: object) -> bool:
        freq = parse_int_frequency(v)
        return freq in allowed_n77_arfcn_post_set if freq is not None else False

    def all_n77_arfcn_in_pre(series: pd.Series) -> bool:
        freqs = series.map(parse_int_frequency)
        freqs_valid = {f for f in freqs if f is not None}
        # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_arfcn_pre_set
        return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_arfcn_pre_set)

    def all_n77_arfcn_in_post(series: pd.Series) -> bool:
        freqs = series.map(parse_int_frequency)
        freqs_valid = {f for f in freqs if f is not None}
        # Node must have at least one valid N77 SSB and ALL of them in allowed_n77_arfcn_post_set
        return bool(freqs_valid) and freqs_valid.issubset(allowed_n77_arfcn_post_set)

    def add_row(
            category: str,
            subcategory: str,
            metric: str,
            value: object,
            extra: str = "",
    ) -> None:
        rows.append(
            {
                "Category": category,
                "SubCategory": subcategory,
                "Metric": metric,
                "Value": value,
                "ExtraInfo": extra,
            }
        )

    def extract_freq_from_nrfrequencyref(value: object) -> int | None:
        """Extract NRFrequency integer from reference string like '...NRFrequency=648672'."""
        if value is None:
            return None
        s = str(value)
        key = "NRFrequency="
        idx = s.rfind(key)
        if idx == -1:
            # fallback: try plain int in the string
            return parse_int_frequency(s)
        substr = s[idx + len(key):]
        digits = []
        for ch in substr:
            if ch.isdigit():
                digits.append(ch)
            else:
                break
        if not digits:
            return None
        try:
            return int("".join(digits))
        except Exception:
            return None

    # Helper embedded inside the main function
    def extract_freq_from_nrfreqrelationref(value: object) -> int | None:
        """Extract NRFreqRelation integer from NRCellRelation-like reference string."""
        if value is None:
            return None
        s = str(value)
        key = "NRFreqRelation="
        idx = s.rfind(key)
        if idx == -1:
            return None
        substr = s[idx + len(key):]
        digits = []
        for ch in substr:
            if ch.isdigit():
                digits.append(ch)
            else:
                break
        if not digits:
            return None
        try:
            return int("".join(digits))
        except Exception:
            return None

    def extract_ssb_from_gutran_sync_ref(value: object) -> int | None:
        """
        Extract SSB integer from references containing:
          GUtranSyncSignalFrequency=647328-30
        In that example, returned SSB is 647328.
        """
        if value is None:
            return None
        s = str(value)
        key = "GUtranSyncSignalFrequency="
        idx = s.rfind(key)
        if idx == -1:
            return None
        substr = s[idx + len(key):]
        digits = []
        for ch in substr:
            if ch.isdigit():
                digits.append(ch)
            else:
                break
        if not digits:
            return None
        try:
            return int("".join(digits))
        except Exception:
            return None

    def extract_nr_network_tail(value: object) -> str:
        """Return substring starting from 'NRNetwork='."""
        if value is None:
            return ""
        s = str(value)
        idx = s.find("NRNetwork=")
        return s[idx:] if idx != -1 else ""

    def normalize_state(value: object) -> str:
        """Normalize state values for robust comparisons."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value).strip().upper()

    def normalize_ip(value: object) -> str:
        """Normalize usedIpAddress values for robust comparisons."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value).strip()


    # ============================ MAIN CODE ================================
    # NR Tables
    process_nr_freq(df_nr_freq, has_value, add_row, is_old, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new)
    process_nr_freq_rel(df_nr_freq_rel, is_old, add_row, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, param_mismatch_rows_nr)
    process_nr_sector_carrier(df_nr_sector_carrier, add_row, allowed_n77_arfcn_pre_set, all_n77_arfcn_in_pre, allowed_n77_arfcn_post_set, all_n77_arfcn_in_post)
    process_nr_cell_du(df_nr_cell_du, add_row, allowed_n77_ssb_pre_set, allowed_n77_ssb_post_set)
    process_nr_cell_relation(df_nr_cell_rel, extract_freq_from_nrfreqrelationref, n77_ssb_pre, n77_ssb_post, add_row)

    # LTE Tables
    process_gu_sync_signal_freq(df_gu_sync_signal_freq, has_value, add_row, is_old, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new)
    process_gu_freq_rel(df_gu_freq_rel, is_old, add_row, n77_ssb_pre, is_new, n77_ssb_post, series_only_not_old_not_new, param_mismatch_rows_gu)
    process_gu_cell_relation(df_gu_cell_rel, n77_ssb_pre, n77_ssb_post, add_row)

    # Externals & Termpoints tables
    nodes_pre = set(load_nodes_names_and_id_from_summary_audit(rows, stage="Pre", module_name=module_name) or [])
    nodes_post = set(load_nodes_names_and_id_from_summary_audit(rows, stage="Post", module_name=module_name) or [])
    process_external_nr_cell_cu(df_external_nr_cell_cu, rows, module_name, n77_ssb_pre, n77_ssb_post, add_row, df_term_point_to_gnodeb, extract_freq_from_nrfrequencyref, extract_nr_network_tail, nodes_pre, nodes_post)
    process_external_gutran_cell(df_external_gutran_cell, extract_ssb_from_gutran_sync_ref, n77_ssb_pre, n77_ssb_post, add_row, normalize_state, df_term_point_to_gnb, rows, module_name, nodes_pre, nodes_post)
    process_termpoint_to_gnodeb(df_term_point_to_gnodeb, add_row, df_external_nr_cell_cu, n77_ssb_post, n77_ssb_pre)
    process_termpoint_to_gnb(df_term_point_to_gnb, normalize_state, normalize_ip, add_row, df_external_gutran_cell, n77_ssb_post, n77_ssb_pre)
    process_term_point_to_enodeb(df_term_point_to_enodeb, normalize_state, add_row)

    # Other Tables
    process_endc_distr_profile(df_endc_distr_profile, n77_ssb_pre, n77_ssb_post, n77b_ssb, add_row)
    process_freq_prio_nr(df_freq_prio_nr, n77_ssb_pre, n77_ssb_post, add_row)
    process_cardinalities(df_nr_freq, add_row, df_nr_freq_rel, df_gu_sync_signal_freq, df_gu_freq_rel)

    # If nothing was added, return at least an informational row
    if not rows:
        rows.append({
            "Category": "Info",
            "SubCategory": "Info",
            "Metric": "SummaryAudit",
            "Value": "No data available",
            "ExtraInfo": "",
        })

    # Build final DataFrame
    df = pd.DataFrame(rows)

    # Custom logical ordering for SummaryAudit (also drives PPT order)
    if not df.empty and all(col in df.columns for col in ["Category", "SubCategory", "Metric"]):
        # We now order only by (Category, SubCategory).
        # Inside each group, rows keep the insertion order.
        desired_order = [
            # NR Frequency NRCellDU
            ("NRCellDU", "NR Frequency Audit"),
            ("NRCellDU", "NR Frequency Inconsistencies"),

            # NR Frequency NRFrequency
            ("NRFrequency", "NR Frequency Audit"),
            ("NRFrequency", "NR Frequency Inconsistencies"),

            # NR Frequency NRFreqRelation
            ("NRFreqRelation", "NR Frequency Audit"),
            ("NRFreqRelation", "NR Frequency Inconsistencies"),

            # NR Frequency NRCellRelation
            ("NRCellRelation", "NR Frequency Audit"),
            ("NRCellRelation", "NR Frequency Inconsistencies"),

            # NEW: ExternalNRCellCU
            ("ExternalNRCellCU", "NR Frequency Audit"),
            ("ExternalNRCellCU", "NR Frequency Inconsistencies"),

            # NEW: TermPointToGNodeB
            ("TermPointToGNodeB", "NR Termpoint Audit"),

            # NR Frequency NRSectorCarrier
            ("NRSectorCarrier", "NR Frequency Audit"),
            ("NRSectorCarrier", "NR Frequency Inconsistencies"),

            # LTE Frequency GUtranSyncSignalFrequency
            ("GUtranSyncSignalFrequency", "LTE Frequency Audit"),
            ("GUtranSyncSignalFrequency", "LTE Frequency Inconsistencies"),

            # LTE Frequency GUtranSyncSignalFrequency
            ("GUtranFreqRelation", "LTE Frequency Audit"),
            ("GUtranFreqRelation", "LTE Frequency Inconsistencies"),

            # LTE Frequency GUtranCellRelation
            ("GUtranCellRelation", "LTE Frequency Audit"),
            ("GUtranCellRelation", "LTE Frequency Inconsistencies"),

            # NEW: ExternalGUtranCell
            ("ExternalGUtranCell", "LTE Frequency Audit"),
            ("ExternalGUtranCell", "LTE Frequency Inconsistencies"),

            # NEW: TermPointToGNB / TermPointToENodeB
            ("TermPointToGNB", "X2 Termpoint Audit"),
            ("TermPointToENodeB", "X2 Termpoint Audit"),

            # EndcDistrProfile
            ("EndcDistrProfile", "ENDC Audit"),
            ("EndcDistrProfile", "ENDC Inconsistencies"),

            # FreqPrioNR
            ("FreqPrioNR", "ENDC Audit"),
            ("FreqPrioNR", "ENDC Inconsistencies"),

            # Cardinality NRFrequency
            ("Cardinality NRFrequency", "Cardinality Audit"),
            ("Cardinality NRFrequency", "Cardinality Inconsistencies"),

            # Cardinality NRFreqRelation
            ("Cardinality NRFreqRelation", "Cardinality Audit"),
            ("Cardinality NRFreqRelation", "Cardinality Inconsistencies"),

            # Cardinality GUtranSyncSignalFrequency
            ("Cardinality GUtranSyncSignalFrequency", "Cardinality Audit"),
            ("Cardinality GUtranSyncSignalFrequency", "Cardinality Inconsistencies"),

            # Cardinality GUtranFreqRelation
            ("Cardinality GUtranFreqRelation", "Cardinality Audit"),
            ("Cardinality GUtranFreqRelation", "Cardinality Inconsistencies"),
        ]

        order_map = {k: i for i, k in enumerate(desired_order)}

        df["_order_"] = df.apply(
            lambda r: order_map.get((r["Category"], r["SubCategory"]), len(order_map)),
            axis=1,
        )

        df = df.sort_values(by=["_order_"], kind="stable").drop(columns="_order_").reset_index(drop=True)

        # Build NR param mismatching dataframe
    df_param_mismatch_nr = pd.DataFrame(param_mismatch_rows_nr, columns=param_mismatch_columns_nr)
    if df_param_mismatch_nr.empty:
        df_param_mismatch_nr = pd.DataFrame(columns=param_mismatch_columns_nr)

    # Build LTE param mismatching dataframe
    df_param_mismatch_gu = pd.DataFrame(param_mismatch_rows_gu, columns=param_mismatch_columns_gu)
    if df_param_mismatch_gu.empty:
        df_param_mismatch_gu = pd.DataFrame(columns=param_mismatch_columns_gu)

    return df, df_param_mismatch_nr, df_param_mismatch_gu

