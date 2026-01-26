from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from src.utils.utils_dataframe import build_row_lookup, pick_non_empty_value, ensure_column_after
from src.utils.utils_parsing import extract_gnbcucp_segment, resolve_nrcell_ref

"""
Helpers to build Correction_Cmd columns and export them to text files.

This module centralizes the logic used by ConsistencyChecks so that:
- Code is reused between GU/NR and new/missing/disc variants.
- The main ConsistencyChecks module stays smaller and easier to read.
"""

# ----------------------------------------------------------------------
#  BUILD CORRECTION COMMANDS
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
#  GU  -  NEW
# ----------------------------------------------------------------------
def build_correction_command_gu_new_relations(df: pd.DataFrame, relations_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Add 'Correction_Cmd' column for GU_new sheet, using relation table as main data source.

    Format example:
      del EUtranCellFDD=<EUtranCellFDDId>,GUtranFreqRelation=<GUtranFreqRelationId>,GUtranCellRelation=<GUtranCellRelationId>
    """
    if df is None or df.empty:
        df = df.copy() if df is not None else pd.DataFrame()
        # Ensure basic columns exist
        for col in ("NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"):
            if col not in df.columns:
                df[col] = ""
        # NEW: ensure Freq_Pre / Freq_Post exist in _new (Pre empty)
        if "Freq_Pre" not in df.columns:
            df["Freq_Pre"] = ""
        if "Freq_Post" not in df.columns:
            df["Freq_Post"] = ""
        # Extra audit columns requested for GU_new
        for col in ("createdBy", "timeOfCreation"):
            if col not in df.columns:
                df[col] = ""
        df["Correction_Cmd"] = ""
    else:
        df = df.copy()

        # Ensure key columns exist
        for col in ("NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"):
            if col not in df.columns:
                df[col] = ""

        # NEW: ensure Freq_Pre / Freq_Post also exist in GU_new
        if "Freq_Pre" not in df.columns:
            df["Freq_Pre"] = ""  # in _new tables Pre must be empty
        if "Freq_Post" not in df.columns:
            df["Freq_Post"] = ""

        # Extra audit columns requested for GU_new
        if "createdBy" not in df.columns:
            df["createdBy"] = ""
        if "timeOfCreation" not in df.columns:
            df["timeOfCreation"] = ""

        df["NodeId"] = df["NodeId"].astype(str).str.strip()
        df["EUtranCellFDDId"] = df["EUtranCellFDDId"].astype(str).str.strip()
        df["GUtranCellRelationId"] = df["GUtranCellRelationId"].astype(str).str.strip()

        relations_lookup = build_row_lookup(relations_df, ["EUtranCellFDDId", "GUtranCellRelationId"])

        def _get_rel_row(row: pd.Series) -> Optional[pd.Series]:
            key = (
                str(row.get("EUtranCellFDDId", "")).strip(),
                str(row.get("GUtranCellRelationId", "")).strip(),
            )
            return relations_lookup.get(key)

        def _from_rel(row: pd.Series, field: str) -> str:
            rel_row = _get_rel_row(row)
            return pick_non_empty_value(rel_row, row, field)

        # Make sure GUtranFreqRelationId, createdBy and timeOfCreation are taken from relations_df
        df["GUtranFreqRelationId"] = df.apply(lambda r: _from_rel(r, "GUtranFreqRelationId"), axis=1)
        df["createdBy"] = df.apply(lambda r: _from_rel(r, "createdBy"), axis=1)
        df["timeOfCreation"] = df.apply(lambda r: _from_rel(r, "timeOfCreation"), axis=1)

        def _build_correction_command(row: pd.Series, rel_row: Optional[pd.Series]) -> str:
            """
            Build correction command for GU_new row.
            Safely returns empty string if mandatory parameters are missing.
            """
            src = rel_row if rel_row is not None else row

            eu_cell = str(src.get("EUtranCellFDDId") or "").strip()
            freq_rel = str(src.get("GUtranFreqRelationId") or "").strip()
            cell_rel = str(src.get("GUtranCellRelationId") or "").strip()

            if not (eu_cell and freq_rel and cell_rel):
                return ""

            return f"del EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}"

        df["Correction_Cmd"] = df.apply(lambda r: _build_correction_command(r, _get_rel_row(r)), axis=1)

    # Final column set: keep only relevant columns and force Correction_Cmd to be last
    desired_cols = [
        "NodeId",
        "EUtranCellFDDId",
        "GUtranFreqRelationId",
        "GUtranCellRelationId",
        "Freq_Pre",
        "Freq_Post",
        "createdBy",
        "timeOfCreation",
        "Correction_Cmd",
    ]
    cols = [c for c in desired_cols if c in df.columns]
    df = df[cols]
    return df


# ----------------------------------------------------------------------
#  GU  -  MISSING
# ----------------------------------------------------------------------
def build_correction_command_gu_missing_relations(
    df: pd.DataFrame,
    relations_df: Optional[pd.DataFrame],
    n77_ssb_pre: Optional[str] = None,
    n77_ssb_post: Optional[str] = None,
) -> pd.DataFrame:
    """
    Add 'Correction_Cmd' column for GU_missing sheet, building a multiline correction script.

    All placeholders are taken from the relation table whenever possible (GU_relations),
    using (EUtranCellFDDId, GUtranCellRelationId) as lookup key.
    """
    if df is None or df.empty:
        df = df.copy() if df is not None else pd.DataFrame()
        # Ensure minimal columns to keep Excel structure stable
        for col in ("NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"):
            if col not in df.columns:
                df[col] = ""
        if "Freq_Pre" not in df.columns:
            df["Freq_Pre"] = ""
        if "Freq_Post" not in df.columns:
            df["Freq_Post"] = ""
        df["Correction_Cmd"] = ""
        # Final projection
        desired_cols = [
            "NodeId",
            "EUtranCellFDDId",
            "GUtranFreqRelationId",
            "GUtranCellRelationId",
            "Freq_Pre",
            "Freq_Post",
            "Correction_Cmd",
        ]
        cols = [c for c in desired_cols if c in df.columns]
        return df[cols]

    df = df.copy()

    # Ensure key columns exist in df (aunque luego algunas se eliminen de la tabla final)
    for col in ("NodeId", "EUtranCellFDDId", "GUtranFreqRelationId", "GUtranCellRelationId"):
        if col not in df.columns:
            df[col] = ""

    # Freq_Pre / Freq_Post may come already from comparison; ensure they exist
    if "Freq_Pre" not in df.columns:
        df["Freq_Pre"] = ""
    if "Freq_Post" not in df.columns:
        df["Freq_Post"] = ""

    df["NodeId"] = df["NodeId"].astype(str).str.strip()
    df["EUtranCellFDDId"] = df["EUtranCellFDDId"].astype(str).str.strip()
    df["GUtranCellRelationId"] = df["GUtranCellRelationId"].astype(str).str.strip()

    relations_lookup = build_row_lookup(relations_df, ["NodeId", "EUtranCellFDDId", "GUtranCellRelationId"])

    def _get_rel_row(row: pd.Series) -> Optional[pd.Series]:
        key = (
            str(row.get("NodeId", "")).strip(),
            str(row.get("EUtranCellFDDId", "")).strip(),
            str(row.get("GUtranCellRelationId", "")).strip(),
        )
        return relations_lookup.get(key)

    def from_rel(row: pd.Series, field: str) -> str:
        rel_row = _get_rel_row(row)
        return pick_non_empty_value(rel_row, row, field)

    # Make sure GUtranFreqRelationId is taken from relations_df
    df["GUtranFreqRelationId"] = df.apply(lambda r: from_rel(r, "GUtranFreqRelationId"), axis=1)

    def _build_correction_command(row: pd.Series, rel_row: Optional[pd.Series]) -> str:
        """
        Build correction command for GU_missing row.
        Safely returns empty string if mandatory parameters are missing.
        """
        enb_func = pick_non_empty_value(rel_row, row, "ENodeBFunctionId")
        eu_cell = pick_non_empty_value(rel_row, row, "EUtranCellFDDId")
        freq_rel = pick_non_empty_value(rel_row, row, "GUtranFreqRelationId")
        cell_rel = pick_non_empty_value(rel_row, row, "GUtranCellRelationId")
        neighbor_ref = pick_non_empty_value(rel_row, row, "neighborCellRef")
        is_endc = pick_non_empty_value(rel_row, row, "isEndcAllowed")
        is_ho = pick_non_empty_value(rel_row, row, "isHoAllowed")
        is_remove = pick_non_empty_value(rel_row, row, "isRemoveAllowed")
        is_voice_ho = pick_non_empty_value(rel_row, row, "isVoiceHoAllowed")
        user_label = pick_non_empty_value(rel_row, row, "userLabel")
        coverage = pick_non_empty_value(rel_row, row, "coverageIndicator")

        # Overwrite GUtranFreqRelationId to a hardcoded value (new SSB) only when old SSB is found
        if n77_ssb_pre and isinstance(freq_rel, str) and freq_rel.startswith(str(n77_ssb_pre)):
            freq_rel = f"{n77_ssb_post}-30-20-0-1" if n77_ssb_post else freq_rel

        if not user_label:
            user_label = "SSBretune"

        if not (enb_func and eu_cell and freq_rel and cell_rel):
            return ""

        # NEW: keep only GUtraNetwork / ExternalGNodeBFunction / ExternalGUtranCell part
        clean_neighbor_ref = neighbor_ref
        if isinstance(neighbor_ref, str) and "GUtraNetwork=" in neighbor_ref:
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
            (
                f"set EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel} coverageIndicator {coverage}"
                if coverage
                else f"set EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}"
            ),
        ]
        # keep non-empty
        lines = [p for p in parts if p]
        return "\n".join(lines)

    df["Correction_Cmd"] = df.apply(lambda r: _build_correction_command(r, _get_rel_row(r)), axis=1)

    # Final column set: keep only relevant columns and force Correction_Cmd to be last
    desired_cols = [
        "NodeId",
        "EUtranCellFDDId",
        "GUtranFreqRelationId",
        "GUtranCellRelationId",
        "Freq_Pre",
        "Freq_Post",
        "Correction_Cmd",
    ]
    cols = [c for c in desired_cols if c in df.columns]
    df = df[cols]
    return df


# ----------------------------------------------------------------------
#  GU  -  DISCREPANCIES
# ----------------------------------------------------------------------
def build_correction_command_gu_discrepancies(
    disc_df: pd.DataFrame,
    relations_df: Optional[pd.DataFrame],
    n77_ssb_pre: Optional[str] = None,
    n77_ssb_post: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build delete+create Correction_Cmd blocks for GU_disc rows using GU_new/GU_missing logic.
    """
    if disc_df is None or disc_df.empty:
        work = disc_df.copy() if disc_df is not None else pd.DataFrame()
        work["Correction_Cmd"] = ""
        return work

    work = disc_df.copy()
    if "NodeId" not in work.columns:
        work["NodeId"] = ""

    # Reuse existing builders (they already include their own internal _build_correction_command)
    del_df = build_correction_command_gu_new_relations(disc_df.copy(), relations_df)
    create_df = build_correction_command_gu_missing_relations(disc_df.copy(), relations_df, n77_ssb_pre, n77_ssb_post)

    del_cmds = del_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)
    create_cmds = create_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)

    def _build_correction_command(del_cmd: str, create_cmd: str) -> str:
        """
        Build combined correction command for GU_disc row.
        Safely returns empty string if both parts are empty.
        """
        del_cmd = (del_cmd or "").strip()
        create_cmd = (create_cmd or "").strip()
        if del_cmd and create_cmd:
            return f"{del_cmd}\n{create_cmd}"
        return del_cmd or create_cmd

    work["Correction_Cmd"] = [
        _build_correction_command(d, c) for d, c in zip(del_cmds, create_cmds)
    ]

    # For _disc we keep all original discrepancy columns + Correction_Cmd
    return work


# ----------------------------------------------------------------------
#  NR  -  NEW
# ----------------------------------------------------------------------
def build_correction_command_nr_new_relations(df: pd.DataFrame, relations_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Add 'Correction_Cmd' column for NR_new sheet, using relation table as main data source.

    Format example:
      del NRCellCU=<NRCellCUId>,NRCellRelation=<NRCellRelationId>
    """
    # -----------------------------
    # Edge case: empty / None df
    # -----------------------------
    if df is None or df.empty:
        df = df.copy() if df is not None else pd.DataFrame()

        # Ensure key columns exist
        for col in ("NodeId", "NRCellCUId", "NRCellRelationId"):
            if col not in df.columns:
                df[col] = ""

        # Helper / frequency columns
        if "GNBCUCPFunctionId" not in df.columns:
            df["GNBCUCPFunctionId"] = ""
        if "Freq_Pre" not in df.columns:
            df["Freq_Pre"] = ""
        if "Freq_Post" not in df.columns:
            df["Freq_Post"] = ""

        # Correction_Cmd column (empty)
        df["Correction_Cmd"] = ""

    else:
        # -----------------------------
        # Normal case
        # -----------------------------
        df = df.copy()

        # Ensure key columns exist
        for col in ("NodeId", "NRCellCUId", "NRCellRelationId"):
            if col not in df.columns:
                df[col] = ""

        # Ensure helper / frequency columns exist
        if "GNBCUCPFunctionId" not in df.columns:
            df["GNBCUCPFunctionId"] = ""
        if "Freq_Pre" not in df.columns:
            df["Freq_Pre"] = ""   # in _new tables Pre must be empty
        if "Freq_Post" not in df.columns:
            df["Freq_Post"] = ""

        # Normalize key columns
        df["NodeId"] = df["NodeId"].astype(str).str.strip()
        df["NRCellCUId"] = df["NRCellCUId"].astype(str).str.strip()
        df["NRCellRelationId"] = df["NRCellRelationId"].astype(str).str.strip()

        # Lookup that also carries nRCellRef to be able to extract GNBCUCPFunction segment
        relations_lookup = build_row_lookup(
            relations_df,
            ["NodeId", "NRCellCUId", "NRCellRelationId"],
            extra_strip_cols=["nRCellRef"],
        )

        def _get_rel_row(row: pd.Series) -> Optional[pd.Series]:
            key = (
                str(row.get("NodeId", "")).strip(),
                str(row.get("NRCellCUId", "")).strip(),
                str(row.get("NRCellRelationId", "")).strip(),
            )
            return relations_lookup.get(key)

        # -----------------------------
        # Build delete commands
        # -----------------------------
        def _build_correction_command(row: pd.Series, rel_row: Optional[pd.Series]) -> str:
            """
            Build correction command for NR_new row.
            Safely returns empty string if mandatory parameters are missing.
            """
            src = rel_row if rel_row is not None else row
            nr_cell_cu = str(src.get("NRCellCUId") or "").strip()
            nr_cell_rel = str(src.get("NRCellRelationId") or "").strip()
            if not (nr_cell_cu and nr_cell_rel):
                return ""
            return f"del NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}"

        df["Correction_Cmd"] = df.apply(lambda r: _build_correction_command(r, _get_rel_row(r)), axis=1)

        # -----------------------------
        # Fill GNBCUCPFunctionId from nRCellRef (same logic style as missing/disc)
        # -----------------------------
        df["GNBCUCPFunctionId"] = df.apply(
            lambda r: extract_gnbcucp_segment(resolve_nrcell_ref(r, relations_lookup)),
            axis=1,
        )

    # Place GNBCUCPFunctionId after NRCellRelationId
    df = ensure_column_after(df, "GNBCUCPFunctionId", "NRCellRelationId")

    # -----------------------------
    # Final column set for NR_new:
    # keep only relevant columns and force Correction_Cmd to be last
    # -----------------------------
    desired_cols = [
        "NodeId",
        "NRCellCUId",
        "NRCellRelationId",
        "GNBCUCPFunctionId",
        "Freq_Pre",
        "Freq_Post",
        "Correction_Cmd",
    ]
    cols = [c for c in desired_cols if c in df.columns]
    df = df[cols]
    return df


# ----------------------------------------------------------------------
#  NR  -  MISSING
# ----------------------------------------------------------------------
def build_correction_command_nr_missing_relations(
    df: pd.DataFrame,
    relations_df: Optional[pd.DataFrame],
    n77_ssb_pre: Optional[str] = None,
    n77_ssb_post: Optional[str] = None,
) -> pd.DataFrame:
    """
    Add 'Correction_Cmd' column for NR_missing sheet, building a multiline correction script.
    """
    if df is None or df.empty:
        df = df.copy() if df is not None else pd.DataFrame()
        # Minimal columns to keep sheet structure stable
        for col in ("NodeId", "NRCellCUId", "NRCellRelationId"):
            if col not in df.columns:
                df[col] = ""
        if "GNBCUCPFunctionId" not in df.columns:
            df["GNBCUCPFunctionId"] = ""
        if "Freq_Pre" not in df.columns:
            df["Freq_Pre"] = ""
        if "Freq_Post" not in df.columns:
            df["Freq_Post"] = ""
        df["Correction_Cmd"] = ""
        df = ensure_column_after(df, "GNBCUCPFunctionId", "NRCellRelationId")

        desired_cols = [
            "NodeId",
            "NRCellCUId",
            "NRCellRelationId",
            "GNBCUCPFunctionId",
            "Freq_Pre",
            "Freq_Post",
            "Correction_Cmd",
        ]
        cols = [c for c in desired_cols if c in df.columns]
        return df[cols]

    df = df.copy()

    # Ensure key columns exist
    for col in ("NodeId", "NRCellCUId", "NRCellRelationId"):
        if col not in df.columns:
            df[col] = ""

    # Freq_Pre / Freq_Post may come from comparison; ensure they exist
    if "Freq_Pre" not in df.columns:
        df["Freq_Pre"] = ""
    if "Freq_Post" not in df.columns:
        df["Freq_Post"] = ""

    if "GNBCUCPFunctionId" not in df.columns:
        df["GNBCUCPFunctionId"] = ""

    df["NodeId"] = df["NodeId"].astype(str).str.strip()
    df["NRCellCUId"] = df["NRCellCUId"].astype(str).str.strip()
    df["NRCellRelationId"] = df["NRCellRelationId"].astype(str).str.strip()

    relations_lookup = build_row_lookup(
        relations_df, ["NodeId", "NRCellCUId", "NRCellRelationId"], extra_strip_cols=["nRCellRef"]
    )

    def _get_rel_row(row: pd.Series) -> Optional[pd.Series]:
        key = (
            str(row.get("NodeId", "")).strip(),
            str(row.get("NRCellCUId", "")).strip(),
            str(row.get("NRCellRelationId", "")).strip(),
        )
        return relations_lookup.get(key)

    def _build_correction_command(row: pd.Series, rel_row: Optional[pd.Series]) -> str:
        """
        Build correction command for NR_missing row.
        Safely returns empty string if mandatory parameters are missing.
        """
        nr_cell_cu = pick_non_empty_value(rel_row, row, "NRCellCUId")
        nr_cell_rel = pick_non_empty_value(rel_row, row, "NRCellRelationId")
        coverage = pick_non_empty_value(rel_row, row, "coverageIndicator")
        is_ho = pick_non_empty_value(rel_row, row, "isHoAllowed")
        is_remove = pick_non_empty_value(rel_row, row, "isRemoveAllowed")
        s_cell_candidate = pick_non_empty_value(rel_row, row, "sCellCandidate")
        nrcell_ref = pick_non_empty_value(rel_row, row, "nRCellRef")
        nrfreq_ref = pick_non_empty_value(rel_row, row, "nRFreqRelationRef")

        if not (nr_cell_cu and nr_cell_rel):
            return ""

        # --------- nRCellRef cleanup: keep everything from GNBCUCPFunction= ---------
        clean_nrcell_ref = ""
        clean_nrcell_ref_short = ""
        if isinstance(nrcell_ref, str) and "GNBCUCPFunction=" in nrcell_ref:
            clean_nrcell_ref = nrcell_ref[nrcell_ref.find("GNBCUCPFunction="):]
            if "ExternalGNBCUCPFunction=" in nrcell_ref:
                clean_nrcell_ref_short = nrcell_ref[nrcell_ref.find("ExternalGNBCUCPFunction="):]

        # --------- nRFreqRelationRef cleanup ---------
        clean_nrfreq_ref = ""
        if isinstance(nrfreq_ref, str) and "GNBCUCPFunction=" in nrfreq_ref:
            sub = nrfreq_ref[nrfreq_ref.find("GNBCUCPFunction="):]
            gnb_part = sub.split(",", 1)[0]
            gnb_val = gnb_part.split("=", 1)[1] if "=" in gnb_part else ""

            m_nr_cell = re.search(r"NRCellCU=([^,]+)", sub)
            m_freq = re.search(r"NRFreqRelation=([^,]+)", sub)
            nr_cell_for_freq = m_nr_cell.group(1) if m_nr_cell else ""
            freq_id = m_freq.group(1) if m_freq else ""

            # Replace old SSB (Pre) with Post SSB using provided values
            if n77_ssb_pre and freq_id == str(n77_ssb_pre):
                freq_id = str(n77_ssb_post)

            if gnb_val and nr_cell_for_freq and freq_id:
                clean_nrfreq_ref = f"GNBCUCPFunction={gnb_val},NRCellCU={nr_cell_for_freq},NRFreqRelation={freq_id}"

        parts = [
            f"set {clean_nrcell_ref_short} nRFrequencyRef NRNetwork=1,NRFrequency={n77_ssb_post}-30" if clean_nrcell_ref_short else "",
            f"crn NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}",
            f"nRCellRef {clean_nrcell_ref}" if clean_nrcell_ref else "",
            f"nRFreqRelationRef {clean_nrfreq_ref}" if clean_nrfreq_ref else "",
            f"isHoAllowed {is_ho}" if is_ho else "",
            f"isRemoveAllowed {is_remove}" if is_remove else "",
            "end",
            (
                f"set NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel} coverageIndicator {coverage}"
                if coverage
                else f"set NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}"
            ),
            (
                f"set NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel} sCellCandidate {s_cell_candidate}"
                if s_cell_candidate
                else ""
            ),
        ]
        lines = [p for p in parts if p]
        return "\n".join(lines)

    df["Correction_Cmd"] = df.apply(lambda r: _build_correction_command(r, _get_rel_row(r)), axis=1)

    # GNBCUCPFunctionId se rellena desde nRCellRef usando la tabla de relaciones
    df["GNBCUCPFunctionId"] = df.apply(
        lambda r: extract_gnbcucp_segment(resolve_nrcell_ref(r, relations_lookup)),
        axis=1,
    )

    df = ensure_column_after(df, "GNBCUCPFunctionId", "NRCellRelationId")

    # Final column set: keep only relevant columns and force Correction_Cmd to be last
    desired_cols = [
        "NodeId",
        "NRCellCUId",
        "NRCellRelationId",
        "GNBCUCPFunctionId",
        "Freq_Pre",
        "Freq_Post",
        "Correction_Cmd",
    ]
    cols = [c for c in desired_cols if c in df.columns]
    df = df[cols]
    return df


# ----------------------------------------------------------------------
#  NR  -  DISCREPANCIES
# ----------------------------------------------------------------------
def build_correction_command_nr_discrepancies(
    disc_df: pd.DataFrame,
    relations_df: Optional[pd.DataFrame],
    n77_ssb_pre: Optional[str] = None,
    n77_ssb_post: Optional[str] = None,
) -> pd.DataFrame:
    """
    Build delete+create Correction_Cmd blocks for NR_disc rows using NR_new/NR_missing logic.
    """
    if disc_df is None or disc_df.empty:
        work = disc_df.copy() if disc_df is not None else pd.DataFrame()
        work["Correction_Cmd"] = ""
        if "NodeId" not in work.columns:
            work["NodeId"] = ""
        if "NRCellRelationId" not in work.columns:
            work["NRCellRelationId"] = ""
        if "GNBCUCPFunctionId" not in work.columns:
            work["GNBCUCPFunctionId"] = ""
        work = ensure_column_after(work, "GNBCUCPFunctionId", "NRCellRelationId")
        return work

    work = disc_df.copy()
    if "NodeId" not in work.columns:
        work["NodeId"] = ""
    if "GNBCUCPFunctionId" not in work.columns:
        work["GNBCUCPFunctionId"] = ""

    for col in ("NodeId", "NRCellCUId", "NRCellRelationId", "nRCellRef"):
        if col not in work.columns:
            work[col] = ""
        work[col] = work[col].astype(str).str.strip()

    relations_lookup = build_row_lookup(
        relations_df, ["NodeId", "NRCellCUId", "NRCellRelationId"], extra_strip_cols=["nRCellRef"]
    )

    work["GNBCUCPFunctionId"] = work.apply(
        lambda r: extract_gnbcucp_segment(resolve_nrcell_ref(r, relations_lookup)), axis=1
    )

    # Reuse existing builders (they already include their own internal _build_correction_command)
    del_df = build_correction_command_nr_new_relations(disc_df.copy(), relations_df)
    create_df = build_correction_command_nr_missing_relations(disc_df.copy(), relations_df, n77_ssb_pre, n77_ssb_post)

    del_cmds = del_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)
    create_cmds = create_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)

    def _build_correction_command(del_cmd: str, create_cmd: str) -> str:
        """
        Build combined correction command for NR_disc row.
        Safely returns empty string if both parts are empty.
        """
        del_cmd = (del_cmd or "").strip()
        create_cmd = (create_cmd or "").strip()
        if del_cmd and create_cmd:
            return f"{del_cmd}\n{create_cmd}"
        return del_cmd or create_cmd

    work["Correction_Cmd"] = [
        _build_correction_command(d, c) for d, c in zip(del_cmds, create_cmds)
    ]

    work = ensure_column_after(work, "GNBCUCPFunctionId", "NRCellRelationId")

    # Remove raw nRCellRef / nrCellRef column if it is completely empty
    for col_name in ("nRCellRef", "nrCellRef"):
        if col_name in work.columns:
            if work[col_name].astype(str).str.strip().eq("").all():
                work = work.drop(columns=[col_name])

    # For _disc we keep all original discrepancy columns + GNBCUCPFunctionId + Correction_Cmd
    return work


# ----------------------------------------------------------------------
#  EXTERNAL CELLS
# ----------------------------------------------------------------------
def build_correction_command_external_nr_cell_cu(n77_ssb_pre, n77_ssb_post, ext_gnb: str, ext_cell: str, nr_tail: str) -> str:
    """
    Build correction command replacing old N77 SSB with new N77 SSB inside nr_tail.
    Adds two hget lines (before and after the set) to show the current nRFrequencyRef, as requested by the slide.
    Safely returns empty string if mandatory parameters are missing.
    """
    if not ext_gnb or not ext_cell or not nr_tail:
        return ""

    nr_tail = str(nr_tail).replace(str(n77_ssb_pre), str(n77_ssb_post))

    return (
        "confb+\n"
        "gs+\n"
        "lt all\n"
        "alt\n"
        f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef 64\n"
        f"set ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU={ext_cell} nRFrequencyRef {nr_tail}\n"
        f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef 64\n"
        "alt"
    )


def build_correction_command_external_gutran_cell(ext_gnb: str, ext_cell: str, ssb_post: object) -> str:
    """
    Build correction command for ExternalGUtranCell switching to Post SSB.
    Safely returns empty string if mandatory parameters are missing.
    """
    if not ext_gnb or not ext_cell or ssb_post is None:
        return ""
    return f"set ExternalGNodeBFunction={ext_gnb},ExternalGUtranCell={ext_cell} gUtranSyncSignalFrequencyRef GUtraNetwork=1,GUtranSyncSignalFrequency={ssb_post}-30\n"


# ----------------------------------------------------------------------
#  TERMPOINTS
# ----------------------------------------------------------------------
def build_correction_command_termpoint_to_gnodeb(ext_gnb: str, ssb_post: int, ssb_pre: int) -> str:
    """
    Build correction command for TermPointToGNodeB.
    Slide point #4: remove the dynamic (blue) hget lines using ssb_post/ssb_pre, and add the new (yellow) hget lines with hardcoded '64'.
    Safely returns empty string if ext_gnb is missing.
    """
    if not ext_gnb:
        return ""

    # NOTE: ssb_post and ssb_pre are kept in the signature for compatibility, but point #4 requires hardcoding the final '64'.
    hardcoded_ref = 64

    return (
        "confb+\n"
        "lt all\n"
        "alt\n"
        f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef {hardcoded_ref}\n"
        f"get ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
        f"bl ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
        "wait 5\n"
        f"deb ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
        "wait 10\n"
        "lt ExternalGNBCUCPFunction\n"
        f"get ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
        f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef {hardcoded_ref}\n"
        "alt"
    )



def build_correction_command_termpoint_to_gnb(df: pd.DataFrame, old_ssb: str, new_ssb: str) -> pd.DataFrame:
    """
    Build the correction command for TermpointToGNB rows needing update.

    Update (per requirements):
      - Keep the structure identical to existing TermPointToGNB command style.
      - Remove the two hget lines that used old/new SSB values.
      - Add a single hget line using a hardcoded '64' suffix (as requested).
      - Replace the second 'lt all' with 'lt ExternalGNodeBFunction' (matches the desired pattern).

    Notes:
      - old_ssb/new_ssb are kept in the signature for backward compatibility with the caller,
        but the new command uses a hardcoded 64 token.
    """
    out = df.copy()
    if out.empty:
        if "Correction_Cmd" not in out.columns:
            out["Correction_Cmd"] = ""
        return out

    required_cols = ["ExternalGNodeBFunctionId", "TermPointToGNBId", "EUtranCellFDDId"]
    for c in required_cols:
        if c not in out.columns:
            out[c] = ""

    def _cmd(ext_gnb: object, tp: object, eutra: object) -> str:
        ext_gnb_s = str(ext_gnb).strip()
        tp_s = str(tp).strip()
        eutra_s = str(eutra).strip()
        if not ext_gnb_s or not tp_s:
            return ""

        # NOTE: '64' is hardcoded by requirement.
        hardcoded_token = "64"

        cmd = []
        cmd.append("confb+")
        cmd.append("lt all")
        cmd.append("alt")
        cmd.append(f"hget ExternalGNodeBFunction={ext_gnb_s},ExternalGUtranCell gUtranSyncSignalFrequencyRef {hardcoded_token}")
        cmd.append(f"get ExternalGNodeBFunction={ext_gnb_s},TermpointToGNB")
        cmd.append(f"bl ExternalGNodeBFunction={ext_gnb_s},TermpointToGNB")
        cmd.append("wait 5")
        cmd.append(f"deb ExternalGNodeBFunction={ext_gnb_s},TermpointToGNB")
        cmd.append("wait 10")
        cmd.append("lt ExternalGNodeBFunction")
        cmd.append(f"get ExternalGNodeBFunction={ext_gnb_s},TermpointToGNB")
        cmd.append(f"hget ExternalGNodeBFunction={ext_gnb_s},ExternalGUtranCell gUtranSyncSignalFrequencyRef {hardcoded_token}")
        cmd.append("alt")

        return "\n".join(cmd)

    out["Correction_Cmd"] = out.apply(lambda r: _cmd(r.get("ExternalGNodeBFunctionId", ""), r.get("TermPointToGNBId", ""), r.get("EUtranCellFDDId", "")), axis=1)
    return out
