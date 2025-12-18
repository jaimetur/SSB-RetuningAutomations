# -*- coding: utf-8 -*-

"""
Helpers to build Correction_Cmd columns and export them to text files.

This module centralizes the logic used by ConsistencyChecks so that:
- Code is reused between GU/NR and new/missing/disc variants.
- The main ConsistencyChecks module stays smaller and easier to read.
"""

from __future__ import annotations

import os
import re
import pandas as pd
from typing import Dict, Optional

from src.utils.utils_dataframe import ensure_column_after, build_row_lookup, pick_non_empty_value
from src.utils.utils_io import to_long_path, pretty_path
from src.utils.utils_parsing import extract_gnbcucp_segment, resolve_nrcell_ref, merge_command_blocks_for_node



# ----------------------------------------------------------------------
#  BUILD CORRECTION COMMANDS
# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
#  GU  -  NEW
# ----------------------------------------------------------------------
def build_gu_new(df: pd.DataFrame, relations_df: Optional[pd.DataFrame]) -> pd.DataFrame:
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

        def _from_rel(row: pd.Series, field: str) -> str:
            key = (
                str(row.get("EUtranCellFDDId", "")).strip(),
                str(row.get("GUtranCellRelationId", "")).strip(),
            )
            rel_row = relations_lookup.get(key)
            return pick_non_empty_value(rel_row, row, field)

        # Make sure GUtranFreqRelationId, createdBy and timeOfCreation are taken from relations_df
        df["GUtranFreqRelationId"] = df.apply(lambda r: _from_rel(r, "GUtranFreqRelationId"), axis=1)
        df["createdBy"] = df.apply(lambda r: _from_rel(r, "createdBy"), axis=1)
        df["timeOfCreation"] = df.apply(lambda r: _from_rel(r, "timeOfCreation"), axis=1)

        def build_command(row: pd.Series) -> str:
            key = (
                str(row.get("EUtranCellFDDId", "")).strip(),
                str(row.get("GUtranCellRelationId", "")).strip(),
            )
            rel_row = relations_lookup.get(key)
            src = rel_row if rel_row is not None else row

            eu_cell = str(src.get("EUtranCellFDDId") or "").strip()
            freq_rel = str(src.get("GUtranFreqRelationId") or "").strip()
            cell_rel = str(src.get("GUtranCellRelationId") or "").strip()

            if not (eu_cell and freq_rel and cell_rel):
                return ""

            return f"del EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}"

        df["Correction_Cmd"] = df.apply(build_command, axis=1)

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
def build_gu_missing(
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

    def from_rel(row: pd.Series, field: str) -> str:
        key = (
            str(row.get("NodeId", "")).strip(),
            str(row.get("EUtranCellFDDId", "")).strip(),
            str(row.get("GUtranCellRelationId", "")).strip(),
        )
        rel_row = relations_lookup.get(key)
        return pick_non_empty_value(rel_row, row, field)

    # Make sure GUtranFreqRelationId is taken from relations_df
    df["GUtranFreqRelationId"] = df.apply(lambda r: from_rel(r, "GUtranFreqRelationId"), axis=1)

    def build_command(row: pd.Series) -> str:
        key = (
            str(row.get("NodeId", "")).strip(),
            str(row.get("EUtranCellFDDId", "")).strip(),
            str(row.get("GUtranCellRelationId", "")).strip(),
        )
        rel_row = relations_lookup.get(key)

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
        if n77_ssb_pre and freq_rel.startswith(str(n77_ssb_pre)):
            freq_rel = f"{n77_ssb_post}-30-20-0-1" if n77_ssb_post else freq_rel

        if not user_label:
            user_label = "SSBretune"

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
            (
                f"set EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel} coverageIndicator {coverage}"
                if coverage
                else f"set EUtranCellFDD={eu_cell},GUtranFreqRelation={freq_rel},GUtranCellRelation={cell_rel}"
            ),
        ]
        # keep non-empty
        lines = [p for p in parts if p]
        return "\n".join(lines)

    df["Correction_Cmd"] = df.apply(build_command, axis=1)

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
#  GU  -  DISC
# ----------------------------------------------------------------------
def build_gu_disc(
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

    del_df = build_gu_new(disc_df.copy(), relations_df)
    create_df = build_gu_missing(disc_df.copy(), relations_df, n77_ssb_pre, n77_ssb_post)

    del_cmds = del_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)
    create_cmds = create_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)

    def combine_cmds(del_cmd: str, create_cmd: str) -> str:
        del_cmd = del_cmd.strip()
        create_cmd = create_cmd.strip()
        if del_cmd and create_cmd:
            return f"{del_cmd}\n{create_cmd}"
        return del_cmd or create_cmd

    work["Correction_Cmd"] = [combine_cmds(d, c) for d, c in zip(del_cmds, create_cmds)]
    # For _disc we keep all original discrepancy columns + Correction_Cmd
    return work


# ----------------------------------------------------------------------
#  NR  -  NEW
# ----------------------------------------------------------------------
def build_nr_new(df: pd.DataFrame, relations_df: Optional[pd.DataFrame]) -> pd.DataFrame:
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

        # -----------------------------
        # Build delete commands
        # -----------------------------
        def build_command(row: pd.Series) -> str:
            key = (
                str(row.get("NodeId", "")).strip(),
                str(row.get("NRCellCUId", "")).strip(),
                str(row.get("NRCellRelationId", "")).strip(),
            )
            rel_row = relations_lookup.get(key)
            src = rel_row if rel_row is not None else row

            nr_cell_cu = str(src.get("NRCellCUId") or "").strip()
            nr_cell_rel = str(src.get("NRCellRelationId") or "").strip()
            if not (nr_cell_cu and nr_cell_rel):
                return ""

            return f"del NRCellCU={nr_cell_cu},NRCellRelation={nr_cell_rel}"

        df["Correction_Cmd"] = df.apply(build_command, axis=1)

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
def build_nr_missing(
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

    def build_command(row: pd.Series) -> str:
        key = (
            str(row.get("NodeId", "")).strip(),
            str(row.get("NRCellCUId", "")).strip(),
            str(row.get("NRCellRelationId", "")).strip(),
        )
        rel_row = relations_lookup.get(key)

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

            # Replace old SSB (Pre) with Post SSB using provided values
            if n77_ssb_pre and freq_id == str(n77_ssb_pre):
                freq_id = str(n77_ssb_post)

            if gnb_val and nr_cell_for_freq and freq_id:
                clean_nrfreq_ref = f"GNBCUCPFunction={gnb_val},NRCellCU={nr_cell_for_freq},NRFreqRelation={freq_id}"

        parts = [
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

    df["Correction_Cmd"] = df.apply(build_command, axis=1)

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
#  NR  -  DISC
# ----------------------------------------------------------------------
def build_nr_disc(
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

    del_df = build_nr_new(disc_df.copy(), relations_df)
    create_df = build_nr_missing(disc_df.copy(), relations_df, n77_ssb_pre, n77_ssb_post)

    del_cmds = del_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)
    create_cmds = create_df.get("Correction_Cmd", pd.Series("", index=disc_df.index)).astype(str)

    def combine_cmds(del_cmd: str, create_cmd: str) -> str:
        del_cmd = del_cmd.strip()
        create_cmd = create_cmd.strip()
        if del_cmd and create_cmd:
            return f"{del_cmd}\n{create_cmd}"
        return del_cmd or create_cmd

    work["Correction_Cmd"] = [combine_cmds(d, c) for d, c in zip(del_cmds, create_cmds)]
    work = ensure_column_after(work, "GNBCUCPFunctionId", "NRCellRelationId")

    # Remove raw nRCellRef / nrCellRef column if it is completely empty
    for col_name in ("nRCellRef", "nrCellRef"):
        if col_name in work.columns:
            if work[col_name].astype(str).str.strip().eq("").all():
                work = work.drop(columns=[col_name])

    # For _disc we keep all original discrepancy columns + GNBCUCPFunctionId + Correction_Cmd
    return work

# ----------------------------- EXTERNAL/TERMPOINTS COMMANDS ----------------------------- #
def export_external_and_termpoint_commands(
    audit_post_excel: str,
    output_dir: str,
) -> int:
    """
    Export correction commands coming from POST Configuration Audit Excel:
      - ExternalNRCellCU (SSB-Post)
      - ExternalNRCellCU (Unknown)
      - ExternalGUtranCell (SSB-Post)
      - ExternalGUtranCell (Unknown)
      - TermPointToGNB
      - TermPointToGNodeB

    Files are written under:
      <output_dir>/Correction_Cmd/ExternalNRCellCU/{SSB-Post,Unknown}
      <output_dir>/Correction_Cmd/ExternalGUtranCell/{SSB-Post,Unknown}
      <output_dir>/Correction_Cmd/TermPointToGNB
      <output_dir>/Correction_Cmd/TermPointToGNodeB

    One text file per NodeId is generated (grouped like other Correction_Cmd exports).

    Returns the number of generated files.
    """

    def _read_sheet_case_insensitive(audit_excel: str, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        Read a sheet using case-insensitive matching. Returns None if not found or on error.
        """
        if not audit_excel or not os.path.isfile(audit_excel):
            return None

        try:
            xl = pd.ExcelFile(audit_excel)
            sheet_map = {s.lower(): s for s in xl.sheet_names}
            real_sheet = sheet_map.get(str(sheet_name).lower())
            if not real_sheet:
                return None
            return xl.parse(real_sheet)
        except Exception:
            return None

    def _export_grouped_commands_from_sheet(
            audit_excel: str,
            sheet_name: str,
            output_dir: str,
            command_column: str = "Correction_Cmd",
            node_column: str = "NodeId",
            filter_column: Optional[str] = None,
            filter_values: Optional[list[str]] = None,
            filename_suffix: Optional[str] = None,
    ) -> int:
        """
        Export Correction_Cmd grouped by NodeId from a given sheet into output_dir.
        Returns how many files were generated.
        """
        df = _read_sheet_case_insensitive(audit_excel, sheet_name)
        if df is None or df.empty:
            return 0

        # Allow backward compatibility with old column name
        if command_column not in df.columns and "Correction Command" in df.columns:
            command_column = "Correction Command"

        if command_column not in df.columns:
            return 0

        if node_column not in df.columns:
            return 0

        if filter_column and filter_column in df.columns and filter_values is not None:
            allowed = {str(v).strip() for v in filter_values}
            df = df[df[filter_column].astype(str).str.strip().isin(allowed)]

        if df.empty:
            return 0

        os.makedirs(output_dir, exist_ok=True)

        work = df.copy()
        work[node_column] = work[node_column].astype(str).str.strip()

        suffix = filename_suffix if filename_suffix else str(sheet_name).strip()
        generated_files = 0

        for node_id, group in work.groupby(node_column):
            node_str = str(node_id).strip()
            if not node_str:
                continue

            # IMPORTANT: do NOT cast the whole column to str before dropna(), otherwise NaN becomes "nan"
            raw_series = group[command_column]
            raw_series = raw_series[raw_series.notna()]

            cmds = (
                raw_series
                .astype(str)
                .map(str.strip)
                .loc[lambda s: (s != "") & (s.str.lower() != "nan") & (s.str.lower() != "none")]
                .tolist()
            )

            if not cmds:
                continue

            file_name = f"{node_str}_{suffix}.txt"
            out_path = os.path.join(output_dir, file_name)
            out_path_long = to_long_path(out_path)

            merged_script = merge_command_blocks_for_node(cmds)
            if not merged_script.strip():
                continue

            with open(out_path_long, "w", encoding="utf-8") as f:
                f.write(merged_script)

            generated_files += 1

        return generated_files

    if not audit_post_excel or not os.path.isfile(audit_post_excel):
        return 0

    base_dir = os.path.join(output_dir, "Correction_Cmd")

    ext_nr_base = os.path.join(base_dir, "ExternalNRCellCU")
    ext_gu_base = os.path.join(base_dir, "ExternalGUtranCell")
    tp_gnb_dir = os.path.join(base_dir, "TermPointToGNB")
    tp_gnodeb_dir = os.path.join(base_dir, "TermPointToGNodeB")

    ext_nr_ssbpost_dir = os.path.join(ext_nr_base, "SSB-Post")
    ext_nr_unknown_dir = os.path.join(ext_nr_base, "Unknown")
    ext_gu_ssbpost_dir = os.path.join(ext_gu_base, "SSB-Post")
    ext_gu_unknown_dir = os.path.join(ext_gu_base, "Unknown")

    os.makedirs(ext_nr_ssbpost_dir, exist_ok=True)
    os.makedirs(ext_nr_unknown_dir, exist_ok=True)
    os.makedirs(ext_gu_ssbpost_dir, exist_ok=True)
    os.makedirs(ext_gu_unknown_dir, exist_ok=True)
    os.makedirs(tp_gnb_dir, exist_ok=True)
    os.makedirs(tp_gnodeb_dir, exist_ok=True)

    generated = 0

    # -----------------------------
    # ExternalNRCellCU - SSB-Post / Unknown
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalNRCellCU",
        output_dir=ext_nr_ssbpost_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["SSB-Post"],
        filename_suffix="ExternalNRCellCU",
    )
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalNRCellCU",
        output_dir=ext_nr_unknown_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["Unknown", "Unkwnow"],
        filename_suffix="ExternalNRCellCU",
    )

    # -----------------------------
    # ExternalGUtranCell - SSB-Post / Unknown
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalGUtranCell",
        output_dir=ext_gu_ssbpost_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["SSB-Post"],
        filename_suffix="ExternalGUtranCell",
    )
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="ExternalGUtranCell",
        output_dir=ext_gu_unknown_dir,
        command_column="Correction_Cmd",
        filter_column="GNodeB_SSB_Target",
        filter_values=["Unknown", "Unkwnow"],
        filename_suffix="ExternalGUtranCell",
    )

    # -----------------------------
    # TermPointToGNodeB
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNodeB",
        output_dir=tp_gnodeb_dir,
        command_column="Correction_Cmd",
        filename_suffix="TermPointToGNodeB",
    )

    # -----------------------------
    # TermPointToGNB
    # -----------------------------
    generated += _export_grouped_commands_from_sheet(
        audit_excel=audit_post_excel,
        sheet_name="TermPointToGNB",
        output_dir=tp_gnb_dir,
        command_column="Correction_Cmd",
        filename_suffix="TermPointToGNB",
    )

    if generated:
        print(
            f"[Consistency Checks] Generated {generated} extra Correction_Cmd files "
            f"from POST Configuration Audit in: '{pretty_path(base_dir)}'"
        )

    return generated



# ----------------------------------------------------------------------
#  EXPORT TEXT FILES
# ----------------------------------------------------------------------
def export_correction_cmd_texts(output_dir: str, dfs_by_category: Dict[str, pd.DataFrame]) -> int:
    """
    Export Correction_Cmd values to text files grouped by NodeId and category.

    For each category (e.g. GU_missing, NR_new), one file per NodeId is created in:
      <output_dir>/Correction_Cmd/<NodeId>_<Category>.txt
    """
    base_dir = os.path.join(output_dir, "Correction_Cmd")
    os.makedirs(base_dir, exist_ok=True)

    new_dir = os.path.join(base_dir, "New Relations")
    missing_dir = os.path.join(base_dir, "Missing Relations")
    discrepancies_dir = os.path.join(base_dir, "Discrepancies")
    os.makedirs(new_dir, exist_ok=True)
    os.makedirs(missing_dir, exist_ok=True)
    os.makedirs(discrepancies_dir, exist_ok=True)

    total_files = 0

    for category, df in dfs_by_category.items():
        if df is None or df.empty:
            continue
        if "NodeId" not in df.columns or "Correction_Cmd" not in df.columns:
            continue

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

            if "new" in category.lower():
                target_dir = new_dir
            elif "missing" in category.lower():
                target_dir = missing_dir
            elif "disc" in category.lower():
                target_dir = discrepancies_dir
            else:
                target_dir = base_dir

            file_name = f"{node_str}_{category}.txt"
            file_path = os.path.join(target_dir, file_name)
            file_path_long = to_long_path(file_path)

            with open(file_path_long, "w", encoding="utf-8") as f:
                f.write("\n\n".join(cmds))

            total_files += 1

    print(
        f"\n[Consistency Checks (Pre/Post Comparison)] "
        f"Generated {total_files} Correction_Cmd text files in: '{pretty_path(base_dir)}'"
    )
    return total_files
