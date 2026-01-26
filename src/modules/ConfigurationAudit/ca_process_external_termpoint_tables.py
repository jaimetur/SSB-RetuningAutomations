# -*- coding: utf-8 -*-
import pandas as pd

from src.modules.Common.correction_commands_builder import build_correction_command_external_nr_cell_cu, build_correction_command_external_gutran_cell, build_correction_command_termpoint_to_gnodeb, build_correction_command_termpoint_to_gnb
from src.utils.utils_dataframe import ensure_column_after
from src.utils.utils_frequency import resolve_column_case_insensitive

# ----------------------------- NEW: ExternalNRCellCU (same value as NRCellRelation old/new counts) -----------------------------
def process_external_nr_cell_cu(df_external_nr_cell_cu, n77_ssb_pre, n77_ssb_post, add_row, df_term_point_to_gnodeb, extract_freq_from_nrfrequencyref, extract_nrnetwork_tail, nodes_pre=None, nodes_post=None):

    def _normalize_state_local(value: object) -> str:
        """
        Normalize administrative / operational state values to uppercase string.
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        return str(value).strip().upper()

    try:
        if df_external_nr_cell_cu is not None and not df_external_nr_cell_cu.empty:
            node_col = resolve_column_case_insensitive(df_external_nr_cell_cu, ["NodeId"])
            freq_col = resolve_column_case_insensitive(df_external_nr_cell_cu, ["nRFrequencyRef", "NRFrequencyRef", "nRFreqRelationRef", "NRFreqRelationRef"])
            ext_gnb_col = resolve_column_case_insensitive(df_external_nr_cell_cu, ["ExternalGNBCUCPFunctionId"])
            cell_col = resolve_column_case_insensitive(df_external_nr_cell_cu, ["ExternalNRCellCUId"])

            # Load node identifiers from SummaryAudit (Pre / Post)
            nodes_without_retune_ids = nodes_pre
            nodes_with_retune_ids = nodes_post

            nodes_without_retune_ids = {str(v) for v in nodes_without_retune_ids or []}
            nodes_with_retune_ids = {str(v) for v in nodes_with_retune_ids or []}

            if node_col and freq_col:
                work = df_external_nr_cell_cu.copy()

                # Normalize base columns to string to avoid mixed-type issues
                work[node_col] = work[node_col].astype(str).str.strip()
                if ext_gnb_col:
                    work[ext_gnb_col] = work[ext_gnb_col].astype(str).str.strip()
                if cell_col:
                    work[cell_col] = work[cell_col].astype(str).str.strip()

                # -------------------------------------------------
                # Frequency (was: GNodeB_SSB_Source)
                # -------------------------------------------------
                work["Frequency"] = work[freq_col].map(lambda v: extract_freq_from_nrfrequencyref(v) if isinstance(v, str) and v.strip() else "")

                old_ssb = str(n77_ssb_pre)
                new_ssb = str(n77_ssb_post)

                # =========================
                # SummaryAudit counts
                # =========================
                count_old = int((work["Frequency"].astype(str) == old_ssb).sum())
                count_new = int((work["Frequency"].astype(str) == new_ssb).sum())

                add_row("ExternalNRCellCU", "NR Frequency Audit", f"External cells to old N77 SSB ({old_ssb}) (from ExternalNRCellCU)", count_old)
                add_row("ExternalNRCellCU", "NR Frequency Audit", f"External cells to new N77 SSB ({new_ssb}) (from ExternalNRCellCU)", count_new)

                # =========================
                # Termpoint
                # =========================
                if ext_gnb_col and "Termpoint" not in work.columns:
                    work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

                # =========================
                # TermpointStatus / TermpointConsolidatedStatus
                # - FIX: missing column OR blank value must NOT force NOT_OK
                # =========================
                if (df_term_point_to_gnodeb is not None and not df_term_point_to_gnodeb.empty and "Termpoint" in work.columns):
                    tp_src = df_term_point_to_gnodeb.copy()

                    node_tp_col = resolve_column_case_insensitive(tp_src, ["NodeId"])
                    ext_tp_col = resolve_column_case_insensitive(tp_src, ["ExternalGNBCUCPFunctionId"])
                    admin_col = resolve_column_case_insensitive(tp_src, ["administrativeState", "AdministrativeState"])
                    oper_col = resolve_column_case_insensitive(tp_src, ["operationalState", "OperationalState"])
                    avail_col = resolve_column_case_insensitive(tp_src, ["availabilityStatus", "AvailabilityStatus"])

                    if node_tp_col and ext_tp_col:
                        tp_src["Termpoint"] = (tp_src[node_tp_col].astype(str).str.strip() + "-" + tp_src[ext_tp_col].astype(str).str.strip())

                        admin_val = tp_src[admin_col] if admin_col else pd.Series("", index=tp_src.index)
                        oper_val = tp_src[oper_col] if oper_col else pd.Series("", index=tp_src.index)
                        avail_val = tp_src[avail_col] if avail_col else pd.Series("", index=tp_src.index)

                        admin_norm = admin_val.astype(str).fillna("").str.strip().str.upper()
                        oper_norm = oper_val.astype(str).fillna("").str.strip().str.upper()
                        avail_raw = avail_val.astype(str).fillna("").str.strip()
                        avail_up = avail_raw.str.upper()

                        admin_disp = admin_norm.replace("", "EMPTY")
                        oper_disp = oper_norm.replace("", "EMPTY")
                        avail_disp = avail_raw.replace("", "EMPTY")

                        tp_src["TermpointStatus"] = ("administrativeState=" + admin_disp + ", operationalState=" + oper_disp + ", availabilityStatus=" + avail_disp)

                        # CHANGE: Missing/blank states must NOT force NOT_OK
                        # - admin OK if Missing/blank OR UNLOCKED
                        # - oper OK if Missing/blank OR ENABLED
                        # - avail OK if Missing/blank
                        admin_ok = (admin_norm == "") | (admin_norm == "EMPTY") | (admin_norm == "UNLOCKED")
                        oper_ok = (oper_norm == "") | (oper_norm == "EMPTY") | (oper_norm == "ENABLED")
                        avail_ok = (avail_raw == "") | (avail_up == "EMPTY")

                        tp_src["TermpointConsolidatedStatus"] = ((admin_ok & oper_ok & avail_ok).map(lambda v: "OK" if v else "NOT_OK"))

                        tp_map = tp_src.drop_duplicates("Termpoint").set_index("Termpoint")

                        work["TermpointStatus"] = work["Termpoint"].map(tp_map["TermpointStatus"])
                        work["TermpointConsolidatedStatus"] = work["Termpoint"].map(tp_map["TermpointConsolidatedStatus"])

                # -------------------------------------------------
                # Place Frequency right after TermpointConsolidatedStatus
                # -------------------------------------------------
                work = ensure_column_after(work, "Frequency", "TermpointConsolidatedStatus")

                # =========================
                # GNodeB_SSB_Target
                # =========================
                if ext_gnb_col:
                    def _detect_gnodeb_target(ext_gnb: object) -> str:
                        val = str(ext_gnb) if ext_gnb is not None else ""
                        if any(n in val for n in nodes_without_retune_ids):
                            return "SSB-Pre"
                        if any(n in val for n in nodes_with_retune_ids):
                            return "SSB-Post"
                        return "Unknown"

                    work["GNodeB_SSB_Target"] = work[ext_gnb_col].map(_detect_gnodeb_target)

                # =========================
                # Correction Command
                # (only for SSB-PRE frequency AND target != SSB-Pre)
                # =========================
                if ext_gnb_col and cell_col:
                    mask_pre = work["Frequency"].astype(str) == old_ssb
                    mask_target = work["GNodeB_SSB_Target"] != "SSB-Pre"
                    mask_final = mask_pre & mask_target

                    # Safely extract NR network tail
                    nr_tail_series = work[freq_col].map(lambda v: extract_nrnetwork_tail(v) if isinstance(v, str) and v.strip() else "")

                    if "Correction_Cmd" not in work.columns:
                        work["Correction_Cmd"] = ""

                    work.loc[mask_final, "Correction_Cmd"] = work.loc[mask_final].apply(
                        lambda r: build_correction_command_external_nr_cell_cu(n77_ssb_pre, n77_ssb_post, r.get(ext_gnb_col, ""), r.get(cell_col, ""), nr_tail_series.loc[r.name]),
                        axis=1,
                    )

                # Write back preserving original columns + new ones
                df_external_nr_cell_cu.loc[:, work.columns] = work

            else:
                add_row("ExternalNRCellCU", "NR Frequency Audit", "ExternalNRCellCU table present but NodeId / nRFrequencyRef missing", "N/A")
        else:
            add_row("ExternalNRCellCU", "NR Frequency Audit", "ExternalNRCellCU table", "Table not found or empty")
    except Exception as ex:
        add_row("ExternalNRCellCU", "NR Frequency Audit", "Error while checking ExternalNRCellCU", f"{type(ex).__name__}: {ex}")


# ----------------------------- NEW: ExternalGUtranCell (old/new counts + OUT_OF_SERVICE row counts) -----------------------------
def process_external_gutran_cell(df_external_gutran_cell, extract_ssb_from_gutran_sync_ref, n77_ssb_pre, n77_ssb_post, add_row, normalize_state, df_term_point_to_gnb, nodes_pre=None, nodes_post=None):
    try:
        if df_external_gutran_cell is not None and not df_external_gutran_cell.empty:
            # NEW: Always work on a full copy (same pattern as NR)
            work = df_external_gutran_cell.copy()

            node_col = resolve_column_case_insensitive(work, ["NodeId"])
            ref_col = resolve_column_case_insensitive(work, ["gUtranSyncSignalFrequencyRef", "GUtranSyncSignalFrequencyRef"])
            status_col = resolve_column_case_insensitive(work, ["serviceStatus", "ServiceStatus"])
            ext_gnb_col = resolve_column_case_insensitive(work, ["ExternalGNodeBFunctionId"])
            cell_col = resolve_column_case_insensitive(work, ["ExternalGUtranCellId"])

            if node_col and ref_col:
                work[node_col] = work[node_col].astype(str).str.strip()
                if ext_gnb_col:
                    work[ext_gnb_col] = work[ext_gnb_col].astype(str).str.strip()
                if cell_col:
                    work[cell_col] = work[cell_col].astype(str).str.strip()

                # -------------------------------------------------
                # Frequency (was: GNodeB_SSB_Source)
                # -------------------------------------------------
                work["Frequency"] = work[ref_col].map(extract_ssb_from_gutran_sync_ref)

                old_ssb = n77_ssb_pre
                new_ssb = n77_ssb_post

                count_old = int((work["Frequency"] == old_ssb).sum())
                count_new = int((work["Frequency"] == new_ssb).sum())

                add_row("ExternalGUtranCell", "LTE Frequency Audit", f"External cells to old N77 SSB ({old_ssb}) (from ExternalGUtranCell)", count_old)
                add_row("ExternalGUtranCell", "LTE Frequency Audit", f"External cells to new N77 SSB ({new_ssb}) (from ExternalGUtranCell)", count_new)

                if status_col:
                    work["_status_norm_"] = work[status_col].map(normalize_state)
                    mask_oos = work["_status_norm_"] == "OUT_OF_SERVICE"

                    count_old_oos = int(((work["Frequency"] == old_ssb) & mask_oos).sum())
                    count_new_oos = int(((work["Frequency"] == new_ssb) & mask_oos).sum())

                    add_row("ExternalGUtranCell", "LTE Frequency Audit", f"External cells to old N77 SSB ({old_ssb}) with serviceStatus=OUT_OF_SERVICE (from ExternalGUtranCell)", count_old_oos, "",)  # keep ExtraInfo empty to avoid huge lists
                    add_row("ExternalGUtranCell", "LTE Frequency Audit", f"External cells to new N77 SSB ({new_ssb}) with serviceStatus=OUT_OF_SERVICE (from ExternalGUtranCell)", count_new_oos, "",)  # keep ExtraInfo empty to avoid huge lists
                else:
                    add_row("ExternalGUtranCell", "LTE Frequency Audit", "External cells OUT_OF_SERVICE checks skipped (serviceStatus missing)", "N/A")
            else:
                add_row("ExternalGUtranCell", "LTE Frequency Audit", "ExternalGUtranCell table present but NodeId / gUtranSyncSignalFrequencyRef column missing", "N/A")
        else:
            add_row("ExternalGUtranCell", "LTE Frequency Audit", "ExternalGUtranCell table", "Table not found or empty")
            return

        # -------------------------------------------------
        # Termpoint / TermpointStatus / TermpointConsolidatedStatus
        # - FIX: missing column OR blank value must NOT force NOT_OK
        # -------------------------------------------------
        if node_col and ext_gnb_col:
            work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

        if (df_term_point_to_gnb is not None and not df_term_point_to_gnb.empty and "Termpoint" in work.columns):
            tp = df_term_point_to_gnb.copy()

            tp_node = resolve_column_case_insensitive(tp, ["NodeId"])
            tp_ext = resolve_column_case_insensitive(tp, ["ExternalGNodeBFunctionId"])
            admin_col_tp = resolve_column_case_insensitive(tp, ["administrativeState", "AdministrativeState"])
            oper_col_tp = resolve_column_case_insensitive(tp, ["operationalState", "OperationalState"])
            avail_col_tp = resolve_column_case_insensitive(tp, ["availabilityStatus", "AvailabilityStatus"])

            if tp_node and tp_ext:
                tp["Termpoint"] = tp[tp_node].astype(str).str.strip() + "-" + tp[tp_ext].astype(str).str.strip()

                admin_val = tp[admin_col_tp] if admin_col_tp else pd.Series("", index=tp.index)
                oper_val = tp[oper_col_tp] if oper_col_tp else pd.Series("", index=tp.index)
                avail_val = tp[avail_col_tp] if avail_col_tp else pd.Series("", index=tp.index)

                admin_norm = admin_val.astype(str).fillna("").str.strip().str.upper()
                oper_norm = oper_val.astype(str).fillna("").str.strip().str.upper()
                avail_raw = avail_val.astype(str).fillna("").str.strip()
                avail_up = avail_raw.str.upper()

                admin_disp = admin_norm.replace("", "EMPTY")
                oper_disp = oper_norm.replace("", "EMPTY")
                avail_disp = avail_raw.replace("", "EMPTY")

                tp["TermpointStatus"] = ("administrativeState=" + admin_disp + ", operationalState=" + oper_disp + ", availabilityStatus=" + avail_disp)

                # OK if missing/blank OR expected value
                admin_ok = (admin_norm == "") | (admin_norm == "EMPTY") | (admin_norm == "UNLOCKED")
                oper_ok = (oper_norm == "") | (oper_norm == "EMPTY") | (oper_norm == "ENABLED")
                avail_ok = (avail_raw == "") | (avail_up == "EMPTY")

                tp["TermpointConsolidatedStatus"] = ((admin_ok & oper_ok & avail_ok).map(lambda v: "OK" if v else "NOT_OK"))

                tp_map = tp.drop_duplicates("Termpoint").set_index("Termpoint")

                work["TermpointStatus"] = work["Termpoint"].map(tp_map["TermpointStatus"])
                work["TermpointConsolidatedStatus"] = work["Termpoint"].map(tp_map["TermpointConsolidatedStatus"])

        # -------------------------------------------------
        # Place Frequency right after TermpointConsolidatedStatus
        # -------------------------------------------------
        work = ensure_column_after(work, "Frequency", "TermpointConsolidatedStatus")

        # -------------------------------------------------
        # GNodeB_SSB_Target (Unknown instead of Other)
        # -------------------------------------------------
        nodes_pre = set(nodes_pre or [])
        nodes_post = set(nodes_post or [])

        def _detect_gnodeb_target_lte(value: object) -> str:
            s = str(value)
            if any(n in s for n in nodes_pre):
                return "SSB-Pre"
            if any(n in s for n in nodes_post):
                return "SSB-Post"
            return "Unknown"

        if ext_gnb_col:
            work["GNodeB_SSB_Target"] = work[ext_gnb_col].map(_detect_gnodeb_target_lte)

        # -------------------------------------------------
        # Correction Command (LTE)
        # -------------------------------------------------
        if "Correction_Cmd" not in work.columns:
            work["Correction_Cmd"] = ""

        mask_pre = work["Frequency"] == n77_ssb_pre
        mask_target = work["GNodeB_SSB_Target"] != "SSB-Pre"

        if ext_gnb_col and cell_col:
            work.loc[mask_pre & mask_target, "Correction_Cmd"] = work.loc[mask_pre & mask_target].apply(lambda r: build_correction_command_external_gutran_cell(r.get(ext_gnb_col, ""), r.get(cell_col, ""), n77_ssb_post), axis=1)

        # -------------------------------------------------
        # Write back preserving original columns + new ones
        # -------------------------------------------------
        df_external_gutran_cell.loc[:, work.columns] = work

    except Exception as ex:
        add_row("ExternalGUtranCell", "LTE Frequency Audit", "Error while checking ExternalGUtranCell", f"ERROR: {ex}")



# ----------------------------- NEW: TermPointToGNodeB (NR Termpoint Audit) -----------------------------
def process_termpoint_to_gnodeb(df_term_point_to_gnodeb, add_row, df_external_nr_cell_cu, n77_ssb_post, n77_ssb_pre, nodes_pre=None, nodes_post=None):
    try:
        if df_term_point_to_gnodeb is None or df_term_point_to_gnodeb.empty:
            add_row("TermPointToGNodeB", "NR Termpoint Audit", "TermPointToGNodeB table", "Table not found or empty")
            return

        node_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["NodeId"])
        ext_gnb_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["ExternalGNBCUCPFunctionId"])
        admin_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["administrativeState", "AdministrativeState"])
        oper_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["operationalState", "OperationalState"])
        avail_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["availabilityStatus", "AvailabilityStatus"])

        if not node_col or not ext_gnb_col:
            add_row("TermPointToGNodeB", "NR Termpoint Audit", "TermPointToGNodeB table present but required columns missing", "N/A")
            return

        work = df_term_point_to_gnodeb.copy()

        # Normalize base columns to string to avoid mixed-type issues
        work[node_col] = work[node_col].astype(str).str.strip()
        work[ext_gnb_col] = work[ext_gnb_col].astype(str).str.strip()

        # -------------------------------------------------
        # Termpoint
        # -------------------------------------------------
        if "Termpoint" not in work.columns:
            work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

        # -------------------------------------------------
        # Normalize states (ALWAYS as Series)
        # -------------------------------------------------
        admin_norm = work[admin_col].fillna("").astype(str).str.strip().str.upper() if admin_col else pd.Series("", index=work.index)
        oper_norm = work[oper_col].fillna("").astype(str).str.strip().str.upper() if oper_col else pd.Series("", index=work.index)
        avail_raw = work[avail_col].fillna("").astype(str).str.strip() if avail_col else pd.Series("", index=work.index)
        avail_up = avail_raw.astype(str).str.upper()

        admin_disp = admin_norm.replace("", "EMPTY")
        oper_disp = oper_norm.replace("", "EMPTY")
        avail_disp = avail_raw.replace("", "EMPTY")

        # -------------------------------------------------
        # TermpointStatus (CONCAT ONLY)
        # -------------------------------------------------
        work["TermpointStatus"] = ("administrativeState=" + admin_disp + ", operationalState=" + oper_disp + ", availabilityStatus=" + avail_disp)

        # -------------------------------------------------
        # TermPointConsolidatedStatus (LOGIC)
        # -------------------------------------------------
        admin_ok = (admin_norm == "") | (admin_norm == "EMPTY") | (admin_norm == "UNLOCKED")
        oper_ok = (oper_norm == "") | (oper_norm == "EMPTY") | (oper_norm == "ENABLED")
        avail_ok = (avail_raw == "") | (avail_up == "EMPTY")

        work["TermPointConsolidatedStatus"] = ((admin_ok & oper_ok & avail_ok).map(lambda v: "OK" if v else "NOT_OK"))

        # -------------------------------------------------
        # SSB needs update
        # (True ONLY if ExternalNRCellCU generates Correction_Cmd)
        # -------------------------------------------------
        if "SSB needs update" not in work.columns:
            if (df_external_nr_cell_cu is not None and not df_external_nr_cell_cu.empty and "Termpoint" in df_external_nr_cell_cu.columns and "Correction_Cmd" in df_external_nr_cell_cu.columns):
                ext_tp = df_external_nr_cell_cu[["Termpoint", "Correction_Cmd"]].copy()
                ext_tp["Termpoint"] = ext_tp["Termpoint"].astype(str).str.strip()

                needs_update = set(ext_tp.loc[ext_tp["Correction_Cmd"].astype(str).str.strip() != "", "Termpoint"])

                work["SSB needs update"] = work["Termpoint"].map(lambda v: v in needs_update)
            else:
                work["SSB needs update"] = False

        # -------------------------------------------------
        # GNodeB_SSB_Target - Same detection logic as ExternalNRCellCU (based on SummaryAudit node ids)
        # -------------------------------------------------
        if "GNodeB_SSB_Target" not in work.columns:
            nodes_without_retune_ids = {str(v) for v in (nodes_pre or [])}
            nodes_with_retune_ids = {str(v) for v in (nodes_post or [])}

            def _detect_gnodeb_target(ext_gnb: object) -> str:
                val = str(ext_gnb) if ext_gnb is not None else ""
                if any(n in val for n in nodes_without_retune_ids):
                    return "SSB-Pre"
                if any(n in val for n in nodes_with_retune_ids):
                    return "SSB-Post"
                return "Unknown"

            work["GNodeB_SSB_Target"] = work[ext_gnb_col].map(_detect_gnodeb_target)

        # -------------------------------------------------
        # Correction Command
        # (ONLY when SSB needs update == True)
        # -------------------------------------------------
        if "Correction_Cmd" not in work.columns:
            work["Correction_Cmd"] = ""

        mask_update = work["SSB needs update"] == True

        work.loc[mask_update, "Correction_Cmd"] = work.loc[mask_update, ext_gnb_col].map(lambda v: build_correction_command_termpoint_to_gnodeb(v, n77_ssb_post, n77_ssb_pre))

        # -------------------------------------------------
        # Write back (NO column removal)
        # -------------------------------------------------
        df_term_point_to_gnodeb.loc[:, work.columns] = work

        # -------------------------------------------------
        # SummaryAudit
        # -------------------------------------------------
        if admin_col:
            add_row("TermPointToGNodeB", "NR Termpoint Audit", "NR to NR TermPoints with administrativeState=LOCKED (from TermPointToGNodeB)", int((admin_norm == "LOCKED").sum()))

        if oper_col:
            add_row("TermPointToGNodeB", "NR Termpoint Audit", "NR to NR TermPoints with operationalState=DISABLED (from TermPointToGNodeB)", int((oper_norm == "DISABLED").sum()))

    except Exception as ex:
        add_row("TermPointToGNodeB", "NR Termpoint Audit", "Error while checking TermPointToGNodeB", f"{type(ex).__name__}: {ex}")



# ----------------------------- NEW: TermPointToGNB (X2 Termpoint Audit, LTE -> NR) -----------------------------
def process_termpoint_to_gnb(df_term_point_to_gnb, normalize_state, normalize_ip, add_row, df_external_gutran_cell, n77_ssb_post, n77_ssb_pre, nodes_pre=None, nodes_post=None):
    try:
        # Initialize locals to avoid UnboundLocalError if early branches happen
        work = None
        node_col = None
        ext_gnb_col = None
        admin_col = None
        oper_col = None
        ip_col = None
        avail_col = None

        if df_term_point_to_gnb is not None and not df_term_point_to_gnb.empty:
            # NEW: Always work on a full copy (same pattern as NR)
            work = df_term_point_to_gnb.copy()

            node_col = resolve_column_case_insensitive(work, ["NodeId"])
            ext_gnb_col = resolve_column_case_insensitive(work, ["ExternalGNodeBFunctionId", "ExternalGNBCUCPFunctionId", "ExternalGnbFunctionId"])
            admin_col = resolve_column_case_insensitive(work, ["administrativeState", "AdministrativeState"])
            oper_col = resolve_column_case_insensitive(work, ["operationalState", "OperationalState"])
            ip_col = resolve_column_case_insensitive(work, ["usedIpAddress", "UsedIpAddress"])
            avail_col = resolve_column_case_insensitive(work, ["availabilityStatus", "AvailabilityStatus"])

            # -------------------------------------------------
            # GNodeB_SSB_Target (same logic as ExternalGUtranCell)
            # -------------------------------------------------
            nodes_without_retune_ids = {str(v) for v in (nodes_pre or [])}
            nodes_with_retune_ids = {str(v) for v in (nodes_post or [])}

            def _detect_gnodeb_target(ext_gnb: object) -> str:
                val = str(ext_gnb) if ext_gnb is not None else ""
                if any(n in val for n in nodes_without_retune_ids):
                    return "SSB-Pre"
                if any(n in val for n in nodes_with_retune_ids):
                    return "SSB-Post"
                return "Unknown"

            if ext_gnb_col:
                work["GNodeB_SSB_Target"] = work[ext_gnb_col].map(_detect_gnodeb_target)
            else:
                if "GNodeB_SSB_Target" not in work.columns:
                    work["GNodeB_SSB_Target"] = "Unknown"


            if not node_col or not ext_gnb_col:
                add_row("TermPointToGNB", "X2 Termpoint Audit", "TermPointToGNB table present but required columns missing (NodeId/ExternalGNBCUCPFunctionId/admin/oper/ip)", "N/A")
                return

            if node_col and ext_gnb_col and (admin_col or oper_col or ip_col):
                work[node_col] = work[node_col].astype(str).str.strip()
                work[ext_gnb_col] = work[ext_gnb_col].astype(str).str.strip()

                # Note: Unique TermPoint identifier is (NodeId + ExternalGNBCUCPFunctionId)
                work["_tp_key_"] = work[node_col] + "|" + work[ext_gnb_col]

                if admin_col:
                    work["_admin_norm_"] = work[admin_col].map(normalize_state)
                    count_admin_locked = int(work.loc[work["_admin_norm_"] == "LOCKED", "_tp_key_"].nunique())
                else:
                    count_admin_locked = 0

                if oper_col:
                    work["_oper_norm_"] = work[oper_col].map(normalize_state)
                    count_oper_disabled = int(work.loc[work["_oper_norm_"] == "DISABLED", "_tp_key_"].nunique())
                else:
                    count_oper_disabled = 0

                if ip_col:
                    work["_ip_norm_"] = work[ip_col].map(normalize_ip)

                    # Match either the literal combined form "0.0.0.0/::" or any representation containing "0.0.0.0" or "::"
                    def _is_zero_ip(v: object) -> bool:
                        s = normalize_ip(v)
                        if not s:
                            return False
                        return s == "0.0.0.0/::" or s == "::" or ("0.0.0.0" in s)

                    count_ip_zero = int(work.loc[work["_ip_norm_"].map(_is_zero_ip), "_tp_key_"].nunique())
                else:
                    count_ip_zero = 0

                add_row("TermPointToGNB", "X2 Termpoint Audit", "LTE to NR TermPoints with administrativeState=LOCKED (from TermPointToGNB)", count_admin_locked)
                add_row("TermPointToGNB", "X2 Termpoint Audit", "LTE to NR TermPoints with operationalState=DISABLED (from TermPointToGNB)", count_oper_disabled)
                add_row("TermPointToGNB", "X2 Termpoint Audit", "LTE to NR TermPoints with usedIpAddress=0.0.0.0/:: (from TermPointToGNB)", count_ip_zero)
            else:
                add_row("TermPointToGNB", "X2 Termpoint Audit", "TermPointToGNB table present but required columns missing (NodeId/ExternalGNBCUCPFunctionId/admin/oper/ip)", "N/A")
        else:
            add_row("TermPointToGNB", "X2 Termpoint Audit", "TermPointToGNB table", "Table not found or empty")
            return

        # -------------------------------------------------
        # Termpoint
        # -------------------------------------------------
        if node_col and ext_gnb_col:
            work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

        # -------------------------------------------------
        # TermpointStatus / ConsolidatedStatus
        # -------------------------------------------------
        admin_norm = work[admin_col].fillna("").astype(str).str.strip().str.upper() if admin_col else pd.Series("", index=work.index)
        oper_norm = work[oper_col].fillna("").astype(str).str.strip().str.upper() if oper_col else pd.Series("", index=work.index)
        avail_raw = work[avail_col].fillna("").astype(str).str.strip() if avail_col else pd.Series("", index=work.index)
        avail_up = avail_raw.astype(str).str.upper()

        admin_disp = admin_norm.replace("", "EMPTY")
        oper_disp = oper_norm.replace("", "EMPTY")
        avail_disp = avail_raw.replace("", "EMPTY")

        work["TermpointStatus"] = ("administrativeState=" + admin_disp + ", operationalState=" + oper_disp + ", availabilityStatus=" + avail_disp)

        admin_ok = (admin_norm == "") | (admin_norm == "EMPTY") | (admin_norm == "UNLOCKED")
        oper_ok = (oper_norm == "") | (oper_norm == "EMPTY") | (oper_norm == "ENABLED")
        avail_ok = (avail_raw == "") | (avail_up == "EMPTY")

        work["TermPointConsolidatedStatus"] = ((admin_ok & oper_ok & avail_ok).map(lambda v: "OK" if v else "NOT_OK"))

        # -------------------------------------------------
        # SSB needs update (driven by ExternalGUtranCell)
        # -------------------------------------------------
        if (df_external_gutran_cell is not None and not df_external_gutran_cell.empty and "Termpoint" in df_external_gutran_cell.columns and "Correction_Cmd" in df_external_gutran_cell.columns):
            needs_update = set(df_external_gutran_cell.loc[df_external_gutran_cell["Correction_Cmd"].astype(str).str.strip() != "", "Termpoint"])
            work["SSB needs update"] = work["Termpoint"].map(lambda v: v in needs_update)
        else:
            work["SSB needs update"] = False

        # -------------------------------------------------
        # Correction Command
        # -------------------------------------------------
        if "Correction_Cmd" not in work.columns:
            work["Correction_Cmd"] = ""

        work.loc[work["SSB needs update"] == True, "Correction_Cmd"] = work.loc[work["SSB needs update"] == True, ext_gnb_col].map(lambda v: build_correction_command_termpoint_to_gnb(v, n77_ssb_post, n77_ssb_pre))

        # -------------------------------------------------
        # Write back (NO column removal)
        # -------------------------------------------------
        df_term_point_to_gnb.loc[:, work.columns] = work

    except Exception as ex:
        add_row("TermPointToGNB", "X2 Termpoint Audit", "Error while checking TermPointToGNB", f"ERROR: {ex}")



# ----------------------------- NEW: TermPointToENodeB (X2 Termpoint Audit, NR -> LTE) -----------------------------
def process_term_point_to_enodeb(df_term_point_to_enodeb, normalize_state, add_row, nodes_pre=None, nodes_post=None):
    try:
        if df_term_point_to_enodeb is not None and not df_term_point_to_enodeb.empty:
            node_col = resolve_column_case_insensitive(df_term_point_to_enodeb, ["NodeId"])
            ext_enb_col = resolve_column_case_insensitive(df_term_point_to_enodeb, ["ExternalENodeBFunctionId"])
            admin_col = resolve_column_case_insensitive(df_term_point_to_enodeb, ["administrativeState", "AdministrativeState"])
            oper_col = resolve_column_case_insensitive(df_term_point_to_enodeb, ["operationalState", "OperationalState"])

            if node_col and ext_enb_col and (admin_col or oper_col):
                cols = [node_col, ext_enb_col] + ([admin_col] if admin_col else []) + ([oper_col] if oper_col else [])
                work = df_term_point_to_enodeb[cols].copy()
                work[node_col] = work[node_col].astype(str).str.strip()
                work[ext_enb_col] = work[ext_enb_col].astype(str).str.strip()

                # Note: Unique TermPoint identifier is (NodeId + ExternalENodeBFunctionId)
                work["_tp_key_"] = work[node_col] + "|" + work[ext_enb_col]

                if admin_col:
                    work["_admin_norm_"] = work[admin_col].map(normalize_state)
                    count_admin_locked = int(work.loc[work["_admin_norm_"] == "LOCKED", "_tp_key_"].nunique())
                else:
                    count_admin_locked = 0

                if oper_col:
                    work["_oper_norm_"] = work[oper_col].map(normalize_state)
                    count_oper_disabled = int(work.loc[work["_oper_norm_"] == "DISABLED", "_tp_key_"].nunique())
                else:
                    count_oper_disabled = 0

                add_row("TermPointToENodeB", "X2 Termpoint Audit", "NR to LTE TermPoints with administrativeState=LOCKED (from TermPointToENodeB)", count_admin_locked)
                add_row("TermPointToENodeB", "X2 Termpoint Audit", "NR to LTE TermPoints with operationalState=DISABLED (from TermPointToENodeB)", count_oper_disabled)
            else:
                add_row("TermPointToENodeB", "X2 Termpoint Audit", "TermPointToENodeB table present but required columns missing (NodeId/ExternalENodeBFunctionId/admin/oper)", "N/A")
        else:
            add_row("TermPointToENodeB", "X2 Termpoint Audit", "TermPointToENodeB table", "Table not found or empty")
    except Exception as ex:
        add_row("TermPointToENodeB", "X2 Termpoint Audit", "Error while checking TermPointToENodeB", f"ERROR: {ex}")

