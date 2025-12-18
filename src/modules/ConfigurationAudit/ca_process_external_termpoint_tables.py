# -*- coding: utf-8 -*-
import pandas as pd

from src.modules.Common.Common_Functions import load_nodes_names_and_id_from_summary_audit
from src.utils.utils_dataframe import ensure_column_after
from src.utils.utils_frequency import resolve_column_case_insensitive

# ----------------------------- NEW: ExternalNRCellCU (same value as NRCellRelation old/new counts) -----------------------------
def process_external_nr_cell_cu(df_external_nr_cell_cu, rows, module_name, n77_ssb_pre, n77_ssb_post, add_row, df_term_point_to_gnodeb, _extract_freq_from_nrfrequencyref, _extract_nrnetwork_tail):
    def _build_external_nrcellcu_correction(ext_gnb: str, ext_cell: str, nr_tail: str) -> str:
        """
        Build correction command replacing old N77 SSB with new N77 SSB inside nr_tail.
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
            f"set ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU={ext_cell} nRFrequencyRef {nr_tail}\n"
            "alt"
        )

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
            freq_col = resolve_column_case_insensitive(
                df_external_nr_cell_cu,
                ["nRFrequencyRef", "NRFrequencyRef", "nRFreqRelationRef", "NRFreqRelationRef"],
            )
            ext_gnb_col = resolve_column_case_insensitive(df_external_nr_cell_cu, ["ExternalGNBCUCPFunctionId"])
            cell_col = resolve_column_case_insensitive(df_external_nr_cell_cu, ["ExternalNRCellCUId"])

            # Load node identifiers from SummaryAudit (Pre / Post)
            nodes_without_retune_ids = load_nodes_names_and_id_from_summary_audit(rows, stage="Pre", module_name=module_name)
            nodes_with_retune_ids = load_nodes_names_and_id_from_summary_audit(rows, stage="Post", module_name=module_name)

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

                # Extract SSB source frequency safely
                work["GNodeB_SSB_Source"] = work[freq_col].map(
                    lambda v: _extract_freq_from_nrfrequencyref(v) if isinstance(v, str) and v.strip() else ""
                )

                old_ssb = str(n77_ssb_pre)
                new_ssb = str(n77_ssb_post)

                # =========================
                # SummaryAudit counts
                # =========================
                count_old = int((work["GNodeB_SSB_Source"].astype(str) == old_ssb).sum())
                count_new = int((work["GNodeB_SSB_Source"].astype(str) == new_ssb).sum())

                add_row(
                    "ExternalNRCellCU",
                    "NR Frequency Audit",
                    f"External cells to old N77 SSB ({old_ssb}) (from ExternalNRCellCU)",
                    count_old,
                )
                add_row(
                    "ExternalNRCellCU",
                    "NR Frequency Audit",
                    f"External cells to new N77 SSB ({new_ssb}) (from ExternalNRCellCU)",
                    count_new,
                )

                # =========================
                # Termpoint
                # =========================
                if ext_gnb_col and "Termpoint" not in work.columns:
                    work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

                # =========================
                # TermpointStatus / TermpointConsolidatedStatus
                # (calculated directly from TermPointToGNodeB raw table)
                # =========================
                if (
                        df_term_point_to_gnodeb is not None
                        and not df_term_point_to_gnodeb.empty
                        and "Termpoint" in work.columns
                ):
                    tp_src = df_term_point_to_gnodeb.copy()

                    node_tp_col = resolve_column_case_insensitive(tp_src, ["NodeId"])
                    ext_tp_col = resolve_column_case_insensitive(tp_src, ["ExternalGNBCUCPFunctionId"])
                    admin_col = resolve_column_case_insensitive(tp_src, ["administrativeState", "AdministrativeState"])
                    oper_col = resolve_column_case_insensitive(tp_src, ["operationalState", "OperationalState"])
                    avail_col = resolve_column_case_insensitive(tp_src, ["availabilityStatus", "AvailabilityStatus"])

                    if node_tp_col and ext_tp_col:
                        tp_src["Termpoint"] = (
                                tp_src[node_tp_col].astype(str).str.strip()
                                + "-"
                                + tp_src[ext_tp_col].astype(str).str.strip()
                        )

                        # Always work with Series to avoid attribute errors
                        admin_val = tp_src[admin_col] if admin_col else pd.Series("", index=tp_src.index)
                        oper_val = tp_src[oper_col] if oper_col else pd.Series("", index=tp_src.index)
                        avail_val = tp_src[avail_col] if avail_col else pd.Series("", index=tp_src.index)

                        avail_txt = avail_val.astype(str).fillna("").replace("", "Empty")

                        tp_src["TermpointStatus"] = (
                                "administrativeState=" + admin_val.astype(str).fillna("") +
                                ", operationalState=" + oper_val.astype(str).fillna("") +
                                ", availabilityStatus=" + avail_txt
                        )

                        admin_ok = admin_val.map(_normalize_state_local) == "UNLOCKED"
                        oper_ok = oper_val.map(_normalize_state_local) == "ENABLED"
                        avail_ok = avail_val.astype(str).fillna("").str.strip() == ""

                        tp_src["TermpointConsolidatedStatus"] = (
                            (admin_ok & oper_ok & avail_ok)
                            .map(lambda v: "OK" if v else "NOT_OK")
                        )

                        tp_map = tp_src.drop_duplicates("Termpoint").set_index("Termpoint")

                        work["TermpointStatus"] = work["Termpoint"].map(tp_map["TermpointStatus"])
                        work["TermpointConsolidatedStatus"] = work["Termpoint"].map(tp_map["TermpointConsolidatedStatus"])

                # -------------------------------------------------
                # Place GNodeB_SSB_Source right after TermpointConsolidatedStatus
                # -------------------------------------------------
                work = ensure_column_after(work, "GNodeB_SSB_Source", "TermpointConsolidatedStatus")

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
                        return "Other"

                    work["GNodeB_SSB_Target"] = work[ext_gnb_col].map(_detect_gnodeb_target)

                # =========================
                # Correction Command
                # (only for SSB-PRE frequency AND target != SSB-Pre)
                # =========================
                if ext_gnb_col and cell_col:
                    mask_pre = work["GNodeB_SSB_Source"].astype(str) == old_ssb
                    mask_target = work["GNodeB_SSB_Target"] != "SSB-Pre"
                    mask_final = mask_pre & mask_target

                    # Safely extract NR network tail
                    nr_tail_series = work[freq_col].map(
                        lambda v: _extract_nrnetwork_tail(v) if isinstance(v, str) and v.strip() else ""
                    )

                    if "Correction_Cmd" not in work.columns:
                        work["Correction_Cmd"] = ""

                    work.loc[mask_final, "Correction_Cmd"] = work.loc[mask_final].apply(
                        lambda r: _build_external_nrcellcu_correction(
                            r.get(ext_gnb_col, ""),
                            r.get(cell_col, ""),
                            nr_tail_series.loc[r.name],
                        ),
                        axis=1,
                    )

                # Write back preserving original columns + new ones
                df_external_nr_cell_cu.loc[:, work.columns] = work

            else:
                add_row(
                    "ExternalNRCellCU",
                    "NR Frequency Audit",
                    "ExternalNRCellCU table present but NodeId / nRFrequencyRef missing",
                    "N/A",
                )
        else:
            add_row(
                "ExternalNRCellCU",
                "NR Frequency Audit",
                "ExternalNRCellCU table",
                "Table not found or empty",
            )
    except Exception as ex:
        add_row(
            "ExternalNRCellCU",
            "NR Frequency Audit",
            "Error while checking ExternalNRCellCU",
            f"{type(ex).__name__}: {ex}",
        )

# ----------------------------- NEW: ExternalGUtranCell (old/new counts + OUT_OF_SERVICE row counts) -----------------------------
def process_external_gutran_cell(df_external_gutran_cell, _extract_ssb_from_gutran_sync_ref, n77_ssb_pre, n77_ssb_post, add_row, _normalize_state, df_term_point_to_gnb, rows, module_name):
    try:
        if df_external_gutran_cell is not None and not df_external_gutran_cell.empty:
            # NEW: Always work on a full copy (same pattern as NR)
            work = df_external_gutran_cell.copy()

            node_col = resolve_column_case_insensitive(work, ["NodeId"])
            ref_col = resolve_column_case_insensitive(work, ["gUtranSyncSignalFrequencyRef", "GUtranSyncSignalFrequencyRef"])
            status_col = resolve_column_case_insensitive(work, ["serviceStatus", "ServiceStatus"])
            ext_gnb_col = resolve_column_case_insensitive(work, ["ExternalGNodeBFunctionId"])

            if node_col and ref_col:
                work[node_col] = work[node_col].astype(str).str.strip()
                work["GNodeB_SSB_Target"] = work[ref_col].map(_extract_ssb_from_gutran_sync_ref)

                old_ssb = n77_ssb_pre
                new_ssb = n77_ssb_post

                # Same logic as GUtranCellRelation: counts of relations/rows pointing to old/new
                count_old = int((work["GNodeB_SSB_Source"] == old_ssb).sum())
                count_new = int((work["GNodeB_SSB_Source"] == new_ssb).sum())

                add_row(
                    "ExternalGUtranCell",
                    "LTE Frequency Audit",
                    f"External cells to old N77 SSB ({old_ssb}) (from ExternalGUtranCell)",
                    count_old,
                )
                add_row(
                    "ExternalGUtranCell",
                    "LTE Frequency Audit",
                    f"External cells to new N77 SSB ({new_ssb}) (from ExternalGUtranCell)",
                    count_new,
                )

                # OUT_OF_SERVICE ROW counts based on serviceStatus and parsed SSB from gUtranSyncSignalFrequencyRef
                if status_col:
                    work["_status_norm_"] = work[status_col].map(_normalize_state)
                    mask_oos = work["_status_norm_"] == "OUT_OF_SERVICE"

                    # Count ROWS (not nodes) and don't dump lists in ExtraInfo
                    count_old_oos = int(((work["GNodeB_SSB_Source"] == old_ssb) & mask_oos).sum())
                    count_new_oos = int(((work["GNodeB_SSB_Source"] == new_ssb) & mask_oos).sum())

                    add_row(
                        "ExternalGUtranCell",
                        "LTE Frequency Audit",
                        f"External cells to old N77 SSB ({old_ssb}) with serviceStatus=OUT_OF_SERVICE (from ExternalGUtranCell)",
                        count_old_oos,
                        "",  # keep ExtraInfo empty to avoid huge lists
                    )
                    add_row(
                        "ExternalGUtranCell",
                        "LTE Frequency Audit",
                        f"External cells to new N77 SSB ({new_ssb}) with serviceStatus=OUT_OF_SERVICE (from ExternalGUtranCell)",
                        count_new_oos,
                        "",  # keep ExtraInfo empty to avoid huge lists
                    )
                else:
                    add_row(
                        "ExternalGUtranCell",
                        "LTE Frequency Audit",
                        "External cells OUT_OF_SERVICE checks skipped (serviceStatus missing)",
                        "N/A",
                    )
            else:
                add_row(
                    "ExternalGUtranCell",
                    "LTE Frequency Audit",
                    "ExternalGUtranCell table present but NodeId / gUtranSyncSignalFrequencyRef column missing",
                    "N/A",
                )
        else:
            add_row(
                "ExternalGUtranCell",
                "LTE Frequency Audit",
                "ExternalGUtranCell table",
                "Table not found or empty",
            )

        # -------------------------------------------------
        # Termpoint / TermpointStatus / ConsolidatedStatus
        # (same logic as ExternalNRCellCU, but LTE side)
        # -------------------------------------------------
        if df_external_gutran_cell is not None and not df_external_gutran_cell.empty:
            if node_col and ext_gnb_col:
                work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

            if (
                    df_term_point_to_gnb is not None
                    and not df_term_point_to_gnb.empty
                    and "Termpoint" in work.columns
            ):
                tp = df_term_point_to_gnb.copy()

                tp_node = resolve_column_case_insensitive(tp, ["NodeId"])
                tp_ext = resolve_column_case_insensitive(tp, ["ExternalGNodeBFunctionId"])
                admin_col_tp = resolve_column_case_insensitive(tp, ["administrativeState", "AdministrativeState"])
                oper_col_tp = resolve_column_case_insensitive(tp, ["operationalState", "OperationalState"])
                avail_col_tp = resolve_column_case_insensitive(tp, ["availabilityStatus", "AvailabilityStatus"])

                if tp_node and tp_ext:
                    tp["Termpoint"] = tp[tp_node].astype(str).str.strip() + "-" + tp[tp_ext].astype(str).str.strip()

                    admin = tp[admin_col_tp].astype(str).str.upper() if admin_col_tp else ""
                    oper = tp[oper_col_tp].astype(str).str.upper() if oper_col_tp else ""
                    avail_raw = tp[avail_col_tp].astype(str).fillna("").str.strip() if avail_col_tp else ""

                    tp["TermpointStatus"] = (
                            "administrativeState=" + admin +
                            ", operationalState=" + oper +
                            ", availabilityStatus=" + avail_raw.replace("", "EMPTY")
                    )

                    tp["TermpointConsolidatedStatus"] = (
                        ((admin == "UNLOCKED") & (oper == "ENABLED") & (avail_raw == ""))
                        .map(lambda v: "OK" if v else "NOT_OK")
                    )

                    tp_map = tp.drop_duplicates("Termpoint").set_index("Termpoint")

                    work["TermpointStatus"] = work["Termpoint"].map(tp_map["TermpointStatus"])
                    work["TermpointConsolidatedStatus"] = work["Termpoint"].map(tp_map["TermpointConsolidatedStatus"])

        # -------------------------------------------------
        # Place GNodeB_SSB_Source right after TermpointConsolidatedStatus
        # -------------------------------------------------
        work = ensure_column_after(work, "GNodeB_SSB_Target", "TermpointConsolidatedStatus")

        # -------------------------------------------------
        # GNodeB_SSB_Target
        # -------------------------------------------------
        nodes_pre = set(load_nodes_names_and_id_from_summary_audit(rows, stage="Pre", module_name=module_name) or [])
        nodes_post = set(load_nodes_names_and_id_from_summary_audit(rows, stage="Post", module_name=module_name) or [])

        def _detect_gnodeb_target_lte(value: object) -> str:
            s = str(value)
            if any(n in s for n in nodes_pre):
                return "SSB-Pre"
            if any(n in s for n in nodes_post):
                return "SSB-Post"
            return "Other"

        if ext_gnb_col:
            work["GNodeB_SSB_Target"] = work[ext_gnb_col].map(_detect_gnodeb_target_lte)

        # -------------------------------------------------
        # Correction Command (LTE)
        # -------------------------------------------------
        if "Correction_Cmd" not in work.columns:
            work["Correction_Cmd"] = ""

        mask_pre = work["GNodeB_SSB_Source"] == n77_ssb_pre
        mask_target = work["GNodeB_SSB_Target"] != "SSB-Pre"

        work.loc[mask_pre & mask_target, "Correction_Cmd"] = work.loc[mask_pre & mask_target].apply(
            lambda r:
            "confb+\n"
            "gs+\n"
            "lt all\n"
            "alt\n"
            f"set ENodeBFunction={r[ext_gnb_col]},ExternalGUtranCell={r.get('ExternalGUtranCellId', '')} "
            f"gUtranSyncSignalFrequencyRef GUtraNetwork=1,GUtranSyncSignalFrequency={n77_ssb_post}-30\n"
            "alt",
            axis=1,
        )

        # -------------------------------------------------
        # Write back preserving original columns + new ones
        # -------------------------------------------------
        if df_external_gutran_cell is not None and not df_external_gutran_cell.empty:
            df_external_gutran_cell.loc[:, work.columns] = work

    except Exception as ex:
        add_row(
            "ExternalGUtranCell",
            "LTE Frequency Audit",
            "Error while checking ExternalGUtranCell",
            f"ERROR: {ex}",
        )

# ----------------------------- NEW: TermPointToGNodeB (NR Termpoint Audit) -----------------------------
def process_term_point_to_gnodeb(df_term_point_to_gnodeb, add_row, df_external_nr_cell_cu, n77_ssb_post, n77_ssb_pre):
    def _build_termpoint_to_gnodeb_correction(ext_gnb: str, ssb_post: int, ssb_pre: int) -> str:
        """
        Build correction command for TermPointToGNodeB.
        Safely returns empty string if ext_gnb is missing.
        """
        if not ext_gnb:
            return ""

        return (
            "confb+\n"
            "lt all\n"
            "alt\n"
            f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef {ssb_post}\n"
            f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef {ssb_pre}\n"
            f"get ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
            f"bl ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
            "wait 5\n"
            f"deb ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
            "wait 60\n"
            f"get ExternalGNBCUCPFunction={ext_gnb},TermpointToGnodeB\n"
            f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef {ssb_post}\n"
            f"hget ExternalGNBCUCPFunction={ext_gnb},ExternalNRCellCU nRFrequencyRef {ssb_pre}\n"
            "alt"
        )

    try:
        if df_term_point_to_gnodeb is None or df_term_point_to_gnodeb.empty:
            add_row(
                "TermPointToGNodeB",
                "NR Termpoint Audit",
                "TermPointToGNodeB table",
                "Table not found or empty",
            )
            return

        node_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["NodeId"])
        ext_gnb_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["ExternalGNBCUCPFunctionId"])
        admin_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["administrativeState", "AdministrativeState"])
        oper_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["operationalState", "OperationalState"])
        avail_col = resolve_column_case_insensitive(df_term_point_to_gnodeb, ["availabilityStatus", "AvailabilityStatus"])

        if not node_col or not ext_gnb_col:
            add_row(
                "TermPointToGNodeB",
                "NR Termpoint Audit",
                "TermPointToGNodeB table present but required columns missing",
                "N/A",
            )
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
        admin_norm = work[admin_col].astype(str).str.upper() if admin_col else pd.Series("", index=work.index)
        oper_norm = work[oper_col].astype(str).str.upper() if oper_col else pd.Series("", index=work.index)
        avail_raw = work[avail_col].astype(str).fillna("").str.strip() if avail_col else pd.Series("", index=work.index)
        avail_norm = avail_raw.replace("", "EMPTY")

        # -------------------------------------------------
        # TermpointStatus (CONCAT ONLY)
        # -------------------------------------------------
        work["TermpointStatus"] = (
                "administrativeState=" + admin_norm +
                ", operationalState=" + oper_norm +
                ", availabilityStatus=" + avail_norm
        )

        # -------------------------------------------------
        # TermPointConsolidatedStatus (LOGIC)
        # -------------------------------------------------
        work["TermPointConsolidatedStatus"] = (
            ((admin_norm == "UNLOCKED") &
             (oper_norm == "ENABLED") &
             (avail_raw == ""))
            .map(lambda v: "OK" if v else "NOT_OK")
        )

        # -------------------------------------------------
        # SSB needs update
        # (True ONLY if ExternalNRCellCU generates Correction_Cmd)
        # -------------------------------------------------
        if "SSB needs update" not in work.columns:
            if (
                    df_external_nr_cell_cu is not None
                    and not df_external_nr_cell_cu.empty
                    and "Termpoint" in df_external_nr_cell_cu.columns
                    and "Correction_Cmd" in df_external_nr_cell_cu.columns
            ):
                ext_tp = df_external_nr_cell_cu[["Termpoint", "Correction_Cmd"]].copy()
                ext_tp["Termpoint"] = ext_tp["Termpoint"].astype(str).str.strip()

                needs_update = set(
                    ext_tp.loc[
                        ext_tp["Correction_Cmd"].astype(str).str.strip() != "",
                        "Termpoint"
                    ]
                )

                work["SSB needs update"] = work["Termpoint"].map(lambda v: v in needs_update)
            else:
                work["SSB needs update"] = False

        # -------------------------------------------------
        # Correction Command
        # (ONLY when SSB needs update == True)
        # -------------------------------------------------
        if "Correction_Cmd" not in work.columns:
            work["Correction_Cmd"] = ""

        mask_update = work["SSB needs update"] == True

        work.loc[mask_update, "Correction_Cmd"] = work.loc[mask_update, ext_gnb_col].map(
            lambda v: _build_termpoint_to_gnodeb_correction(v, n77_ssb_post, n77_ssb_pre)
        )

        # -------------------------------------------------
        # Write back (NO column removal)
        # -------------------------------------------------
        df_term_point_to_gnodeb.loc[:, work.columns] = work

        # -------------------------------------------------
        # SummaryAudit
        # -------------------------------------------------
        if admin_col:
            add_row(
                "TermPointToGNodeB",
                "NR Termpoint Audit",
                "NR to NR TermPoints with administrativeState=LOCKED (from TermPointToGNodeB)",
                int((admin_norm == "LOCKED").sum()),
            )

        if oper_col:
            add_row(
                "TermPointToGNodeB",
                "NR Termpoint Audit",
                "NR to NR TermPoints with operationalState=DISABLED (from TermPointToGNodeB)",
                int((oper_norm == "DISABLED").sum()),
            )

    except Exception as ex:
        add_row(
            "TermPointToGNodeB",
            "NR Termpoint Audit",
            "Error while checking TermPointToGNodeB",
            f"{type(ex).__name__}: {ex}",
        )

# ----------------------------- NEW: TermPointToGNB (X2 Termpoint Audit, LTE -> NR) -----------------------------
def process_term_point_to_gnb(df_term_point_to_gnb, _normalize_state, _normalize_ip, add_row, df_external_gutran_cell, n77_ssb_post, n77_ssb_pre):
    try:
        if df_term_point_to_gnb is not None and not df_term_point_to_gnb.empty:
            # NEW: Always work on a full copy (same pattern as NR)
            work = df_term_point_to_gnb.copy()

            node_col = resolve_column_case_insensitive(work, ["NodeId"])
            ext_gnb_col = resolve_column_case_insensitive(work, ["ExternalGNodeBFunctionId", "ExternalGNBCUCPFunctionId", "ExternalGnbFunctionId"])
            admin_col = resolve_column_case_insensitive(work, ["administrativeState", "AdministrativeState"])
            oper_col = resolve_column_case_insensitive(work, ["operationalState", "OperationalState"])
            ip_col = resolve_column_case_insensitive(work, ["usedIpAddress", "UsedIpAddress"])
            avail_col = resolve_column_case_insensitive(work, ["availabilityStatus", "AvailabilityStatus"])

            if node_col and ext_gnb_col and (admin_col or oper_col or ip_col):
                work[node_col] = work[node_col].astype(str).str.strip()
                work[ext_gnb_col] = work[ext_gnb_col].astype(str).str.strip()

                # Note: Unique TermPoint identifier is (NodeId + ExternalGNBCUCPFunctionId)
                work["_tp_key_"] = work[node_col] + "|" + work[ext_gnb_col]

                if admin_col:
                    work["_admin_norm_"] = work[admin_col].map(_normalize_state)
                    count_admin_locked = int(work.loc[work["_admin_norm_"] == "LOCKED", "_tp_key_"].nunique())
                else:
                    count_admin_locked = 0

                if oper_col:
                    work["_oper_norm_"] = work[oper_col].map(_normalize_state)
                    count_oper_disabled = int(work.loc[work["_oper_norm_"] == "DISABLED", "_tp_key_"].nunique())
                else:
                    count_oper_disabled = 0

                if ip_col:
                    work["_ip_norm_"] = work[ip_col].map(_normalize_ip)

                    # Match either the literal combined form "0.0.0.0/::" or any representation containing "0.0.0.0" or "::"
                    def _is_zero_ip(v: object) -> bool:
                        s = _normalize_ip(v)
                        if not s:
                            return False
                        return s == "0.0.0.0/::" or s == "::" or ("0.0.0.0" in s)

                    count_ip_zero = int(work.loc[work["_ip_norm_"].map(_is_zero_ip), "_tp_key_"].nunique())
                else:
                    count_ip_zero = 0

                add_row(
                    "TermPointToGNB",
                    "X2 Termpoint Audit",
                    "LTE to NR TermPoints with administrativeState=LOCKED (from TermPointToGNB)",
                    count_admin_locked,
                )
                add_row(
                    "TermPointToGNB",
                    "X2 Termpoint Audit",
                    "LTE to NR TermPoints with operationalState=DISABLED (from TermPointToGNB)",
                    count_oper_disabled,
                )
                add_row(
                    "TermPointToGNB",
                    "X2 Termpoint Audit",
                    "LTE to NR TermPoints with usedIpAddress=0.0.0.0/:: (from TermPointToGNB)",
                    count_ip_zero,
                )
            else:
                add_row(
                    "TermPointToGNB",
                    "X2 Termpoint Audit",
                    "TermPointToGNB table present but required columns missing (NodeId/ExternalGNBCUCPFunctionId/admin/oper/ip)",
                    "N/A",
                )
        else:
            add_row(
                "TermPointToGNB",
                "X2 Termpoint Audit",
                "TermPointToGNB table",
                "Table not found or empty",
            )

        # -------------------------------------------------
        # Termpoint
        # -------------------------------------------------
        if df_term_point_to_gnb is not None and not df_term_point_to_gnb.empty:
            if node_col and ext_gnb_col:
                work["Termpoint"] = work[node_col] + "-" + work[ext_gnb_col]

        # -------------------------------------------------
        # TermpointStatus / ConsolidatedStatus
        # -------------------------------------------------
        admin_norm = work[admin_col].astype(str).str.upper() if admin_col else ""
        oper_norm = work[oper_col].astype(str).str.upper() if oper_col else ""
        avail_raw = work[avail_col].astype(str).fillna("").str.strip() if avail_col else ""

        work["TermpointStatus"] = (
                "administrativeState=" + admin_norm +
                ", operationalState=" + oper_norm +
                ", availabilityStatus=" + avail_raw.replace("", "EMPTY")
        )

        work["TermPointConsolidatedStatus"] = (
            ((admin_norm == "UNLOCKED") & (oper_norm == "ENABLED") & (avail_raw == ""))
            .map(lambda v: "OK" if v else "NOT_OK")
        )

        # -------------------------------------------------
        # SSB needs update (driven by ExternalGUtranCell)
        # -------------------------------------------------
        if (
                df_external_gutran_cell is not None
                and not df_external_gutran_cell.empty
                and "Termpoint" in df_external_gutran_cell.columns
                and "Correction_Cmd" in df_external_gutran_cell.columns
        ):
            needs_update = set(
                df_external_gutran_cell.loc[
                    df_external_gutran_cell["Correction_Cmd"].astype(str).str.strip() != "",
                    "Termpoint"
                ]
            )
            work["SSB needs update"] = work["Termpoint"].map(lambda v: v in needs_update)
        else:
            work["SSB needs update"] = False

        # -------------------------------------------------
        # Correction Command
        # -------------------------------------------------
        if "Correction_Cmd" not in work.columns:
            work["Correction_Cmd"] = ""

        work.loc[work["SSB needs update"] == True, "Correction_Cmd"] = work.loc[
            work["SSB needs update"] == True, ext_gnb_col
        ].map(
            lambda v:
            "confb+\n"
            "lt all\n"
            "alt\n"
            f"hget ExternalGNodeBFunction={v},ExternalGUtranCell GUtranSyncSignalFrequency {n77_ssb_post}-30\n"
            f"hget ExternalGNodeBFunction={v},ExternalGUtranCell GUtranSyncSignalFrequency {n77_ssb_pre}-30\n"
            f"get ExternalGNodeBFunction={v},TermpointToGNB\n"
            f"bl ExternalGNodeBFunction={v},TermpointToGNB\n"
            "wait 5\n"
            f"deb ExternalGNodeBFunction={v},TermpointToGNB\n"
            "wait 60\n"
            f"get ExternalGNodeBFunction={v},TermpointToGNB\n"
            "alt"
        )

        # -------------------------------------------------
        # Write back (NO column removal)
        # -------------------------------------------------
        if df_term_point_to_gnb is not None and not df_term_point_to_gnb.empty:
            df_term_point_to_gnb.loc[:, work.columns] = work

    except Exception as ex:
        add_row(
            "TermPointToGNB",
            "X2 Termpoint Audit",
            "Error while checking TermPointToGNB",
            f"ERROR: {ex}",
        )

# ----------------------------- NEW: TermPointToENodeB (X2 Termpoint Audit, NR -> LTE) -----------------------------
def process_term_point_to_enodeb(df_term_point_to_enodeb, _normalize_state, add_row):
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
                    work["_admin_norm_"] = work[admin_col].map(_normalize_state)
                    count_admin_locked = int(work.loc[work["_admin_norm_"] == "LOCKED", "_tp_key_"].nunique())
                else:
                    count_admin_locked = 0

                if oper_col:
                    work["_oper_norm_"] = work[oper_col].map(_normalize_state)
                    count_oper_disabled = int(work.loc[work["_oper_norm_"] == "DISABLED", "_tp_key_"].nunique())
                else:
                    count_oper_disabled = 0

                add_row(
                    "TermPointToENodeB",
                    "X2 Termpoint Audit",
                    "NR to LTE TermPoints with administrativeState=LOCKED (from TermPointToENodeB)",
                    count_admin_locked,
                )
                add_row(
                    "TermPointToENodeB",
                    "X2 Termpoint Audit",
                    "NR to LTE TermPoints with operationalState=DISABLED (from TermPointToENodeB)",
                    count_oper_disabled,
                )
            else:
                add_row(
                    "TermPointToENodeB",
                    "X2 Termpoint Audit",
                    "TermPointToENodeB table present but required columns missing (NodeId/ExternalENodeBFunctionId/admin/oper)",
                    "N/A",
                )
        else:
            add_row(
                "TermPointToENodeB",
                "X2 Termpoint Audit",
                "TermPointToENodeB table",
                "Table not found or empty",
            )
    except Exception as ex:
        add_row(
            "TermPointToENodeB",
            "X2 Termpoint Audit",
            "Error while checking TermPointToENodeB",
            f"ERROR: {ex}",
        )