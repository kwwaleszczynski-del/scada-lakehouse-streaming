"""Automated Bronze -> Silver SCADA Data Quality (DQ) rules and sensor fault recovery."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .dst_and_counters import reconcile_cumulative_counter


FAULT_CLASSES = [
    "HEALTHY",
    "FLATLINE",
    "SPIKE_OR_BOUNDS",
    "COUNTER_RESET",
    "CROSS_SENSOR_MISMATCH",
]


def run_silver_dq_engine(
    silver_1m: pd.DataFrame,
    flatline_window_min: int = 30,
) -> pd.DataFrame:
    """Apply physical bounds, flatline, counter monotonicity, and cross-sensor consistency rules.

    Produces:
      - dq_flag: predicted fault class in FAULT_CLASSES
      - wind_speed_clean_ms, poa_clean_wm2, active_power_clean_kw, delta_kwh_reconciled
    """
    df = reconcile_cumulative_counter(silver_1m)

    # 1. Counter reset / rollover check (delta < 0 before reconciliation)
    flag_counter = df["counter_event"] != "NORMAL"

    # 2. Physical bounds & nighttime zero-offset check
    is_pv_asset = df["asset_type"].isin(["pv", "meter_pv"])
    is_wind_asset = df["asset_type"].isin(["wind", "meter_wind"])
    flag_spike = (
        (is_pv_asset & (df["poa_wm2"] > 1350.0))
        | (is_pv_asset & (df["solar_elevation_deg"] < -4.0) & (df["poa_wm2"] > 35.0))
        | (is_wind_asset & ((df["wind_speed_ms"] > 38.0) | (df["wind_speed_ms"] < 0.0)))
        | (df["active_power_kw"] > df["rated_kw"] * 1.08)
    )

    # 3. Rolling flatline detector (std == 0 for >= flatline_window_min on active sensor)
    # Using centered/run-length segment detection with 2-minute onset warmup (typical causal buffer lag)
    flag_flatline = np.zeros(len(df), dtype=bool)
    for _, idx in df.groupby("asset_id").groups.items():
        sub = df.loc[idx]
        atype = sub["asset_type"].iloc[0]
        if atype == "wind":
            vals = sub["wind_speed_ms"].to_numpy()
            active = vals > 2.0
        elif atype == "pv":
            vals = sub["poa_wm2"].to_numpy()
            active = vals > 50.0
        else:
            continue

        # Identify contiguous runs where diff == 0
        diffs = np.abs(np.diff(vals, prepend=vals[0] - 999.0))
        run_starts = np.where(diffs > 1e-6)[0]
        run_ends = np.append(run_starts[1:], len(vals))
        local_flat = np.zeros(len(vals), dtype=bool)
        for s_i, e_i in zip(run_starts, run_ends):
            if (e_i - s_i) >= flatline_window_min and np.all(active[s_i:e_i]):
                # Causal warmup: first 3 minutes of a flatline are still within turbulence deadband
                local_flat[s_i + 3 : e_i] = True
        flag_flatline[idx] = local_flat

    # 4. Cross-sensor consistency check (active power > 60% rated while primary resource sensor near zero)
    flag_cross = (
        ((df["asset_type"] == "wind") & (df["active_power_kw"] > 0.60 * df["rated_kw"]) & (df["wind_speed_ms"] < 2.5))
        | ((df["asset_type"] == "pv") & (df["active_power_kw"] > 0.60 * df["rated_kw"]) & (df["poa_wm2"] < 40.0))
    ) & (~flag_spike) & (~flag_flatline)

    # Priority assignment
    dq_flag = np.full(len(df), "HEALTHY", dtype=object)
    dq_flag = np.where(flag_cross, "CROSS_SENSOR_MISMATCH", dq_flag)
    dq_flag = np.where(flag_flatline, "FLATLINE", dq_flag)
    dq_flag = np.where(flag_spike, "SPIKE_OR_BOUNDS", dq_flag)
    dq_flag = np.where(flag_counter, "COUNTER_RESET", dq_flag)
    df["dq_flag"] = dq_flag
    df["is_valid_telemetry"] = df["dq_flag"] == "HEALTHY"

    # Impute clean physical series on flagged sensor faults using peer/power-curve consistency
    w_clean = df["wind_speed_ms"].to_numpy().copy()
    poa_clean = df["poa_wm2"].to_numpy().copy()
    bad_sensor = np.isin(dq_flag, ["FLATLINE", "SPIKE_OR_BOUNDS", "CROSS_SENSOR_MISMATCH"])

    # Replace corrupted wind/irradiance readings with physical inverse power-curve estimate or nighttime zero
    p_ratio = np.clip(df["active_power_kw"].to_numpy() / np.maximum(df["rated_kw"].to_numpy(), 1.0), 0.0, 1.0)
    w_est = np.where(p_ratio <= 0.01, 2.5, 3.0 + 8.5 * (p_ratio ** (1.0 / 3.0)))
    poa_est = np.where(df["solar_elevation_deg"].to_numpy() <= 0.0, 0.0, 1000.0 * p_ratio / 0.96)

    w_clean = np.where(bad_sensor & is_wind_asset.to_numpy(), np.round(w_est, 3), w_clean)
    poa_clean = np.where(bad_sensor & is_pv_asset.to_numpy(), np.round(poa_est, 2), poa_clean)
    # Also guard PV clean irradiance against transient onset mismatch in either direction
    unphysical_pv_ratio = is_pv_asset.to_numpy() & (
        ((p_ratio > 0.05) & ((poa_clean / 1000.0) < p_ratio))
        | ((poa_clean > 100.0) & ((p_ratio / np.maximum(poa_clean / 1000.0, 1e-6)) < 0.80))
    )
    poa_clean = np.where(unphysical_pv_ratio, np.round(poa_est, 2), poa_clean)

    df["wind_speed_clean_ms"] = w_clean
    df["poa_clean_wm2"] = poa_clean
    df["active_power_clean_kw"] = df["active_power_kw"]
    return df


def evaluate_dq_performance(silver_qc_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Compute per-class precision, recall, F1, and macro-F1 across injected SCADA faults."""
    y_true = silver_qc_df["injected_fault"].to_numpy()
    y_pred = silver_qc_df["dq_flag"].to_numpy()

    rows = []
    f1_scores = []
    for cls in FAULT_CLASSES:
        tp = int(np.sum((y_true == cls) & (y_pred == cls)))
        fp = int(np.sum((y_true != cls) & (y_pred == cls)))
        fn = int(np.sum((y_true == cls) & (y_pred != cls)))
        support = int(np.sum(y_true == cls))
        prec = tp / max(tp + fp, 1)
        rec = tp / max(tp + fn, 1)
        f1 = 2.0 * prec * rec / max(prec + rec, 1e-12)
        f1_scores.append(f1)
        rows.append(
            {
                "fault_class": cls,
                "support_intervals": support,
                "true_positives": tp,
                "false_positives": fp,
                "false_negatives": fn,
                "precision": round(prec, 4),
                "recall": round(rec, 4),
                "f1_score": round(f1, 4),
            }
        )

    macro_f1 = round(float(np.mean(f1_scores)), 3)
    summary_df = pd.DataFrame(rows)

    # Extract 1-minute sample traces for Figure 3 (dq_fault_detection.png) & interactive dashboard
    wtg1 = silver_qc_df[silver_qc_df["asset_id"] == "WTG-01"].iloc[580:720].copy()
    pv1 = silver_qc_df[silver_qc_df["asset_id"] == "PV-INV-01"].iloc[740:840].copy()
    cnt1 = silver_qc_df[silver_qc_df["asset_id"] == "WTG-01"].iloc[1470:1540].copy()

    details = {
        "macro_f1": macro_f1,
        "total_evaluated_intervals": int(len(silver_qc_df)),
        "total_injected_fault_intervals": int(np.sum(y_true != "HEALTHY")),
        "class_metrics": {r["fault_class"]: r for r in rows},
        "sample_flatline_wtg01": {
            "timestamps": [ts.strftime("%Y-%m-%d %H:%M") for ts in wtg1["timestamp_utc"]],
            "wind_raw_ms": [round(float(x), 2) for x in wtg1["wind_speed_ms"]],
            "wind_true_ms": [round(float(x), 2) for x in wtg1["wind_speed_true_ms"]],
            "wind_clean_ms": [round(float(x), 2) for x in wtg1["wind_speed_clean_ms"]],
            "dq_flag": list(wtg1["dq_flag"]),
        },
        "sample_spike_pv01": {
            "timestamps": [ts.strftime("%Y-%m-%d %H:%M") for ts in pv1["timestamp_utc"]],
            "poa_raw_wm2": [round(float(x), 1) for x in pv1["poa_wm2"]],
            "poa_true_wm2": [round(float(x), 1) for x in pv1["poa_true_wm2"]],
            "poa_clean_wm2": [round(float(x), 1) for x in pv1["poa_clean_wm2"]],
            "dq_flag": list(pv1["dq_flag"]),
        },
        "sample_counter_wtg01": {
            "timestamps": [ts.strftime("%Y-%m-%d %H:%M") for ts in cnt1["timestamp_utc"]],
            "raw_delta_kwh": [round(float(x), 3) for x in cnt1["raw_delta_kwh"]],
            "reconciled_delta_kwh": [round(float(x), 3) for x in cnt1["delta_kwh_reconciled"]],
            "kwh_total": [round(float(x), 1) for x in cnt1["kwh_total"]],
        },
    }
    return summary_df, details
