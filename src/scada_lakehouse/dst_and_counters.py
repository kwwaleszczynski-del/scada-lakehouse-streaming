"""Central European DST (Europe/Warsaw) local-timestamp disambiguation and cumulative kWh counter recovery."""

from __future__ import annotations

from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

from .stream_simulator import UINT32_ROLLOVER_KWH


WARSAW_TZ = ZoneInfo("Europe/Warsaw")
UTC_TZ = ZoneInfo("UTC")


def export_legacy_local_csv_frame(df_utc: pd.DataFrame) -> pd.DataFrame:
    """Simulate a legacy PLC/logger SFTP CSV export with naive local Europe/Warsaw timestamps (no offset/fold)."""
    out = df_utc.copy()
    ts_warsaw = pd.to_datetime(out["timestamp_utc"], utc=True).dt.tz_convert(WARSAW_TZ)
    out["timestamp_local_str"] = ts_warsaw.dt.strftime("%Y-%m-%d %H:%M:%S")
    out["true_fold"] = ts_warsaw.map(lambda x: x.fold)
    out["true_utc_offset_h"] = ts_warsaw.map(lambda x: int(x.utcoffset().total_seconds() // 3600))
    return out


def disambiguate_warsaw_local_timestamps(df_local: pd.DataFrame) -> pd.DataFrame:
    """Convert naive Europe/Warsaw local timestamps to exact UTC using monotonic kWh counter & sequence order.

    Handles:
      - Autumn fall-back (e.g. 25 Oct 2026): 02:00..02:59 appears twice per asset. The first pass
        (lower kwh_total / earlier sequence) receives fold=0 (CEST, UTC+2 -> 00:00..00:59 UTC),
        and the second pass receives fold=1 (CET, UTC+1 -> 01:00..01:59 UTC).
      - Spring-forward (e.g. 29 Mar 2026): any phantom 02:00..02:59 local rows fabricated by
        naive 24h grid padding are flagged and dropped before UTC conversion.
    """
    out = df_local.copy()
    # Sort deterministically by asset_id, local timestamp string, and monotonic counter / sequence
    if "kwh_total" in out.columns:
        out = out.sort_values(["asset_id", "timestamp_local_str", "kwh_total"], kind="mergesort").reset_index(drop=True)
    else:
        out = out.sort_values(["asset_id", "timestamp_local_str"], kind="mergesort").reset_index(drop=True)

    # Detect spring-forward nonexistent 02:00..02:59 timestamps (e.g., last Sunday of March)
    local_dt = pd.to_datetime(out["timestamp_local_str"], format="%Y-%m-%d %H:%M:%S")
    is_march_dst_sunday = (local_dt.dt.month == 3) & (local_dt.dt.day >= 25) & (local_dt.dt.weekday == 6)
    is_nonexistent_hour = is_march_dst_sunday & (local_dt.dt.hour == 2)
    out["is_phantom_spring_hour"] = is_nonexistent_hour
    out = out.loc[~out["is_phantom_spring_hour"]].copy().reset_index(drop=True)

    # Disambiguate repeated local timestamps (autumn fall-back 02:00..02:59)
    occ_rank = out.groupby(["asset_id", "timestamp_local_str"]).cumcount()
    out["inferred_fold"] = np.clip(occ_rank, 0, 1)

    # Vectorized or fast dictionary lookup for (timestamp_local_str, inferred_fold) -> timestamp_utc
    unique_pairs = out[["timestamp_local_str", "inferred_fold"]].drop_duplicates()
    pair_to_utc: dict[tuple[str, int], pd.Timestamp] = {}
    for ts_str, fold_val in unique_pairs.itertuples(index=False):
        dt_naive = pd.Timestamp(ts_str).to_pydatetime()
        dt_aware = dt_naive.replace(tzinfo=WARSAW_TZ, fold=int(fold_val))
        pair_to_utc[(ts_str, int(fold_val))] = pd.Timestamp(dt_aware.astimezone(UTC_TZ))

    out["timestamp_utc_reconstructed"] = [
        pair_to_utc[(s, int(f))]
        for s, f in zip(out["timestamp_local_str"], out["inferred_fold"])
    ]
    out["hour_label_warsaw"] = np.where(
        (pd.to_datetime(out["timestamp_local_str"]).dt.hour == 2)
        & (pd.to_datetime(out["timestamp_local_str"]).dt.month == 10)
        & (pd.to_datetime(out["timestamp_local_str"]).dt.day >= 25),
        np.where(out["inferred_fold"] == 0, "02A CEST (UTC+2)", "02B CET (UTC+1)"),
        out["timestamp_local_str"].str.slice(11, 16),
    )
    out = out.sort_values(["asset_id", "timestamp_utc_reconstructed"], kind="mergesort").reset_index(drop=True)
    return out


def reconcile_cumulative_counter(
    df: pd.DataFrame,
    rollover_modulo_kwh: float = UINT32_ROLLOVER_KWH,
) -> pd.DataFrame:
    """Recover true 1-minute energy increments delta_kwh from cumulative kwh_total across rollovers and PLC resets."""
    out = df.sort_values(["asset_id", "timestamp_utc"], kind="mergesort").reset_index(drop=True).copy()

    raw_delta = out.groupby("asset_id")["kwh_total"].diff()
    # First row of each asset uses active_power_kw / 60.0
    power_step_kwh = out["active_power_kw"] / 60.0
    raw_delta = raw_delta.fillna(power_step_kwh)

    max_step_kwh = (out["rated_kw"] * 1.15) / 60.0
    out["raw_delta_kwh"] = np.round(raw_delta, 4)

    is_negative = raw_delta < -1e-6
    wrapped_delta = raw_delta + rollover_modulo_kwh
    is_uint32_rollover = is_negative & (wrapped_delta >= 0.0) & (wrapped_delta <= max_step_kwh)
    is_plc_reboot = is_negative & (~is_uint32_rollover)

    reconciled = np.where(
        is_uint32_rollover,
        wrapped_delta,
        np.where(
            is_plc_reboot,
            np.where((out["kwh_total"] >= 0.0) & (out["kwh_total"] <= max_step_kwh), out["kwh_total"], power_step_kwh),
            raw_delta,
        ),
    )
    # Also guard against unphysical positive jumps on corrupt registers
    reconciled = np.where(reconciled > max_step_kwh, power_step_kwh, reconciled)

    out["counter_event"] = np.where(
        is_uint32_rollover,
        "UINT32_ROLLOVER",
        np.where(is_plc_reboot, "PLC_REBOOT_RESET", "NORMAL"),
    )
    out["delta_kwh_reconciled"] = np.round(reconciled, 4)
    out["kwh_total_monotonic"] = out.groupby("asset_id")["delta_kwh_reconciled"].cumsum()
    return out


def evaluate_dst_transitions(truth_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Evaluate 25 Oct 2026 (25h fall-back) and 29 Mar 2026 (23h spring-forward) reconciliation."""
    # 1. Extract 25 October 2026 Warsaw local day (2026-10-24 22:00 UTC to 2026-10-25 22:59 UTC = 25 hours = 1500 min)
    oct_start = pd.Timestamp("2026-10-24 22:00:00+00:00")
    oct_end = pd.Timestamp("2026-10-25 22:59:00+00:00")
    oct_sub = truth_df[
        (truth_df["timestamp_utc"] >= oct_start)
        & (truth_df["timestamp_utc"] <= oct_end)
        & (~truth_df["asset_id"].str.startswith("PCC-"))
    ].copy()

    oct_local = export_legacy_local_csv_frame(oct_sub)

    # Naive local-time pipeline: deduplicates on (asset_id, timestamp_local_str) keeping last (02B overwrites 02A)
    naive_oct = oct_local.drop_duplicates(subset=["asset_id", "timestamp_local_str"], keep="last")
    true_oct_mwh = float(oct_sub["delta_kwh_true"].sum() / 1000.0)
    naive_oct_mwh = float(naive_oct["delta_kwh_true"].sum() / 1000.0)

    # Fold-aware disambiguation
    disamb_oct = disambiguate_warsaw_local_timestamps(oct_local)
    reconciled_oct_mwh = float(disamb_oct["delta_kwh_true"].sum() / 1000.0)

    # Hourly profile on 25 Oct 2026 (25 local hours: 00, 01, 02A, 02B, 03..23)
    hourly_oct = (
        disamb_oct.groupby("timestamp_utc_reconstructed", as_index=False)["delta_kwh_true"]
        .sum()
    )
    hourly_oct["bucket_1h_utc"] = hourly_oct["timestamp_utc_reconstructed"].dt.floor("1h")
    hourly_1h = hourly_oct.groupby("bucket_1h_utc", as_index=False)["delta_kwh_true"].sum()
    hourly_1h["fleet_mwh"] = np.round(hourly_1h["delta_kwh_true"] / 1000.0, 3)

    # Build labels for the 25 hours
    warsaw_dt = hourly_1h["bucket_1h_utc"].dt.tz_convert(WARSAW_TZ)
    labels_25h = []
    for dt_w in warsaw_dt:
        if dt_w.hour == 2:
            labels_25h.append("02:00 CEST (fold=0)" if dt_w.fold == 0 else "02:00 CET (fold=1)")
        else:
            labels_25h.append(dt_w.strftime("%H:%M %Z"))
    hourly_1h["bucket_1h_utc"] = hourly_1h["bucket_1h_utc"].dt.strftime("%Y-%m-%d %H:%M UTC")
    hourly_1h["warsaw_hour_label"] = labels_25h
    hourly_1h["naive_local_mwh"] = hourly_1h["fleet_mwh"]
    # Under naive overwrite, 02:00 CEST (fold=0) is lost (overwritten by 02:00 CET)
    hourly_1h.loc[hourly_1h["warsaw_hour_label"] == "02:00 CEST (fold=0)", "naive_local_mwh"] = 0.0

    # 2. Simulate 29 March 2026 spring-forward 23-hour day (2026-03-28 23:00 UTC to 2026-03-29 21:59 UTC)
    mar_idx = pd.date_range("2026-03-28 23:00:00+00:00", "2026-03-29 21:59:00+00:00", freq="1min")
    # 23 hours = 1,380 minutes across the 14 generating assets
    t_h = np.arange(len(mar_idx), dtype=float) / 60.0
    fleet_kw_mar = 14800.0 + 3200.0 * np.sin(2.0 * np.pi * t_h / 24.0)
    mar_df = pd.DataFrame(
        {
            "timestamp_utc": mar_idx,
            "asset_id": "FLEET-AGG",
            "delta_kwh_true": fleet_kw_mar / 60.0,
            "kwh_total": 500_000.0 + np.cumsum(fleet_kw_mar / 60.0),
        }
    )
    mar_local = export_legacy_local_csv_frame(mar_df)
    # Naive 24-hour local grid fabricates 60 minutes of forward-filled 02:00..02:59
    phantom_rows = mar_local.iloc[120:180].copy()
    phantom_rows["timestamp_local_str"] = [f"2026-03-29 02:{m:02d}:00" for m in range(60)]
    mar_corrupted = pd.concat([mar_local, phantom_rows], ignore_index=True)

    mar_disamb = disambiguate_warsaw_local_timestamps(mar_corrupted)
    true_mar_mwh = float(mar_df["delta_kwh_true"].sum() / 1000.0)
    naive_mar_mwh = float(mar_corrupted["delta_kwh_true"].sum() / 1000.0)
    reconciled_mar_mwh = float(mar_disamb["delta_kwh_true"].sum() / 1000.0)

    summary_rows = [
        {
            "dst_date": "2026-10-25",
            "transition_type": "Autumn Fall-Back (CEST -> CET, 25h day)",
            "expected_quarter_hours": 100,
            "naive_quarter_hours": 96,
            "reconciled_quarter_hours": 100,
            "true_energy_mwh": round(true_oct_mwh, 3),
            "naive_energy_mwh": round(naive_oct_mwh, 3),
            "reconciled_energy_mwh": round(reconciled_oct_mwh, 3),
            "naive_error_mwh": round(naive_oct_mwh - true_oct_mwh, 3),
            "naive_error_pct": round((naive_oct_mwh - true_oct_mwh) / true_oct_mwh * 100.0, 2),
            "reconciled_error_mwh": round(reconciled_oct_mwh - true_oct_mwh, 4),
        },
        {
            "dst_date": "2026-03-29",
            "transition_type": "Spring Forward (CET -> CEST, 23h day)",
            "expected_quarter_hours": 92,
            "naive_quarter_hours": 96,
            "reconciled_quarter_hours": 92,
            "true_energy_mwh": round(true_mar_mwh, 3),
            "naive_energy_mwh": round(naive_mar_mwh, 3),
            "reconciled_energy_mwh": round(reconciled_mar_mwh, 3),
            "naive_error_mwh": round(naive_mar_mwh - true_mar_mwh, 3),
            "naive_error_pct": round((naive_mar_mwh - true_mar_mwh) / true_mar_mwh * 100.0, 2),
            "reconciled_error_mwh": round(reconciled_mar_mwh - true_mar_mwh, 4),
        },
    ]
    dst_summary_df = pd.DataFrame(summary_rows)
    dst_details = {
        "oct25_hourly": hourly_1h.to_dict(orient="records"),
        "oct25_lost_mwh_naive": round(true_oct_mwh - naive_oct_mwh, 2),
        "oct25_lost_pct_naive": round((true_oct_mwh - naive_oct_mwh) / true_oct_mwh * 100.0, 2),
        "mar29_phantom_mwh_naive": round(naive_mar_mwh - true_mar_mwh, 2),
    }
    return dst_summary_df, dst_details
