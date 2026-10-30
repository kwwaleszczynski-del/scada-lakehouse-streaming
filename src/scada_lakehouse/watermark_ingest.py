"""Event-time watermarking and idempotent merge vs naive ingestion-time windowing."""

from __future__ import annotations

import numpy as np
import pandas as pd


KEY_1M = ["asset_id", "timestamp_utc"]
KEY_15M = ["asset_id", "bucket_15m"]


def ingest_naive_processing_time(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate Bronze telemetry by 15-minute processing/ingestion-time buckets without deduplication.

    Demonstrates the two classic streaming failure modes when grouping by broker arrival time
    (ingested_at) instead of payload event time (timestamp_utc):
      1. Outage starvation followed by reconnect burst spike: buffered 1-minute packets arriving
         during a single 15-minute reconnect window sum together with live traffic, inflating
         apparent 15-minute settlement energy and average power of that bucket.
      2. QoS-1 duplicate retry double-counting (every retried packet is summed again).
    """
    df = bronze_df.copy()
    df["bucket_15m"] = df["ingested_at"].dt.floor("15min")

    agg = (
        df.groupby(["asset_id", "bucket_15m"], as_index=False)
        .agg(
            naive_record_count=("delta_kwh_true", "count"),
            naive_energy_kwh=("delta_kwh_true", "sum"),
            rated_kw=("rated_kw", "first"),
        )
    )
    agg["naive_power_kw"] = agg["naive_energy_kwh"] * 4.0
    return agg


def ingest_event_time_watermarked(
    bronze_df: pd.DataFrame,
    watermark_hours: float = 6.0,
    enable_late_backfill_merge: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplicate on (asset_id, timestamp_utc) and apply event-time watermark horizon W [hours].

    Packets with event-time lag (ingested_at - timestamp_utc) <= W are admitted directly in the
    streaming watermark window. Packets exceeding W are routed to the late-arrival backfill
    partition and (when enable_late_backfill_merge=True) upserted via idempotent ON CONFLICT merge.
    """
    df = bronze_df.copy()
    df["arrival_lag_min"] = (df["ingested_at"] - df["timestamp_utc"]).dt.total_seconds() / 60.0
    horizon_min = watermark_hours * 60.0

    df["within_watermark"] = df["arrival_lag_min"] <= horizon_min
    if not enable_late_backfill_merge:
        df = df.loc[df["within_watermark"]].copy()

    # Idempotent upsert on primary key (asset_id, timestamp_utc) keeping latest arrival
    silver_1m = (
        df.sort_values("ingested_at", kind="mergesort")
        .drop_duplicates(subset=["asset_id", "timestamp_utc"], keep="last")
        .sort_values(["asset_id", "timestamp_utc"], kind="mergesort")
        .reset_index(drop=True)
    )

    silver_1m["bucket_15m"] = silver_1m["timestamp_utc"].dt.floor("15min")
    gold_15m = (
        silver_1m.groupby(["asset_id", "bucket_15m"], as_index=False)
        .agg(
            sample_count_1m=("delta_kwh_true", "count"),
            watermarked_energy_kwh=("delta_kwh_true", "sum"),
            rated_kw=("rated_kw", "first"),
        )
    )
    gold_15m["watermarked_power_kw"] = gold_15m["watermarked_energy_kwh"] * 4.0
    gold_15m["completeness_pct"] = np.round(gold_15m["sample_count_1m"] / 15.0 * 100.0, 2)
    return silver_1m, gold_15m


def _score_mode(
    counted: pd.DataFrame,
    placement_col: str,
    comp_df: pd.DataFrame,
    truth_df: pd.DataFrame,
    affected_mask: np.ndarray,
    burst_keys: pd.DataFrame,
) -> dict:
    """Score one ingestion mode from the 1-minute records it sums and its 15-minute estimate.

    counted: every 1-minute record the mode sums (duplicates included if the mode keeps them).
    placement_col: timestamp the mode buckets by (ingested_at for naive, timestamp_utc for event time).
    comp_df: ground-truth 15-minute table with the mode's est_energy_kwh and est_power_kw.
    """
    n_delayed = int(truth_df["is_outage_backfill"].sum())
    true_kwh = float(truth_df["delta_kwh_true"].sum())

    # A true asset-minute counts as complete when it is summed into its own event-time quarter-hour
    in_true_bucket = counted[placement_col].dt.floor("15min") == counted["timestamp_utc"].dt.floor("15min")
    placed = counted.loc[in_true_bucket].drop_duplicates(subset=KEY_1M)
    unique_kwh = float(counted.drop_duplicates(subset=KEY_1M)["delta_kwh_true"].sum())
    dup_kwh = float(counted["delta_kwh_true"].sum()) - unique_kwh

    aff = comp_df.loc[affected_mask]
    burst = pd.merge(burst_keys, comp_df, on=KEY_15M, how="left")
    burst = burst.loc[burst["true_energy_kwh"] > 0.0]
    return {
        "completeness_pct": len(placed) / len(truth_df) * 100.0,
        "delayed_intervals_recovered_pct": int(placed["is_outage_backfill"].sum()) / max(n_delayed, 1) * 100.0,
        "duplicate_energy_pct": dup_kwh / true_kwh * 100.0,
        "reconnect_bucket_energy_ratio_median_x": float((burst["est_energy_kwh"] / burst["true_energy_kwh"]).median()),
        "affected_buckets_energy_nmae_pct": float(
            (aff["est_energy_kwh"] - aff["true_energy_kwh"]).abs().sum() / aff["true_energy_kwh"].sum() * 100.0
        ),
        "asset_15m_power_rmse_kw": float(np.sqrt(np.mean((comp_df["est_power_kw"] - comp_df["true_power_kw"]) ** 2))),
        "total_energy_mwh": float(comp_df["est_energy_kwh"].sum()) / 1000.0,
    }


def benchmark_ingestion_modes(
    truth_df: pd.DataFrame,
    bronze_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """Compare Naive Ingestion-Time, Fixed Watermark Drop, and Event-Time Watermark + Idempotent Merge.

    Every metric is computed from the simulated Bronze stream against the ground-truth 1-minute table.
    """
    # Ground truth 15-minute settlement table
    t_df = truth_df.copy()
    t_df["bucket_15m"] = t_df["timestamp_utc"].dt.floor("15min")
    truth_15m = (
        t_df.groupby(KEY_15M, as_index=False)
        .agg(
            true_energy_kwh=("delta_kwh_true", "sum"),
            true_power_kw=("active_power_true_kw", "mean"),
            has_outage=("is_outage_backfill", "any"),
            rated_kw=("rated_kw", "first"),
        )
    )

    naive_15m = ingest_naive_processing_time(bronze_df)
    merged_naive = pd.merge(
        truth_15m,
        naive_15m[["asset_id", "bucket_15m", "naive_record_count", "naive_energy_kwh", "naive_power_kw"]],
        on=KEY_15M,
        how="left",
    ).fillna({"naive_record_count": 0, "naive_energy_kwh": 0.0, "naive_power_kw": 0.0})

    # Buckets touched by an LTE outage: the silent outage buckets plus the buckets its backlog lands in
    delayed = truth_df.loc[truth_df["is_outage_backfill"]].copy()
    delayed["bucket_15m"] = delayed["ingested_at"].dt.floor("15min")
    landing_keys = pd.MultiIndex.from_frame(delayed[KEY_15M].drop_duplicates())
    affected_mask = truth_15m["has_outage"].to_numpy() | pd.MultiIndex.from_frame(truth_15m[KEY_15M]).isin(landing_keys)

    # Main reconnect bucket per outage: the quarter-hour that receives most of the buffered backlog
    bursts = (
        delayed.groupby(["outage_id", "asset_id"])
        .agg(
            outage_minutes=("timestamp_utc", "count"),
            outage_start_utc=("timestamp_utc", "min"),
            bucket_15m=("bucket_15m", lambda s: s.value_counts().idxmax()),
        )
        .reset_index()
    )
    burst_keys = bursts[KEY_15M]
    max_lag_h = float((delayed["ingested_at"] - delayed["timestamp_utc"]).max().total_seconds() / 3600.0)

    # Sweep watermark horizons W = [0.0 (no backfill), 0.5, 1.0, 2.0, 4.0, 6.0] hours
    horizons = [
        ("Naive Ingestion-Time (no dedup)", 0.0, False, True),
        ("Event-Time Watermark W = 0.5 h (drop late)", 0.5, False, False),
        ("Event-Time Watermark W = 1.0 h (drop late)", 1.0, False, False),
        ("Event-Time Watermark W = 2.0 h (drop late)", 2.0, False, False),
        ("Event-Time Watermark W = 4.0 h (drop late)", 4.0, False, False),
        ("Event-Time Watermark W = 6.0 h + Idempotent Upsert", 6.0, True, False),
    ]

    total_true_kwh = float(truth_15m["true_energy_kwh"].sum())

    rows = []
    est_by_mode: list[pd.DataFrame] = []
    for mode_name, w_h, allow_upsert, is_naive in horizons:
        if is_naive:
            counted = bronze_df
            placement_col = "ingested_at"
            comp_df = merged_naive.rename(columns={"naive_energy_kwh": "est_energy_kwh", "naive_power_kw": "est_power_kw"})
        else:
            s_1m, g_15m = ingest_event_time_watermarked(
                bronze_df,
                watermark_hours=w_h,
                enable_late_backfill_merge=allow_upsert,
            )
            counted = s_1m
            placement_col = "timestamp_utc"
            comp_df = pd.merge(
                truth_15m,
                g_15m[["asset_id", "bucket_15m", "watermarked_energy_kwh", "watermarked_power_kw"]],
                on=KEY_15M,
                how="left",
            ).fillna({"watermarked_energy_kwh": 0.0, "watermarked_power_kw": 0.0})
            comp_df = comp_df.rename(
                columns={"watermarked_energy_kwh": "est_energy_kwh", "watermarked_power_kw": "est_power_kw"}
            )

        scores = _score_mode(counted, placement_col, comp_df, truth_df, affected_mask, burst_keys)
        rows.append(
            {
                "mode": mode_name,
                "watermark_horizon_h": w_h,
                **{k: round(v, 2) for k, v in scores.items()},
                "true_energy_mwh": round(total_true_kwh / 1000.0, 2),
            }
        )
        est_by_mode.append(comp_df)

    bench_df = pd.DataFrame(rows)

    # Per-outage reconnect burst table (naive ingestion-time bucketing)
    bursts = pd.merge(bursts, merged_naive, on=KEY_15M, how="left")
    bursts["naive_to_true_energy_x"] = bursts["naive_energy_kwh"] / bursts["true_energy_kwh"].where(
        bursts["true_energy_kwh"] > 0.0
    )
    bursts = bursts.sort_values("outage_id").reset_index(drop=True)
    burst_table = pd.DataFrame(
        {
            "outage_id": bursts["outage_id"],
            "asset_id": bursts["asset_id"],
            "outage_minutes": bursts["outage_minutes"],
            "outage_start_utc": bursts["outage_start_utc"].dt.strftime("%Y-%m-%d %H:%M"),
            "reconnect_bucket_utc": bursts["bucket_15m"].dt.strftime("%Y-%m-%d %H:%M"),
            "naive_packets_in_bucket": bursts["naive_record_count"].astype(int),
            "true_energy_kwh": bursts["true_energy_kwh"].round(1),
            "naive_energy_kwh": bursts["naive_energy_kwh"].round(1),
            "naive_to_true_energy_x": bursts["naive_to_true_energy_x"].round(2),
        }
    )

    # Extract detailed 15-minute trace for featured LTE outage scenarios for plots & interactive dashboard
    scenario_traces: dict[str, dict] = {}
    featured_outages = ["OUT-06", "OUT-01", "OUT-10", "OUT-11"]
    pad = pd.Timedelta(hours=3)
    for oid in featured_outages:
        b = bursts.loc[bursts["outage_id"] == oid].iloc[0]
        aid = b["asset_id"]
        t0 = b["outage_start_utc"].floor("15min") - pad
        t1 = b["outage_start_utc"] + pd.Timedelta(minutes=int(b["outage_minutes"])) + pad
        in_win = (
            (merged_naive["asset_id"] == aid)
            & (merged_naive["bucket_15m"] >= t0)
            & (merged_naive["bucket_15m"] <= t1)
        ).to_numpy()
        sub = merged_naive.loc[in_win]
        rated_mw = float(sub["rated_kw"].iloc[0]) / 1000.0

        scenario_traces[oid] = {
            "outage_id": oid,
            "asset_id": aid,
            "title": f"{oid}: {aid} ({rated_mw:g} MW), {int(b['outage_minutes'])} min LTE outage",
            "outage_minutes": int(b["outage_minutes"]),
            "reconnect_ratio_x": round(float(b["naive_to_true_energy_x"]), 2),
            "timestamps": [ts.strftime("%Y-%m-%d %H:%M") for ts in sub["bucket_15m"]],
            "is_outage_bucket": [bool(x) for x in sub["has_outage"]],
            "true_power_kw": [round(float(x), 1) for x in sub["true_power_kw"]],
            "naive_power_kw": [round(float(x), 1) for x in sub["naive_power_kw"]],
            "watermarked_power_kw": [round(float(x), 1) for x in est_by_mode[-1].loc[in_win, "est_power_kw"]],
            "mode_power_kw": [[round(float(x), 1) for x in est.loc[in_win, "est_power_kw"]] for est in est_by_mode],
            "naive_record_count": [int(x) for x in sub["naive_record_count"]],
        }

    ratios = burst_table["naive_to_true_energy_x"].dropna()
    details = {
        "n_outages": int(len(burst_table)),
        "n_delayed_intervals": int(len(delayed)),
        "max_arrival_lag_h": round(max_lag_h, 2),
        "naive_reconnect_ratio_median_x": round(float(ratios.median()), 2),
        "naive_reconnect_ratio_min_x": round(float(ratios.min()), 2),
        "naive_reconnect_ratio_max_x": round(float(ratios.max()), 2),
        "reconnect_bursts": burst_table,
        "scenarios": scenario_traces,
    }
    return bench_df, details
