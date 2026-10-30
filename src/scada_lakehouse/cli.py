"""End-to-end runner and CLI entry point for scada-lakehouse-streaming."""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

from .stream_simulator import build_fleet_specs, simulate_fleet_stream
from .watermark_ingest import benchmark_ingestion_modes, ingest_event_time_watermarked
from .dst_and_counters import evaluate_dst_transitions
from .dq_engine import evaluate_dq_performance, run_silver_dq_engine
from .medallion_store import (
    build_gold_15m_table,
    generate_timescaledb_ddl,
    write_medallion_parquet_store,
)
from .plots import generate_all_figures


REPO_ROOT = Path(__file__).resolve().parents[2]


def run_pipeline(repo_root: Path = REPO_ROOT) -> dict:
    """Execute the full Bronze -> Silver -> Gold simulation, verification, plotting, and dashboard export."""
    results_dir = repo_root / "results"
    docs_dir = repo_root / "docs"
    img_dir = docs_dir / "img"
    lake_dir = repo_root / "data_lake"
    results_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)

    specs = build_fleet_specs()
    truth_df, bronze_df, outages = simulate_fleet_stream(seed=42)

    bench_df, ingest_details = benchmark_ingestion_modes(truth_df, bronze_df)
    dst_df, dst_details = evaluate_dst_transitions(truth_df)

    silver_1m, _ = ingest_event_time_watermarked(
        bronze_df, watermark_hours=6.0, enable_late_backfill_merge=True
    )
    silver_qc_df = run_silver_dq_engine(silver_1m)
    dq_df, dq_details = evaluate_dq_performance(silver_qc_df)

    gold_df = build_gold_15m_table(silver_qc_df)
    parquet_paths = {
        k: Path(v).relative_to(repo_root).as_posix()
        for k, v in write_medallion_parquet_store(bronze_df, silver_qc_df, gold_df, lake_dir).items()
    }

    # Save CSV and SQL artifacts in results/
    bench_df.to_csv(results_dir / "ingestion_benchmark.csv", index=False)
    ingest_details["reconnect_bursts"].to_csv(results_dir / "reconnect_bursts.csv", index=False)
    dq_df.to_csv(results_dir / "dq_confusion_summary.csv", index=False)
    dst_df.to_csv(results_dir / "dst_reconciliation.csv", index=False)

    # Representative 200-row sample of Gold 15-minute settlement table across wind, PV, and PCC meters
    gold_sample = (
        gold_df[gold_df["asset_id"].isin(["PCC-MTR-01", "PCC-MTR-02", "WTG-01", "PV-INV-01"])]
        .iloc[80:280]
        .copy()
    )
    gold_sample.to_csv(results_dir / "gold_15min_sample.csv", index=False)

    ddl_sql = generate_timescaledb_ddl()
    (results_dir / "timescaledb_schema.sql").write_text(ddl_sql, encoding="utf-8")

    # Generate 4 Matplotlib PNG charts
    generate_all_figures(
        bench_df=bench_df,
        ingest_details=ingest_details,
        dst_df=dst_df,
        dst_details=dst_details,
        dq_df=dq_df,
        dq_details=dq_details,
        img_dir=img_dir,
    )

    wind_specs = [s for s in specs if s.asset_type == "wind"]
    pv_specs = [s for s in specs if s.asset_type == "pv"]
    naive_row = bench_df.iloc[0]
    upsert_row = bench_df.iloc[-1]
    oct_row = dst_df.loc[dst_df["dst_date"] == "2026-10-25"].iloc[0]
    mar_row = dst_df.loc[dst_df["dst_date"] == "2026-03-29"].iloc[0]
    verification_payload = {
        "fleet": {
            "n_assets": len(specs),
            "n_wind_turbines": len(wind_specs),
            "wind_capacity_mw": sum(s.rated_kw for s in wind_specs) / 1000.0,
            "n_pv_inverters": len(pv_specs),
            "pv_capacity_mw": sum(s.rated_kw for s in pv_specs) / 1000.0,
            "n_pcc_meters": sum(1 for s in specs if s.asset_type.startswith("meter_")),
            "total_capacity_mw": sum(s.rated_kw for s in wind_specs + pv_specs) / 1000.0,
            "truth_1m_rows": int(len(truth_df)),
            "bronze_raw_rows": int(len(bronze_df)),
            "silver_dedup_rows": int(len(silver_qc_df)),
            "gold_15m_rows": int(len(gold_df)),
            "duplicate_retry_packets": int(bronze_df["is_duplicate_retry"].sum()),
            "duplicate_retry_rate_pct": round(float(bronze_df["is_duplicate_retry"].sum()) / len(truth_df) * 100.0, 2),
            "n_lte_outages": len(outages),
            "n_delayed_intervals": ingest_details["n_delayed_intervals"],
            "max_arrival_lag_h": ingest_details["max_arrival_lag_h"],
        },
        "watermark_headline": {
            "naive_reconnect_ratio_median_x": ingest_details["naive_reconnect_ratio_median_x"],
            "naive_reconnect_ratio_min_x": ingest_details["naive_reconnect_ratio_min_x"],
            "naive_reconnect_ratio_max_x": ingest_details["naive_reconnect_ratio_max_x"],
            "naive_affected_buckets_nmae_pct": float(naive_row["affected_buckets_energy_nmae_pct"]),
            "naive_duplicate_energy_pct": float(naive_row["duplicate_energy_pct"]),
            "upsert_delayed_recovery_pct": float(upsert_row["delayed_intervals_recovered_pct"]),
            "upsert_duplicate_energy_pct": float(upsert_row["duplicate_energy_pct"]),
            "upsert_affected_buckets_nmae_pct": float(upsert_row["affected_buckets_energy_nmae_pct"]),
        },
        "dst_headline": {
            "oct25_expected_qh": int(oct_row["expected_quarter_hours"]),
            "oct25_naive_qh": int(oct_row["naive_quarter_hours"]),
            "oct25_reconciled_qh": int(oct_row["reconciled_quarter_hours"]),
            "oct25_utc_timestamps_matched_pct": float(oct_row["utc_timestamps_matched_pct"]),
            "oct25_lost_mwh_naive": dst_details["oct25_lost_mwh_naive"],
            "oct25_lost_pct_naive": dst_details["oct25_lost_pct_naive"],
            "oct25_reconciled_error_mwh": float(oct_row["reconciled_error_mwh"]),
            "mar29_expected_qh": int(mar_row["expected_quarter_hours"]),
            "mar29_naive_qh": int(mar_row["naive_quarter_hours"]),
            "mar29_reconciled_qh": int(mar_row["reconciled_quarter_hours"]),
            "mar29_phantom_mwh_naive": dst_details["mar29_phantom_mwh_naive"],
        },
        "dq_headline": {
            "macro_f1": dq_details["macro_f1"],
            "macro_precision": dq_details["macro_precision"],
            "macro_recall": dq_details["macro_recall"],
            "total_evaluated_intervals": dq_details["total_evaluated_intervals"],
            "total_injected_fault_intervals": dq_details["total_injected_fault_intervals"],
            "class_metrics": dq_details["class_metrics"],
        },
        "ingestion_benchmark": bench_df.to_dict(orient="records"),
        "dst_reconciliation": dst_df.to_dict(orient="records"),
        "reconnect_bursts": ingest_details["reconnect_bursts"].to_dict(orient="records"),
        "parquet_files": parquet_paths,
    }
    (results_dir / "verification.json").write_text(
        json.dumps(verification_payload, indent=2, default=str), encoding="utf-8"
    )

    # Build docs/data.js for interactive GitHub Pages dashboard
    dashboard_data = {
        "headline": verification_payload,
        "scenarios": ingest_details["scenarios"],
        "oct25_hourly": dst_details["oct25_hourly"],
        "dq_samples": {
            "flatline": dq_details["sample_flatline_wtg01"],
            "spike": dq_details["sample_spike_pv01"],
            "counter": dq_details["sample_counter_wtg01"],
        },
        "outage_schedule": [
            {
                "outage_id": o.outage_id,
                "asset_id": o.asset_id,
                "router_id": o.router_id,
                "start_utc": o.start_utc.strftime("%Y-%m-%d %H:%M UTC"),
                "duration_min": o.duration_min,
                "flush_duration_min": o.flush_duration_min,
                "scenario_label": o.scenario_label,
            }
            for o in outages
        ],
        "timescaledb_sql": ddl_sql,
    }
    data_js_content = "window.DATA = " + json.dumps(dashboard_data, indent=2, default=str) + ";\n"
    (docs_dir / "data.js").write_text(data_js_content, encoding="utf-8")

    return verification_payload


def main() -> None:
    payload = run_pipeline()
    wh = payload["watermark_headline"]
    dq = payload["dq_headline"]
    dst = payload["dst_headline"]
    print(
        f"scada-lakehouse-streaming pipeline complete:\n"
        f"  Bronze rows: {payload['fleet']['bronze_raw_rows']:,} -> Silver rows: {payload['fleet']['silver_dedup_rows']:,} -> Gold 15m rows: {payload['fleet']['gold_15m_rows']:,}\n"
        f"  Naive reconnect-bucket energy ratio (median over outages): {wh['naive_reconnect_ratio_median_x']:.2f}x | Affected-bucket nMAE: {wh['naive_affected_buckets_nmae_pct']:.2f}%\n"
        f"  Upsert (W=6h) recovery: {wh['upsert_delayed_recovery_pct']:.1f}% | Duplicate energy counted: {wh['upsert_duplicate_energy_pct']:.2f}%\n"
        f"  DST 25-Oct-2026 (25h) naive loss: -{dst['oct25_lost_mwh_naive']:.2f} MWh ({dst['oct25_lost_pct_naive']:.2f}%) | Reconciled error: {dst['oct25_reconciled_error_mwh']:.3f} MWh\n"
        f"  Silver DQ macro-F1: {dq['macro_f1']:.3f}"
    )


if __name__ == "__main__":
    main()
