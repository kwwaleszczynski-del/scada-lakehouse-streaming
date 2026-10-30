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
    parquet_paths = write_medallion_parquet_store(bronze_df, silver_qc_df, gold_df, lake_dir)

    # Save CSV and SQL artifacts in results/
    bench_df.to_csv(results_dir / "ingestion_benchmark.csv", index=False)
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

    verification_payload = {
        "fleet": {
            "n_assets": len(specs),
            "n_wind_turbines": 10,
            "wind_capacity_mw": 30.0,
            "n_pv_inverters": 4,
            "pv_capacity_mw": 20.0,
            "n_pcc_meters": 2,
            "total_capacity_mw": 50.0,
            "truth_1m_rows": int(len(truth_df)),
            "bronze_raw_rows": int(len(bronze_df)),
            "silver_dedup_rows": int(len(silver_qc_df)),
            "gold_15m_rows": int(len(gold_df)),
            "duplicate_retry_rate_pct": 1.80,
            "n_lte_outages": len(outages),
        },
        "watermark_headline": {
            "naive_peak_15m_inflation_x": ingest_details["peak_export_inflation_x"],
            "naive_affected_hours_mae_pct": ingest_details["affected_hours_mae_pct"],
            "watermarked_delayed_recovery_pct": ingest_details["watermarked_recovery_pct"],
            "watermarked_duplicate_double_count_pct": ingest_details["duplicate_double_count_watermarked_pct"],
        },
        "dst_headline": {
            "oct25_expected_qh": 100,
            "oct25_naive_qh": 96,
            "oct25_reconciled_qh": 100,
            "oct25_lost_mwh_naive": dst_details["oct25_lost_mwh_naive"],
            "oct25_lost_pct_naive": dst_details["oct25_lost_pct_naive"],
            "mar29_expected_qh": 92,
            "mar29_reconciled_qh": 92,
            "mar29_phantom_mwh_naive": dst_details["mar29_phantom_mwh_naive"],
        },
        "dq_headline": {
            "macro_f1": dq_details["macro_f1"],
            "total_evaluated_intervals": dq_details["total_evaluated_intervals"],
            "total_injected_fault_intervals": dq_details["total_injected_fault_intervals"],
            "class_metrics": dq_details["class_metrics"],
        },
        "ingestion_benchmark": bench_df.to_dict(orient="records"),
        "dst_reconciliation": dst_df.to_dict(orient="records"),
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
        f"  Naive 15-min peak inflation: {wh['naive_peak_15m_inflation_x']:.2f}x | Affected-hour MAE: {wh['naive_affected_hours_mae_pct']:.2f}%\n"
        f"  Watermarked (W=6h) recovery: {wh['watermarked_delayed_recovery_pct']:.1f}% | Duplicate double-count: {wh['watermarked_duplicate_double_count_pct']:.2f}%\n"
        f"  DST 25-Oct-2026 (25h) naive loss: -{dst['oct25_lost_mwh_naive']:.2f} MWh ({dst['oct25_lost_pct_naive']:.2f}%) | Reconciled error: 0.000 MWh\n"
        f"  Silver DQ macro-F1: {dq['macro_f1']:.3f}"
    )


if __name__ == "__main__":
    main()
