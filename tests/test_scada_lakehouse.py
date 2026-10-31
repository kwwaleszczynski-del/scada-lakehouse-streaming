"""Unit, invariant and consistency tests for scada-lakehouse-streaming.

The tests check properties of the simulated pipeline (conservation, monotonicity, independent
recomputation of each reported metric) rather than fixed target values.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from scada_lakehouse.cli import run_pipeline
from scada_lakehouse.stream_simulator import (
    UINT32_ROLLOVER_KWH,
    build_fleet_specs,
    build_outage_schedule,
    simulate_fleet_stream,
    solar_elevation_deg,
)
from scada_lakehouse.watermark_ingest import (
    benchmark_ingestion_modes,
    ingest_event_time_watermarked,
    ingest_naive_processing_time,
)
from scada_lakehouse.dst_and_counters import (
    disambiguate_warsaw_local_timestamps,
    evaluate_dst_transitions,
    export_legacy_local_csv_frame,
    reconcile_cumulative_counter,
)
from scada_lakehouse.dq_engine import (
    FAULT_CLASSES,
    evaluate_dq_performance,
    run_silver_dq_engine,
)
from scada_lakehouse.medallion_store import (
    build_gold_15m_table,
    generate_timescaledb_ddl,
    write_medallion_parquet_store,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def sim_bundle():
    truth_df, bronze_df, outages = simulate_fleet_stream(seed=42)
    silver_1m, gold_wm_15m = ingest_event_time_watermarked(
        bronze_df, watermark_hours=6.0, enable_late_backfill_merge=True
    )
    silver_qc_df = run_silver_dq_engine(silver_1m)
    dq_df, dq_details = evaluate_dq_performance(silver_qc_df)
    gold_df = build_gold_15m_table(silver_qc_df)
    bench_df, ingest_details = benchmark_ingestion_modes(truth_df, bronze_df)
    dst_df, dst_details = evaluate_dst_transitions(truth_df)
    return {
        "truth_df": truth_df,
        "bronze_df": bronze_df,
        "outages": outages,
        "silver_1m": silver_1m,
        "silver_qc_df": silver_qc_df,
        "dq_df": dq_df,
        "dq_details": dq_details,
        "gold_df": gold_df,
        "bench_df": bench_df,
        "ingest_details": ingest_details,
        "dst_df": dst_df,
        "dst_details": dst_details,
    }


def test_01_fleet_specification_counts():
    specs = build_fleet_specs()
    assert len(specs) == 16
    assert sum(1 for s in specs if s.asset_type == "wind") == 10
    assert sum(1 for s in specs if s.asset_type == "pv") == 4
    assert sum(1 for s in specs if s.asset_type.startswith("meter_")) == 2


def test_02_outage_schedule_has_18_events_in_bounds():
    outages = build_outage_schedule()
    assert len(outages) == 18
    durations = [o.duration_min for o in outages]
    assert min(durations) >= 45
    assert max(durations) <= 240


def test_03_solar_elevation_day_night_cycle():
    ts = pd.to_datetime(["2026-10-21 00:00:00+00:00", "2026-10-21 10:30:00+00:00"])
    el = solar_elevation_deg(pd.DatetimeIndex(ts))
    assert el[0] < -20.0
    assert el[1] > 15.0


def test_04_ground_truth_and_bronze_duplicate_ratio(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    bronze_df = sim_bundle["bronze_df"]
    assert len(truth_df) == 16 * 7 * 1440  # 161,280 rows
    dup_ratio = (len(bronze_df) - len(truth_df)) / len(truth_df)
    assert abs(dup_ratio - 0.018) < 1e-4  # simulator input: 1.8 % QoS-1 retries
    assert int(bronze_df["is_duplicate_retry"].sum()) == len(bronze_df) - len(truth_df)


def test_05_bronze_arrival_sorted_by_ingested_at(sim_bundle):
    bronze_df = sim_bundle["bronze_df"]
    assert bronze_df["ingested_at"].is_monotonic_increasing
    # Event time is NOT monotonic in Bronze because of LTE router backfill bursts
    assert not bronze_df["timestamp_utc"].is_monotonic_increasing


def test_06_upsert_silver_has_exactly_the_true_keys(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    silver_1m = sim_bundle["silver_1m"]
    assert len(silver_1m) == len(truth_df)
    assert not silver_1m.duplicated(subset=["asset_id", "timestamp_utc"]).any()
    keys = ["asset_id", "timestamp_utc"]
    assert silver_1m[keys].sort_values(keys).reset_index(drop=True).equals(
        truth_df[keys].sort_values(keys).reset_index(drop=True)
    )


def test_07_recovery_matches_independent_lag_oracle(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    bench_df = sim_bundle["bench_df"]
    delayed = truth_df.loc[truth_df["is_outage_backfill"]]
    lag_h = (delayed["ingested_at"] - delayed["timestamp_utc"]).dt.total_seconds() / 3600.0

    # Drop-late modes recover exactly the delayed minutes whose first arrival lag is <= W
    for _, row in bench_df.iloc[1:-1].iterrows():
        expected = float((lag_h <= row["watermark_horizon_h"]).mean() * 100.0)
        assert row["delayed_intervals_recovered_pct"] == pytest.approx(expected, abs=0.01)
    # Naive bucketing never puts a delayed minute in its own quarter-hour; the upsert merges all of them
    assert bench_df.iloc[0]["delayed_intervals_recovered_pct"] == 0.0
    assert bench_df.iloc[-1]["delayed_intervals_recovered_pct"] == pytest.approx(100.0)
    # Completeness and recovery grow with the watermark horizon
    wm = bench_df.iloc[1:]
    assert np.all(np.diff(wm["delayed_intervals_recovered_pct"].to_numpy()) >= 0.0)
    assert np.all(np.diff(wm["completeness_pct"].to_numpy()) >= 0.0)
    # Naive completeness = share of minutes that were not delayed (jitter never crosses a bucket edge)
    expected_naive = (1.0 - len(delayed) / len(truth_df)) * 100.0
    assert bench_df.iloc[0]["completeness_pct"] == pytest.approx(expected_naive, abs=0.01)


def test_08_naive_energy_is_conserved_but_misplaced(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    bronze_df = sim_bundle["bronze_df"]
    bench_df = sim_bundle["bench_df"]
    naive_row = bench_df.iloc[0]
    true_kwh = truth_df["delta_kwh_true"].sum()
    dup_kwh = bronze_df.loc[bronze_df["is_duplicate_retry"], "delta_kwh_true"].sum()

    # Naive total = true energy + every duplicate retry summed again
    assert naive_row["duplicate_energy_pct"] == pytest.approx(dup_kwh / true_kwh * 100.0, abs=0.01)
    assert naive_row["total_energy_mwh"] == pytest.approx((true_kwh + dup_kwh) / 1000.0, abs=0.01)
    # Event-time modes never double count and never exceed the true energy of a reconnect bucket
    for _, row in bench_df.iloc[1:].iterrows():
        assert row["duplicate_energy_pct"] == 0.0
        assert row["reconnect_bucket_energy_ratio_median_x"] <= 1.0 + 1e-9
        assert row["total_energy_mwh"] <= naive_row["true_energy_mwh"] + 0.01
    # Error in outage-affected buckets falls as W grows and vanishes once nothing is late
    mae = bench_df["affected_buckets_energy_nmae_pct"].to_numpy()
    assert mae[0] > mae[1:].max()
    assert np.all(np.diff(mae[1:]) <= 1e-9)
    assert mae[-1] == pytest.approx(0.0, abs=1e-9)
    assert bench_df.iloc[-1]["asset_15m_power_rmse_kw"] == pytest.approx(0.0, abs=1e-9)


def test_09_every_reconnect_bucket_receives_the_whole_backlog(sim_bundle):
    bursts = sim_bundle["ingest_details"]["reconnect_bursts"]
    outages = sim_bundle["outages"]
    assert len(bursts) == len(outages)
    by_id = {o.outage_id: o for o in outages}
    for _, b in bursts.iterrows():
        assert b["outage_minutes"] == by_id[b["outage_id"]].duration_min
        # Flush lasts < 15 min after a quarter-hour-aligned reconnect, so the backlog lands in one bucket
        assert b["naive_packets_in_bucket"] >= b["outage_minutes"]
        if b["true_energy_kwh"] > 0.0:
            assert b["naive_to_true_energy_x"] > 1.0
    naive_15m = ingest_naive_processing_time(sim_bundle["bronze_df"])
    assert int(naive_15m["naive_record_count"].max()) == int(bursts["naive_packets_in_bucket"].max())


def test_10_dst_october_fallback_matches_calendar_and_lost_hour(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    dst_df = sim_bundle["dst_df"]
    oct_row = dst_df.loc[dst_df["dst_date"] == "2026-10-25"].iloc[0]
    assert int(oct_row["expected_quarter_hours"]) == 25 * 4
    assert int(oct_row["naive_quarter_hours"]) == 24 * 4
    assert int(oct_row["reconciled_quarter_hours"]) == int(oct_row["expected_quarter_hours"])
    assert oct_row["utc_timestamps_matched_pct"] == pytest.approx(100.0)
    assert abs(oct_row["reconciled_error_mwh"]) < 1e-4
    # Oracle: naive overwrite loses exactly the 02:00 CEST hour = 00:00-00:59 UTC (generating units only)
    lost = truth_df[
        (truth_df["timestamp_utc"] >= pd.Timestamp("2026-10-25 00:00:00+00:00"))
        & (truth_df["timestamp_utc"] < pd.Timestamp("2026-10-25 01:00:00+00:00"))
        & (~truth_df["asset_id"].str.startswith("PCC-"))
    ]["delta_kwh_true"].sum() / 1000.0
    assert -oct_row["naive_error_mwh"] == pytest.approx(lost, abs=1e-3)
    hourly = pd.DataFrame(sim_bundle["dst_details"]["oct25_hourly"])
    assert hourly["fleet_mwh"].sum() - hourly["naive_local_mwh"].sum() == pytest.approx(lost, abs=0.01)


def test_11_dst_march_spring_forward_matches_calendar(sim_bundle):
    dst_df = sim_bundle["dst_df"]
    mar_row = dst_df.loc[dst_df["dst_date"] == "2026-03-29"].iloc[0]
    assert int(mar_row["expected_quarter_hours"]) == 23 * 4
    assert int(mar_row["naive_quarter_hours"]) == 24 * 4
    assert int(mar_row["reconciled_quarter_hours"]) == int(mar_row["expected_quarter_hours"])
    assert mar_row["utc_timestamps_matched_pct"] == pytest.approx(100.0)
    assert mar_row["naive_error_mwh"] > 0.0
    assert abs(mar_row["reconciled_error_mwh"]) < 1e-4


def test_12_warsaw_fold_disambiguation_exact_timestamps():
    # Two rows at 2026-10-25 02:15:00 local time with increasing kwh_total
    df_local = pd.DataFrame(
        {
            "asset_id": ["WTG-01", "WTG-01"],
            "timestamp_local_str": ["2026-10-25 02:15:00", "2026-10-25 02:15:00"],
            "kwh_total": [1000.0, 1035.0],
            "delta_kwh_true": [0.5, 0.6],
        }
    )
    res = disambiguate_warsaw_local_timestamps(df_local)
    assert list(res["inferred_fold"]) == [0, 1]
    assert res["timestamp_utc_reconstructed"].iloc[0] == pd.Timestamp("2026-10-25 00:15:00+00:00")
    assert res["timestamp_utc_reconstructed"].iloc[1] == pd.Timestamp("2026-10-25 01:15:00+00:00")


def test_13_uint32_counter_rollover_exact_recovery():
    ts = pd.date_range("2026-10-20 00:00:00+00:00", periods=4, freq="1min")
    df = pd.DataFrame(
        {
            "timestamp_utc": ts,
            "asset_id": "WTG-01",
            "rated_kw": 3000.0,
            "active_power_kw": [2400.0, 2400.0, 2400.0, 2400.0],
            "kwh_total": [
                UINT32_ROLLOVER_KWH - 60.0,
                UINT32_ROLLOVER_KWH - 20.0,
                20.0,  # wrapped past UINT32_ROLLOVER_KWH by +40.0 kWh
                60.0,
            ],
        }
    )
    rec = reconcile_cumulative_counter(df)
    assert rec["counter_event"].iloc[2] == "UINT32_ROLLOVER"
    assert np.allclose(rec["delta_kwh_reconciled"].to_numpy(), [40.0, 40.0, 40.0, 40.0], atol=1e-3)


def test_14_plc_reboot_counter_reset_recovery():
    ts = pd.date_range("2026-10-20 00:00:00+00:00", periods=4, freq="1min")
    df = pd.DataFrame(
        {
            "timestamp_utc": ts,
            "asset_id": "WTG-02",
            "rated_kw": 3000.0,
            "active_power_kw": [1800.0, 1800.0, 1800.0, 1800.0],
            "kwh_total": [150_000.0, 150_030.0, 30.0, 60.0],
        }
    )
    rec = reconcile_cumulative_counter(df)
    assert rec["counter_event"].iloc[2] == "PLC_REBOOT_RESET"
    assert np.allclose(rec["delta_kwh_reconciled"].to_numpy(), [30.0, 30.0, 30.0, 30.0], atol=1e-3)


def test_15_dq_scores_are_consistent_with_confusion_counts(sim_bundle):
    dq_df = sim_bundle["dq_df"]
    dq_details = sim_bundle["dq_details"]
    assert set(dq_df["fault_class"]) == set(FAULT_CLASSES)
    assert int(dq_df["support_intervals"].sum()) == len(sim_bundle["silver_qc_df"])
    for _, r in dq_df.iterrows():
        tp, fp, fn = r["true_positives"], r["false_positives"], r["false_negatives"]
        assert tp + fn == r["support_intervals"]
        assert tp > 0  # every injected class is detected at least once
        p, rc = tp / (tp + fp), tp / (tp + fn)
        assert r["precision"] == pytest.approx(p, abs=1e-4)
        assert r["recall"] == pytest.approx(rc, abs=1e-4)
        assert r["f1_score"] == pytest.approx(2 * p * rc / (p + rc), abs=1e-4)
    assert dq_details["macro_f1"] == pytest.approx(dq_df["f1_score"].mean(), abs=1e-3)


def test_16_dq_misses_are_window_onsets_and_no_false_alarms(sim_bundle):
    q = sim_bundle["silver_qc_df"].sort_values(["asset_id", "timestamp_utc"]).reset_index(drop=True)
    run_id = (q["injected_fault"] != q.groupby("asset_id")["injected_fault"].shift()).cumsum()
    minute_in_window = q.groupby(run_id).cumcount()
    missed = (q["injected_fault"] != "HEALTHY") & (q["dq_flag"] != q["injected_fault"])
    false_alarm = (q["injected_fault"] == "HEALTHY") & (q["dq_flag"] != "HEALTHY")
    assert missed.any()
    assert (minute_in_window[missed] < 3).all()
    assert not false_alarm.any()


def test_17_dq_imputation_removes_unphysical_spikes(sim_bundle):
    silver_qc_df = sim_bundle["silver_qc_df"]
    assert silver_qc_df["poa_wm2"].max() > 1500.0
    assert silver_qc_df["poa_clean_wm2"].max() <= 1350.0
    assert silver_qc_df["wind_speed_ms"].max() > 40.0
    assert silver_qc_df["wind_speed_clean_ms"].max() <= 38.0


def test_18_gold_15m_settlement_completeness_and_row_count(sim_bundle):
    gold_df = sim_bundle["gold_df"]
    # 16 assets x 7 days x 96 quarter-hours = 10,752 rows
    assert len(gold_df) == 16 * 7 * 96
    assert (gold_df["data_completeness_pct"] == 100.0).all()
    assert (gold_df["energy_kwh"] >= 0.0).all()


def test_19_gold_pv_performance_ratio_range(sim_bundle):
    gold_df = sim_bundle["gold_df"]
    pv_day = gold_df[(gold_df["asset_type"] == "pv") & (gold_df["poa_mean_wm2"] >= 200.0)]
    assert len(pv_day) > 100
    pr = pv_day["iec_61724_pr"].dropna()
    assert pr.between(0.75, 1.02).all()


def test_20_medallion_parquet_roundtrip(tmp_path: Path, sim_bundle):
    paths = write_medallion_parquet_store(
        sim_bundle["bronze_df"].iloc[:500],
        sim_bundle["silver_qc_df"].iloc[:500],
        sim_bundle["gold_df"].iloc[:200],
        tmp_path,
    )
    for k, p in paths.items():
        assert Path(p).exists()
        df_loaded = pd.read_parquet(p)
        assert len(df_loaded) > 0


def test_21_timescaledb_ddl_contains_hypertables_and_cagg():
    sql = generate_timescaledb_ddl()
    assert "create_hypertable" in sql
    assert "scada_silver_1m" in sql
    assert "ON CONFLICT (asset_id, timestamp_utc) DO UPDATE" in sql
    assert "WITH (timescaledb.continuous)" in sql
    assert "time_bucket('15 minutes', timestamp_utc)" in sql
    assert "add_compression_policy" in sql


def test_22_exported_local_csv_roundtrip_on_non_dst_day(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    sub = truth_df[
        (truth_df["asset_id"] == "WTG-01")
        & (truth_df["timestamp_utc"] >= pd.Timestamp("2026-10-21 00:00:00+00:00"))
        & (truth_df["timestamp_utc"] < pd.Timestamp("2026-10-21 06:00:00+00:00"))
    ].copy()
    local_df = export_legacy_local_csv_frame(sub)
    disamb = disambiguate_warsaw_local_timestamps(local_df)
    assert (disamb["timestamp_utc_reconstructed"].to_numpy() == sub["timestamp_utc"].to_numpy()).all()


def test_23_flatline_rule_flags_long_frozen_runs_after_warmup():
    n = 60
    wind = 8.0 + 0.1 * np.sin(np.arange(n, dtype=float))
    wind[10:50] = 7.5  # 40-minute frozen anemometer
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range("2026-10-20 00:00:00+00:00", periods=n, freq="1min"),
            "asset_id": "WTG-01",
            "asset_type": "wind",
            "rated_kw": 3000.0,
            "active_power_kw": 1500.0,
            "kwh_total": 1000.0 + 25.0 * np.arange(n, dtype=float),
            "wind_speed_ms": wind,
            "poa_wm2": 0.0,
            "solar_elevation_deg": 10.0,
        }
    )
    out = run_silver_dq_engine(df)
    flagged = np.flatnonzero(out["dq_flag"].to_numpy() == "FLATLINE")
    assert flagged.min() == 13 and flagged.max() == 49


def test_24_pipeline_writes_consistent_relative_outputs(tmp_path: Path):
    payload = run_pipeline(tmp_path)
    for p in payload["parquet_files"].values():
        assert not Path(p).is_absolute()
        assert (tmp_path / p).exists()
    verification = json.loads((tmp_path / "results" / "verification.json").read_text(encoding="utf-8"))
    data_js = (tmp_path / "docs" / "data.js").read_text(encoding="utf-8")
    dashboard = json.loads(data_js.removeprefix("window.DATA = ").rstrip().rstrip(";"))
    assert dashboard["headline"] == verification
    bench = pd.read_csv(tmp_path / "results" / "ingestion_benchmark.csv")
    wh = verification["watermark_headline"]
    assert wh["naive_affected_buckets_nmae_pct"] == bench.iloc[0]["affected_buckets_energy_nmae_pct"]
    assert wh["upsert_delayed_recovery_pct"] == bench.iloc[-1]["delayed_intervals_recovered_pct"]
    for scenario in dashboard["scenarios"].values():
        assert len(scenario["mode_power_kw"]) == len(bench)
        assert all(len(trace) == len(scenario["timestamps"]) for trace in scenario["mode_power_kw"])


def test_25_committed_results_match_a_fresh_run(sim_bundle):
    results = REPO_ROOT / "results"
    pd.testing.assert_frame_equal(
        pd.read_csv(results / "ingestion_benchmark.csv"), sim_bundle["bench_df"], check_dtype=False, atol=0.02
    )
    pd.testing.assert_frame_equal(
        pd.read_csv(results / "reconnect_bursts.csv"),
        sim_bundle["ingest_details"]["reconnect_bursts"],
        check_dtype=False,
        atol=0.2,
    )
    pd.testing.assert_frame_equal(
        pd.read_csv(results / "dst_reconciliation.csv"), sim_bundle["dst_df"], check_dtype=False, atol=0.002
    )
    pd.testing.assert_frame_equal(
        pd.read_csv(results / "dq_confusion_summary.csv"), sim_bundle["dq_df"], check_dtype=False
    )


def test_26_committed_outputs_hold_no_absolute_local_paths():
    pattern = re.compile(r"[A-Za-z]:\\\\|/home/|/Users/")
    for path in [*sorted((REPO_ROOT / "results").glob("*")), REPO_ROOT / "docs" / "data.js"]:
        assert not pattern.search(path.read_text(encoding="utf-8")), path.name


def test_27_readme_tables_match_committed_results():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    rows = [line for line in readme.splitlines() if line.startswith("| ")]
    results = REPO_ROOT / "results"

    def assert_row(cells: list[str]) -> None:
        assert any(all(c in line for c in cells) for line in rows), cells

    for i, r in pd.read_csv(results / "ingestion_benchmark.csv").iterrows():
        cells = [
            f"`{r['completeness_pct']:.2f} %`",
            f"`{r['delayed_intervals_recovered_pct']:.2f} %`",
            f"`{r['duplicate_energy_pct']:.2f} %`",
            f"`{r['reconnect_bucket_energy_ratio_median_x']:.2f}x`",
            f"`{r['affected_buckets_energy_nmae_pct']:.2f} %`",
            f"`{r['asset_15m_power_rmse_kw']:,.2f} kW`",
        ]
        assert_row(cells if i == 0 else cells + [f"`{r['watermark_horizon_h']:.1f} h`"])
    event_time = pd.read_csv(results / "ingestion_benchmark.csv").iloc[1:]
    assert_row([f"`{w:.1f} h`" for w in event_time["watermark_horizon_h"]])
    assert_row([f"`{p:.2f} %`" for p in event_time["delayed_intervals_recovered_pct"]])
    for _, r in pd.read_csv(results / "dst_reconciliation.csv").iterrows():
        assert_row(
            [
                f"`{r['dst_date']}`",
                f"`{r['expected_quarter_hours']}`",
                f"`{r['naive_quarter_hours']}`",
                f"`{r['reconciled_quarter_hours']}`",
                f"`{r['utc_timestamps_matched_pct']:.2f} %`",
                f"`{r['naive_error_mwh']:+.3f} MWh`",
                f"`{r['naive_error_pct']:+.2f} %`",
                f"`{abs(r['reconciled_error_mwh']):.3f} MWh`",
            ]
        )
    for _, r in pd.read_csv(results / "dq_confusion_summary.csv").iterrows():
        assert_row(
            [
                f"`{r['fault_class']}`",
                f"`{r['support_intervals']:,}`",
                f"`{r['true_positives']:,}`",
                f"`{r['false_positives']:,}`",
                f"`{r['false_negatives']:,}`",
                f"`{r['precision']:.4f}`",
                f"`{r['recall']:.4f}`",
                f"`{r['f1_score']:.4f}`",
            ]
        )
    v = json.loads((results / "verification.json").read_text(encoding="utf-8"))
    wh, dq, fleet = v["watermark_headline"], v["dq_headline"], v["fleet"]
    for token in [
        f"`{wh['naive_reconnect_ratio_median_x']:.2f}x`",
        f"`{wh['naive_reconnect_ratio_min_x']:.2f}x`",
        f"`{wh['naive_reconnect_ratio_max_x']:.2f}x`",
        f"`{dq['macro_f1']:.3f}`",
        f"`{dq['macro_precision']:.4f}`",
        f"`{dq['macro_recall']:.4f}`",
        f"`{dq['total_injected_fault_intervals']:,}`",
        f"`{fleet['bronze_raw_rows']:,}`",
        f"`{fleet['truth_1m_rows']:,}`",
        f"`{fleet['duplicate_retry_packets']:,}`",
        f"`{fleet['duplicate_retry_rate_pct']:.2f} %`",
        f"`{fleet['n_delayed_intervals']:,}`",
        f"`{fleet['max_arrival_lag_h']:.2f} h`",
    ]:
        assert token in readme, token
    nmae_at = [
        f"`{r['affected_buckets_energy_nmae_pct']:.2f} %` at `W = {r['watermark_horizon_h']:g} h`"
        for _, r in event_time.iloc[-2:].iterrows()
    ]
    assert_row([*nmae_at, f"longest lag of `{fleet['max_arrival_lag_h']:.2f} h`"])
