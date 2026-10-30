"""Comprehensive unit and verification test suite for scada-lakehouse-streaming (22 tests)."""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

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
    assert abs(dup_ratio - 0.018) < 1e-4
    assert int(bronze_df["is_duplicate_retry"].sum()) == len(bronze_df) - len(truth_df)


def test_05_bronze_arrival_sorted_by_ingested_at(sim_bundle):
    bronze_df = sim_bundle["bronze_df"]
    assert bronze_df["ingested_at"].is_monotonic_increasing
    # Event time is NOT monotonic in Bronze because of LTE router backfill bursts
    assert not bronze_df["timestamp_utc"].is_monotonic_increasing


def test_06_watermarked_idempotent_upsert_recovers_100_percent(sim_bundle):
    truth_df = sim_bundle["truth_df"]
    silver_1m = sim_bundle["silver_1m"]
    assert len(silver_1m) == len(truth_df)
    # Zero duplicate (asset_id, timestamp_utc) keys
    assert not silver_1m.duplicated(subset=["asset_id", "timestamp_utc"]).any()


def test_07_watermark_horizon_monotonic_completeness(sim_bundle):
    bench_df = sim_bundle["bench_df"]
    wm_rows = bench_df.iloc[1:]
    rec = wm_rows["delayed_intervals_recovered_pct"].to_numpy()
    assert np.all(np.diff(rec) >= 0.0)
    assert rec[-1] == pytest.approx(100.0, abs=1e-6)
    assert rec[0] < 30.0  # W = 0.5h only recovers final 30m of >= 45m outages


def test_08_naive_ingestion_time_peak_inflation_and_mae(sim_bundle):
    bench_df = sim_bundle["bench_df"]
    naive_row = bench_df.iloc[0]
    wm6_row = bench_df.iloc[-1]
    assert naive_row["peak_15m_export_inflation_x"] == pytest.approx(3.80, abs=0.05)
    assert naive_row["affected_hours_energy_mae_pct"] == pytest.approx(14.20, abs=0.1)
    assert wm6_row["duplicate_double_count_pct"] == pytest.approx(0.0, abs=1e-6)
    assert wm6_row["affected_hours_energy_mae_pct"] == pytest.approx(0.0, abs=1e-6)


def test_09_naive_processing_time_reconnect_burst_packets(sim_bundle):
    bronze_df = sim_bundle["bronze_df"]
    naive_15m = ingest_naive_processing_time(bronze_df)
    # During reconnect bursts, a single 15-minute ingestion bucket receives > 100 packets
    assert int(naive_15m["naive_record_count"].max()) > 100


def test_10_dst_october_fallback_25_hours_100_quarter_hours(sim_bundle):
    dst_df = sim_bundle["dst_df"]
    oct_row = dst_df.loc[dst_df["dst_date"] == "2026-10-25"].iloc[0]
    assert int(oct_row["expected_quarter_hours"]) == 100
    assert int(oct_row["naive_quarter_hours"]) == 96
    assert int(oct_row["reconciled_quarter_hours"]) == 100
    assert oct_row["naive_error_mwh"] < -10.0
    assert abs(oct_row["reconciled_error_mwh"]) < 1e-4


def test_11_dst_march_spring_forward_23_hours_92_quarter_hours(sim_bundle):
    dst_df = sim_bundle["dst_df"]
    mar_row = dst_df.loc[dst_df["dst_date"] == "2026-03-29"].iloc[0]
    assert int(mar_row["expected_quarter_hours"]) == 92
    assert int(mar_row["naive_quarter_hours"]) == 96
    assert int(mar_row["reconciled_quarter_hours"]) == 92
    assert mar_row["naive_error_mwh"] > 10.0
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


def test_15_dq_engine_macro_f1_score(sim_bundle):
    dq_details = sim_bundle["dq_details"]
    assert dq_details["macro_f1"] == pytest.approx(0.986, abs=0.005)


def test_16_dq_engine_all_fault_classes_detected(sim_bundle):
    dq_df = sim_bundle["dq_df"]
    assert set(dq_df["fault_class"]) == set(FAULT_CLASSES)
    for _, row in dq_df.iterrows():
        assert row["precision"] >= 0.94
        assert row["recall"] >= 0.94
        assert row["f1_score"] >= 0.95


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


def test_19_gold_pv_iec_61724_performance_ratio_range(sim_bundle):
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
