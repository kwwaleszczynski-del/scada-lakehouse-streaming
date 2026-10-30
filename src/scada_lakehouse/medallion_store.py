"""Medallion lakehouse (Bronze -> Silver -> Gold) Parquet persistence and TimescaleDB DDL generator."""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd


def build_gold_15m_table(silver_qc_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate QC-flagged Silver 1-minute telemetry into 15-minute settlement and IEC 61724-1 / 61400-12-1 Gold KPIs."""
    df = silver_qc_df.copy()
    df["bucket_15m"] = df["timestamp_utc"].dt.floor("15min")

    gold = (
        df.groupby(["asset_id", "asset_type", "bucket_15m"], as_index=False)
        .agg(
            rated_kw=("rated_kw", "first"),
            samples_total=("delta_kwh_reconciled", "count"),
            samples_valid=("is_valid_telemetry", "sum"),
            energy_kwh=("delta_kwh_reconciled", "sum"),
            avg_power_kw=("active_power_clean_kw", "mean"),
            wind_speed_mean_ms=("wind_speed_clean_ms", "mean"),
            poa_mean_wm2=("poa_clean_wm2", "mean"),
            outage_backfill_samples=("is_outage_backfill", "sum"),
        )
        .sort_values(["asset_id", "bucket_15m"], kind="mergesort")
        .reset_index(drop=True)
    )

    gold["energy_kwh"] = np.round(gold["energy_kwh"], 3)
    gold["avg_power_kw"] = np.round(gold["avg_power_kw"], 2)
    gold["wind_speed_mean_ms"] = np.round(gold["wind_speed_mean_ms"], 3)
    gold["poa_mean_wm2"] = np.round(gold["poa_mean_wm2"], 2)
    gold["data_completeness_pct"] = np.round(gold["samples_total"] / 15.0 * 100.0, 2)
    gold["valid_sensor_pct"] = np.round(gold["samples_valid"] / np.maximum(gold["samples_total"], 1) * 100.0, 2)
    gold["capacity_factor_pct"] = np.round(gold["avg_power_kw"] / gold["rated_kw"] * 100.0, 2)

    # IEC 61724-1 Performance Ratio for PV when POA >= 50 W/m2
    ref_yield = gold["poa_mean_wm2"] / 1000.0
    spec_yield = gold["avg_power_kw"] / gold["rated_kw"]
    pr = np.where(
        gold["asset_type"].isin(["pv", "meter_pv"]) & (gold["poa_mean_wm2"] >= 50.0),
        np.clip(spec_yield / np.maximum(ref_yield, 1e-6), 0.0, 1.05),
        np.nan,
    )
    gold["iec_61724_pr"] = np.round(pr, 4)
    return gold


def write_medallion_parquet_store(
    bronze_df: pd.DataFrame,
    silver_df: pd.DataFrame,
    gold_df: pd.DataFrame,
    lake_root: Path,
) -> dict[str, str]:
    """Write partitioned/columnar Parquet files for Bronze, Silver, and Gold tables."""
    bronze_dir = lake_root / "bronze"
    silver_dir = lake_root / "silver"
    gold_dir = lake_root / "gold"
    for d in (bronze_dir, silver_dir, gold_dir):
        d.mkdir(parents=True, exist_ok=True)

    bronze_path = bronze_dir / "scada_bronze_raw.parquet"
    silver_path = silver_dir / "scada_silver_1m.parquet"
    gold_path = gold_dir / "scada_gold_15m.parquet"

    bronze_df.to_parquet(bronze_path, index=False)
    silver_df.to_parquet(silver_path, index=False)
    gold_df.to_parquet(gold_path, index=False)

    return {
        "bronze_parquet": str(bronze_path),
        "silver_parquet": str(silver_path),
        "gold_parquet": str(gold_path),
    }


def generate_timescaledb_ddl() -> str:
    """Return production TimescaleDB DDL and SQL for Bronze hypertable, Silver idempotent upsert, and Gold continuous aggregate."""
    return """-- =============================================================================
-- TimescaleDB Medallion Architecture for Streaming Hybrid SCADA Telemetry
-- Bronze (append log) -> Silver (deduplicated UTC 1-min) -> Gold (15-min KPIs)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. BRONZE LAYER: Raw MQTT / Kafka ingestion log partitioned by broker arrival time
CREATE TABLE IF NOT EXISTS scada_bronze_raw (
    ingested_at         TIMESTAMPTZ      NOT NULL,
    timestamp_utc       TIMESTAMPTZ      NOT NULL,
    asset_id            TEXT             NOT NULL,
    asset_type          TEXT             NOT NULL,
    kafka_partition     SMALLINT         NOT NULL,
    router_id           TEXT             NOT NULL,
    wind_speed_ms       DOUBLE PRECISION,
    poa_wm2             DOUBLE PRECISION,
    active_power_kw     DOUBLE PRECISION NOT NULL,
    kwh_total           DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
    'scada_bronze_raw',
    'ingested_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 2. SILVER LAYER: Deduplicated, UTC-normalized, QC-flagged 1-minute hypertable
CREATE TABLE IF NOT EXISTS scada_silver_1m (
    timestamp_utc           TIMESTAMPTZ      NOT NULL,
    asset_id                TEXT             NOT NULL,
    asset_type              TEXT             NOT NULL,
    rated_kw                DOUBLE PRECISION NOT NULL,
    wind_speed_clean_ms     DOUBLE PRECISION,
    poa_clean_wm2           DOUBLE PRECISION,
    active_power_clean_kw   DOUBLE PRECISION NOT NULL,
    delta_kwh_reconciled    DOUBLE PRECISION NOT NULL,
    kwh_total_monotonic     DOUBLE PRECISION NOT NULL,
    dq_flag                 TEXT             NOT NULL DEFAULT 'HEALTHY',
    is_valid_telemetry      BOOLEAN          NOT NULL DEFAULT TRUE,
    is_outage_backfill      BOOLEAN          NOT NULL DEFAULT FALSE,
    ingested_at             TIMESTAMPTZ      NOT NULL,
    PRIMARY KEY (asset_id, timestamp_utc)
);

SELECT create_hypertable(
    'scada_silver_1m',
    'timestamp_utc',
    chunk_time_interval => INTERVAL '1 day',
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

-- Idempotent watermarked upsert (resolves LTE reconnect backfill bursts & QoS-1 retries)
-- Parameterized batch insert from streaming consumer:
/*
INSERT INTO scada_silver_1m (
    timestamp_utc, asset_id, asset_type, rated_kw,
    wind_speed_clean_ms, poa_clean_wm2, active_power_clean_kw,
    delta_kwh_reconciled, kwh_total_monotonic,
    dq_flag, is_valid_telemetry, is_outage_backfill, ingested_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (asset_id, timestamp_utc) DO UPDATE SET
    wind_speed_clean_ms   = EXCLUDED.wind_speed_clean_ms,
    poa_clean_wm2         = EXCLUDED.poa_clean_wm2,
    active_power_clean_kw = EXCLUDED.active_power_clean_kw,
    delta_kwh_reconciled  = EXCLUDED.delta_kwh_reconciled,
    kwh_total_monotonic   = EXCLUDED.kwh_total_monotonic,
    dq_flag               = EXCLUDED.dq_flag,
    is_valid_telemetry    = EXCLUDED.is_valid_telemetry,
    is_outage_backfill    = EXCLUDED.is_outage_backfill,
    ingested_at           = EXCLUDED.ingested_at
WHERE EXCLUDED.ingested_at >= scada_silver_1m.ingested_at;
*/

-- Columnar compression on Silver chunks older than 7 days (after watermark + backfill horizon closes)
ALTER TABLE scada_silver_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'asset_id',
    timescaledb.compress_orderby = 'timestamp_utc DESC'
);

SELECT add_compression_policy('scada_silver_1m', INTERVAL '7 days', if_not_exists => TRUE);

-- 3. GOLD LAYER: 15-minute settlement & IEC 61724-1 / IEC 61400-12-1 continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS scada_gold_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', timestamp_utc)                              AS bucket_15m,
    asset_id,
    asset_type,
    MAX(rated_kw)                                                         AS rated_kw,
    COUNT(*)                                                              AS samples_total,
    COUNT(*) FILTER (WHERE is_valid_telemetry)                            AS samples_valid,
    ROUND((COUNT(*)::NUMERIC / 15.0) * 100.0, 2)                          AS data_completeness_pct,
    SUM(delta_kwh_reconciled)                                             AS energy_kwh,
    AVG(active_power_clean_kw)                                            AS avg_power_kw,
    AVG(wind_speed_clean_ms)                                              AS wind_speed_mean_ms,
    AVG(poa_clean_wm2)                                                    AS poa_mean_wm2,
    (AVG(active_power_clean_kw) / NULLIF(MAX(rated_kw), 0)) * 100.0       AS capacity_factor_pct,
    CASE
        WHEN asset_type IN ('pv', 'meter_pv') AND AVG(poa_clean_wm2) >= 50.0
        THEN (AVG(active_power_clean_kw) / NULLIF(MAX(rated_kw), 0))
             / NULLIF(AVG(poa_clean_wm2) / 1000.0, 0)
        ELSE NULL
    END                                                                   AS iec_61724_pr
FROM scada_silver_1m
GROUP BY bucket_15m, asset_id, asset_type
WITH NO DATA;

-- Refresh policy covers the 6-hour LTE router watermark window every 5 minutes
SELECT add_continuous_aggregate_policy(
    'scada_gold_15m',
    start_offset      => INTERVAL '6 hours',
    end_offset        => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists     => TRUE
);
"""
