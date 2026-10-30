-- =============================================================================
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
