# Streaming SCADA telemetry, late-arriving backfill and Medallion lakehouse (TimescaleDB / Parquet)

[![tests](https://github.com/kwwaleszczynski-del/scada-lakehouse-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/kwwaleszczynski-del/scada-lakehouse-streaming/actions/workflows/ci.yml)

Field SCADA telemetry from onshore wind farms, solar PV plants and point-of-common-coupling (PCC) revenue meters rarely
arrives at the broker as a clean, ordered 1-minute clock. Cellular LTE routers at remote substations drop carrier links
for 45 minutes to 4 hours while the local PLC or RTU buffers 1-minute registers in a ring buffer and flushes the backlog
in a single high-rate burst upon reconnect; MQTT QoS-1 delivery retries inject `1.8 %` duplicate packets; legacy data
loggers export local `Europe/Warsaw` CSV timestamps across 23-hour spring-forward and 25-hour autumn fall-back Daylight
Saving Time (DST) transitions; and field sensors suffer frozen ultrasonic anemometer flatlines, pyranometer EMI spikes,
and cumulative `uint32` energy register rollovers.

This repository models a `50.0 MW` hybrid renewable fleet (`10 x 3.0 MW` wind turbines `WTG-01..10`, `4 x 5.0 MW` solar
PV inverter stations `PV-INV-01..04`, and `2` PCC revenue meters `PCC-MTR-01..02` publishing 1-minute telemetry on
`scada.telemetry.v1` over `161,280` ground-truth asset-minutes / `164,183` raw Bronze arrivals) and verifies each stage
of a **Bronze -> Silver -> Gold** Parquet and **TimescaleDB** continuous-aggregate pipeline against known physical
injections:

1. **Event-time watermarking and idempotent upsert vs naive ingestion-time windowing** (`src/scada_lakehouse/watermark_ingest.py`):
   quantifies how grouping by broker arrival time (`ingested_at`) without deduplication inflates apparent 15-minute peak
   feeder export by **`3.8x`** during LTE reconnect bursts and introduces **`14.2 %`** interval energy MAE across affected
   hours, whereas event-time watermarking (`W = 6.0 h`) with idempotent `ON CONFLICT (asset_id, timestamp_utc) DO UPDATE`
   recovers **`100.0 %`** of delayed intervals with **`0.00 %`** duplicate double-counting.
2. **Central European (`Europe/Warsaw` <-> `UTC`) DST transition reconciliation** (`src/scada_lakehouse/dst_and_counters.py`):
   shows how naive local-time deduplication during the 25-hour autumn fall-back day (`25 October 2026`, where
   `02:00..02:59` occurs twice as `02A CEST UTC+2` and `02B CET UTC+1`) overwrites the first hour and loses `4`
   quarter-hours (`-4.0 %` of daily intervals), while deterministic PEP 495 `fold=0/1` disambiguation via monotonic
   cumulative `kWh_total` continuity recovers all `100` quarter-hours (`0.000 MWh` reconciliation error) and rejects
   phantom `02:00` rows on the 23-hour spring-forward day (`29 March 2026`, `92` quarter-hours).
3. **Automated Bronze -> Silver SCADA Data Quality (DQ) and counter rollover recovery** (`src/scada_lakehouse/dq_engine.py`):
   separates frozen anemometer/pyranometer flatlines (`std == 0` for `>= 30 min`), unphysical irradiance spikes
   (`G_poa > 1,350 W/m2` or nighttime offset `> 35 W/m2`), cross-sensor power-vs-resource mismatches, and cumulative
   active-energy counter rollovers (`uint32` wrap at `4,294,967.296 kWh` vs PLC reboot zero-resets), achieving a
   5-class macro-F1 of **`0.986`**.
4. **Columnar Parquet & TimescaleDB continuous aggregates** (`src/scada_lakehouse/medallion_store.py`): writes Bronze,
   Silver (1-minute), and Gold (15-minute settlement & IEC 61724-1 / IEC 61400-12-1 KPI) Parquet tables alongside
   production TimescaleDB DDL (`create_hypertable`, `time_bucket('15 minutes', timestamp_utc)`,
   `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)`, `add_continuous_aggregate_policy`, and
   `add_compression_policy`).

**Interactive dashboard:** https://kwwaleszczynski-del.github.io/scada-lakehouse-streaming/

## Key results

### 1. Event-time watermarking vs naive processing-time windowing across 18 LTE router outages

Evaluated across `164,183` raw Bronze packets (`161,280` physical 1-minute asset intervals plus `2,903` QoS-1 duplicate
retries = `1.80 %`, `results/ingestion_benchmark.csv`):

| Pipeline ingestion mode | Watermark horizon $`W`$ | Completeness | Delayed LTE backfill recovered | Duplicate double-count | Peak 15-min export inflation | Affected-hour energy MAE | Fleet 15-min power RMSE |
|---|---:|---:|---:|---:|---:|---:|---:|
| **Naive Ingestion-Time** (`ingested_at`, no dedup) | `0.0 h` | `98.12 %` | `0.0 %` | `1.80 %` | **`3.80x`** | **`14.20 %`** | `1,033.9 kW` |
| Event-Time Watermark (drop late `> W`) | `0.5 h` | `98.97 %` | `22.1 %` | `0.00 %` | `1.00x` | `9.85 %` | `283.3 kW` |
| Event-Time Watermark (drop late `> W`) | `1.0 h` | `99.30 %` | `46.7 %` | `0.00 %` | `1.00x` | `6.74 %` | `261.2 kW` |
| Event-Time Watermark (drop late `> W`) | `2.0 h` | `99.75 %` | `81.0 %` | `0.00 %` | `1.00x` | `2.40 %` | `206.4 kW` |
| Event-Time Watermark (drop late `> W`) | `4.0 h` | `100.00 %` | `99.6 %` | `0.00 %` | `1.00x` | `0.05 %` | `3.4 kW` |
| **Event-Time Watermark + Idempotent Upsert** | **`6.0 h`** | **`100.00 %`** | **`100.0 %`** | **`0.00 %`** | **`1.00x`** | **`0.00 %`** | **`0.0 kW`** |

- **Why naive ingestion-time windowing inflates 15-minute peak export by `3.8x`:** when the substation cellular router
  buffers `42` minutes of 1-minute telemetry on the PLC ring buffer and flushes that backlog inside a single 15-minute
  reconnect bucket alongside the `15` live minutes, grouping by `time_bucket('15 minutes', ingested_at)` sums `57`
  one-minute energy increments into one quarter-hour (`57 / 15 = 3.80x`), while the preceding outage buckets read
  `0.00 MW`.
- **Why idempotent upsert on `(asset_id, timestamp_utc)` eliminates both burst distortion and retry inflation:** every
  buffered or retried packet carries its original PLC acquisition timestamp `timestamp_utc`. Upserting into the Silver
  hypertable (`ON CONFLICT (asset_id, timestamp_utc) DO UPDATE`) places every delayed 1-minute sample back into its true
  historical quarter-hour and collapses QoS-1 duplicates to a single record.

![LTE backfill and watermark recovery](docs/img/lte_backfill_watermark.png)

### 2. Central European (`Europe/Warsaw`) DST transition reconciliation

Evaluated across legacy local-time SFTP CSV exports on the 25-hour autumn fall-back day (`25 October 2026`, `CEST UTC+2`
to `CET UTC+1`) and the 23-hour spring-forward day (`29 March 2026`, `CET UTC+1` to `CEST UTC+2`,
`results/dst_reconciliation.csv`):

| DST transition date | Transition regime | Expected quarter-hours | Naive local-key quarter-hours | Reconciled UTC quarter-hours | Naive daily energy error | Reconciled UTC error |
|---|---|---:|---:|---:|---:|---:|
| **`2026-10-25`** | Autumn Fall-Back (`CEST -> CET`, 25 h day) | `100` | `96` (`02B` overwrites `02A`) | **`100`** | **`-28.03 MWh` (`-6.02 %`)** | **`0.000 MWh` (`0.00 %`)** |
| **`2026-03-29`** | Spring Forward (`CET -> CEST`, 23 h day) | `92` | `96` (`4` phantom `02:00` qh) | **`92`** | **`+16.74 MWh` (`+4.91 %`)** | **`0.000 MWh` (`0.00 %`)** |

![DST 25-hour fall-back reconciliation](docs/img/dst_transition_reconciliation.png)

### 3. Automated Bronze -> Silver SCADA Data Quality (DQ) verification

Evaluated across all `161,280` deduplicated 1-minute asset-intervals (`results/dq_confusion_summary.csv`):

| SCADA fault class | Injected physical signature | Support (1-min intervals) | Precision | Recall | F1 score |
|---|---|---:|---:|---:|---:|
| `HEALTHY` | Normal operating telemetry | `160,511` | `0.9998` | `1.0000` | **`0.9999`** |
| `FLATLINE` | Frozen anemometer / pyranometer (`std == 0` for `>= 30 min`) | `500` | `1.0000` | `0.9520` | **`0.9754`** |
| `SPIKE_OR_BOUNDS` | Irradiance EMI spike (`> 1,350 W/m2`), night offset (`> 35 W/m2`), wind `> 38 m/s` | `101` | `1.0000` | `0.9406` | **`0.9694`** |
| `COUNTER_RESET` | Cumulative `kWh_total` `uint32` rollover (`4,294,967.296 kWh`) or PLC reboot reset | `8` | `1.0000` | `1.0000` | **`1.0000`** |
| `CROSS_SENSOR_MISMATCH` | Power `> 60 % P_rated` while primary wind (`< 2.5 m/s`) or POA (`< 40 W/m2`) sensor dead | `160` | `1.0000` | `0.9688` | **`0.9841`** |
| **5-class Macro-F1** | Unsupervised rule-based Silver QC engine | `161,280` | `1.0000` | `0.9723` | **`0.986`** |

![SCADA sensor and counter fault QC](docs/img/dq_fault_detection.png)

![Medallion completeness and RMSE](docs/img/medallion_latency_completeness.png)

## Method

### 1. Event-time watermarking vs processing-time windowing

Each telemetry packet $`m`$ emitted by asset $`a`$ carries an acquisition event timestamp $`t_{\text{utc}}(m)`$ stamped
by the PLC real-time clock and a broker arrival timestamp $`t_{\text{ing}}(m) = t_{\text{utc}}(m) + \Delta t_{\text{net}}(m)`$.
During an LTE router drop over $`[t_0, t_0 + D]`$, packets accumulate on the local RTU and arrive in a reconnect burst
at $`t_{\text{ing}} \in [t_0 + D, t_0 + D + \tau_{\text{flush}}]`$:

```math
\hat{E}_{a,k}^{\text{naive}} = \sum_{m \,:\, t_{\text{ing}}(m) \in B_k} \Delta E(m), \qquad
\hat{E}_{a,k}^{\text{silver}} = \sum_{t_{\text{utc}} \in B_k} \Delta E^{\star}_{a}(t_{\text{utc}})
```

where $`B_k = [t_k, t_k + 15\text{ min})`$, and $`\Delta E^{\star}_{a}(t_{\text{utc}})`$ is the deduplicated Silver record
maintained via idempotent upsert (`ON CONFLICT (asset_id, timestamp_utc) DO UPDATE`) inside watermark horizon
$`W = 6\text{ h}`$.

### 2. Modulo-$`2^{32}`$ cumulative energy counter recovery

Revenue meters and inverter PLCs accumulate active energy in a 32-bit unsigned pulse register scaled by
$`w_p = 0.001\text{ kWh/pulse}`$, wrapping at $`M_{32} = 2^{32} \times 10^{-3} = 4,294,967.296\text{ kWh}`$. For raw
register difference $`\Delta C_t = C_t - C_{t-1}`$ and maximum physically possible 1-minute export
$`\Delta E_{\max} = 1.15 \, P_{\text{rated}} / 60`$:

```math
\Delta E_t^{\text{rec}} =
\begin{cases}
\Delta C_t & 0 \le \Delta C_t \le \Delta E_{\max} \\[1ex]
\Delta C_t + M_{32} & \Delta C_t < 0 \text{ and } 0 \le \Delta C_t + M_{32} \le \Delta E_{\max} \quad (\text{uint32 rollover}) \\[1ex]
C_t & \Delta C_t < 0 \text{ and } 0 \le C_t \le \Delta E_{\max} \quad (\text{PLC zero-reset}) \\[1ex]
P_t / 60 & \text{otherwise (corrupt register fallback)}
\end{cases}
```

### 3. TimescaleDB continuous aggregate refresh policy

The Gold 15-minute view (`results/timescaledb_schema.sql`) combines `time_bucket('15 minutes', timestamp_utc)` with a
continuous aggregate refresh window spanning `[NOW() - INTERVAL '6 hours', NOW() - INTERVAL '1 minute']` every 5 minutes
so any LTE backfill burst upserted into `scada_silver_1m` automatically recomputes the affected 15-minute Gold buckets
before chunks age past the 7-day columnar compression boundary (`add_compression_policy('scada_silver_1m', INTERVAL '7 days')`).

## Limitations

- **Watermark horizon vs chunk compression lock:** if a remote substation experiences an extended fiber or cellular cut
  lasting longer than the 7-day TimescaleDB compression delay (`INTERVAL '7 days'`), backfilling into already-compressed
  columnar chunks requires decompressing the affected chunk (`decompress_chunk`) or routing ultra-late historical
  recoveries through a staging backfill procedure.
- **Unsynchronized PLC clocks (NTP drift):** event-time watermarking assumes the local PLC/RTU clock remained synced to
  GPS or NTP prior to the LTE drop. If a legacy logger's crystal oscillator drifts by several minutes during a prolonged
  comms outage, `timestamp_utc` itself shifts across 15-minute settlement boundaries.
- **Single-plant common-mode weather:** when all anemometers on a wind cluster ice over simultaneously during a freezing
  fog event, peer-turbine imputation loses its clean reference and must fall back to nacelle anemometer or inverse power
  curve estimation.

## Reproduce

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
python -m scada_lakehouse
```

`python -m scada_lakehouse` runs the 7-day 1-minute hybrid fleet simulation (`161,280` ground-truth intervals, `164,183`
Bronze arrivals), writes Bronze/Silver/Gold Parquet files in `data_lake/`, exports CSV benchmarks and
`results/timescaledb_schema.sql` in `results/`, regenerates the four PNG figures in `docs/img/`, and updates
`docs/data.js` for the interactive dashboard.

Repository layout:

```
src/scada_lakehouse/
  stream_simulator.py   16-asset wind/PV/meter 1-min telemetry, 18 LTE outages, 1.8% retries, sensor/counter faults
  watermark_ingest.py   naive ingestion-time bucketing vs event-time watermarking + idempotent upsert
  dst_and_counters.py   Europe/Warsaw 23h/25h fold=0/1 disambiguation and uint32 / PLC counter reconciliation
  dq_engine.py          automated Silver DQ rules (bounds, 30-min flatline, counter reset, cross-sensor mismatch)
  medallion_store.py    Bronze/Silver/Gold Parquet writer and TimescaleDB hypertable + continuous aggregate DDL
  plots.py              4 engineering Matplotlib figures in docs/img/
  cli.py                end-to-end pipeline runner
tests/                  22 pytest unit and verification tests
results/                CSV benchmarks, verification.json, and timescaledb_schema.sql
docs/                   GitHub Pages interactive dashboard (index.html + data.js) and PNG figures
```

MIT licence.
