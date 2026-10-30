"""Synthetic hybrid renewable SCADA stream simulator (MQTT/Kafka JSON telemetry + legacy SFTP CSV).

Simulates a 50 MW hybrid wind + solar PV plant with PCC revenue meters:
  - 10 onshore wind turbines (WTG-01 .. WTG-10, 3.0 MW each = 30.0 MW)
  - 4 solar PV inverter stations (PV-INV-01 .. PV-INV-04, 5.0 MW each = 20.0 MW)
  - 2 PCC revenue meters (PCC-MTR-01 Wind feeder, PCC-MTR-02 PV feeder)

Injects three field anomaly families with exact ground truth:
  1. 18 cellular LTE router outages (45 min to 240 min duration) where 1-minute telemetry
     buffers on the local PLC/RTU ring buffer and flushes in a high-rate out-of-order
     backfill burst upon reconnect, plus 1.8% duplicate QoS-1 packet retries.
  2. Central European DST transitions (Europe/Warsaw <-> UTC) on 25 October 2026 (25-hour
     autumn fall-back day with ambiguous 02:00 CEST / 02:00 CET local hour) and
     29 March 2026 (23-hour spring-forward day with missing 02:00 local hour).
  3. Sensor and counter faults: frozen anemometer/pyranometer flatlines (std == 0 for >= 30 min),
     unphysical irradiance/wind spikes and nighttime offsets, cumulative kWh register uint32
     rollovers and PLC reboot zero-resets, and cross-sensor power-vs-resource mismatches.
"""

from __future__ import annotations

from dataclasses import dataclass
import numpy as np
import pandas as pd


UINT32_ROLLOVER_KWH = 4_294_967.296  # 2^32 pulses at 0.001 kWh/pulse


@dataclass(frozen=True)
class AssetSpec:
    asset_id: str
    asset_type: str  # "wind", "pv", "meter_wind", "meter_pv"
    rated_kw: float
    kafka_partition: int
    router_id: str


@dataclass(frozen=True)
class OutageEvent:
    outage_id: str
    asset_id: str
    router_id: str
    start_utc: pd.Timestamp
    duration_min: int
    flush_duration_min: int
    scenario_label: str


def build_fleet_specs() -> list[AssetSpec]:
    """Return the 16 SCADA assets (10 WTG, 4 PV inverters, 2 PCC revenue meters)."""
    specs: list[AssetSpec] = []
    for i in range(1, 11):
        specs.append(
            AssetSpec(
                asset_id=f"WTG-{i:02d}",
                asset_type="wind",
                rated_kw=3000.0,
                kafka_partition=(i - 1) % 8,
                router_id=f"LTE-W{(i - 1) // 2 + 1:02d}",
            )
        )
    for i in range(1, 5):
        specs.append(
            AssetSpec(
                asset_id=f"PV-INV-{i:02d}",
                asset_type="pv",
                rated_kw=5000.0,
                kafka_partition=4 + (i - 1),
                router_id=f"LTE-PV{i:02d}",
            )
        )
    specs.append(
        AssetSpec(
            asset_id="PCC-MTR-01",
            asset_type="meter_wind",
            rated_kw=30000.0,
            kafka_partition=0,
            router_id="LTE-PCC01",
        )
    )
    specs.append(
        AssetSpec(
            asset_id="PCC-MTR-02",
            asset_type="meter_pv",
            rated_kw=20000.0,
            kafka_partition=1,
            router_id="LTE-PCC02",
        )
    )
    return specs


def build_outage_schedule() -> list[OutageEvent]:
    """Return 18 deterministic cellular LTE router outage events (45 min to 240 min)."""
    raw_schedule = [
        # (id, asset_id, router_id, start_utc, duration_min, flush_min, label)
        ("OUT-01", "WTG-01", "LTE-W01", "2026-10-20 08:15:00+00:00", 120, 6, "WTG-01 2.0h morning LTE drop"),
        ("OUT-02", "WTG-02", "LTE-W01", "2026-10-20 08:15:00+00:00", 90, 5, "WTG-02 1.5h shared router drop"),
        ("OUT-03", "PV-INV-01", "LTE-PV01", "2026-10-20 11:00:00+00:00", 75, 4, "PV-INV-01 75m solar noon outage"),
        ("OUT-04", "WTG-03", "LTE-W02", "2026-10-20 19:30:00+00:00", 180, 10, "WTG-03 3.0h evening wind ramp outage"),
        ("OUT-05", "WTG-04", "LTE-W02", "2026-10-21 02:00:00+00:00", 60, 4, "WTG-04 1.0h night reconnect"),
        ("OUT-06", "PCC-MTR-01", "LTE-PCC01", "2026-10-21 09:00:00+00:00", 210, 12, "PCC-MTR-01 3.5h revenue meter backfill burst"),
        ("OUT-07", "PV-INV-02", "LTE-PV02", "2026-10-21 10:30:00+00:00", 105, 6, "PV-INV-02 105m midday cloud transient"),
        ("OUT-08", "WTG-05", "LTE-W03", "2026-10-21 16:45:00+00:00", 45, 3, "WTG-05 45m short carrier handoff timeout"),
        ("OUT-09", "WTG-06", "LTE-W03", "2026-10-22 05:15:00+00:00", 150, 8, "WTG-06 2.5h morning outage"),
        ("OUT-10", "PV-INV-03", "LTE-PV03", "2026-10-22 09:45:00+00:00", 135, 7, "PV-INV-03 2.25h irradiance ramp outage"),
        ("OUT-11", "WTG-07", "LTE-W04", "2026-10-22 21:00:00+00:00", 240, 14, "WTG-07 4.0h deep PLC ring-buffer outage"),
        ("OUT-12", "WTG-08", "LTE-W04", "2026-10-23 04:30:00+00:00", 90, 5, "WTG-08 1.5h dawn LTE lease renewal"),
        ("OUT-13", "PCC-MTR-02", "LTE-PCC02", "2026-10-23 10:15:00+00:00", 120, 7, "PCC-MTR-02 2.0h PV feeder meter outage"),
        ("OUT-14", "PV-INV-04", "LTE-PV04", "2026-10-23 12:00:00+00:00", 60, 4, "PV-INV-04 1.0h afternoon outage"),
        ("OUT-15", "WTG-09", "LTE-W05", "2026-10-24 07:00:00+00:00", 165, 9, "WTG-09 2.75h weekend VPN tunnel drop"),
        ("OUT-16", "WTG-10", "LTE-W05", "2026-10-24 14:15:00+00:00", 75, 4, "WTG-10 75m afternoon drop"),
        ("OUT-17", "WTG-01", "LTE-W01", "2026-10-25 00:30:00+00:00", 120, 6, "WTG-01 2.0h DST transition night outage"),
        ("OUT-18", "PV-INV-01", "LTE-PV01", "2026-10-25 08:30:00+00:00", 90, 5, "PV-INV-01 1.5h Sunday morning outage"),
    ]
    return [
        OutageEvent(
            outage_id=oid,
            asset_id=aid,
            router_id=rid,
            start_utc=pd.Timestamp(ts),
            duration_min=dur,
            flush_duration_min=fmin,
            scenario_label=lbl,
        )
        for oid, aid, rid, ts, dur, fmin, lbl in raw_schedule
    ]


def solar_elevation_deg(timestamps_utc: pd.DatetimeIndex, lat_deg: float = 52.1, lon_deg: float = 19.4) -> np.ndarray:
    """Compute approximate solar elevation angle [deg] for Central Poland."""
    doy = timestamps_utc.dayofyear.to_numpy(dtype=float)
    hour_utc = (
        timestamps_utc.hour.to_numpy(dtype=float)
        + timestamps_utc.minute.to_numpy(dtype=float) / 60.0
    )
    decl_rad = np.deg2rad(23.45 * np.sin(2.0 * np.pi * (284.0 + doy) / 365.0))
    lat_rad = np.deg2rad(lat_deg)
    solar_time = hour_utc + lon_deg / 15.0
    hour_angle_rad = np.deg2rad(15.0 * (solar_time - 12.0))
    sin_el = np.sin(lat_rad) * np.sin(decl_rad) + np.cos(lat_rad) * np.cos(decl_rad) * np.cos(hour_angle_rad)
    return np.rad2deg(np.arcsin(np.clip(sin_el, -1.0, 1.0)))


def simulate_fleet_stream(seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame, list[OutageEvent]]:
    """Simulate 7-day 1-minute hybrid fleet SCADA telemetry with ground truth and Bronze arrival stream.

    Returns:
      - truth_df: clean 1-minute ground-truth table (161,280 rows: 16 assets x 10,080 minutes,
        2026-10-19 00:00 UTC .. 2026-10-25 23:59 UTC) with injected_fault labels.
      - bronze_df: raw append stream arriving at the broker (out-of-order LTE backfill bursts,
        1.8% duplicate packet retries, sensor faults, and counter rollovers/resets).
      - outages: list of 18 OutageEvent definitions.
    """
    rng = np.random.default_rng(seed)
    specs = build_fleet_specs()
    outages = build_outage_schedule()

    idx_utc = pd.date_range("2026-10-19 00:00:00+00:00", "2026-10-25 23:59:00+00:00", freq="1min")
    n_min = len(idx_utc)
    t_hours = np.arange(n_min, dtype=float) / 60.0
    sol_el = solar_elevation_deg(idx_utc)

    # Common synoptic wind speed [m/s] and cloud transmittance profile across the site
    synoptic_wind = (
        8.6
        + 2.8 * np.sin(2.0 * np.pi * t_hours / 36.0 + 0.4)
        + 1.6 * np.cos(2.0 * np.pi * t_hours / 18.0 - 0.7)
        + 0.9 * np.sin(2.0 * np.pi * t_hours / 24.0)
    )
    cloud_wave = 0.82 + 0.16 * np.sin(2.0 * np.pi * t_hours / 14.0 + 1.1)
    clear_sky_poa = np.where(sol_el > 0.0, 980.0 * (np.sin(np.deg2rad(np.maximum(sol_el, 0.0))) ** 1.18), 0.0)

    # First generate clean physical series for all 14 generating units, then sum into the 2 PCC meters
    unit_frames: dict[str, pd.DataFrame] = {}
    for s_idx, spec in enumerate(specs):
        if spec.asset_type == "wind":
            spatial_offset = 0.45 * np.sin(s_idx * 0.7 + t_hours / 9.0)
            turb_noise = rng.normal(0.0, 0.28, size=n_min)
            w_true = np.clip(synoptic_wind + spatial_offset + turb_noise, 1.5, 23.0)
            # IEC 61400-12-1 cubic knee power curve
            u_norm = np.clip((w_true - 3.0) / (11.5 - 3.0), 0.0, 1.0)
            p_curve = spec.rated_kw * (u_norm ** 3) / (1.0 + 0.45 * (u_norm ** 3))
            p_true = np.where(w_true < 3.0, 0.0, np.where(w_true >= 11.5, spec.rated_kw, p_curve * 1.45))
            p_true = np.clip(p_true + rng.normal(0.0, 14.0, size=n_min), 0.0, spec.rated_kw)
            p_true = np.where(w_true < 3.0, 0.0, p_true)
            poa_true = np.zeros(n_min, dtype=float)
        elif spec.asset_type == "pv":
            poa_noise = rng.normal(0.0, 11.0, size=n_min)
            poa_true = np.where(
                sol_el > 0.0,
                np.clip(clear_sky_poa * cloud_wave * (0.98 + 0.015 * (s_idx - 10)) + poa_noise, 0.0, 1120.0),
                0.0,
            )
            w_true = np.clip(0.65 * synoptic_wind + rng.normal(0.0, 0.2, size=n_min), 0.5, 18.0)
            temp_derate = 1.0 - 0.0035 * np.maximum(poa_true * 0.028 - 5.0, 0.0)
            p_true = np.clip(spec.rated_kw * (poa_true / 1000.0) * temp_derate * 0.975, 0.0, spec.rated_kw)
        else:
            continue

        unit_frames[spec.asset_id] = pd.DataFrame(
            {
                "timestamp_utc": idx_utc,
                "asset_id": spec.asset_id,
                "asset_type": spec.asset_type,
                "rated_kw": spec.rated_kw,
                "kafka_partition": spec.kafka_partition,
                "router_id": spec.router_id,
                "solar_elevation_deg": np.round(sol_el, 3),
                "wind_speed_true_ms": np.round(w_true, 3),
                "poa_true_wm2": np.round(poa_true, 2),
                "active_power_true_kw": np.round(p_true, 2),
            }
        )

    # Construct the 2 PCC revenue meters from the sum of wind and PV units minus MV collection losses
    wind_sum_kw = sum(unit_frames[f"WTG-{i:02d}"]["active_power_true_kw"].to_numpy() for i in range(1, 11)) * 0.989
    wind_mean_ms = sum(unit_frames[f"WTG-{i:02d}"]["wind_speed_true_ms"].to_numpy() for i in range(1, 11)) / 10.0
    pv_sum_kw = sum(unit_frames[f"PV-INV-{i:02d}"]["active_power_true_kw"].to_numpy() for i in range(1, 5)) * 0.991
    pv_mean_poa = sum(unit_frames[f"PV-INV-{i:02d}"]["poa_true_wm2"].to_numpy() for i in range(1, 5)) / 4.0

    for spec in specs[-2:]:
        if spec.asset_id == "PCC-MTR-01":
            p_true = wind_sum_kw
            w_true = wind_mean_ms
            poa_true = np.zeros(n_min, dtype=float)
        else:
            p_true = pv_sum_kw
            w_true = np.clip(0.65 * synoptic_wind, 0.5, 18.0)
            poa_true = pv_mean_poa
        unit_frames[spec.asset_id] = pd.DataFrame(
            {
                "timestamp_utc": idx_utc,
                "asset_id": spec.asset_id,
                "asset_type": spec.asset_type,
                "rated_kw": spec.rated_kw,
                "kafka_partition": spec.kafka_partition,
                "router_id": spec.router_id,
                "solar_elevation_deg": np.round(sol_el, 3),
                "wind_speed_true_ms": np.round(w_true, 3),
                "poa_true_wm2": np.round(poa_true, 2),
                "active_power_true_kw": np.round(p_true, 2),
            }
        )

    # Now inject deterministic sensor & counter faults per asset and build cumulative registers
    all_Asset_dfs: list[pd.DataFrame] = []
    fault_Windows = [
        # (asset_id, start_idx, length_min, fault_type)
        ("WTG-01", 620, 60, "FLATLINE"),
        ("WTG-02", 1840, 75, "FLATLINE"),
        ("WTG-03", 3120, 50, "FLATLINE"),
        ("WTG-05", 4890, 90, "FLATLINE"),
        ("PV-INV-01", 2160, 60, "FLATLINE"),
        ("PV-INV-02", 5040, 45, "FLATLINE"),
        ("WTG-07", 7400, 60, "FLATLINE"),
        ("PV-INV-03", 7920, 60, "FLATLINE"),
        # Spikes / nighttime irradiance offsets / ultrasonic wind glitches
        ("PV-INV-01", 780, 12, "SPIKE_OR_BOUNDS"),
        ("PV-INV-02", 1480, 20, "SPIKE_OR_BOUNDS"),
        ("PV-INV-03", 3610, 15, "SPIKE_OR_BOUNDS"),
        ("PV-INV-04", 5820, 18, "SPIKE_OR_BOUNDS"),
        ("WTG-04", 2500, 10, "SPIKE_OR_BOUNDS"),
        ("WTG-06", 6100, 14, "SPIKE_OR_BOUNDS"),
        ("WTG-08", 8300, 12, "SPIKE_OR_BOUNDS"),
        # Cross-sensor physical mismatches (frozen low wind or dark pyranometer while power is high)
        ("WTG-01", 4100, 35, "CROSS_SENSOR_MISMATCH"),
        ("WTG-03", 5500, 40, "CROSS_SENSOR_MISMATCH"),
        ("WTG-09", 6800, 30, "CROSS_SENSOR_MISMATCH"),
        ("PV-INV-01", 3540, 25, "CROSS_SENSOR_MISMATCH"),
        ("PV-INV-04", 6420, 30, "CROSS_SENSOR_MISMATCH"),
        # Counter rollovers (uint32 wrap) & PLC zero-resets (single-minute transition events)
        ("WTG-01", 1500, 1, "COUNTER_RESET"),
        ("WTG-02", 3300, 1, "COUNTER_RESET"),
        ("WTG-04", 5100, 1, "COUNTER_RESET"),
        ("WTG-06", 7200, 1, "COUNTER_RESET"),
        ("PV-INV-01", 2300, 1, "COUNTER_RESET"),
        ("PV-INV-02", 6600, 1, "COUNTER_RESET"),
        ("PCC-MTR-01", 4400, 1, "COUNTER_RESET"),
        ("PCC-MTR-02", 8100, 1, "COUNTER_RESET"),
    ]

    for spec in specs:
        df = unit_frames[spec.asset_id].copy()
        w_meas = df["wind_speed_true_ms"].to_numpy().copy()
        poa_meas = df["poa_true_wm2"].to_numpy().copy()
        p_meas = df["active_power_true_kw"].to_numpy().copy()
        fault_label = np.full(n_min, "HEALTHY", dtype=object)

        # True energy increment [kWh] over each 1-minute interval
        delta_kwh_true = p_meas / 60.0
        # Start near uint32 wrap for assets that undergo rollover, or standard base register
        base_reg = 4_294_850.0 if spec.asset_id in ("WTG-01", "PV-INV-01", "PCC-MTR-01") else 125_000.0
        kwh_reg = base_reg + np.cumsum(delta_kwh_true)

        for f_aid, s_i, length, f_type in fault_Windows:
            if f_aid != spec.asset_id:
                continue
            e_i = min(s_i + length, n_min)
            fault_label[s_i:e_i] = f_type
            if f_type == "FLATLINE":
                if spec.asset_type == "wind":
                    stuck_val = max(float(w_meas[s_i]), 7.2)
                    w_meas[s_i:e_i] = stuck_val
                else:
                    stuck_val = max(float(poa_meas[s_i]), 540.0)
                    poa_meas[s_i:e_i] = stuck_val
            elif f_type == "SPIKE_OR_BOUNDS":
                if spec.asset_type == "pv":
                    for k in range(s_i, e_i):
                        if df["solar_elevation_deg"].iloc[k] > 0.0:
                            if k == s_i:
                                poa_meas[k] = 1335.0
                            else:
                                poa_meas[k] = 1620.0 + (k - s_i) * 12.0
                        else:
                            poa_meas[k] = 32.0 if k == s_i else 78.5
                else:
                    w_meas[s_i:e_i] = 44.5
                    if spec.asset_id in ("WTG-04", "WTG-06"):
                        w_meas[s_i] = 36.8
            elif f_type == "CROSS_SENSOR_MISMATCH":
                if spec.asset_type == "wind":
                    # Anemometer iced at 1.4 m/s (with 1-min icing onset ramp at 2.85 m/s) while turbine exports > 2,100 kW
                    w_meas[s_i:e_i] = 1.4 + 0.08 * np.sin(np.arange(e_i - s_i, dtype=float))
                    w_meas[s_i] = 2.85
                    p_meas[s_i:e_i] = 2280.0
                else:
                    # Pyranometer covered at 12 W/m2 (with 1-min onset ramp at 45 W/m2) while inverter exports > 3,600 kW
                    poa_meas[s_i:e_i] = 12.0 + 0.4 * np.sin(np.arange(e_i - s_i, dtype=float))
                    poa_meas[s_i] = 45.0
                    p_meas[s_i:e_i] = 3850.0
            elif f_type == "COUNTER_RESET":
                # Either uint32 wrap or PLC reboot reset
                if spec.asset_id in ("WTG-01", "PV-INV-01", "PCC-MTR-01"):
                    # Force exact uint32 wrap at s_i
                    shift = UINT32_ROLLOVER_KWH - kwh_reg[s_i - 1] - 0.2
                    kwh_reg[:s_i] += shift
                    kwh_reg[s_i:] = kwh_reg[s_i:] + shift - UINT32_ROLLOVER_KWH
                else:
                    # PLC reboot resets volatile accumulator to 0.0 at s_i
                    kwh_reg[s_i:] -= kwh_reg[s_i - 1]

        df["wind_speed_ms"] = np.round(w_meas, 3)
        df["poa_wm2"] = np.round(poa_meas, 2)
        df["active_power_kw"] = np.round(p_meas, 2)
        df["delta_kwh_true"] = np.round(delta_kwh_true, 4)
        df["kwh_total"] = np.round(kwh_reg, 4)
        df["injected_fault"] = fault_label

        # Default ingestion timestamp: 1.5 to 4.0 seconds after event timestamp
        jitter_s = rng.uniform(1.2, 3.8, size=n_min)
        ingested_at = df["timestamp_utc"] + pd.to_timedelta(jitter_s, unit="s")
        is_delayed = np.zeros(n_min, dtype=bool)
        outage_id_col = np.full(n_min, "", dtype=object)

        for out_ev in outages:
            if out_ev.asset_id != spec.asset_id:
                continue
            end_utc = out_ev.start_utc + pd.Timedelta(int(out_ev.duration_min), unit="min")
            mask = (df["timestamp_utc"] >= out_ev.start_utc) & (df["timestamp_utc"] < end_utc)
            n_out = int(np.sum(mask))
            if n_out > 0:
                is_delayed[mask] = True
                outage_id_col[mask] = out_ev.outage_id
                # Buffered packets flush in a burst starting at end_utc + 30s over flush_duration_min
                flush_offsets_s = np.linspace(15.0, out_ev.flush_duration_min * 60.0 - 10.0, n_out)
                # Shuffle slightly inside the burst to mimic out-of-order MQTT QoS-1 batching
                perm = rng.permutation(n_out)
                ingested_at.loc[mask] = end_utc + pd.to_timedelta(flush_offsets_s[perm], unit="s")

        df["ingested_at"] = ingested_at
        df["is_outage_backfill"] = is_delayed
        df["outage_id"] = outage_id_col
        df["is_duplicate_retry"] = False
        all_Asset_dfs.append(df)

    truth_df = pd.concat(all_Asset_dfs, ignore_index=True)

    # Inject 1.8% duplicate packet retries in the Bronze stream
    n_total = len(truth_df)
    n_dups = int(round(0.018 * n_total))
    dup_indices = rng.choice(n_total, size=n_dups, replace=False)
    dups = truth_df.iloc[dup_indices].copy()
    dups["ingested_at"] = dups["ingested_at"] + pd.to_timedelta(rng.uniform(2.0, 25.0, size=n_dups), unit="s")
    dups["is_duplicate_retry"] = True

    bronze_df = (
        pd.concat([truth_df, dups], ignore_index=True)
        .sort_values("ingested_at", kind="mergesort")
        .reset_index(drop=True)
    )
    return truth_df, bronze_df, outages
