"""Matplotlib figure generator for scada-lakehouse-streaming (4 engineering charts in docs/img/)."""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


BLUE = "#1f4e78"
RED = "#c0392b"
GREEN = "#27ae60"
ORANGE = "#d35400"
GREY = "#7f8c8d"


def _apply_style() -> None:
    plt.rcdefaults()
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9.5,
            "axes.titlesize": 10.5,
            "axes.labelsize": 9.5,
            "xtick.labelsize": 8.5,
            "ytick.labelsize": 8.5,
            "legend.fontsize": 8.2,
            "figure.facecolor": "#ffffff",
            "axes.facecolor": "#ffffff",
            "axes.edgecolor": "#cccccc",
            "axes.grid": True,
            "grid.color": "#e6e6e6",
            "grid.linewidth": 0.7,
        }
    )


def _save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor="#ffffff")
    plt.close(fig)


def generate_all_figures(
    bench_df: pd.DataFrame,
    ingest_details: dict,
    dst_df: pd.DataFrame,
    dst_details: dict,
    dq_df: pd.DataFrame,
    dq_details: dict,
    img_dir: Path,
) -> None:
    """Generate all 4 PNG figures in docs/img/."""
    _apply_style()

    # -------------------------------------------------------------------------
    # Figure 1: lte_backfill_watermark.png
    # 2-panel: LTE outage & reconnect packet burst in ingestion time vs reconstructed 15-min power in event time
    # -------------------------------------------------------------------------
    sc = ingest_details["scenarios"]["OUT-06"]
    x_idx = np.arange(len(sc["timestamps"]))
    tick_step = max(len(x_idx) // 8, 1)
    tick_pos = x_idx[::tick_step]
    tick_lbl = [sc["timestamps"][i][11:16] for i in tick_pos]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.3))

    ax1.bar(x_idx, sc["naive_record_count"], color=ORANGE, alpha=0.82, width=0.72, label="Arriving 1-min packets / 15-min bucket")
    ax1.axhline(15.0, color=BLUE, ls="--", lw=1.2, label="Nominal 15 packets / quarter-hour")
    out_idx = np.flatnonzero(sc["is_outage_bucket"])
    ax1.axvspan(
        out_idx[0] - 0.5, out_idx[-1] + 0.5, color=RED, alpha=0.10,
        label=f"{sc['outage_minutes']} min LTE router outage (buffered on PLC)",
    )
    ax1.set_xticks(tick_pos, tick_lbl)
    ax1.set_xlabel(f"UTC time on {sc['timestamps'][0][:10]} [HH:MM]")
    ax1.set_ylabel("Packets arriving in ingestion window [-]")
    ax1.set_title("A. Broker arrival burst in processing time (ingested_at)")
    ax1.legend(loc="upper left", frameon=True)

    true_mw = np.array(sc["true_power_kw"]) / 1000.0
    naive_mw = np.array(sc["naive_power_kw"]) / 1000.0
    wm_mw = np.array(sc["watermarked_power_kw"]) / 1000.0

    upsert_rec = float(bench_df["delayed_intervals_recovered_pct"].iloc[-1])
    ax2.plot(
        x_idx, naive_mw, color=RED, lw=1.5, ls="-", marker="o", ms=3.2,
        label=f"Naive ingestion-time window ({sc['reconnect_ratio_x']:.1f}x true energy in reconnect bucket)",
    )
    ax2.plot(x_idx, true_mw, color=GREY, lw=2.2, ls=":", label=f"True {sc['asset_id']} export [MW]")
    ax2.plot(
        x_idx, wm_mw, color=GREEN, lw=1.6, ls="--",
        label=f"Event-time watermark W=6h + upsert ({upsert_rec:.1f}% rec.)",
    )
    ax2.set_xticks(tick_pos, tick_lbl)
    ax2.set_xlabel(f"UTC time on {sc['timestamps'][0][:10]} [HH:MM]")
    ax2.set_ylabel("15-min average export power [MW]")
    ax2.set_title("B. Reconstructed 15-min settlement power (timestamp_utc)")
    ax2.legend(loc="upper left", frameon=True)

    _save(fig, img_dir / "lte_backfill_watermark.png")

    # -------------------------------------------------------------------------
    # Figure 2: dst_transition_reconciliation.png
    # 25-hour October fall-back day showing naive local-time overwrite losing 1 hour vs UTC fold-aware disambiguation
    # -------------------------------------------------------------------------
    oct_rows = dst_details["oct25_hourly"]
    h_idx = np.arange(len(oct_rows))
    true_h_mwh = np.array([r["fleet_mwh"] for r in oct_rows], dtype=float)
    naive_h_mwh = np.array([r["naive_local_mwh"] for r in oct_rows], dtype=float)
    h_labels = [r["warsaw_hour_label"][:5] for r in oct_rows]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.3))

    oct_qh = int(dst_df.loc[dst_df["dst_date"] == "2026-10-25", "reconciled_quarter_hours"].iloc[0])
    rep_idx = [i for i, r in enumerate(oct_rows) if r["warsaw_hour_label"].startswith("02:00")]
    ax1.bar(
        h_idx - 0.18, true_h_mwh, width=0.36, color=BLUE,
        label=f"UTC fold-aware disambiguation ({len(oct_rows)} h = {oct_qh} qh)",
    )
    ax1.bar(h_idx + 0.18, naive_h_mwh, width=0.36, color=RED, alpha=0.85, label="Naive local-time key overwrite (02B overwrites 02A)")
    ax1.axvspan(min(rep_idx) - 0.4, max(rep_idx) + 0.4, color=ORANGE, alpha=0.14, label="Repeated 02:00 CEST (fold=0) / 02:00 CET (fold=1)")
    ax1.set_xticks(h_idx[::2], [h_labels[i] for i in h_idx[::2]], rotation=30)
    ax1.set_xlabel("Europe/Warsaw local hour on 2026-10-25 (25-hour fall-back day)")
    ax1.set_ylabel("Hourly fleet generation [MWh]")
    ax1.set_title("A. Hourly generation across 25-hour autumn DST transition")
    ax1.legend(loc="upper right", frameon=True)

    cum_true = np.cumsum(true_h_mwh)
    cum_naive = np.cumsum(naive_h_mwh)
    ax2.plot(h_idx, cum_true, color=BLUE, lw=2.0, marker="s", ms=3.5, label=f"Reconciled UTC cumulative ({cum_true[-1]:.1f} MWh)")
    ax2.plot(h_idx, cum_naive, color=RED, lw=1.8, ls="--", marker="o", ms=3.2, label=f"Naive local overwrite ({cum_naive[-1]:.1f} MWh, -{cum_true[-1]-cum_naive[-1]:.2f} MWh)")
    ax2.fill_between(h_idx, cum_naive, cum_true, color=RED, alpha=0.12, label="Lost 02:00 CEST hour deficit")
    ax2.set_xticks(h_idx[::2], [h_labels[i] for i in h_idx[::2]], rotation=30)
    ax2.set_xlabel("Europe/Warsaw local hour on 2026-10-25")
    ax2.set_ylabel("Cumulative daily fleet energy [MWh]")
    ax2.set_title("B. Cumulative daily settlement energy drift")
    ax2.legend(loc="upper left", frameon=True)

    _save(fig, img_dir / "dst_transition_reconciliation.png")

    # -------------------------------------------------------------------------
    # Figure 3: dq_fault_detection.png
    # 3-panel: flatline, irradiance spike, and counter uint32 rollover recovery on 1-minute telemetry
    # -------------------------------------------------------------------------
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(12.6, 4.0))

    fl = dq_details["sample_flatline_wtg01"]
    t_fl = np.arange(len(fl["timestamps"]))
    ax1.plot(t_fl, fl["wind_true_ms"], color=GREY, lw=1.4, ls=":", label="True wind speed [m/s]")
    ax1.plot(t_fl, fl["wind_raw_ms"], color=RED, lw=1.6, label="Raw frozen ultrasonic anemometer")
    ax1.plot(t_fl, fl["wind_clean_ms"], color=GREEN, lw=1.4, ls="--", label="Silver QC-imputed wind speed")
    flat_idx = np.flatnonzero(np.asarray(fl["dq_flag"]) == "FLATLINE")
    ax1.axvspan(flat_idx[0], flat_idx[-1] + 1, color=RED, alpha=0.10, label="Flagged FLATLINE (std=0 >= 30m)")
    ax1.set_xlabel("Elapsed minutes (WTG-01) [min]")
    ax1.set_ylabel("Wind speed [m/s]")
    ax1.set_title("A. Frozen anemometer flatline QC")
    ax1.legend(loc="lower left", frameon=True, fontsize=7.5)

    sp = dq_details["sample_spike_pv01"]
    t_sp = np.arange(len(sp["timestamps"]))
    ax2.plot(t_sp, sp["poa_raw_wm2"], color=RED, lw=1.6, label="Raw pyranometer (EMI spike > 1350 W/m2)")
    ax2.plot(t_sp, sp["poa_true_wm2"], color=GREY, lw=1.8, ls=":", label="True POA irradiance [W/m2]")
    ax2.plot(t_sp, sp["poa_clean_wm2"], color=GREEN, lw=1.4, ls="--", label="Silver QC-cleaned irradiance")
    ax2.axhline(1350.0, color=ORANGE, ls="--", lw=1.1, label="Physical upper bound (1,350 W/m2)")
    ax2.set_xlabel("Elapsed minutes (PV-INV-01) [min]")
    ax2.set_ylabel("POA irradiance [W/m2]")
    ax2.set_title("B. Pyranometer EMI spike & bounds QC")
    ax2.legend(loc="upper right", frameon=True, fontsize=7.5)

    cn = dq_details["sample_counter_wtg01"]
    t_cn = np.arange(len(cn["timestamps"]))
    raw_cn = np.asarray(cn["raw_delta_kwh"], dtype=float)
    rec_cn = np.asarray(cn["reconciled_delta_kwh"], dtype=float)
    wrap_i = int(np.argmin(raw_cn))
    y_lim = 1.6 * float(np.max(np.abs(rec_cn)))
    ax3.plot(t_cn, raw_cn, color=RED, lw=1.5, label=f"Raw diff (uint32 wrap: {raw_cn[wrap_i]:.3g} kWh)")
    ax3.plot(t_cn, rec_cn, color=BLUE, lw=1.8, label="Reconciled modulo-2^32 delta_kWh")
    ax3.set_ylim(-y_lim, y_lim)
    ax3.annotate(
        f"Raw wrap delta = {raw_cn[wrap_i]:,.0f} kWh\n(reconciled to {rec_cn[wrap_i]:+.1f} kWh)",
        xy=(wrap_i, -0.92 * y_lim),
        xytext=(max(wrap_i - 24, 1), -0.7 * y_lim),
        arrowprops={"arrowstyle": "->", "color": RED, "lw": 1.1},
        fontsize=8.0,
        color=RED,
    )
    ax3.set_xlabel("Elapsed minutes (WTG-01) [min]")
    ax3.set_ylabel("1-min energy increment [kWh]")
    ax3.set_title("C. Cumulative counter rollover recovery")
    ax3.legend(loc="upper right", frameon=True, fontsize=7.5)

    _save(fig, img_dir / "dq_fault_detection.png")

    # -------------------------------------------------------------------------
    # Figure 4: medallion_latency_completeness.png
    # Delayed-interval recovery & outage-affected energy nMAE / RMSE across naive and watermarked modes
    # -------------------------------------------------------------------------
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11.2, 4.2))

    labels = [
        "Naive\n(No Dedup)" if w == 0.0 else (f"W = {w:.1f} h\n+ Upsert" if "Upsert" in m else f"Watermark\nW = {w:.1f} h")
        for m, w in zip(bench_df["mode"], bench_df["watermark_horizon_h"])
    ]
    x = np.arange(len(bench_df))
    comp_vals = bench_df["delayed_intervals_recovered_pct"].to_numpy()
    mae_vals = bench_df["affected_buckets_energy_nmae_pct"].to_numpy()
    rmse_vals = bench_df["asset_15m_power_rmse_kw"].to_numpy()

    colors = [RED, ORANGE, ORANGE, BLUE, BLUE, GREEN]
    bars = ax1.bar(x, comp_vals, color=colors, width=0.58)
    for b, v in zip(bars, comp_vals):
        ax1.text(b.get_x() + b.get_width() / 2.0, v + 1.8, f"{v:.1f}%", ha="center", va="bottom", fontsize=8.2)
    ax1.set_xticks(x, labels, fontsize=8.0)
    ax1.set_ylim(0, 114.0)
    ax1.set_ylabel("Delayed LTE backfill intervals recovered [%]")
    ax1.set_title("A. Late-arriving interval recovery vs watermark horizon W")

    ax2_twin = ax2.twinx()
    ax2_twin.grid(False)
    l1 = ax2.plot(x, mae_vals, color=RED, lw=2.0, marker="o", ms=5.0, label="Outage-affected bucket energy nMAE [%] (left)")
    l2 = ax2_twin.plot(x, rmse_vals, color=BLUE, lw=1.8, ls="--", marker="s", ms=4.8, label="Per-asset 15-min power RMSE [kW] (right)")
    ax2.set_xticks(x, labels, fontsize=8.0)
    ax2.set_ylabel("Outage-affected bucket energy nMAE [%]", color=RED)
    ax2_twin.set_ylabel("Per-asset 15-min power RMSE [kW]", color=BLUE)
    ax2.set_ylim(-0.03 * max(mae_vals), max(mae_vals) * 1.18)
    ax2_twin.set_ylim(-20.0, max(rmse_vals) * 1.18)
    ax2.set_title("B. Outage-affected energy nMAE & power RMSE by mode")

    lines = l1 + l2
    lbls = [l.get_label() for l in lines]
    ax2.legend(lines, lbls, loc="upper right", frameon=True)

    _save(fig, img_dir / "medallion_latency_completeness.png")
