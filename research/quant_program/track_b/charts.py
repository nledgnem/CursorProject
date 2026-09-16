"""Track B figures (built from results/tables/track_b/*)."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib.paths import FIGURES, TABLES  # noqa: E402

T = TABLES / "track_b"
F = FIGURES / "track_b"
F.mkdir(parents=True, exist_ok=True)
SURF, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
FAM_COLOR = {"funding": C[0], "basis": C[1], "listings": C[2], "mechanics": C[3], "liquidation": C[4],
             "oi_price": C[5], "interaction": C[6], "cross_venue": C[7]}
plt.rcParams.update({"figure.facecolor": SURF, "axes.facecolor": SURF, "savefig.facecolor": SURF,
                     "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
                     "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.spines.top": False,
                     "axes.spines.right": False, "font.size": 9, "axes.titlesize": 10, "legend.frameon": False})


def save(fig, name):
    fig.tight_layout()
    fig.savefig(F / name, dpi=150)
    plt.close(fig)


def signal_overview():
    r = pd.read_csv(T / "signal_results.csv")
    r = r[r["OOS_net_mean"].notna() | r["IS_net_mean"].notna()].copy()
    r = r.sort_values("OOS_net_mean", na_position="first")
    fig, ax = plt.subplots(figsize=(9, 0.32 * len(r) + 1.5))
    y = np.arange(len(r))
    col = [FAM_COLOR.get(f, MUTED) for f in r["family"]]
    ax.scatter(r["IS_net_mean"] * 100, y + 0.15, marker="s", s=22, color=AXIS, label="in-sample (<2024)", zorder=3)
    lo = (r["OOS_net_mean"] - r["OOS_net_lo"]) * 100
    hi = (r["OOS_net_hi"] - r["OOS_net_mean"]) * 100
    ax.errorbar(r["OOS_net_mean"] * 100, y - 0.15, xerr=[lo, hi], fmt="none", ecolor=AXIS, elinewidth=1.2, zorder=2)
    ax.scatter(r["OOS_net_mean"] * 100, y - 0.15, s=34, color=col, label="out-of-sample (2024+), 95% CI", zorder=4)
    ax.axvline(0, color=INK2, lw=1)
    ax.set_yticks(y, r["signal"], fontsize=8)
    ax.set_xlabel("net return per event at primary horizon, % (BTC-hedged, after costs and funding)")
    ax.set_title("Track B pre-registered signals: in-sample vs out-of-sample", loc="left")
    ax.legend(fontsize=8, loc="lower right")
    save(fig, "B1_signal_overview_IS_vs_OOS.png")


def cost_resilience():
    r = pd.read_csv(T / "signal_results.csv").dropna(subset=["breakeven_rt_bp_ALL", "median_round_trip_bp"])
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    for fam, g in r.groupby("family"):
        ax.scatter(g["median_round_trip_bp"], g["breakeven_rt_bp_ALL"], s=40, color=FAM_COLOR.get(fam, MUTED), label=fam)
        for x in g.itertuples():
            ax.annotate(x.signal.split("_")[0], (x.median_round_trip_bp, x.breakeven_rt_bp_ALL), fontsize=7,
                        xytext=(3, 3), textcoords="offset points", color=INK2)
    m = max(r["median_round_trip_bp"].max(), 50) * 1.2
    ax.plot([0, m], [0, m], color=INK2, lw=1, ls="--", label="break-even = modelled cost")
    ax.plot([0, m], [0, 2 * m], color=AXIS, lw=1, ls=":", label="2x cost buffer")
    ax.axhline(0, color=INK2, lw=0.8)
    ax.set_xlabel("median modelled round-trip cost, bp")
    ax.set_ylabel("gross (hedged + funding) edge per event, bp")
    ax.set_title("Cost resilience: edge before costs vs modelled costs", loc="left")
    ax.legend(fontsize=7, ncol=2)
    save(fig, "B2_cost_resilience.png")


def portfolios(top: int = 6):
    ps = pd.read_csv(T / "portfolio_stats.csv")
    port = pd.read_csv(T / "portfolios_daily.csv", index_col=0, parse_dates=True)
    cards = T / "scorecards.csv"
    if cards.exists():
        order = pd.read_csv(cards)["name"].tolist()
    else:
        order = ps[ps.period == "OOS"].sort_values("Sharpe", ascending=False)["signal"].tolist()
    names = [n for n in order if n in port.columns][:top]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    for k, n in enumerate(names):
        s = port[n]
        first = s.ne(0).idxmax()
        eq = (1 + s.loc[first:]).cumprod()
        axes[0].plot(eq.index, eq, color=C[k % len(C)], lw=1.5, label=n)
        so = s.loc["2024-01-01":]
        axes[1].plot(so.index, (1 + so).cumprod(), color=C[k % len(C)], lw=1.5, label=n)
    for ax, t in zip(axes, ("Full history (event-portfolio, BTC-hedged, net)", "Out-of-sample 2024+")):
        ax.set_title(t, loc="left")
        ax.set_yscale("log")
        ax.axhline(1, color=INK2, lw=0.8)
    axes[0].legend(fontsize=7)
    save(fig, "B3_top_signal_portfolios.png")


def correlations():
    c = pd.read_csv(T / "correlations.csv", index_col=0)
    fig, ax = plt.subplots(figsize=(0.42 * len(c) + 3, 0.42 * len(c) + 2))
    im = ax.imshow(c.to_numpy(), cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(len(c)), c.columns, rotation=90, fontsize=7)
    ax.set_yticks(range(len(c)), c.index, fontsize=7)
    for i in range(len(c)):
        for j in range(len(c)):
            v = c.iat[i, j]
            if pd.notna(v) and abs(v) >= 0.2 and i != j:
                ax.text(j, i, f"{v:.1f}", ha="center", va="center", fontsize=6)
    ax.grid(False)
    ax.set_title("Daily return correlations (2021+): signal portfolios vs BTC, trend, majors-alts", loc="left")
    fig.colorbar(im, ax=ax, shrink=0.6)
    save(fig, "B4_correlations.png")


def intraday():
    d = pd.read_csv(T / "intraday_hour_dow.csv")
    h = d[d.bucket.str.startswith("hour_")].copy()
    h["hour"] = h["bucket"].str[5:].astype(int)
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.6), sharey=True)
    for ax, a in zip(axes, ("BTCUSDT", "ETHUSDT", "SOLUSDT")):
        for k, per in enumerate(("IS", "OOS")):
            g = h[(h.asset == a) & (h.period == per)].sort_values("hour")
            ax.errorbar(g["hour"] + (k - 0.5) * 0.3, g["mean_bp"], yerr=[g["mean_bp"] - g["lo_bp"], g["hi_bp"] - g["mean_bp"]],
                        fmt="o", ms=3, color=C[k], ecolor=AXIS, elinewidth=1, capsize=0, label=per)
        for s in (0, 8, 16):
            ax.axvline(s, color=C[3], lw=0.8, ls=":")
        ax.axhline(0, color=INK2, lw=0.8)
        ax.set_title(f"{a} mean 1h return by UTC hour (dotted = funding)", loc="left", fontsize=9)
        ax.set_xlabel("hour (UTC)")
    axes[0].set_ylabel("bp (95% CI)")
    axes[0].legend(fontsize=8)
    save(fig, "B5_intraday_hour_of_day.png")


def carry():
    p = T / "carry_trade_events.csv"
    if not p.exists():
        return
    c = pd.read_csv(p, parse_dates=["ts"])
    c["period"] = np.where(c["ts"] < "2024-01-01", "IS (<2024)", "OOS (2024+)")
    g = c.groupby("period")[["funding_received", "premium_gain", "cost", "net"]].mean() * 100
    g["cost"] = -g["cost"]
    fig, ax = plt.subplots(figsize=(7, 3.6))
    x = np.arange(len(g))
    for k, col in enumerate(["funding_received", "premium_gain", "cost", "net"]):
        ax.bar(x + (k - 1.5) * 0.2, g[col], 0.18, color=C[k], label=col)
    ax.axhline(0, color=INK2, lw=0.8)
    ax.set_xticks(x, g.index)
    ax.set_ylabel("% per 5-day trade")
    ax.set_title("Delta-neutral funding carry on extreme-funding events: P&L decomposition", loc="left")
    ax.legend(fontsize=8)
    save(fig, "B6_carry_decomposition.png")


if __name__ == "__main__":
    for fn in (signal_overview, cost_resilience, portfolios, correlations, intraday, carry):
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as e:  # noqa: BLE001
            print("FAILED", fn.__name__, repr(e))
