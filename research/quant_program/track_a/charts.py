"""Track A figures (built from results/tables/track_a/*)."""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qlib import stats  # noqa: E402
from qlib.paths import FIGURES, TABLES  # noqa: E402

T = TABLES / "track_a"
F = FIGURES / "track_a"
F.mkdir(parents=True, exist_ok=True)
SURF, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
plt.rcParams.update({"figure.facecolor": SURF, "axes.facecolor": SURF, "savefig.facecolor": SURF,
                     "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
                     "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.spines.top": False,
                     "axes.spines.right": False, "font.size": 9, "axes.titlesize": 10, "legend.frameon": False})
SYS_LABEL = {"GERHARD_SMA120": "Gerhard SMA120", "LL3_k0.30": "LL 3-state", "LL2_k0.30": "LL 2-state"}
SIGMA_ORDER = ["<0.5", "0.5-1", "1-1.5", "1.5-2", "2-2.5", "2.5-3", ">3"]


def save(fig, name):
    fig.tight_layout()
    fig.savefig(F / name, dpi=150)
    plt.close(fig)


def persistence():
    t = pd.read_csv(T / "conditional_by_system.csv")
    systems = [s for s in SYS_LABEL if s in set(t["system"])]
    fig, axes = plt.subplots(2, len(systems), figsize=(4 * len(systems), 6.5), sharey=True, squeeze=False)
    for j, s in enumerate(systems):
        for i, feat in enumerate(("ret_z20", "brk_vol")):
            ax = axes[i, j]
            g = t[(t.system == s) & (t.feature == feat)].set_index("bucket").reindex(SIGMA_ORDER)
            x = np.arange(len(g))
            for k, (oc, lab) in enumerate((("confirmed", "P(3-close confirmation)"), ("valid_10d", "P(still valid at day 10)"))):
                y = g[f"{oc}_rate"] * 100
                err = np.vstack([(g[f"{oc}_rate"] - g[f"{oc}_lo"]) * 100, (g[f"{oc}_hi"] - g[f"{oc}_rate"]) * 100])
                ax.errorbar(x + (k - 0.5) * 0.18, y, yerr=err, fmt="o", color=C[k], ms=5, ecolor=AXIS, elinewidth=1.5,
                            capsize=0, label=lab)
            for xi, n in zip(x, g["n"].fillna(0).astype(int)):
                ax.text(xi, 2, f"n={n}", ha="center", fontsize=7, color=MUTED)
            ax.set_xticks(x, SIGMA_ORDER, fontsize=8)
            ax.set_ylim(0, 105)
            ax.set_title(f"{SYS_LABEL[s]} — day-1 {'return z' if feat == 'ret_z20' else 'break / σ'}", loc="left")
        axes[0, j].legend(fontsize=7, loc="lower right")
    axes[0, 0].set_ylabel("%, BTC+ETH+SOL pooled (95% CI)")
    axes[1, 0].set_ylabel("%, BTC+ETH+SOL pooled (95% CI)")
    save(fig, "A1_persistence_by_day1_strength.png")


def economics():
    ev = pd.read_parquet(T / "events.parquet")
    systems = [s for s in SYS_LABEL if s in set(ev["system"])]
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    for ax, feat in zip(axes, ("ret_z20", "brk_vol")):
        x = np.arange(len(SIGMA_ORDER))
        for k, s in enumerate(systems):
            g = ev[ev.system == s].assign(bucket=lambda d: pd.cut(d[feat], [-np.inf, .5, 1, 1.5, 2, 2.5, 3, np.inf],
                                                                   labels=SIGMA_ORDER))
            means, los, his = [], [], []
            for b in SIGMA_ORDER:
                h = g[g.bucket == b]
                if len(h) >= 5:
                    m, lo, hi = stats.cluster_bootstrap_ci(h["incr_m1"], h["cluster"], np.mean, 500)
                else:
                    m = lo = hi = np.nan
                means.append(m * 100), los.append(lo * 100), his.append(hi * 100)
            means, los, his = map(np.array, (means, los, his))
            ax.errorbar(x + (k - 1) * 0.22, means, yerr=[means - los, his - means], fmt="o", color=C[k], ms=5,
                        ecolor=AXIS, elinewidth=1.5, capsize=0, label=SYS_LABEL[s])
        ax.axhline(0, color=INK2, lw=1)
        ax.set_xticks(x, SIGMA_ORDER)
        ax.set_title(f"Value of acting after 1 close vs 3, by day-1 {'return z' if feat == 'ret_z20' else 'break / σ'}",
                     loc="left")
        ax.set_xlabel("day-1 strength bucket")
    axes[0].set_ylabel("incremental P&L per signal, % (95% CI)")
    axes[0].legend(fontsize=8)
    save(fig, "A2_economics_early_entry_by_strength.png")


def sensitivity():
    s = pd.read_csv(T / "sensitivity_thresholds.csv")
    s = s[s.asset == "BTC"]
    systems = [x for x in SYS_LABEL if x in set(s["system"])]
    fig, axes = plt.subplots(2, len(systems), figsize=(4 * len(systems), 6), squeeze=False)
    for j, sy in enumerate(systems):
        for i, win in enumerate(("full", "test_2024+")):
            ax = axes[i, j]
            g = s[(s.system == sy) & (s.window == win)]
            for k, feat in enumerate(("ret_z20", "brk_vol", "range_breakout_20", "move_atr")):
                h = g[g.feature == feat].sort_values("threshold")
                ax.plot(h["threshold"], h["Sharpe"], marker="o", ms=3, lw=1.6, color=C[k], label=feat)
            if len(g):
                ax.axhline(g["M0_sharpe"].iloc[0], color=INK2, lw=1, ls="--", label="3-close (M0)")
            ax.set_title(f"BTC {SYS_LABEL[sy]} — {win}", loc="left")
            ax.set_xlabel("fast-track threshold")
        axes[0, j].legend(fontsize=7)
    axes[0, 0].set_ylabel("Sharpe")
    axes[1, 0].set_ylabel("Sharpe")
    save(fig, "A3_threshold_sensitivity_BTC.png")


def models_oos():
    b = pd.read_csv(T / "bootstrap_vs_M0.csv")
    b = b[(b.exposure == "long_flat") & (b.window == "walk_forward_oos")]
    b["label"] = b["asset"] + " " + b["system"].map(SYS_LABEL)
    labels = sorted(b["label"].unique())
    models = [m for m in ("M1_1close", "M2_2close", "M3_linear", "M4_strength", "M5_override", "M6_probability")
              if m in set(b["model"])]
    fig, ax = plt.subplots(figsize=(11, 4.2))
    x = np.arange(len(labels))
    wdt = 0.8 / len(models)
    for k, m in enumerate(models):
        g = b[b.model == m].set_index("label").reindex(labels)
        ax.errorbar(x + (k - len(models) / 2) * wdt + wdt / 2, g["dSharpe"],
                    yerr=[g["dSharpe"] - g["dSharpe_lo"], g["dSharpe_hi"] - g["dSharpe"]], fmt="o", ms=4,
                    color=C[k], ecolor=AXIS, elinewidth=1.2, capsize=0, label=m)
    ax.axhline(0, color=INK2, lw=1)
    ax.set_xticks(x, labels, rotation=25, ha="right", fontsize=8)
    ax.set_ylabel("Sharpe minus 3-close, walk-forward OOS (95% CI)")
    ax.set_title("Out-of-sample execution-model comparison (long/flat)", loc="left")
    ax.legend(fontsize=7, ncol=3)
    save(fig, "A4_models_walk_forward_vs_3close.png")


def rarity():
    ev = pd.read_parquet(T / "events.parquet")
    ev = ev[ev.system == "GERHARD_SMA120"]
    big = ev[ev["ret_z20"] >= 3]
    tab = big.groupby(["year", "asset"]).size().unstack(fill_value=0)
    fig, ax = plt.subplots(figsize=(8, 3.4))
    bottom = np.zeros(len(tab))
    for k, a in enumerate(tab.columns):
        ax.bar(tab.index.astype(str), tab[a], bottom=bottom, color=C[k], label=a, width=0.6)
        bottom += tab[a].to_numpy()
    ax.set_ylabel("first closes with return z ≥ 3")
    ax.set_title("How rare are August-2026-like first closes? (Gerhard SMA120)", loc="left")
    ax.legend(fontsize=8)
    save(fig, "A5_rarity_large_day1_moves.png")


def auc_heat():
    a = pd.read_csv(T / "auc_by_system.csv")
    # One feature order for both panels (ordered by Gerhard confirmation AUC). Panels must NOT share the y axis:
    # with sharey=True the second panel's tick labels silently relabel the first panel's rows.
    base = a[(a.outcome == "confirmed")].pivot(index="feature", columns="system", values="AUC")
    order = base.mean(axis=1).sort_values(ascending=False).index.tolist()
    order += sorted(set(a["feature"]) - set(order))
    fig, axes = plt.subplots(1, 2, figsize=(11, 5), sharey=False)
    for ax, oc in zip(axes, ("confirmed", "valid_10d")):
        g = a[a.outcome == oc].pivot(index="feature", columns="system", values="AUC")
        g = g[[c for c in SYS_LABEL if c in g.columns]].reindex(order)
        im = ax.imshow(g.to_numpy(), cmap="RdBu", vmin=0.3, vmax=0.7, aspect="auto")
        for i in range(g.shape[0]):
            for j in range(g.shape[1]):
                v = g.iat[i, j]
                ax.text(j, i, "" if pd.isna(v) else f"{v:.2f}", ha="center", va="center", fontsize=7)
        ax.set_xticks(range(g.shape[1]), [SYS_LABEL[c] for c in g.columns], fontsize=8)
        # same row order in both panels: label rows once (left panel) to avoid overlapping text
        ax.set_yticks(range(g.shape[0]), g.index if oc == "confirmed" else [""] * g.shape[0], fontsize=8)
        ax.set_title(f"AUC of day-1 feature for outcome: {oc}", loc="left")
        ax.grid(False)
    fig.colorbar(im, ax=axes, shrink=0.7)
    fig.savefig(F / "A6_auc_day1_features.png", dpi=150, bbox_inches="tight")
    plt.close(fig)


if __name__ == "__main__":
    for fn in (persistence, economics, sensitivity, models_oos, rarity, auc_heat):
        try:
            fn()
            print("ok", fn.__name__)
        except Exception as e:  # noqa: BLE001
            print("FAILED", fn.__name__, repr(e))
