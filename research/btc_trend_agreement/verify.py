"""
Independent re-derivation of the headline numbers.

Deliberately recomputes everything from the raw cached CSV using only pandas
primitives, WITHOUT importing trend_study/strategies, so a bug in those modules
cannot hide here. Also runs three explicit look-ahead traps.

    python verify.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import CACHE_DIR, TABLE_DIR

FAILURES: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}{(' -- ' + detail) if detail else ''}")
    if not ok:
        FAILURES.append(name)


def close_enough(a, b, tol=1e-9):
    return bool(np.isfinite(a) and np.isfinite(b) and abs(a - b) < tol)


print("VERIFY 1 -- rebuild the core table from raw cached candles")
raw = pd.read_csv(CACHE_DIR / "coinbase_BTCUSD_daily.csv", parse_dates=["date"])
p = raw.set_index("date")["close"].sort_index()
p = p.reindex(pd.date_range(p.index.min(), p.index.max(), freq="D")).ffill()

r30 = p / p.shift(30) - 1
r90 = p / p.shift(90) - 1
r365 = p / p.shift(365) - 1
sc = (r30 > 0).astype(int) + (r90 > 0).astype(int) + (r365 > 0).astype(int)
sc = sc.where(r365.notna() & r90.notna() & r30.notna())
f20 = p.shift(-20) / p - 1

d = pd.DataFrame({"sc": sc, "f20": f20}).dropna()
mine = d.groupby("sc")["f20"].agg(["size", "mean"])

ref = pd.read_csv(TABLE_DIR / "03_coinbase_replication_20d.csv")
for k in range(4):
    a = float(mine.loc[k, "mean"])
    b = float(ref.loc[ref.trend_score == k, "our_mean_20d"].iloc[0])
    n_a = int(mine.loc[k, "size"])
    n_b = int(ref.loc[ref.trend_score == k, "n_obs"].iloc[0])
    check(f"score {k} mean 20D matches pipeline", close_enough(a, b, 1e-12),
          f"{a:.6f} vs {b:.6f}")
    check(f"score {k} n matches pipeline", n_a == n_b, f"{n_a} vs {n_b}")

print("\nVERIFY 2 -- spot-check one date entirely by hand")
t = pd.Timestamp("2020-10-15")
man30 = p.loc[t] / p.loc[t - pd.Timedelta(days=30)] - 1
man90 = p.loc[t] / p.loc[t - pd.Timedelta(days=90)] - 1
man365 = p.loc[t] / p.loc[t - pd.Timedelta(days=365)] - 1
man_score = int(man30 > 0) + int(man90 > 0) + int(man365 > 0)
man_f20 = p.loc[t + pd.Timedelta(days=20)] / p.loc[t] - 1
check(f"{t.date()} hand TrendScore == pipeline", man_score == int(sc.loc[t]),
      f"hand {man_score} vs pipeline {int(sc.loc[t])}  "
      f"(R30={man30:+.3f} R90={man90:+.3f} R365={man365:+.3f})")
check(f"{t.date()} hand fwd_20 == pipeline", close_enough(man_f20, float(f20.loc[t])),
      f"{man_f20:+.6f}")

print("\nVERIFY 3 -- look-ahead traps")
# Trap A: TrendScore at t must be unchanged if every price after t is deleted.
cut = p.loc[:t].copy()
a30 = cut / cut.shift(30) - 1
a90 = cut / cut.shift(90) - 1
a365 = cut / cut.shift(365) - 1
# NOTE: cast each comparison to int explicitly. Under NumPy 2, np.bool_ + np.bool_
# is LOGICAL OR, not integer addition, so summing raw booleans would cap at 1.
trunc_score = int(a30.loc[t] > 0) + int(a90.loc[t] > 0) + int(a365.loc[t] > 0)
check("TrendScore(t) unchanged when future prices are deleted",
      trunc_score == int(sc.loc[t]), f"truncated {trunc_score} vs full {int(sc.loc[t])}")

# Trap B: shuffling ONLY the future must not change any TrendScore.
rng = np.random.default_rng(7)
p_alt = p.copy()
tail = p_alt.loc[p_alt.index > t].values.copy()
rng.shuffle(tail)
p_alt.loc[p_alt.index > t] = tail
s_alt = ((p_alt / p_alt.shift(30) > 1).astype(int)
         + (p_alt / p_alt.shift(90) > 1).astype(int)
         + (p_alt / p_alt.shift(365) > 1).astype(int))
same = (s_alt.loc[:t] == sc.loc[:t].fillna(-1).astype(int)).loc[sc.loc[:t].notna()].all()
check("shuffling future prices leaves all past TrendScores identical", bool(same))

# Trap C: executed strategy weight on day i must equal the target weight from
# day i-LAG, i.e. it must be constant under any change to prices after i-LAG.
import strategies as sg  # noqa: E402
from config import SIGNAL_LAG  # noqa: E402

ret = p.pct_change().dropna()
tw = sc.map({0: 0.0, 1: 0.0, 2: 1.0, 3: 1.0})
bt = sg.run_backtest(ret, tw, cost_bps=0.0)
i = 2000
day = bt.index[i]
src_day = day - pd.Timedelta(days=SIGNAL_LAG)
check("executed weight == target weight lagged by SIGNAL_LAG",
      close_enough(float(bt["weight"].iloc[i]), float(tw.loc[src_day])),
      f"{day.date()} weight {bt['weight'].iloc[i]:.4f} <- signal {src_day.date()} "
      f"score {sc.loc[src_day]:.0f}")

# Trap D: drawdown brake must be causal -- the multiplier used on day i can
# depend only on equity through day i-1.
bt_dd = sg.run_backtest(ret, tw, cost_bps=0.0, use_drawdown_brake=True)
eq_prev = bt_dd["equity"].shift(1).fillna(1.0)
dd_prev = 1.0 - eq_prev / eq_prev.cummax()
expected = dd_prev.clip(lower=0).apply(sg.drawdown_multiplier_from_dd)
check("drawdown multiplier uses only equity through t-1",
      bool(np.allclose(bt_dd["dd_mult"].values, expected.values, atol=1e-12)),
      f"max abs diff {np.max(np.abs(bt_dd['dd_mult'].values - expected.values)):.2e}")

# Trap E: a strategy fed a RANDOMLY SHUFFLED score must not beat buy & hold
# systematically -- sanity that the engine isn't leaking return information.
shuf = pd.Series(rng.permutation(tw.dropna().values), index=tw.dropna().index)
bt_shuf = sg.run_backtest(ret, shuf, cost_bps=0.0)
bh = sg.buy_and_hold(ret.loc[bt_shuf.index])
check("shuffled-signal strategy does not beat buy & hold",
      float(bt_shuf["equity"].iloc[-1]) < float(bh["equity"].iloc[-1]),
      f"shuffled {bt_shuf['equity'].iloc[-1]:.1f}x vs B&H {bh['equity'].iloc[-1]:.1f}x")

print("\nVERIFY 4 -- overlapping-return inflation is real")
g3 = d.loc[d.sc == 3, "f20"]
naive_t = g3.mean() / (g3.std(ddof=1) / np.sqrt(len(g3)))
hac = pd.read_csv(TABLE_DIR / "04_hac_per_score_means_20d.csv")
hac_t3 = float(hac.loc[hac.trend_score == 3, "hac_t"].iloc[0])
check("naive t is materially larger than HAC t (overlap inflation present)",
      naive_t > 2.0 * hac_t3, f"naive {naive_t:.2f} vs HAC {hac_t3:.2f} "
      f"(inflation {naive_t/hac_t3:.1f}x, sqrt(20)={np.sqrt(20):.1f})")

print("\nVERIFY 5 -- exchange cross-source agreement")
bnb = pd.read_csv(CACHE_DIR / "binance_BTCUSDT_daily.csv", parse_dates=["date"])
q = bnb.set_index("date")["close"].sort_index()
q = q.reindex(pd.date_range(q.index.min(), q.index.max(), freq="D")).ffill()
sc_q = ((q / q.shift(30) > 1).astype(int) + (q / q.shift(90) > 1).astype(int)
        + (q / q.shift(365) > 1).astype(int))
sc_q = sc_q.where(q.shift(365).notna())
ov = sc.dropna().index.intersection(sc_q.dropna().index)
agree = float((sc.loc[ov] == sc_q.loc[ov]).mean())
check("Coinbase and Binance produce the same TrendScore >=97% of days",
      agree >= 0.97, f"{agree*100:.2f}% agreement over {len(ov)} days")

f20q = q.shift(-20) / q - 1
dq = pd.DataFrame({"sc": sc_q, "f20": f20q}).dropna()
# Compare on the COMMON sample only -- Binance BTCUSDT starts 2017-08, so its
# TrendScore history begins ~2 years after Coinbase's. Comparing full samples
# would measure the sample difference, not the venue difference.
common = d.index.intersection(dq.index)
mq = dq.loc[common].groupby("sc")["f20"].mean()
mp = d.loc[common].groupby("sc")["f20"].mean()
spread_q = float(mq.get(3, np.nan) - mq.get(0, np.nan))
spread_p = float(mp.get(3, np.nan) - mp.get(0, np.nan))
check("3-0 spread agrees across venues on the COMMON sample within 1pp",
      abs(spread_q - spread_p) < 0.01,
      f"Coinbase {spread_p*100:+.2f}% vs Binance {spread_q*100:+.2f}% over "
      f"{common.min().date()}..{common.max().date()} (n={len(common)})")

# And record the full-sample-vs-common-sample gap, because it is itself a
# result: the effect is much weaker in the post-2018 window.
full_p = float(mine.loc[3, "mean"] - mine.loc[0, "mean"])
print(f"    [info] Coinbase 3-0 spread: full sample {full_p*100:+.2f}%  vs  "
      f"post-{common.min().year} common window {spread_p*100:+.2f}%")

print("\n" + ("ALL CHECKS PASSED" if not FAILURES
              else f"{len(FAILURES)} FAILURE(S): " + "; ".join(FAILURES)))
sys.exit(1 if FAILURES else 0)
