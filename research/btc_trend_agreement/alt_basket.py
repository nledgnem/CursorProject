"""
Does BTC TrendScore work as a crypto-beta risk-on filter on a BROAD alt basket?

THE PREDICTION BEING TESTED (fixed before running)
--------------------------------------------------
If TrendScore works because it identifies a market-wide risk-on regime rather
than because of anything BTC-specific, then:

  H1  A broad, point-in-time alt basket shows a LARGER TrendScore>=2 on-off
      spread than BTC itself, and it survives the 2022+ out-of-sample split.
  H2  The spread SCALES WITH BETA -- sorting basket members into trailing-beta
      quintiles, the on-off spread should increase monotonically from Q1 to Q5.
  H3  A 200-day SMA on BTC should NOT capture H1/H2 as well as TrendScore does
      (this is what separated them on SOL).

H2 is the real test, because it can fail. A spurious result has no reason to
order itself by beta.

FAILURE CONDITION: if H2 does not hold, the earlier SOL result is treated as
multiple-testing noise and the crypto-beta story is rejected.

UNIVERSE AND POINT-IN-TIME CONSTRUCTION
---------------------------------------
Universe      Binance spot USDT pairs. Stablecoins, BTC itself, wrapped BTC/ETH
              and leveraged UP/DOWN/BULL/BEAR tokens excluded.
Eligibility   at date t, a coin is eligible if it listed >= 180 days before t.
              That is knowable at t and makes trailing beta/volume estimable.
Selection     monthly rebalance; rank eligible coins by TRAILING 30-day quote
              volume and take the top N. Uses only past data.
Weighting     equal weight, membership frozen between rebalances.
Beta          trailing 180-day daily-return beta vs BTC, computed at the
              rebalance date from past data only.

SURVIVORSHIP BIAS -- STATED PLAINLY
------------------------------------
Binance's exchangeInfo returns only CURRENTLY TRADING symbols, so coins that
were delisted or died are absent. This is a real limitation and it cannot be
fixed from public endpoints (there is no historical symbol-list API, and the
repo's own single_coin_panel is likewise survivor-only and starts 2024-03).

Direction of the bias, which matters more than its existence:
  * Coins die disproportionately during and after risk-off periods, i.e. when
    BTC TrendScore is LOW.
  * Excluding them removes catastrophic returns mostly from the LOW-score
    bucket, making the low-score bucket look BETTER than reality.
  * That SHRINKS the on-off spread.
So survivorship bias works AGAINST the hypothesis here. A positive result is
conservative; a null result is genuinely ambiguous.

The trailing-volume screen also partially mitigates it: dying coins lose volume
and drop out of the top-N before delisting, while their bad returns are still
captured for as long as they remain in the basket.

    python alt_basket.py [--refresh] [--top-n 50]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import statsmodels.api as sm

sys.path.insert(0, str(Path(__file__).resolve().parent))

import data_io
import stats_tools as st
import trend_study as ts
from config import (CACHE_DIR, FIGURE_DIR, OOS_SPLIT_DATE, PRIMARY_HORIZON,
                    TABLE_DIR, hac_lags)

LAGS = hac_lags(PRIMARY_HORIZON)
MA_WINDOW = 200
MIN_LISTED_DAYS = 180
BETA_WINDOW = 180
VOL_WINDOW = 30
N_BETA_BUCKETS = 5

STABLE_TOKENS = {
    "USDT", "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "USDP", "PAX", "SUSD",
    "UST", "USTC", "USDD", "EUR", "EURI", "GBP", "AEUR", "PYUSD", "USD1",
    "XUSD", "BFUSD", "USDE", "USDS",
}
EXCLUDE_BASE = {"BTC", "WBTC", "BTCB", "BETH", "WBETH", "BTCDOM", "PAXG", "XAUT"}


def log(m: str) -> None:
    print(m, flush=True)


def pct(x) -> str:
    return "n/a" if not np.isfinite(x) else f"{x*100:+.2f}%"


# --------------------------------------------------------------------------
# Universe + data
# --------------------------------------------------------------------------
def list_universe() -> list[str]:
    r = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=60)
    r.raise_for_status()
    out = []
    for s in r.json()["symbols"]:
        if s["quoteAsset"] != "USDT" or s["status"] != "TRADING":
            continue
        if not s.get("isSpotTradingAllowed"):
            continue
        base = s["baseAsset"]
        if base in STABLE_TOKENS or base in EXCLUDE_BASE:
            continue
        if any(base.endswith(x) for x in ("UP", "DOWN", "BULL", "BEAR")):
            continue
        out.append(s["symbol"])
    return sorted(out)


def fetch_symbol(symbol: str) -> pd.DataFrame:
    """Daily UTC close + quote volume for one symbol, paginated."""
    rows = []
    cur = int(pd.Timestamp("2017-01-01", tz="UTC").timestamp() * 1000)
    end = int(pd.Timestamp(data_io._utc_now()).timestamp() * 1000)
    while cur < end:
        try:
            r = requests.get("https://api.binance.com/api/v3/klines",
                             params={"symbol": symbol, "interval": "1d",
                                     "startTime": cur, "limit": 1000}, timeout=30)
            if r.status_code in (429, 418):
                time.sleep(3.0)
                continue
            r.raise_for_status()
            batch = r.json()
        except requests.RequestException:
            time.sleep(2.0)
            continue
        if not batch:
            break
        rows.extend(batch)
        cur = batch[-1][0] + 86_400_000
        time.sleep(0.12)
    if not rows:
        return pd.DataFrame(columns=["date", "symbol", "close", "quote_volume"])
    df = pd.DataFrame(rows)
    out = pd.DataFrame({
        "date": pd.to_datetime(df[0], unit="ms", utc=True).dt.tz_localize(None).dt.normalize(),
        "symbol": symbol,
        "close": df[4].astype(float),
        "quote_volume": df[7].astype(float),
    })
    today = pd.Timestamp(data_io._utc_now().date())
    return out[out["date"] < today].drop_duplicates(subset="date").reset_index(drop=True)


def clean_redenominations(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Truncate each symbol at the first ticker-reuse / redenomination break.

    Binance reuses a ticker after a token swap, reverse split or chain
    migration, so a single symbol's price series can silently splice two
    different units together. Left uncleaned these produce impossible returns:
    LUNAUSDT shows +17,739,900% on 2022-05-31 because old LUNA (crashed to
    $0.00005) and Terra 2.0 LUNA ($8.87) share the ticker.

    These breaks do NOT show up as extreme single-DAY returns, because the
    rescaling happens during a trading suspension -- the series simply has a
    gap. So the test is on the ratio between consecutive AVAILABLE closes plus
    the length of the gap, not on daily returns.

    Break criteria (either):
      * ratio > 5x or < 1/5x across a gap of >= 2 days (halt + rescale), or
      * ratio > 50x or < 1/50x on any step (unambiguous rescale).

    On a break the symbol is TRUNCATED -- everything from the break onward is
    dropped, the pre-break history is kept. This deliberately preserves genuine
    catastrophic crashes (LUNA's real -99.97% collapse stays in the data; only
    its fake resurrection is removed) while treating the old asset as dead,
    which is what economically happened.
    """
    keep, breaks = [], []
    for sym, g in panel.groupby("symbol", sort=False):
        g = g.sort_values("date").reset_index(drop=True)
        ratio = g["close"] / g["close"].shift(1)
        gap = g["date"].diff().dt.days
        flag = (((ratio > 5) | (ratio < 0.2)) & (gap >= 2)) | (ratio > 50) | (ratio < 0.02)
        if flag.any():
            i = int(flag.idxmax())
            breaks.append({
                "symbol": sym, "break_date": g.loc[i, "date"].date(),
                "prev_date": g.loc[i - 1, "date"].date(),
                "gap_days": int(gap.iloc[i]),
                "price_before": float(g.loc[i - 1, "close"]),
                "price_after": float(g.loc[i, "close"]),
                "ratio": float(ratio.iloc[i]),
                "rows_kept": i, "rows_dropped": len(g) - i,
            })
            g = g.iloc[:i]
        if len(g) >= MIN_LISTED_DAYS:
            keep.append(g)
    return pd.concat(keep, ignore_index=True), pd.DataFrame(breaks)


def load_panel(refresh: bool) -> pd.DataFrame:
    path = CACHE_DIR / "binance_alt_panel.parquet"
    if path.exists() and not refresh:
        return pd.read_parquet(path)
    syms = list_universe()
    log(f"  universe: {len(syms)} Binance USDT spot pairs (stables/BTC/leveraged excluded)")
    frames = []
    for i, s in enumerate(syms, 1):
        try:
            d = fetch_symbol(s)
        except Exception as exc:  # noqa: BLE001
            log(f"    !! {s} failed: {exc}")
            continue
        if len(d) >= MIN_LISTED_DAYS:
            frames.append(d)
        if i % 50 == 0:
            log(f"    fetched {i}/{len(syms)} ({len(frames)} kept)")
    panel = pd.concat(frames, ignore_index=True)
    panel.to_parquet(path, index=False)
    return panel


# --------------------------------------------------------------------------
# Basket construction (point-in-time)
# --------------------------------------------------------------------------
def build_basket(panel: pd.DataFrame, top_n: int):
    close = panel.pivot(index="date", columns="symbol", values="close").sort_index()
    qvol = panel.pivot(index="date", columns="symbol", values="quote_volume").sort_index()
    full = pd.date_range(close.index.min(), close.index.max(), freq="D")
    close = close.reindex(full)
    qvol = qvol.reindex(full)

    listed_days = close.notna().cumsum()          # days of history as of t
    eligible = listed_days >= MIN_LISTED_DAYS
    trail_vol = qvol.rolling(VOL_WINDOW, min_periods=VOL_WINDOW).sum()
    trail_vol = trail_vol.where(eligible)

    rebal = pd.date_range(close.index.min(), close.index.max(), freq="MS")
    rebal = [d for d in rebal if d in close.index]

    members: dict[pd.Timestamp, list[str]] = {}
    for d in rebal:
        row = trail_vol.loc[d].dropna()
        if len(row) < 10:
            continue
        members[d] = list(row.sort_values(ascending=False).head(top_n).index)
    return close, members


def member_map(index: pd.DatetimeIndex, members: dict) -> pd.Series:
    keys = sorted(members)
    out = pd.Series(index=index, dtype=object)
    for i, d in enumerate(index):
        prior = [k for k in keys if k <= d]
        if prior:
            out.iloc[i] = members[prior[-1]]
    return out


def basket_forward_returns(close: pd.DataFrame, memb: pd.Series, h: int) -> pd.Series:
    """Equal-weighted mean of member h-day forward returns, membership fixed at t."""
    fwd = close.shift(-h) / close - 1.0
    vals = []
    for d in memb.index:
        m = memb.loc[d]
        if not isinstance(m, list):
            vals.append(np.nan)
            continue
        row = fwd.loc[d, [c for c in m if c in fwd.columns]]
        vals.append(row.mean() if row.notna().sum() >= 5 else np.nan)
    return pd.Series(vals, index=memb.index, name=f"basket_fwd_{h}")


def trailing_beta(close: pd.DataFrame, btc: pd.Series, dates) -> pd.DataFrame:
    """Trailing BETA_WINDOW-day beta vs BTC at each rebalance date (past data only)."""
    ret = close.pct_change(fill_method=None)
    bret = btc.pct_change(fill_method=None).reindex(ret.index)
    out = {}
    for d in dates:
        window = ret.loc[d - pd.Timedelta(days=BETA_WINDOW):d]
        bw = bret.loc[d - pd.Timedelta(days=BETA_WINDOW):d]
        var = bw.var()
        if not np.isfinite(var) or var == 0:
            continue
        cov = window.apply(lambda c: c.cov(bw))
        b = cov / var
        out[d] = b[window.notna().sum() >= BETA_WINDOW * 0.7]
    return pd.DataFrame(out).T


def spread_test(sig: pd.Series, fwd: pd.Series, s=None, e=None) -> dict:
    d = pd.concat([sig.rename("on"), fwd.rename("fwd")], axis=1).dropna()
    if s:
        d = d[d.index >= s]
    if e:
        d = d[d.index < e]
    if len(d) < 150 or d["on"].nunique() < 2:
        return {}
    on, off = d.loc[d.on == 1, "fwd"], d.loc[d.on == 0, "fwd"]
    res = sm.OLS(d["fwd"].values, sm.add_constant(d["on"].values)).fit(
        cov_type="HAC", cov_kwds={"maxlags": LAGS})
    return {"n": len(d), "n_on": len(on), "n_off": len(off),
            "mean_on": on.mean(), "mean_off": off.mean(),
            "spread": on.mean() - off.mean(),
            "hac_t": float(res.tvalues[1]), "hac_p": float(res.pvalues[1])}


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--top-n", type=int, default=50)
    ap.add_argument("--sweep", action="store_true", help="also write the basket-size robustness table")
    args = ap.parse_args()

    log("SECTION A -- data")
    panel = load_panel(args.refresh)
    log(f"  raw panel: {panel.symbol.nunique()} symbols, "
        f"{panel.date.min().date()} -> {panel.date.max().date()}, {len(panel):,} rows")
    panel, breaks = clean_redenominations(panel)
    breaks.to_csv(TABLE_DIR / "33_redenomination_breaks.csv", index=False)
    log(f"  redenomination / ticker-reuse breaks found: {len(breaks)}")
    for _, b in breaks.iterrows():
        log(f"    {b['symbol']:<12} {b['prev_date']} -> {b['break_date']} "
            f"(gap {b['gap_days']}d)  {b['price_before']:.8g} -> {b['price_after']:.8g} "
            f"= {b['ratio']:,.1f}x  | truncated, {b['rows_dropped']} rows dropped")
    log(f"  clean panel: {panel.symbol.nunique()} symbols, {len(panel):,} rows")

    px, _ = data_io.load_prices()
    btc = px["BTC"].dropna()
    score = ts.trend_score(btc)
    sig_ts = (score >= 2).astype(float).where(score.notna())
    sma = btc.rolling(MA_WINDOW).mean()
    sig_ma = (btc > sma).astype(float).where(sma.notna())

    close, members = build_basket(panel, args.top_n)
    log(f"  basket: top {args.top_n} by trailing {VOL_WINDOW}d volume, "
        f"{len(members)} monthly rebalances "
        f"({min(members).date()} -> {max(members).date()})")
    sizes = [len(v) for v in members.values()]
    log(f"  members per rebalance: min {min(sizes)}, median {int(np.median(sizes))}, max {max(sizes)}")

    idx = close.index
    memb = member_map(idx, members)
    fwd20 = basket_forward_returns(close, memb, PRIMARY_HORIZON)

    # ---------------- H1: basket-level spread ----------------
    log("\nSECTION B -- H1: broad alt basket, 20D on-off spread")
    eras = [("full sample", None, None),
            ("train (pre-2022)", None, OOS_SPLIT_DATE),
            ("OOS (2022+)", OOS_SPLIT_DATE, None)]
    rows = []
    for sname, sser in (("BTC TrendScore >= 2", sig_ts), ("BTC > SMA200", sig_ma)):
        for era, s, e in eras:
            r = spread_test(sser, fwd20, s, e)
            if r:
                rows.append({"signal": sname, "era": era, "target": f"alt basket (top {args.top_n})", **r})
    h1 = pd.DataFrame(rows)
    h1.to_csv(TABLE_DIR / "30_alt_basket_spread.csv", index=False)
    for _, r in h1.iterrows():
        log(f"  {r['signal']:<20} {r['era']:<17} on-off = {pct(r['spread'])}  "
            f"(HAC t={r['hac_t']:5.2f}, p={r['hac_p']:.4f}, n={int(r['n'])})")

    # ---------------- H2: beta ordering ----------------
    log(f"\nSECTION C -- H2: does the spread scale with beta? ({N_BETA_BUCKETS} buckets)")
    rebal_dates = sorted(members)
    betas = trailing_beta(close, btc, rebal_dates)
    log(f"  trailing {BETA_WINDOW}d betas computed at {len(betas)} rebalance dates")

    bucket_fwd = {}
    bucket_beta = {q: [] for q in range(N_BETA_BUCKETS)}
    for q in range(N_BETA_BUCKETS):
        vals = pd.Series(index=idx, dtype=float)
        for d in idx:
            m = memb.loc[d]
            if not isinstance(m, list):
                continue
            prior = [k for k in rebal_dates if k <= d and k in betas.index]
            if not prior:
                continue
            b = betas.loc[prior[-1], [c for c in m if c in betas.columns]].dropna()
            if len(b) < N_BETA_BUCKETS * 3:
                continue
            try:
                lab = pd.qcut(b, N_BETA_BUCKETS, labels=False, duplicates="drop")
            except ValueError:
                continue
            sel = list(b.index[lab == q])
            if len(sel) < 3:
                continue
            fr = (close.shift(-PRIMARY_HORIZON) / close - 1.0).loc[d, sel]
            if fr.notna().sum() >= 3:
                vals.loc[d] = fr.mean()
                bucket_beta[q].append(float(b.loc[sel].mean()))
        bucket_fwd[q] = vals

    rows = []
    for q in range(N_BETA_BUCKETS):
        mean_beta = float(np.nanmean(bucket_beta[q])) if bucket_beta[q] else np.nan
        for era, s, e in eras:
            for sname, sser in (("BTC TrendScore >= 2", sig_ts), ("BTC > SMA200", sig_ma)):
                r = spread_test(sser, bucket_fwd[q], s, e)
                if r:
                    rows.append({"beta_bucket": f"Q{q+1}", "approx_beta": round(mean_beta, 2),
                                 "signal": sname, "era": era, **r})
    h2 = pd.DataFrame(rows)
    h2.to_csv(TABLE_DIR / "31_alt_basket_beta_buckets.csv", index=False)
    log("  realised mean trailing beta per bucket: " +
        "  ".join(f"Q{q+1}={np.nanmean(bucket_beta[q]):.2f}" for q in range(N_BETA_BUCKETS)))
    for sname in ("BTC TrendScore >= 2", "BTC > SMA200"):
        for era, _, _ in eras:
            sub = h2[(h2.signal == sname) & (h2.era == era)].sort_values("beta_bucket")
            if sub.empty:
                continue
            spreads = sub["spread"].tolist()
            mono = all(x < y for x, y in zip(spreads, spreads[1:]))
            log(f"  {sname:<20} {era:<17} " +
                " ".join(f"{b}={pct(v)}" for b, v in zip(sub.beta_bucket, spreads)) +
                f"   monotone={mono}")
            rho = np.corrcoef(range(len(spreads)), spreads)[0, 1] if len(spreads) > 2 else np.nan
            log(f"    {'':<38}rank-corr(beta bucket, spread) = {rho:+.3f}")

    # ---------------- Breadth: per-coin ----------------
    log("\nSECTION D -- breadth: how many individual coins show a positive spread?")
    fwd_all = close.shift(-PRIMARY_HORIZON) / close - 1.0
    rows = []
    for sym in close.columns:
        f = fwd_all[sym].dropna()
        if len(f) < 400:
            continue
        rec = {"symbol": sym, "n_days": len(f)}
        ok = True
        for era, s, e in eras:
            r = spread_test(sig_ts, f, s, e)
            if not r:
                ok = False
                break
            rec[f"spread_{era.split(' ')[0]}"] = r["spread"]
            rec[f"t_{era.split(' ')[0]}"] = r["hac_t"]
        if ok:
            rows.append(rec)
    breadth = pd.DataFrame(rows)
    breadth.to_csv(TABLE_DIR / "32_alt_breadth_per_coin.csv", index=False)
    for era in ("full", "train", "OOS"):
        c = breadth[f"spread_{era}"]
        t = breadth[f"t_{era}"]
        log(f"  {era:<6}: {int((c > 0).sum())}/{len(c)} coins positive "
            f"({(c > 0).mean()*100:.0f}%), median spread {pct(c.median())}, "
            f"{int((t > 1.96).sum())} with HAC t>1.96")

    # ---------------- basket-size robustness ----------------
    if args.sweep:
        log("")
        log("SECTION E -- basket-size robustness (H1 across N)")
        srows = []
        for n in (25, 50, 100):
            cl, mem = build_basket(panel, n)
            mm = member_map(cl.index, mem)
            fw = basket_forward_returns(cl, mm, PRIMARY_HORIZON)
            for sname, sser in (("BTC TrendScore >= 2", sig_ts), ("BTC > SMA200", sig_ma)):
                for era, st_, e_ in eras:
                    r = spread_test(sser, fw, st_, e_)
                    if r:
                        srows.append({"top_n": n, "signal": sname, "era": era,
                                      "spread": r["spread"], "hac_t": r["hac_t"],
                                      "hac_p": r["hac_p"]})
        sw = pd.DataFrame(srows)
        sw.to_csv(TABLE_DIR / "34_alt_basket_size_robustness.csv", index=False)
        for era, _, _ in eras:
            line = f"  {era:<17}"
            for sname in ("BTC TrendScore >= 2", "BTC > SMA200"):
                vals = sw[(sw.era == era) & (sw.signal == sname)].sort_values("top_n")
                line += f" | {sname}: " + " ".join(
                    f"N{int(r.top_n)}={pct(r.spread)}" for _, r in vals.iterrows())
            log(line)

    # ---------------- figure ----------------
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.8))
    for ax, era in zip(axes, ("full sample", "OOS (2022+)")):
        for sname, col in (("BTC TrendScore >= 2", "#1F6F8B"), ("BTC > SMA200", "#B23A48")):
            sub = h2[(h2.signal == sname) & (h2.era == era)].sort_values("beta_bucket")
            if sub.empty:
                continue
            ax.plot(sub.beta_bucket, sub.spread * 100, "o-", color=col, lw=2, label=sname)
        ax.axhline(0, color="#333", lw=0.8)
        ax.set_title(era)
        ax.set_xlabel("trailing-beta bucket (low → high)")
        ax.grid(alpha=0.25)
    axes[0].set_ylabel("20D forward-return spread, signal on − off (%)")
    axes[0].legend(frameon=False, fontsize=9)
    fig.suptitle("H2: does the risk-on effect scale with beta?  "
                 f"(top-{args.top_n} point-in-time alt basket)", y=1.0)
    fig.savefig(FIGURE_DIR / "fig12_alt_basket_beta.png", dpi=150,
                bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log("\n-> figure fig12_alt_basket_beta.png")
    log("-> tables 30, 31, 32 written")


if __name__ == "__main__":
    main()
