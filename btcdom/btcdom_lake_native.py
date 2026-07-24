"""Lake-native BTC dominance — replacement for the dead btcdom feeds.

WHY THIS EXISTS
---------------
The MSM's BTCDOM columns (btcd_index_decision, sma_30, BTCDOM_Trend,
btcdom_7d_ret) went NaN after 2026-01-26 because both upstream sources died:

    btcdom_reconstructed.csv  last date 2026-01-29  (~6 months stale)
    binance_btcdom.csv        last date 2026-03-02  (~5 months stale)

Worse than the NaNs: `BTCDOM_Trend` still reports a value on every row
("Falling" on all recent rows) because it is carried/filled rather than
recomputed. Consumers therefore read a stale default that looks valid.
Lake-native dominance says the trend is currently RISING, so the phantom
value is not merely missing — it is wrong.

THE FIX
-------
Compute dominance from the lake's own marketcap table, which is refreshed
daily by the main pipeline and therefore cannot silently die independently
of everything else:

    dominance = BTC marketcap / total marketcap (stables + wrapped excluded)

Validated against btcdom_reconstructed over the 575-day overlap:
    corr(daily pct change) = +0.914
    corr(levels)           = +0.892

That is a close enough tracker to drive the same Rising/Falling gate the
MSM already consumes, with the advantage that it shares a freshness fate
with the rest of the lake.

A basket-ratio construction (BTC vs equal-weight top-20 alts) was also
tested and tracked worse (+0.658 daily / -0.547 levels), so dominance is
the chosen proxy.

SCOPE
-----
This module only *computes and writes* the series. It does not modify the
MSM pipeline. Wiring it in is a separate, deliberate step — see
`emit_msm_columns()` for the exact column contract the MSM expects.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

LAKE = Path(r"G:\My Drive\Render Exports")
OUT_DIR = Path(__file__).resolve().parent

# Freshness guard. BACKTEST.md §23 notes Environment_APR was silently frozen
# for ~5 weeks once and now has a 3-day fail-fast; apply the same discipline
# here so this replacement cannot rot the way the feed it replaces did.
MAX_STALENESS_DAYS = 3

SMA_WINDOW = 30

# Wrapped/derivative tokens that double-count an underlying. The lake's
# stablecoins.csv covers stables; these are the wrapped-asset equivalents,
# which would otherwise inflate "total marketcap" with the same coin twice.
WRAPPED_EXCLUSIONS = {
    "STETH", "WBETH", "WETH", "RETH", "EETH", "WBTC", "SOLVBTC", "UNIBTC",
    "TBTC", "BTCB", "BTCT", "VBTC", "BBTC", "CBBTC", "LBTC",
}


@dataclass
class BtcdomResult:
    series: pd.DataFrame          # date-indexed: dominance_pct, sma_30, trend, ret_7d
    as_of: pd.Timestamp
    excluded_count: int
    universe_count: int


def _excluded_assets(lake: Path) -> set[str]:
    """Stablecoins (from the lake's own list + dim_asset flags) plus wrapped."""
    excluded: set[str] = set(WRAPPED_EXCLUSIONS)

    stables = pd.read_csv(lake / "stablecoins.csv")
    # File carries symbol / coingecko_id / is_stable; take the flagged symbols.
    if "is_stable" in stables.columns:
        stables = stables[stables["is_stable"].astype(bool)]
    excluded |= set(stables["symbol"].astype(str).str.upper())

    dim = pd.read_parquet(lake / "dim_asset.parquet")
    flag_cols = [c for c in ("is_stable", "is_wrapped_stable") if c in dim.columns]
    if flag_cols:
        mask = dim[flag_cols].any(axis=1)
        excluded |= set(dim.loc[mask, "asset_id"].astype(str).str.upper())

    return excluded


def compute(lake: Path = LAKE) -> BtcdomResult:
    """Compute the lake-native dominance series through the lake's latest date."""
    mc = pd.read_parquet(lake / "silver_fact_marketcap.parquet")
    mc["date"] = pd.to_datetime(mc["date"])

    as_of = mc["date"].max()
    staleness = (pd.Timestamp.utcnow().tz_localize(None) - as_of).days
    if staleness > MAX_STALENESS_DAYS:
        raise RuntimeError(
            f"silver_fact_marketcap is stale: max date {as_of.date()} is "
            f"{staleness}d old (limit {MAX_STALENESS_DAYS}d). Refusing to emit "
            f"a dominance series that would silently freeze — this is exactly "
            f"the failure mode this module replaces."
        )

    wide = mc.pivot_table(index="date", columns="asset_id", values="market_cap")
    wide = wide.sort_index()

    excluded = _excluded_assets(lake)
    keep = [c for c in wide.columns if str(c).upper() not in excluded]
    if "BTC" not in keep:
        raise RuntimeError("BTC missing from marketcap universe after exclusions.")
    wide = wide[keep]

    total = wide.sum(axis=1, min_count=1)
    dominance = wide["BTC"] / total * 100.0

    out = pd.DataFrame({"dominance_pct": dominance})
    out["sma_30"] = out["dominance_pct"].rolling(SMA_WINDOW).mean()
    out["ret_7d"] = out["dominance_pct"].pct_change(7)
    # Trend is only meaningful once the SMA has filled; leave it NA otherwise
    # rather than emitting a default that reads as a real signal.
    out["trend"] = np.where(
        out["sma_30"].isna(), None,
        np.where(out["dominance_pct"] > out["sma_30"], "Rising", "Falling"),
    )

    return BtcdomResult(
        series=out,
        as_of=as_of,
        excluded_count=len(excluded),
        universe_count=len(keep),
    )


def validate_against_dead_feed(res: BtcdomResult, lake: Path = LAKE) -> pd.Series:
    """Correlation vs btcdom_reconstructed over the period it was still alive."""
    old = pd.read_csv(lake / "btcdom_reconstructed.csv")
    old["date"] = pd.to_datetime(old["date"])
    old = old.set_index("date")["reconstructed_index_value"]

    joined = pd.concat(
        [old.rename("old"), res.series["dominance_pct"].rename("new")], axis=1
    ).dropna()
    changes = joined.pct_change().dropna()
    return pd.Series(
        {
            "overlap_days": len(joined),
            "overlap_start": joined.index.min().date(),
            "overlap_end": joined.index.max().date(),
            "corr_daily_change": changes["old"].corr(changes["new"]),
            "corr_levels": joined["old"].corr(joined["new"]),
        }
    )


def emit_msm_columns(res: BtcdomResult, decision_dates: pd.Series) -> pd.DataFrame:
    """Re-emit the four columns the MSM expects, on its decision dates.

    Column contract (matches msm_timeseries.csv):
        btcd_index_decision  index level at the decision date
        sma_30               30d SMA of that level
        BTCDOM_Trend         "Rising" / "Falling"  (NA until SMA is warm)
        btcdom_7d_ret        7-day return of the level

    Uses as-of (backward) alignment so a decision date never reads a value
    published after it.
    """
    dd = pd.to_datetime(pd.Series(decision_dates)).sort_values()
    s = res.series.sort_index()
    aligned = pd.merge_asof(
        pd.DataFrame({"decision_date": dd}),
        s.reset_index().rename(columns={"date": "src_date"}),
        left_on="decision_date",
        right_on="src_date",
        direction="backward",
    )
    return pd.DataFrame(
        {
            "decision_date": aligned["decision_date"],
            "btcd_index_decision": aligned["dominance_pct"],
            "sma_30": aligned["sma_30"],
            "BTCDOM_Trend": aligned["trend"],
            "btcdom_7d_ret": aligned["ret_7d"],
        }
    )


def main() -> None:
    res = compute()
    print(f"Lake-native BTC dominance — as of {res.as_of.date()}")
    print(f"  universe: {res.universe_count} assets "
          f"({res.excluded_count} stable/wrapped excluded)")

    v = validate_against_dead_feed(res)
    print(f"  validation vs btcdom_reconstructed "
          f"({v['overlap_start']} → {v['overlap_end']}, {v['overlap_days']}d):")
    print(f"    corr(daily change) = {v['corr_daily_change']:+.3f}")
    print(f"    corr(levels)       = {v['corr_levels']:+.3f}")

    tail = res.series.dropna(subset=["sma_30"]).tail(10)
    print("\n  last 10 days:")
    print(tail.to_string())

    latest = res.series.iloc[-1]
    print(f"\n  CURRENT: dominance={latest['dominance_pct']:.2f}%  "
          f"sma30={latest['sma_30']:.2f}%  trend={latest['trend']}  "
          f"7d_ret={latest['ret_7d']:+.4f}")

    out_csv = OUT_DIR / "btcdom_lake_native.csv"
    res.series.to_csv(out_csv)
    print(f"\n  wrote {out_csv}")

    # Show what the MSM rows would look like if rebuilt.
    msm = pd.read_csv(LAKE / "msm_timeseries.csv")
    rebuilt = emit_msm_columns(res, msm["decision_date"])
    print("\n  MSM columns rebuilt (last 8 decision dates):")
    print(rebuilt.tail(8).to_string(index=False))


if __name__ == "__main__":
    main()
