from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path
from typing import Dict, List, Iterable, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd

from data_loader import DataLoader


getcontext().prec = 28

# Basket construction constants (previously the literal `head(20)`).
TARGET_BASKET_N = 20          # constituents per rebalance
CANDIDATE_BUFFER_MULT = 2     # pull 2x candidates so we can refill around price gaps
MIN_BASKET_N = 15             # fail loud below this many valid-priced constituents


def d(x) -> Decimal:
    if isinstance(x, Decimal):
        return x
    if x is None:
        return Decimal("0")
    return Decimal(str(x))


def _to_decimal_opt(x) -> Optional[Decimal]:
    """
    Convert to Decimal, returning None for anything not a usable finite number
    (None, NaN, +/-inf, unparseable).

    This exists because plain ``d()`` maps a float NaN to ``Decimal('NaN')``,
    which then flows silently into arithmetic. A NaN rebalance price produced
    NaN clamp bounds, and ``max(lb, min(ub, p_raw))`` raised
    ``decimal.InvalidOperation`` the first time the reconstruction reached a
    basket with a gappy constituent price (the 2026-01-29+ window). Callers on
    the price-ingest path must use this and treat None as "no valid price".
    """
    if x is None:
        return None
    try:
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return None
        dv = x if isinstance(x, Decimal) else Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if dv.is_nan() or dv.is_infinite():
        return None
    return dv


def calculate_constituent_weights(marketcaps: pd.Series) -> pd.Series:
    if marketcaps.empty:
        return marketcaps
    mc_dict = {idx: Decimal(str(val)) for idx, val in marketcaps.items()}
    total_mc = sum(mc_dict.values())
    if total_mc <= 0:
        n = len(mc_dict)
        eq_weight = Decimal("1") / Decimal(str(n))
        return pd.Series([eq_weight] * n, index=list(mc_dict.keys()))
    weights = {k: (v / total_mc) for k, v in mc_dict.items()}
    total_w = sum(weights.values())
    norm_factor = Decimal("1") / total_w
    for k in weights:
        weights[k] = weights[k] * norm_factor
    return pd.Series(
        [weights.get(idx, Decimal("0")) for idx in marketcaps.index],
        index=marketcaps.index,
    )


@dataclass
class RebalanceParams:
    date: date
    weights: Dict[str, Decimal]
    quantities: Dict[str, Decimal]
    rebalance_prices: Dict[str, Decimal]
    divisor: Decimal
    delta: Decimal = Decimal("0.3")


class IndexCalculator:
    def __init__(
        self,
        data_loader: DataLoader,
        base_index_level: Decimal = Decimal("1000"),
        delta: Decimal = Decimal("0.3"),
        max_ffill_days: int = 3,
    ) -> None:
        self.dl = data_loader
        self.base_index_level = base_index_level
        self.delta = delta
        self.max_ffill_days = max_ffill_days

    def backfill(
        self,
        start_date: date,
        end_date: date,
        rebalance_dates: List[date],
    ) -> pd.DataFrame:
        if not rebalance_dates:
            raise ValueError("rebalance_dates must be non-empty")
        all_days = self.dl.iter_days(start_date, end_date)
        rebalance_dates = sorted(set(rebalance_dates))
        btc_ids = self.dl.get_btc_asset_ids()
        prices_btc = self.dl.get_prices(btc_ids, start_date, end_date)
        btc_price_by_date: Dict[date, Decimal] = {}
        for d_val, grp in prices_btc.groupby("date"):
            close_val = grp["close"].iloc[0]
            btc_dec = _to_decimal_opt(close_val)
            if btc_dec is None or btc_dec <= 0:
                # Missing/NaN BTC close: leave the date absent. The rebalance loop
                # raises loudly if a REBALANCE date is missing; _apply_segment skips
                # a missing SEGMENT day (producing a gap, never a NaN index row).
                continue
            btc_price_by_date[d_val] = btc_dec
        results: List[Dict[str, object]] = []
        last_index_value: Decimal | None = None
        last_clamped_prices: Dict[str, Decimal] = {}
        for i, rebalance_date in enumerate(rebalance_dates):
            if rebalance_date > end_date:
                break
            next_reb = rebalance_dates[i + 1] if i + 1 < len(rebalance_dates) else end_date + timedelta(days=1)
            if rebalance_date not in btc_price_by_date:
                raise ValueError(f"Missing BTC price on rebalance date {rebalance_date}")
            params = self._build_rebalance_params(
                rebalance_date=rebalance_date,
                btc_price_by_date=btc_price_by_date,
                last_index_value=last_index_value,
            )
            segment_days = [d_val for d_val in all_days if rebalance_date <= d_val < next_reb]
            seg_prices = self._load_prices_for_universe(
                asset_ids=list(params.weights.keys()),
                days=segment_days,
            )
            last_clamped_prices = self._apply_segment(
                params=params,
                segment_days=segment_days,
                btc_price_by_date=btc_price_by_date,
                prices=seg_prices,
                last_clamped_prices=last_clamped_prices,
                results=results,
            )
            if results:
                last_index_value = d(results[-1]["reconstructed_index_value"])
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values("date").reset_index(drop=True)
        return df

    def _build_rebalance_params(
        self,
        rebalance_date: date,
        btc_price_by_date: Dict[date, Decimal],
        last_index_value: Decimal | None,
    ) -> RebalanceParams:
        uni = self.dl.get_eligible_universe_on_date(rebalance_date)
        if uni.empty:
            raise ValueError(f"No eligible universe on rebalance date {rebalance_date}")
        uni_sorted = uni.sort_values("marketcap", ascending=False)
        # Pull a buffer beyond the target basket size so we can still fill
        # TARGET_BASKET_N valid-priced constituents when a top-ranked asset has a
        # missing / NaN / non-positive close on this rebalance date.
        #
        # Historically every top-20 asset had a valid price, so this selects the
        # SAME 20 and leaves the historical index byte-identical. It only diverges
        # on dates where a would-be constituent's price is a gap -- exactly the
        # 2026-01-29+ window that used to build a NaN rebalance price and crash
        # `max(lb, min(ub, p_raw))` with decimal.InvalidOperation.
        candidates = uni_sorted.head(TARGET_BASKET_N * CANDIDATE_BUFFER_MULT)
        candidate_ids = candidates["asset_id"].tolist()
        candidate_mc = candidates.set_index("asset_id")["marketcap"]
        btc_px = btc_price_by_date[rebalance_date]
        prices_universe = self.dl.get_prices(candidate_ids, rebalance_date, rebalance_date)
        if prices_universe.empty:
            raise ValueError(f"Missing prices for universe on rebalance date {rebalance_date}")

        # Keep only candidates that have a usable (finite, positive) close.
        close_by_asset: Dict[str, Decimal] = {}
        for aid in candidate_ids:
            row = prices_universe[prices_universe["asset_id"] == aid]
            if row.empty:
                continue
            px = _to_decimal_opt(row["close"].iloc[0])
            if px is None or px <= 0:
                continue
            close_by_asset[aid] = px

        # Select the TARGET_BASKET_N largest-cap assets that have a valid price.
        asset_ids = [aid for aid in candidate_ids if aid in close_by_asset][:TARGET_BASKET_N]
        n_valid = len(asset_ids)
        dropped = [aid for aid in candidate_ids[:TARGET_BASKET_N] if aid not in close_by_asset]
        if n_valid < MIN_BASKET_N:
            raise ValueError(
                f"Only {n_valid} of the top {TARGET_BASKET_N} constituents have a valid price on "
                f"rebalance date {rebalance_date} (floor={MIN_BASKET_N}). "
                f"Missing/NaN-priced among the top {TARGET_BASKET_N}: {dropped[:10]}. "
                f"Refusing to reconstruct a degenerate basket."
            )
        if dropped:
            # Loud but non-fatal: a top-N name had no price and the basket was
            # refilled from the next-largest valid-priced candidate(s).
            print(
                f"[BTCDOM] {rebalance_date}: {len(dropped)} top-{TARGET_BASKET_N} asset(s) had no "
                f"valid price ({dropped[:10]}); basket refilled to {n_valid} constituents."
            )

        mc_series = candidate_mc.loc[asset_ids]
        weights_series = calculate_constituent_weights(mc_series)
        price_by_asset: Dict[str, Decimal] = {aid: close_by_asset[aid] for aid in asset_ids}
        rebalance_prices: Dict[str, Decimal] = {}
        for aid in asset_ids:
            alt_px = price_by_asset[aid]  # guaranteed finite and > 0 by selection above
            rebalance_prices[aid] = btc_px / alt_px
        quantities: Dict[str, Decimal] = {}
        for aid in asset_ids:
            w = d(weights_series.loc[aid])
            p0 = rebalance_prices[aid]
            quantities[aid] = w / p0 if p0 != 0 else Decimal("0")
        index_target = self.base_index_level if last_index_value is None else last_index_value
        numerator = Decimal("0")
        for aid in asset_ids:
            numerator += quantities[aid] * rebalance_prices[aid]
        if numerator == 0:
            raise ValueError(f"Zero numerator on rebalance date {rebalance_date}")
        divisor = numerator / index_target
        weights_dict = {aid: d(weights_series.loc[aid]) for aid in asset_ids}
        assert abs(sum(weights_dict.values()) - Decimal("1.0")) < Decimal("0.0001"), "Weights do not sum to 1!"
        return RebalanceParams(
            date=rebalance_date,
            weights=weights_dict,
            quantities=quantities,
            rebalance_prices=rebalance_prices,
            divisor=divisor,
            delta=self.delta,
        )

    def _load_prices_for_universe(
        self,
        asset_ids: List[str],
        days: Iterable[date],
    ) -> pd.DataFrame:
        if not asset_ids or not days:
            return pd.DataFrame(columns=["date", "asset_id", "close"])
        start = min(days)
        end = max(days)
        return self.dl.get_prices(asset_ids, start, end)

    def _apply_segment(
        self,
        params: RebalanceParams,
        segment_days: List[date],
        btc_price_by_date: Dict[date, Decimal],
        prices: pd.DataFrame,
        last_clamped_prices: Dict[str, Decimal],
        results: List[Dict[str, object]],
    ) -> Dict[str, Decimal]:
        if not segment_days:
            return last_clamped_prices
        price_lookup: Dict[Tuple[date, str], Decimal] = {}
        for (d_val, aid), row in prices.groupby(["date", "asset_id"]).first().iterrows():
            price_lookup[(d_val, aid)] = d(row["close"])
        last_raw_price_date: Dict[str, date] = {}
        for (d_val, aid) in price_lookup.keys():
            prev = last_raw_price_date.get(aid)
            if prev is None or d_val < prev:
                last_raw_price_date[aid] = d_val
        lower_bound: Dict[str, Decimal] = {}
        upper_bound: Dict[str, Decimal] = {}
        for aid, p0 in params.rebalance_prices.items():
            lower_bound[aid] = p0 * (Decimal("1") - params.delta)
            upper_bound[aid] = p0 * (Decimal("1") + params.delta)
        for d_val in segment_days:
            if d_val not in btc_price_by_date:
                continue
            btc_px = btc_price_by_date[d_val]
            clamped_prices: Dict[str, Decimal] = {}
            for aid, w in params.weights.items():
                raw_alt: Decimal | None = price_lookup.get((d_val, aid))
                if raw_alt is not None and not (isinstance(raw_alt, Decimal) and raw_alt.is_nan()):
                    last_raw_price_date[aid] = d_val
                if raw_alt is None or raw_alt == 0 or (isinstance(raw_alt, Decimal) and raw_alt.is_nan()):
                    raw_alt = None
                if raw_alt is None:
                    last_date = last_raw_price_date.get(aid)
                    if last_date is not None:
                        delta_days = (d_val - last_date).days
                        if delta_days <= self.max_ffill_days:
                            raw_alt = price_lookup.get((last_date, aid))
                            if raw_alt is not None and isinstance(raw_alt, Decimal) and raw_alt.is_nan():
                                raw_alt = None
                        else:
                            raw_alt = None
                    else:
                        raw_alt = None
                if (
                    raw_alt is None
                    or raw_alt == 0
                    or (isinstance(raw_alt, Decimal) and raw_alt.is_nan())
                ):
                    p_raw = None
                else:
                    p_raw = btc_px / raw_alt
                lb, ub = lower_bound[aid], upper_bound[aid]
                if p_raw is None:
                    prev_clamped = last_clamped_prices.get(aid)
                    p_clamped = params.rebalance_prices[aid] if prev_clamped is None else prev_clamped
                else:
                    p_clamped = max(lb, min(ub, p_raw))
                clamped_prices[aid] = p_clamped
            numerator = Decimal("0")
            for aid, q in params.quantities.items():
                numerator += q * clamped_prices[aid]
            index_value = numerator / params.divisor
            results.append({
                "date": d_val,
                "reconstructed_index_value": index_value,
                "daily_divisor": params.divisor,
            })
            last_clamped_prices = clamped_prices
        return last_clamped_prices


def compare_to_benchmark(
    reconstructed_df: pd.DataFrame,
    official_binance_csv: str | None = None,
) -> tuple[plt.Figure, List[plt.Axes]]:
    if reconstructed_df.empty:
        raise ValueError("reconstructed_df is empty")
    df = reconstructed_df.copy().sort_values("date")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["recon_float"] = df["reconstructed_index_value"].astype(float)
    base_dir = Path(__file__).resolve().parent
    out_dir = base_dir / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    if official_binance_csv is None:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df["date"], df["recon_float"], label="Reconstructed BTCDOM", color="tab:blue")
        ax.set_title("Reconstructed BTCDOM Index")
        ax.set_xlabel("Date")
        ax.set_ylabel("Index Level")
        ax.legend()
        ax.grid(True, alpha=0.3)
        fig.autofmt_xdate()
        plt.tight_layout()
        out_path = out_dir / "btcdom_reconstructed.png"
        fig.savefig(out_path, dpi=150)
        plt.close()
        return fig, [ax]
    bench = pd.read_csv(official_binance_csv)
    date_col = next((c for c in ("date", "open_time", "timestamp") if c in bench.columns), None)
    if date_col is None:
        raise ValueError("Could not find a date-like column in Binance CSV.")
    bench["date"] = pd.to_datetime(bench[date_col]).dt.date
    price_col = next((c for c in ("index_price", "close", "btcdom", "price") if c in bench.columns), None)
    if price_col is None:
        raise ValueError("Could not find a price column in Binance CSV.")
    bench = bench[["date", price_col]].rename(columns={price_col: "binance_index"})
    merged = df.merge(bench, on="date", how="inner")
    if merged.empty:
        raise ValueError("No overlapping dates between reconstructed index and Binance CSV.")
    merged["binance_float"] = merged["binance_index"].astype(float)
    merged["error"] = merged["recon_float"] - merged["binance_float"]
    fig, (ax_top, ax_err) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    ax_top.plot(merged["date"], merged["recon_float"], label="Recon", color="tab:blue")
    ax_top.plot(merged["date"], merged["binance_float"], label="Binance", color="tab:orange")
    ax_top.set_title("BTCDOM: Reconstructed vs Binance index")
    ax_top.set_ylabel("BTCDOM index")
    ax_top.legend()
    ax_top.grid(True, alpha=0.3)
    ax_err.plot(merged["date"], merged["error"], label="Recon - Binance", color="tab:green")
    ax_err.axhline(0.0, color="gray", linestyle="--", linewidth=1)
    ax_err.set_title("Reconstruction error")
    ax_err.set_xlabel("Timestamp")
    ax_err.set_ylabel("Error (recon - binance)")
    ax_err.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    plt.tight_layout()
    out_path = out_dir / "btcdom_reconstructed_vs_binance.png"
    fig.savefig(out_path, dpi=150)
    plt.close()
    return fig, [ax_top, ax_err]
