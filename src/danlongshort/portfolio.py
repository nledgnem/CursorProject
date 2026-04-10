from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import statsmodels.api as sm
import yaml

from src.providers.coingecko import fetch_price_history


@dataclass(frozen=True)
class Position:
    ticker: str
    side: str
    notional_usd: float
    entry_price: float | None
    entry_date: str | None

    @property
    def direction(self) -> float:
        s = (self.side or "").strip().upper()
        if s == "LONG":
            return 1.0
        if s == "SHORT":
            return -1.0
        raise ValueError(f"Invalid side={self.side!r} (expected LONG/SHORT)")


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _default_persistent_dir() -> Path:
    # Render persistent disk is mounted at /data. Locally, fall back to repo-relative.
    if os.name != "nt" and Path("/data").exists():
        return Path("/data")
    return Path(__file__).resolve().parents[2] / "data" / "state"


def load_positions_csv(path: Path) -> list[Position]:
    if not path.exists():
        raise FileNotFoundError(f"Positions CSV not found: {path}")

    df = pd.read_csv(path)
    required = {"ticker", "side", "notional_usd", "entry_price", "entry_date"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Positions CSV missing columns: {missing}")

    out: list[Position] = []
    for _, r in df.iterrows():
        ticker = str(r.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        side = str(r.get("side") or "").strip().upper()
        notional_raw = r.get("notional_usd")
        try:
            notional = float(notional_raw)
        except Exception:
            raise ValueError(f"{ticker}: invalid notional_usd={notional_raw!r}")
        entry_raw = r.get("entry_price")
        entry_price: float | None
        try:
            entry_price = float(entry_raw) if pd.notna(entry_raw) and str(entry_raw).strip() else None
        except Exception:
            entry_price = None
        entry_date_raw = r.get("entry_date")
        entry_date = str(entry_date_raw).strip() if pd.notna(entry_date_raw) and str(entry_date_raw).strip() else None
        out.append(Position(ticker=ticker, side=side, notional_usd=notional, entry_price=entry_price, entry_date=entry_date))
    return out


def load_symbol_to_coingecko_id(
    allowlist_csv: Path,
    *,
    override_yaml: Path | None = None,
) -> dict[str, str]:
    """
    Map symbol -> coingecko_id using:
    1) optional override YAML (symbol: coingecko_id), then
    2) the repo allowlist (data/perp_allowlist.csv)
    danlongshort avoids dependency on local parquet price panels.
    """
    out: dict[str, str] = {}

    if override_yaml and override_yaml.exists():
        try:
            raw = yaml.safe_load(override_yaml.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for k, v in raw.items():
                    sym = str(k or "").strip().upper()
                    cg = str(v or "").strip().lower()
                    if sym and cg:
                        out[sym] = cg
        except Exception as e:
            print(f"[WARN] Could not read danlongshort symbol override yaml {override_yaml}: {e}")

    if not allowlist_csv.exists():
        out.setdefault("BTC", "bitcoin")
        return out
    df = pd.read_csv(allowlist_csv)
    if "symbol" not in df.columns or "coingecko_id" not in df.columns:
        out.setdefault("BTC", "bitcoin")
        return out
    for _, r in df.iterrows():
        sym = str(r.get("symbol") or "").strip().upper()
        cg = str(r.get("coingecko_id") or "").strip().lower()
        if sym and cg:
            out.setdefault(sym, cg)
    # common aliases
    out.setdefault("BTC", "bitcoin")
    return out


def _cache_path() -> Path:
    base = Path(os.environ.get("DANLONGSHORT_PERSIST_DIR", "")).expanduser().resolve() if os.environ.get("DANLONGSHORT_PERSIST_DIR", "").strip() else _default_persistent_dir()
    base.mkdir(parents=True, exist_ok=True)
    return (base / "danlongshort_price_cache.parquet").resolve()


def _cache_is_fresh(path: Path, *, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    age_s = (datetime.now(timezone.utc) - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)).total_seconds()
    return age_s <= max_age_hours * 3600.0


def fetch_30d_closes_usd(
    tickers: Iterable[str],
    symbol_to_cg: dict[str, str],
    *,
    window_days: int = 30,
    cache_max_age_hours: float = 12.0,
    enable_cache: bool = True,
) -> pd.DataFrame:
    """
    Returns long-form dataframe: date (datetime64[ns, UTC-naive but UTC calendar), ticker, close_usd
    """
    tickers_u = sorted({str(t).strip().upper() for t in tickers if str(t).strip()})
    if not tickers_u:
        return pd.DataFrame(columns=["date", "ticker", "close_usd"])

    end_d = _utc_today()
    # ask a little extra to ensure we can form 30 returns after alignment
    start_d = end_d - timedelta(days=window_days + 5)

    cache_file = _cache_path()
    cached: pd.DataFrame | None = None
    if enable_cache and _cache_is_fresh(cache_file, max_age_hours=cache_max_age_hours):
        try:
            cached = pd.read_parquet(cache_file)
        except Exception:
            cached = None

    need: list[str] = tickers_u
    rows: list[dict] = []

    if cached is not None and {"date", "ticker", "close_usd"} <= set(cached.columns):
        c = cached.copy()
        c["ticker"] = c["ticker"].astype(str).str.upper()
        c["date"] = pd.to_datetime(c["date"], errors="coerce")
        c = c.dropna(subset=["date", "ticker", "close_usd"])
        c = c[(c["date"].dt.date >= start_d) & (c["date"].dt.date <= end_d)]
        have = set(c["ticker"].unique().tolist())
        need = [t for t in tickers_u if t not in have]
        rows.extend(c.to_dict(orient="records"))

    warnings: list[str] = []
    for tkr in need:
        cg_id = symbol_to_cg.get(tkr)
        if not cg_id:
            warnings.append(f"[WARN] Missing CoinGecko id mapping for {tkr}; skipping price fetch.")
            continue
        prices, _, _ = fetch_price_history(cg_id, start_d, end_d)
        if not prices or len(prices) < int(window_days * 0.7):
            warnings.append(f"[WARN] Insufficient CoinGecko price history for {tkr} (n={len(prices)}); skipping.")
            continue
        for d, px in prices.items():
            rows.append({"date": pd.Timestamp(d), "ticker": tkr, "close_usd": float(px)})

    for w in warnings:
        print(w)

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["date", "ticker", "close_usd"])

    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "ticker", "close_usd"])
    out = out.sort_values(["ticker", "date"]).drop_duplicates(subset=["ticker", "date"], keep="last")
    out = out[(out["date"].dt.date >= start_d) & (out["date"].dt.date <= end_d)]

    if enable_cache:
        try:
            out.to_parquet(cache_file, index=False)
        except Exception as e:
            print(f"[WARN] Could not write price cache to {cache_file}: {e}")

    # Keep only the last ~window_days+1 rows per ticker (to compute returns).
    out = out.groupby("ticker", as_index=False, group_keys=False).apply(lambda g: g.tail(window_days + 5))
    return out.reset_index(drop=True)


def _compute_beta(alt_lr: pd.Series, btc_lr: pd.Series) -> float | None:
    aligned = pd.concat([alt_lr.rename("alt"), btc_lr.rename("btc")], axis=1).dropna()
    if len(aligned) < 20:
        return None
    x = sm.add_constant(aligned["btc"].to_numpy(), has_constant="add")
    y = aligned["alt"].to_numpy()
    try:
        res = sm.OLS(y, x).fit()
        beta = float(res.params[1])
        if not np.isfinite(beta):
            return None
        return beta
    except Exception:
        return None


def compute_30d_betas(
    closes_long: pd.DataFrame,
    *,
    btc_ticker: str = "BTC",
) -> dict[str, float]:
    """
    closes_long: date,ticker,close_usd
    """
    btc_ticker = btc_ticker.upper()
    if closes_long.empty:
        return {btc_ticker: 1.0}

    pivot = closes_long.pivot(index="date", columns="ticker", values="close_usd").sort_index()
    lpx = np.log(pivot)
    lr = lpx.diff()
    if btc_ticker not in lr.columns:
        return {btc_ticker: 1.0}

    btc_lr = lr[btc_ticker]
    betas: dict[str, float] = {btc_ticker: 1.0}
    for tkr in lr.columns:
        tkr_u = str(tkr).upper()
        if tkr_u == btc_ticker:
            continue
        b = _compute_beta(lr[tkr], btc_lr)
        if b is None:
            continue
        betas[tkr_u] = b
    return betas


def latest_prices_from_closes(closes_long: pd.DataFrame) -> dict[str, float]:
    if closes_long.empty:
        return {}
    g = closes_long.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).tail(1)
    return {str(r["ticker"]).upper(): float(r["close_usd"]) for _, r in g.iterrows()}


def compute_portfolio_snapshot(
    positions: list[Position],
    betas: dict[str, float],
    latest_prices: dict[str, float],
) -> tuple[pd.DataFrame, dict[str, float | str]]:
    """
    Returns (per_position_table, summary_metrics)
    """
    rows: list[dict] = []
    gross = 0.0
    net_notional = 0.0
    long_total = 0.0
    short_total = 0.0

    for p in positions:
        beta = float(betas.get(p.ticker, 1.0 if p.ticker == "BTC" else np.nan))
        if not np.isfinite(beta):
            beta = np.nan
        exposure = p.notional_usd * p.direction * (beta if np.isfinite(beta) else 0.0)
        px = latest_prices.get(p.ticker)

        pnl_usd = np.nan
        if px is not None and p.entry_price is not None and p.entry_price > 0:
            pnl_usd = p.direction * p.notional_usd * (float(px) / float(p.entry_price) - 1.0)

        rows.append(
            {
                "ticker": p.ticker,
                "side": p.side,
                "notional_usd": p.notional_usd,
                "current_price": px if px is not None else np.nan,
                "beta_30d": beta,
                "beta_weighted_exposure_usd": exposure,
                "unrealized_pnl_usd": pnl_usd,
            }
        )

        gross += abs(p.notional_usd)
        net_notional += p.notional_usd * p.direction
        if p.direction > 0:
            long_total += p.notional_usd
        else:
            short_total += p.notional_usd

    tbl = pd.DataFrame(rows)
    if tbl.empty:
        tbl = pd.DataFrame(
            columns=[
                "ticker",
                "side",
                "notional_usd",
                "current_price",
                "beta_30d",
                "beta_weighted_exposure_usd",
                "unrealized_pnl_usd",
            ]
        )

    net_beta_exposure = float(tbl["beta_weighted_exposure_usd"].sum()) if not tbl.empty else 0.0

    largest_conc = 0.0
    if gross > 0 and not tbl.empty:
        largest_conc = float((tbl["notional_usd"].abs().max() / gross) * 100.0)

    lsr = np.nan
    if gross > 0:
        lsr = net_notional / gross

    btc_adjust_usd = -net_beta_exposure  # because BTC beta=1, exposure per $ notional equals $ notional

    summary: dict[str, float | str] = {
        "net_beta_exposure_usd": net_beta_exposure,
        "gross_notional_usd": gross,
        "net_notional_usd": net_notional,
        "net_long_short_ratio": float(lsr) if np.isfinite(lsr) else np.nan,
        "largest_position_concentration_pct": largest_conc,
        "long_total_notional_usd": long_total,
        "short_total_notional_usd": short_total,
        "btc_adjustment_usd_to_neutral": btc_adjust_usd,
        "unrealized_pnl_total_usd": float(tbl["unrealized_pnl_usd"].sum(skipna=True)) if not tbl.empty else 0.0,
    }
    return tbl, summary


def btc_rebalance_target_notional(
    positions: list[Position],
    betas: dict[str, float],
) -> dict[str, float | str]:
    """
    Compute the BTC leg required to achieve beta-neutrality adjusting only BTC.
    Returns: {required_btc_side, required_btc_notional_usd, current_btc_exposure_usd, other_exposure_usd}
    """
    other_exposure = 0.0
    btc_exposure = 0.0
    for p in positions:
        beta = float(betas.get(p.ticker, 1.0 if p.ticker == "BTC" else np.nan))
        if not np.isfinite(beta):
            continue
        exp = p.notional_usd * p.direction * beta
        if p.ticker == "BTC":
            btc_exposure += exp
        else:
            other_exposure += exp

    needed_btc_exposure = -other_exposure
    side = "LONG" if needed_btc_exposure >= 0 else "SHORT"
    return {
        "other_exposure_usd": other_exposure,
        "current_btc_exposure_usd": btc_exposure,
        "required_btc_side": side,
        "required_btc_notional_usd": abs(needed_btc_exposure),
    }

