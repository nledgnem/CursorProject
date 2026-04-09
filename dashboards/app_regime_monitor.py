from __future__ import annotations

import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import polars as pl
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from repo_paths import macro_state_db_path  # noqa: E402
from src.macro_regime.gate_policy import (  # noqa: E402
    ENVIRONMENT_APR_ENTRY_GATE_PCT,
    FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING,
    calculate_risk_weight,
)

DEFAULT_DB_PATH = macro_state_db_path()

# Optional explorer inputs (used only when the user opens the ticker APR expander).
DATA_LAKE_DIR = REPO_ROOT / "data" / "curated" / "data_lake"
DIM_ASSET_PARQUET = DATA_LAKE_DIR / "dim_asset.parquet"
SILVER_FACT_FUNDING_PARQUET = DATA_LAKE_DIR / "silver_fact_funding.parquet"
PLOT_BG = "#0F172A"
GRID = "#334155"
TEXT_MUTED = "#94a3b8"
TEXT_MAIN = "#e2e8f0"

# Fragmentation Spread hard gate (raw decimal, no percentage scaling).
# Canonical values live in src/macro_regime/gate_policy.py.

SPREAD_LABEL_TOXIC = "Chaos/Contagion"
SPREAD_LABEL_HEALTHY = "Healthy"


@dataclass(frozen=True)
class Regime:
    name: str
    color: str


def _connect(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path), check_same_thread=False)


def _load_latest(conn: sqlite3.Connection) -> Optional[pd.Series]:
    df = pd.read_sql_query(
        """
SELECT *
FROM macro_features
WHERE decision_date IS NOT NULL
ORDER BY decision_date DESC
LIMIT 1
        """.strip(),
        conn,
    )
    if df.empty:
        return None
    return df.iloc[0]


def _load_last_n_days(conn: sqlite3.Connection, n: int = 90) -> pd.DataFrame:
    df = pd.read_sql_query(
        f"""
SELECT decision_date, Environment_APR, w_risk, Fragmentation_Spread
FROM macro_features
WHERE decision_date IS NOT NULL
ORDER BY decision_date DESC
LIMIT {int(n)}
        """.strip(),
        conn,
    )
    if df.empty:
        return df
    df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
    df = df.dropna(subset=["decision_date"]).sort_values("decision_date")
    return df


def _parse_symbol_list(raw: str) -> list[str]:
    parts = [p.strip() for p in (raw or "").split(",")]
    syms = [p.upper() for p in parts if p]
    # de-duplicate while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for s in syms:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


@st.cache_data(ttl=3600)
def _load_symbol_to_asset_id() -> dict[str, str]:
    """
    Map user-entered symbol (e.g. "BTC") to canonical asset_id (used in parquet rows).
    """
    if not DIM_ASSET_PARQUET.exists():
        return {}
    # Load only the minimal columns needed.
    df = pd.read_parquet(DIM_ASSET_PARQUET, columns=["asset_id", "symbol"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["asset_id"] = df["asset_id"].astype(str)
    mapping = dict(zip(df["symbol"].tolist(), df["asset_id"].tolist()))
    return mapping


@st.cache_data(ttl=3600)
def _compute_daily_apr_by_ticker(symbols: tuple[str, ...], start_date, end_date) -> pd.DataFrame:
    """
    Compute annualized daily APR (%) per ticker, derived from `silver_fact_funding.parquet`.

    funding_rate_raw_pct is defined as the native 8-hour decimal funding rate.
    annualized APR decimal = daily_mean_funding_rate * 1095 (365*3 blocks/day).
    display APR % = annualized_apr_decimal * 100.
    """
    if not SILVER_FACT_FUNDING_PARQUET.exists():
        raise FileNotFoundError(f"Missing parquet: {SILVER_FACT_FUNDING_PARQUET}")

    if not symbols:
        return pd.DataFrame(columns=["date", "symbol", "daily_apr_pct"])

    mapping = _load_symbol_to_asset_id()
    missing = [s for s in symbols if s not in mapping]
    if missing:
        raise KeyError(f"Symbols not found in dim_asset.parquet: {missing}")

    asset_ids = [mapping[s] for s in symbols]

    rate_col = "funding_rate_raw_pct"
    # Polars will filter by this column; if schema differs we fail loudly.
    lf = pl.scan_parquet(str(SILVER_FACT_FUNDING_PARQUET))
    cols = set(lf.collect_schema().names())
    if rate_col not in cols:
        if "funding_rate" in cols:
            rate_col = "funding_rate"
        else:
            raise KeyError(f"Expected funding column not found in parquet. Have: {sorted(cols)}")

    lf = lf.with_columns(pl.col("date").cast(pl.Date))
    lf = lf.filter(
        pl.col("asset_id").is_in(asset_ids)
        & (pl.col("date") >= pl.lit(start_date))
        & (pl.col("date") <= pl.lit(end_date))
    )

    daily = (
        lf.group_by(["asset_id", "date"])
        .agg(pl.col(rate_col).mean().alias("daily_mean_rate"))
        .with_columns(
            (pl.col("daily_mean_rate") * 1095.0 * 100.0).alias("daily_apr_pct")
        )
    )

    # Join back symbol labels for plotting (do it in pandas for simplicity).
    map_pdf = pd.DataFrame({"asset_id": asset_ids, "symbol": list(symbols)})
    daily_pdf = daily.collect().to_pandas()
    if not daily_pdf.empty:
        pdf = daily_pdf.merge(map_pdf, on="asset_id", how="left").loc[:, ["date", "symbol", "daily_apr_pct"]]
    else:
        pdf = pd.DataFrame(columns=["date", "symbol", "daily_apr_pct"])
    pdf = pdf.sort_values(["symbol", "date"]).reset_index(drop=True)
    return pdf


def _regime_from_environment_apr(apr_pct: float) -> Regime:
    # Pure presentation mapping for the UI, calibrated by .cursorrules.
    if pd.isna(apr_pct):
        return Regime("Unknown", "#6b7280")
    if apr_pct < ENVIRONMENT_APR_ENTRY_GATE_PCT:
        return Regime("The Cold Flush", "#2563eb")
    if ENVIRONMENT_APR_ENTRY_GATE_PCT <= apr_pct < 5.0:
        return Regime("The Recovery Ramp", "#f59e0b")
    if 5.0 <= apr_pct <= 15.0:
        return Regime("The Golden Pocket", "#16a34a")
    return Regime("Leverage Exhaustion", "#dc2626")


def _warning_flag(window: pd.DataFrame) -> Tuple[str, str]:
    if window is None or len(window) < 2:
        return ("OK", "#16a34a")
    last = window.iloc[-1]
    prev = window.iloc[-2]
    apr = float(last["Environment_APR"]) if pd.notna(last["Environment_APR"]) else float("nan")
    spr_last = float(last["Fragmentation_Spread"]) if pd.notna(last["Fragmentation_Spread"]) else float("nan")
    spr_prev = float(prev["Fragmentation_Spread"]) if pd.notna(prev["Fragmentation_Spread"]) else float("nan")

    widening = pd.notna(spr_last) and pd.notna(spr_prev) and (spr_last > spr_prev)
    high_apr = pd.notna(apr) and (apr >= 15.0)
    if widening and high_apr:
        return ("ROTATION / EXHAUSTION WARNING", "#dc2626")
    return ("OK", "#16a34a")


def _compute_spread_danger_threshold(d: pd.DataFrame) -> float:
    # Fixed idiosyncratic toxic ceiling (raw decimal).
    return float(FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING)


def _compute_spread_thresholds(d: pd.DataFrame) -> tuple[float, float]:
    """
    Fixed spread thresholds to preserve the function signature.
    Returns (elevated_threshold, critical_threshold) where both map to the
    idiosyncratic toxic ceiling.
    """
    thr = float(FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING)
    return thr, thr


def _spread_severity(value: float, elevated: float, critical: float) -> tuple[str, str]:
    if pd.isna(value):
        return "Unknown", TEXT_MUTED
    toxic_ceiling = float(FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING)
    if pd.notna(value) and value >= toxic_ceiling:
        return SPREAD_LABEL_TOXIC, "#EF4444"
    return SPREAD_LABEL_HEALTHY, "#64748B"


def _format_delta(v: Optional[float], suffix: str = "") -> Optional[str]:
    if v is None or pd.isna(v):
        return None
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}{suffix}"


def _safe_float(x) -> float:
    try:
        return float(x) if pd.notna(x) else float("nan")
    except Exception:
        return float("nan")


def _get_step(d: pd.DataFrame, k_back: int) -> Optional[pd.Series]:
    if d is None or d.empty:
        return None
    if len(d) <= k_back:
        return None
    return d.iloc[-(k_back + 1)]


def _deployment_label(w: float) -> str:
    if pd.isna(w):
        return "Unknown"
    if w <= 0.05:
        return "Defensive"
    if w >= 0.95:
        return "Risk-On"
    return "Scaling"


def _classify_apr_zone(env_apr_pct: float) -> tuple[str, str, str]:
    """
    Returns (zone_name, rule_text, implication).
    env_apr_pct is in percentage points (e.g., 4.0 == 4% APR).
    """
    if pd.isna(env_apr_pct):
        return "Unknown", "APR is NaN", "Cannot classify regime; verify upstream data."
    if env_apr_pct < ENVIRONMENT_APR_ENTRY_GATE_PCT:
        return (
            "Cold Flush",
            f"<{ENVIRONMENT_APR_ENTRY_GATE_PCT:.0f}% APR",
            "Capital defense regime; deployment should be near zero.",
        )
    if ENVIRONMENT_APR_ENTRY_GATE_PCT <= env_apr_pct < 5.0:
        return (
            "Recovery Ramp",
            f"{ENVIRONMENT_APR_ENTRY_GATE_PCT:.0f}–5% APR",
            "Scale risk gradually; avoid binary switches.",
        )
    if 5.0 <= env_apr_pct <= 15.0:
        return "Golden Pocket", "5–15% APR", "Max deployment allowed; monitor stress for divergence."
    return "Leverage Exhaustion", ">15% APR", "De-risk mechanically; asymmetry increases in right tail."


def _classify_gate_state(w: float) -> tuple[str, str]:
    """
    Returns (gate_state, rule_text).
    """
    if pd.isna(w):
        return "Unknown", "w_risk is NaN"
    if w <= 0.05:
        return "Gate at floor", "w_risk ≤ 0.05"
    if w >= 0.95:
        return "Gate at ceiling", "w_risk ≥ 0.95"
    return "Gate in ramp", "0.05 < w_risk < 0.95"


def _gate_vs_regime_consistency(apr_zone: str, w: float) -> tuple[str, str]:
    """
    Returns (label, explanation) strictly from apr zone + w_risk.
    """
    gate_state, gate_rule = _classify_gate_state(w)
    if apr_zone == "Cold Flush":
        if pd.notna(w) and w <= 0.05:
            return "Consistent", f"{gate_state} matches Cold Flush intent ({gate_rule})."
        return "Conflict", f"Cold Flush expects floor gate; observed {gate_state} ({gate_rule})."
    if apr_zone == "Golden Pocket":
        if pd.notna(w) and w >= 0.95:
            return "Consistent", f"{gate_state} matches Golden Pocket intent ({gate_rule})."
        return "Conflict", f"Golden Pocket expects ceiling gate; observed {gate_state} ({gate_rule})."
    if apr_zone == "Leverage Exhaustion":
        if pd.isna(w):
            return "Unknown", "w_risk missing."
        if w < 0.95:
            return "Consistent", f"Exhaustion implies de-risking; observed {gate_state} ({gate_rule})."
        return "Conflict", f"Exhaustion implies de-risking; observed {gate_state} ({gate_rule})."
    if apr_zone == "Recovery Ramp":
        return "Contextual", f"Recovery Ramp supports gradual scaling; observed {gate_state} ({gate_rule})."
    return "Unknown", "Unable to classify consistency."


def _stress_regime_consistency(apr_zone: str, stress_tier: str) -> tuple[str, str]:
    """
    Returns (label, explanation) based on tier vs zone intent.
    """
    if apr_zone == "Cold Flush":
        if stress_tier == SPREAD_LABEL_TOXIC:
            return "Consistent with defense", "Chaos/Contagion spread reinforces defense under Cold Flush."
        return "Consistent with defense", "Spread healthy, but APR < 2% still keeps defense stance."
    if apr_zone == "Golden Pocket":
        if stress_tier == SPREAD_LABEL_TOXIC:
            return "Conflict", "Golden Pocket with Chaos/Contagion: gate veto (Gate OFF / Stables)."
        return "Consistent", "Spread healthy supports risk-on deployment."
    if apr_zone == "Recovery Ramp":
        if stress_tier == SPREAD_LABEL_TOXIC:
            return "Conflict", "Recovery with Chaos/Contagion: scaling veto (Gate OFF / Stables)."
        return "Contextual", "Recovery depends on stability; spread healthy supports gradual scaling."
    if apr_zone == "Leverage Exhaustion":
        return "Consistent with de-risking", "Overheated zone; stress tier informs urgency but stance remains defensive."
    return "Unknown", "Unable to classify stress-regime relationship."


def _detect_rule_conflicts(apr_zone: str, w: float, stress_tier: str) -> list[str]:
    conflicts: list[str] = []
    g_label, g_expl = _gate_vs_regime_consistency(apr_zone, w)
    if g_label == "Conflict":
        conflicts.append(f"Gate vs regime conflict: {g_expl}")
    s_label, s_expl = _stress_regime_consistency(apr_zone, stress_tier)
    if s_label == "Conflict":
        conflicts.append(f"Stress vs regime conflict: {s_expl}")
    return conflicts


def _format_spread(value: float) -> str:
    """
    Primary display format: raw fixed-decimal for traceability.
    """
    if pd.isna(value):
        return "NaN"
    return f"{value:.6f}"


def _format_spread_micro_label(value: float) -> str:
    if pd.isna(value):
        return "micro-units: NaN"
    return f"{value * 1_000_000:.0f} micro-units"


def _regime_interpretation(regime: Regime) -> str:
    mapping = {
        "The Cold Flush": "Capital defense regime; risk deployment should be near zero.",
        "The Recovery Ramp": "Early recovery; scale risk gradually to avoid whipsaw.",
        "The Golden Pocket": "Healthy trend baseline; maximum allowed deployment.",
        "Leverage Exhaustion": "Overheated conditions; mechanical de-risking is required.",
    }
    return mapping.get(regime.name, "Regime classification unavailable.")


def _operational_takeaway(regime: Regime, w_risk_action: str, spread_sev: str) -> str:
    if spread_sev == SPREAD_LABEL_TOXIC:
        return "Gate OFF / Stables. Chaos/Contagion veto triggered."
    if regime.name == "The Cold Flush":
        return "Hold reserve. Wait for Recovery Ramp confirmation before scaling risk."
    if regime.name == "The Recovery Ramp":
        return "Scale in gradually. Focus on smooth execution and avoid binary switches."
    if regime.name == "The Golden Pocket":
        if w_risk_action == "De-risking":
            return "De-risking while in Golden Pocket—verify if APR is rolling over or stress rising."
        return "Max deployment allowed. Monitor spread for fragmentation early-warning."
    if regime.name == "Leverage Exhaustion":
        return "De-risk mechanically. High APR regime with asymmetry to the downside."
    return "Review latest data; stance could not be determined."


def _card(title: str, value: str, subtitle: str = "", accent: str = GRID) -> None:
    st.markdown(
        f"""
<div style="padding: 12px 14px; border-radius: 14px; border: 1px solid {accent}; background: {PLOT_BG};">
  <div style="font-size: 12px; color: {TEXT_MUTED}; letter-spacing: 0.02em;">{title}</div>
  <div style="margin-top: 2px; font-size: 18px; font-weight: 800; color: {TEXT_MAIN};">{value}</div>
  <div style="margin-top: 6px; font-size: 12px; color: {TEXT_MUTED};">{subtitle}</div>
</div>
        """.strip(),
        unsafe_allow_html=True,
    )


def _decision_card(title: str, metric_value: str, state_label: str, implication: str, accent: str = GRID) -> None:
    st.markdown(
        f"""
<div style="padding: 12px 14px; border-radius: 14px; border: 1px solid {accent}; background: {PLOT_BG};">
  <div style="font-size: 12px; color: {TEXT_MUTED}; letter-spacing: 0.02em;">{title}</div>
  <div style="margin-top: 2px; font-size: 20px; font-weight: 900; color: {TEXT_MAIN}; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;">{metric_value}</div>
  <div style="margin-top: 6px; font-size: 12px; font-weight: 800; color: {TEXT_MAIN};">{state_label}</div>
  <div style="margin-top: 4px; font-size: 12px; color: {TEXT_MUTED};">{implication}</div>
</div>
        """.strip(),
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=60)
def _load_state(db_path_str: str, n: int = 90) -> tuple[Optional[pd.Series], pd.DataFrame]:
    db_path = Path(db_path_str)
    conn = _connect(db_path)
    try:
        latest = _load_latest(conn)
        window = _load_last_n_days(conn, n)
    finally:
        conn.close()
    return latest, window

def _w_risk_action(now: float, prev: float) -> str:
    if pd.isna(now) or pd.isna(prev):
        return "Unknown"
    if abs(now - prev) < 1e-9:
        return "No Change"
    return "Scaling In" if now > prev else "De-risking"


def _render_summary_ribbon(window: pd.DataFrame, latest: pd.Series, regime: Regime) -> None:
    if window is None or window.empty:
        return

    d = window.copy().sort_values("decision_date")
    last = d.iloc[-1]
    prev_1w = d.iloc[-2] if len(d) >= 2 else None

    env = float(last["Environment_APR"]) if pd.notna(last.get("Environment_APR")) else float("nan")
    env_prev = float(prev_1w["Environment_APR"]) if prev_1w is not None and pd.notna(prev_1w.get("Environment_APR")) else float("nan")
    env_delta = (env - env_prev) if (pd.notna(env) and pd.notna(env_prev)) else None

    w_now = float(last["w_risk"]) if pd.notna(last.get("w_risk")) else float("nan")
    w_prev = float(prev_1w["w_risk"]) if prev_1w is not None and pd.notna(prev_1w.get("w_risk")) else float("nan")
    w_action = _w_risk_action(w_now, w_prev) if (pd.notna(w_now) and pd.notna(w_prev)) else "Unknown"

    spread = float(last["Fragmentation_Spread"]) if pd.notna(last.get("Fragmentation_Spread")) else float("nan")
    spread_thr = float(FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING)
    spread_toxic = pd.notna(spread) and pd.notna(spread_thr) and spread >= spread_thr
    spread_status = SPREAD_LABEL_TOXIC if spread_toxic else SPREAD_LABEL_HEALTHY
    gate_allowed = pd.notna(env) and pd.notna(spread) and (env >= ENVIRONMENT_APR_ENTRY_GATE_PCT) and (spread < spread_thr)

    c1, c2, c3, c4, c5 = st.columns([1.25, 1.55, 1.2, 1.35, 1.65])
    c1.metric("Regime", regime.name)
    c2.metric(
        "Environment_APR",
        f"{env:.2f}%" if pd.notna(env) else "NaN",
        delta=f"{env_delta:+.2f}%" if env_delta is not None else None,
    )
    c3.metric("w_risk", f"{w_now:.2f}" if pd.notna(w_now) else "NaN")
    c4.metric(
        "Fragmentation_Spread",
        f"{spread:.6f}" if pd.notna(spread) else "NaN",
        delta=f"toxic>={spread_thr:.6f}",
    )
    stance = f"{w_action}" if gate_allowed and w_action != "Unknown" else "Gate OFF / Stables"
    c5.markdown(
        f"""
<div style="padding: 10px 12px; border-radius: 12px; border: 1px solid {GRID}; background: {PLOT_BG};">
  <div style="font-size: 12px; color: #94a3b8;">Stance</div>
  <div style="font-size: 16px; font-weight: 700; color: #e2e8f0;">{stance} · Spread {spread_status}</div>
</div>
        """.strip(),
        unsafe_allow_html=True,
    )


def _add_terminal_badge(fig: go.Figure, yref: str, value_text: str, y: float, color: str) -> None:
    if pd.isna(y):
        return
    fig.add_annotation(
        x=1.01,
        xref="paper",
        y=y,
        yref=yref,
        text=value_text,
        showarrow=False,
        xanchor="left",
        align="left",
        font=dict(color="#0b1220", size=12, family="monospace"),
        bgcolor=color,
        bordercolor=color,
        borderwidth=1,
        opacity=1.0,
    )


def _render_institutional_chart(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        st.info("Not enough data to plot.")
        return

    d = df.copy()
    d["decision_date"] = pd.to_datetime(d["decision_date"], errors="coerce")
    d = d.dropna(subset=["decision_date"]).sort_values("decision_date")
    for c in ("Environment_APR", "w_risk", "Fragmentation_Spread"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.5, 0.25, 0.25],
        vertical_spacing=0.05,
        subplot_titles=(
            "1. Macro Environment (APR)",
            "2. Capital Gate (w_risk)",
            "3. Crash Sensor (Spread)",
        ),
    )

    # --- PANE 1: Environment APR (state) ---
    fig.add_trace(
        go.Scatter(
            x=d["decision_date"],
            y=d["Environment_APR"],
            mode="lines",
            line=dict(color="#F8FAFC", width=2),
            name="APR (%)",
            hovertemplate="APR: %{y:.2f}%<extra></extra>",
        ),
        row=1,
        col=1,
    )

    apr_max = float(d["Environment_APR"].max()) if pd.notna(d["Environment_APR"]).any() else 20.0
    # Regime bands (higher opacity for dark-mode legibility)
    fig.add_hrect(
        y0=15,
        y1=apr_max + 5,
        fillcolor="rgba(220, 38, 38, 0.12)",
        line_width=0,
        row=1,
        col=1,
    )  # Exhaustion (Crimson)
    fig.add_hrect(
        y0=5,
        y1=15,
        fillcolor="rgba(217, 119, 6, 0.12)",
        line_width=0,
        row=1,
        col=1,
    )  # Golden Pocket (Amber)
    fig.add_hrect(
        y0=2,
        y1=5,
        fillcolor="rgba(5, 150, 105, 0.12)",
        line_width=0,
        row=1,
        col=1,
    )  # Recovery (Green)
    fig.add_hrect(
        y0=-30,
        y1=float(ENVIRONMENT_APR_ENTRY_GATE_PCT),
        fillcolor="rgba(37, 99, 235, 0.12)",
        line_width=0,
        row=1,
        col=1,
    )  # Cold Flush (Blue)

    # Crisp physical boundaries
    fig.add_hline(
        y=15,
        line_dash="dot",
        line_color="rgba(255, 255, 255, 0.3)",
        annotation_text="15% (Exhaustion Gate)",
        annotation_position="top left",
        annotation_font=dict(color="rgba(255,255,255,0.5)", size=10),
        row=1,
        col=1,
    )
    fig.add_hline(
        y=5,
        line_dash="dot",
        line_color="rgba(255, 255, 255, 0.3)",
        annotation_text="5% (Golden Pocket Entry)",
        annotation_position="top left",
        annotation_font=dict(color="rgba(255,255,255,0.5)", size=10),
        row=1,
        col=1,
    )
    fig.add_hline(
        y=float(ENVIRONMENT_APR_ENTRY_GATE_PCT),
        line_dash="dot",
        line_color="rgba(255, 255, 255, 0.3)",
        annotation_text=f"{ENVIRONMENT_APR_ENTRY_GATE_PCT:.0f}% (Recovery Ramp Entry)",
        annotation_position="top left",
        annotation_font=dict(color="rgba(255,255,255,0.5)", size=10),
        row=1,
        col=1,
    )

    # --- PANE 2: w_risk (deployment) ---
    fig.add_trace(
        go.Scatter(
            x=d["decision_date"],
            y=d["w_risk"],
            mode="lines",
            fill="tozeroy",
            line=dict(color="#2DD4BF", width=2, shape="hv"),
            fillcolor="rgba(45, 212, 191, 0.10)",
            name="w_risk",
            hovertemplate="w_risk: %{y:.2f}<extra></extra>",
        ),
        row=2,
        col=1,
    )

    # --- PANE 3: Fragmentation Spread (warning) ---
    spread_thr = float(FRAGMENTATION_IDIOSYNCRATIC_TOXIC_CEILING)
    spread = d["Fragmentation_Spread"]
    bar_colors = ["#EF4444" if (pd.notna(v) and v >= spread_thr) else "#64748B" for v in spread.tolist()]

    fig.add_trace(
        go.Bar(
            x=d["decision_date"],
            y=d["Fragmentation_Spread"],
            marker_color=bar_colors,
            name="Fragmentation_Spread (raw)",
            hovertemplate="Fragmentation_Spread: %{y:.6f}<br>micro-units: %{customdata:.0f}<extra></extra>",
            customdata=(d["Fragmentation_Spread"] * 1_000_000.0),
        ),
        row=3,
        col=1,
    )
    fig.add_hline(
        y=spread_thr,
        line_dash="dash",
        line_color="#EF4444",
        line_width=1,
        row=3,
        col=1,
        annotation_text=f"Toxic ceiling {spread_thr:.6f}",
        annotation_position="top left",
        annotation_font=dict(color="#EF4444", size=10),
    )

    fig.update_layout(
        height=800,
        template="plotly_dark",
        hovermode="x unified",
        showlegend=False,
        margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor=PLOT_BG,
        plot_bgcolor=PLOT_BG,
    )

    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        showspikes=True,
        spikecolor="white",
        spikesnap="cursor",
        spikemode="across",
        spikethickness=1,
    )
    fig.update_yaxes(
        title_text="APR (%)",
        showgrid=True,
        gridcolor=GRID,
        row=1,
        col=1,
    )
    fig.update_yaxes(
        title_text="Weight",
        range=[0, 1.05],
        showgrid=False,
        tickvals=[0, 0.5, 1],
        row=2,
        col=1,
    )
    fig.update_yaxes(
        title_text="Fragmentation_Spread (raw)",
        showgrid=True,
        gridcolor=GRID,
        row=3,
        col=1,
    )

    # Regime transition flags: draw faint vertical dotted lines when crossing boundaries.
    env = d["Environment_APR"].astype(float)
    bounds = (ENVIRONMENT_APR_ENTRY_GATE_PCT, 5.0, 15.0)
    for b in bounds:
        crossed = (env.shift(1) < b) & (env >= b) | (env.shift(1) >= b) & (env < b)
        for x in d.loc[crossed.fillna(False), "decision_date"].tolist():
            fig.add_vline(
                x=x,
                line_dash="dot",
                line_width=1,
                line_color="rgba(255,255,255,0.10)",
            )

    for annotation in fig["layout"]["annotations"]:
        annotation["font"] = dict(size=12, color="#BDC3C7")

    # Right-edge terminal badges.
    last = d.iloc[-1]
    last_apr = float(last["Environment_APR"]) if pd.notna(last.get("Environment_APR")) else float("nan")
    last_w = float(last["w_risk"]) if pd.notna(last.get("w_risk")) else float("nan")
    last_s = float(last["Fragmentation_Spread"]) if pd.notna(last.get("Fragmentation_Spread")) else float("nan")
    apr_zone, apr_rule, _ = _classify_apr_zone(last_apr)
    gate_state, _ = _classify_gate_state(last_w)
    elev_thr, crit_thr = _compute_spread_thresholds(d)
    stress_tier, _stress_color = _spread_severity(last_s, elev_thr, crit_thr)
    _add_terminal_badge(
        fig,
        "y",
        f"[ {last_apr:.2f}% · {apr_zone} ]" if pd.notna(last_apr) else "[ NaN ]",
        last_apr,
        "#F8FAFC",
    )
    _add_terminal_badge(
        fig,
        "y2",
        f"[ {last_w:.2f} · {gate_state} ]" if pd.notna(last_w) else "[ NaN ]",
        last_w,
        "#2DD4BF",
    )
    badge_spread_color = "#EF4444" if stress_tier == SPREAD_LABEL_TOXIC else "#64748B"
    _add_terminal_badge(
        fig,
        "y3",
        f"[ {_format_spread(last_s)} · {stress_tier} ]" if pd.notna(last_s) else "[ NaN ]",
        last_s,
        badge_spread_color,
    )

    st.plotly_chart(fig, width="stretch")


def _password_gate() -> None:
    """Optional team login when DASHBOARD_PASSWORD is set (e.g. on Render)."""
    pwd = os.environ.get("DASHBOARD_PASSWORD", "").strip()
    if not pwd:
        return
    if st.session_state.get("_dashboard_auth_ok"):
        return
    st.title("Macro Regime Monitor")
    entered = st.text_input("Team password", type="password")
    if st.button("Continue"):
        if entered == pwd:
            st.session_state._dashboard_auth_ok = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()


def main() -> None:
    st.set_page_config(page_title="Macro Regime Monitor", layout="wide")
    _password_gate()

    # ----------------------------
    # Layer 1 — Header / identity
    # ----------------------------
    header_l, header_r = st.columns([0.72, 0.28])
    with header_l:
        st.markdown(
            f"<div style='font-size: 28px; font-weight: 900; color: {TEXT_MAIN};'>Macro Regime Monitor</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<div style='color: {TEXT_MUTED}; font-size: 13px;'>Read-only monitor · Source of truth: <code>macro_state.db</code> · Cadence: 00:05 / 08:05 / 16:05 UTC</div>",
            unsafe_allow_html=True,
        )

    with st.sidebar:
        st.header("Data Source")
        db_path_str = st.text_input("SQLite DB path", str(DEFAULT_DB_PATH))
        db_path = Path(db_path_str).expanduser()
        refresh = st.button("Refresh")
        st.divider()
        st.caption("Run the 8-hour pulse with `python scripts/live/live_data_fetcher.py`.")

    if refresh:
        st.cache_data.clear()

    if not db_path.exists():
        st.error(f"DB not found: {db_path}")
        st.stop()

    latest, window = _load_state(str(db_path), 90)

    if latest is None:
        st.warning("No rows found in macro_features. Ingest data first.")
        st.stop()

    d = window.copy().sort_values("decision_date") if window is not None else pd.DataFrame()
    last = d.iloc[-1] if d is not None and len(d) else None
    prev_1w = _get_step(d, 1)
    prev_4w = _get_step(d, 4)

    env_now = _safe_float(last.get("Environment_APR")) if last is not None else float("nan")
    env_1w = _safe_float(prev_1w.get("Environment_APR")) if prev_1w is not None else float("nan")
    env_4w = _safe_float(prev_4w.get("Environment_APR")) if prev_4w is not None else float("nan")
    env_d1 = (env_now - env_1w) if (pd.notna(env_now) and pd.notna(env_1w)) else None
    env_d4 = (env_now - env_4w) if (pd.notna(env_now) and pd.notna(env_4w)) else None

    w_now = _safe_float(last.get("w_risk")) if last is not None else float("nan")
    w_prev = _safe_float(prev_1w.get("w_risk")) if prev_1w is not None else float("nan")
    w_action = _w_risk_action(w_now, w_prev) if (pd.notna(w_now) and pd.notna(w_prev)) else "Unknown"

    spread_now = _safe_float(last.get("Fragmentation_Spread")) if last is not None else float("nan")
    spread_elev, spread_crit = _compute_spread_thresholds(d)
    spread_sev, spread_color = _spread_severity(spread_now, spread_elev, spread_crit)

    decision_date = str(last.get("decision_date")) if last is not None else str(latest.get("decision_date"))
    regime = _regime_from_environment_apr(env_now)
    apr_zone, apr_rule, apr_implication = _classify_apr_zone(env_now)
    deploy = _deployment_label(w_now)
    gate_state, gate_rule = _classify_gate_state(w_now)
    takeaway = _operational_takeaway(regime, w_action, spread_sev)

    with header_r:
        _card("As of", decision_date, "Latest decision date", accent=GRID)

    st.divider()

    # ----------------------------
    # Layer 2 — Decision summary row
    # ----------------------------
    row = st.columns([1.15, 1.35, 1.10, 1.25, 1.35, 1.05, 0.95])
    with row[0]:
        _decision_card(
            "Current Regime",
            f"{apr_zone}",
            f"Derived from Environment_APR: {env_now:.2f}% ({apr_rule})" if pd.notna(env_now) else "Derived from Environment_APR: NaN",
            apr_implication,
            accent=regime.color,
        )
    with row[1]:
        rationale = f"{apr_zone} + Stress={spread_sev} + Gate={gate_state}"
        _decision_card(
            "Recommended Stance",
            deploy if deploy != "Unknown" else "Unknown",
            f"Rule-derived: {rationale}",
            takeaway,
            accent=GRID,
        )
    with row[2]:
        _decision_card(
            "Capital Gate",
            f"{w_now:.2f}" if pd.notna(w_now) else "NaN",
            f"{gate_state} ({gate_rule})",
            f"vs prior decision: {w_prev:.2f} → {w_action}" if pd.notna(w_prev) else f"Δ unavailable → {w_action}",
            accent="#2DD4BF",
        )
    with row[3]:
        thr_txt = f"Toxic ceiling >= {_format_spread(spread_crit)} (raw decimal)"
        _decision_card(
            "Stress State",
            _format_spread(spread_now),
            f"{spread_sev} ({thr_txt})",
            f"Derived from Fragmentation_Spread vs fixed Idiosyncratic Gate {_format_spread(spread_crit)}. {_format_spread_micro_label(spread_now)}.",
            accent=spread_color,
        )
    with row[4]:
        deltas = " | ".join(
            [t for t in [
                f"1W {_format_delta(env_d1, 'pp')}" if env_d1 is not None else None,
                f"4W {_format_delta(env_d4, 'pp')}" if env_d4 is not None else None,
            ] if t]
        ) or "Deltas unavailable (insufficient history)"
        _decision_card(
            "Environment APR",
            f"{env_now:.2f}%" if pd.notna(env_now) else "NaN",
            f"Zone: {apr_zone} ({apr_rule})",
            deltas,
            accent=GRID,
        )
    with row[5]:
        _decision_card(
            "Gate Change",
            f"{w_action}",
            "Derived from w_risk vs prior decision",
            "Shows scaling direction; not an on/off switch.",
            accent=GRID,
        )
    with row[6]:
        _decision_card(
            "As of",
            decision_date,
            "Latest decision_date row in macro_state.db",
            "Freshness is required for PM trust.",
            accent=GRID,
        )

    st.markdown(
        f"""
<div style="margin-top: 10px; padding: 10px 14px; border-radius: 14px; border: 1px solid {GRID}; background: {PLOT_BG};">
  <div style="font-size: 12px; color: {TEXT_MUTED};">Operational takeaway</div>
  <div style="margin-top: 2px; font-size: 15px; font-weight: 800; color: {TEXT_MAIN};">{takeaway}</div>
</div>
        """.strip(),
        unsafe_allow_html=True,
    )

    st.divider()

    # ----------------------------
    # Layer 3 — Supporting diagnostics
    # ----------------------------
    diag = st.columns([1.2, 1.2, 1.2, 1.4])
    with diag[0]:
        _decision_card(
            "APR Zone",
            f"{env_now:.2f}%" if pd.notna(env_now) else "NaN",
            f"{apr_zone} ({apr_rule})",
            "Derived directly from Environment_APR thresholds.",
            accent=GRID,
        )
    with diag[1]:
        gv_label, gv_expl = _gate_vs_regime_consistency(apr_zone, w_now)
        _decision_card(
            "Gate vs Regime",
            f"{w_now:.2f}" if pd.notna(w_now) else "NaN",
            f"{gv_label}",
            gv_expl,
            accent=GRID if gv_label != "Conflict" else "#EF4444",
        )
    with diag[2]:
        sr_label, sr_expl = _stress_regime_consistency(apr_zone, spread_sev)
        _decision_card(
            "Stress-Regime Consistency",
            _format_spread(spread_now),
            sr_label,
            f"{sr_expl} {_format_spread_micro_label(spread_now)}.",
            accent=GRID if sr_label not in {"Conflict"} else "#EF4444",
        )
    with diag[3]:
        conflicts = _detect_rule_conflicts(apr_zone, w_now, spread_sev)
        if conflicts:
            _decision_card(
                "Rule Conflict",
                f"{len(conflicts)}",
                "Conflict detected",
                conflicts[0],
                accent="#EF4444",
            )
        else:
            _decision_card(
                "Rule Conflict",
                "0",
                "No conflict",
                "No detected contradictions between regime, gate, and stress rules.",
                accent="#64748B",
            )

    st.markdown(f"<div style='margin-top: 14px; font-size: 16px; font-weight: 900; color: {TEXT_MAIN};'>Historical context</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='color: {TEXT_MUTED}; font-size: 12px;'>Forensic view of state, gate, and stress over the last 90 decision points.</div>", unsafe_allow_html=True)
    _render_institutional_chart(window)

    with st.expander("Raw rows (latest 90)"):
        st.dataframe(window, width="stretch", hide_index=True)

    with st.expander("Daily APR by Ticker (from silver_fact_funding.parquet)"):
        st.markdown(
            "Select tickers (symbols) and we will compute **annualized daily APR (%)** from `silver_fact_funding.parquet`."
        )
        st.caption("Computation: daily mean funding_rate_raw_pct × 1095, shown as APR%. Funding_rate_raw_pct is the native 8-hour decimal funding rate.")
        sym_map = _load_symbol_to_asset_id()
        all_symbols = sorted(sym_map.keys())

        filter_q = st.text_input("Filter available symbols", value="", help="Type to search, e.g. 'BTC' or 'SOL'.")
        if filter_q:
            q = filter_q.upper().strip()
            options = [s for s in all_symbols if q in s]
        else:
            options = all_symbols

        default_sel = [s for s in ("BTC", "ETH") if s in options][:2]
        selected_syms = st.multiselect(
            "Select tickers (dropdown)",
            options=options,
            default=default_sel,
        )

        manual_raw_symbols = st.text_input(
            "Add symbols manually (comma-separated)",
            value="",
            help="Optional: if you know a symbol, you can type it here too.",
        )
        manual_syms = _parse_symbol_list(manual_raw_symbols)

        # Union: dropdown selections + manual additions (preserve order).
        syms: list[str] = []
        for s in list(selected_syms) + manual_syms:
            s = str(s).upper().strip()
            if not s:
                continue
            if s in syms:
                continue
            syms.append(s)

        default_start = None
        default_end = None
        # Default date range to the last 60 days available in the dashboard DB (fallback if parquet is missing).
        try:
            if not window.empty and "decision_date" in window.columns:
                last_dec = pd.to_datetime(window["decision_date"]).max()
                default_end = last_dec.date()
                default_start = (last_dec.date() - pd.Timedelta(days=60)).date()
        except Exception:
            default_start = None
            default_end = None

        start_d = st.date_input("Start date (UTC)", value=default_start) if default_start else st.date_input("Start date (UTC)", value=pd.Timestamp.utcnow().date() - pd.Timedelta(days=60))
        end_d = st.date_input("End date (UTC)", value=default_end) if default_end else st.date_input("End date (UTC)", value=pd.Timestamp.utcnow().date())

        if st.button("Plot daily APR"):
            if not syms:
                st.error("Please enter at least one symbol.")
            elif not DIM_ASSET_PARQUET.exists() or not SILVER_FACT_FUNDING_PARQUET.exists():
                st.error(
                    "Ticker APR explorer is unavailable on this deployment: missing `dim_asset.parquet` and/or `silver_fact_funding.parquet`."
                )
            else:
                try:
                    missing = [s for s in syms if s not in sym_map]
                    if missing:
                        st.error(f"Unknown symbols (not found in dim_asset.parquet): {missing}")
                        st.stop()

                    df_apr = _compute_daily_apr_by_ticker(tuple(syms), start_d, end_d)
                    if df_apr.empty:
                        st.warning("No data for the selected symbol(s)/date range.")
                    else:
                        fig = go.Figure()
                        for sym in sorted(df_apr["symbol"].unique()):
                            d = df_apr[df_apr["symbol"] == sym].sort_values("date")
                            fig.add_trace(
                                go.Scatter(
                                    x=d["date"],
                                    y=d["daily_apr_pct"],
                                    mode="lines",
                                    name=sym,
                                )
                            )
                        fig.update_layout(
                            height=450,
                            template="plotly_dark",
                            paper_bgcolor=PLOT_BG,
                            plot_bgcolor=PLOT_BG,
                            margin=dict(l=10, r=10, t=40, b=10),
                            legend_title_text="Ticker",
                            xaxis_title="Date",
                            yaxis_title="Daily Annualized APR (%)",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        st.dataframe(df_apr.tail(20), hide_index=True)
                except Exception as e:
                    st.exception(e)


if __name__ == "__main__":
    main()

