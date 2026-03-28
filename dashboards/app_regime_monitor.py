from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = REPO_ROOT / "data" / "state" / "macro_state.db"
PLOT_BG = "#0F172A"
GRID = "#334155"
TEXT_MUTED = "#94a3b8"
TEXT_MAIN = "#e2e8f0"


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


def _regime_from_environment_apr(apr_pct: float) -> Regime:
    # Pure presentation mapping for the UI, calibrated by .cursorrules.
    if pd.isna(apr_pct):
        return Regime("Unknown", "#6b7280")
    if apr_pct < 2.0:
        return Regime("The Cold Flush", "#2563eb")
    if 2.0 <= apr_pct < 5.0:
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
    s = pd.to_numeric(d.get("Fragmentation_Spread"), errors="coerce")
    if s is None:
        return float("nan")
    s = s.dropna()
    if len(s) < 20:
        return float("nan")
    return float(s.quantile(0.85))


def _compute_spread_thresholds(d: pd.DataFrame) -> tuple[float, float]:
    """
    Returns (elevated_threshold, critical_threshold) using historical quantiles.
    """
    s = pd.to_numeric(d.get("Fragmentation_Spread"), errors="coerce")
    if s is None:
        return float("nan"), float("nan")
    s = s.dropna()
    if len(s) < 20:
        return float("nan"), float("nan")
    return float(s.quantile(0.70)), float(s.quantile(0.85))


def _spread_severity(value: float, elevated: float, critical: float) -> tuple[str, str]:
    if pd.isna(value):
        return "Unknown", TEXT_MUTED
    if pd.notna(critical) and value > critical:
        return "Critical", "#EF4444"
    if pd.notna(elevated) and value > elevated:
        return "Elevated", "#F59E0B"
    return "Normal", "#64748B"


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
    if env_apr_pct < 2.0:
        return "Cold Flush", "<2% APR", "Capital defense regime; deployment should be near zero."
    if 2.0 <= env_apr_pct < 5.0:
        return "Recovery Ramp", "2–5% APR", "Scale risk gradually; avoid binary switches."
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
        if stress_tier in {"Elevated", "Critical"}:
            return "Consistent with defense", "Stress elevated supports caution in Cold Flush."
        return "Consistent with defense", "Stress normal; still remain defensive under Cold Flush."
    if apr_zone == "Golden Pocket":
        if stress_tier == "Critical":
            return "Conflict", "Golden Pocket with critical stress: do not trust full risk-on sizing."
        if stress_tier == "Elevated":
            return "Caution", "Golden Pocket with elevated stress: monitor for fragmentation / rollover."
        return "Consistent", "Stress normal supports risk-on deployment."
    if apr_zone == "Recovery Ramp":
        if stress_tier == "Critical":
            return "Conflict", "Recovery with critical stress: scaling in is not supported."
        return "Contextual", "Recovery depends on stability; use stress tier to modulate sizing."
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
    if regime.name == "The Cold Flush":
        if spread_sev in {"Elevated", "Critical"}:
            return "Hold reserve. Stress elevated inside cold regime; avoid aggressive re-risking."
        return "Hold reserve. Wait for Recovery Ramp confirmation before scaling risk."
    if regime.name == "The Recovery Ramp":
        if spread_sev == "Critical":
            return "Scale in cautiously. Recovery improving, but stress is critical—reduce sizing."
        return "Scale in gradually. Focus on smooth execution and avoid binary switches."
    if regime.name == "The Golden Pocket":
        if spread_sev == "Critical":
            return "Risk-on regime, but stress contradicts—tighten sizing / monitor for exhaustion."
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
    spread_thr = _compute_spread_danger_threshold(d)
    spread_status = "Danger" if pd.notna(spread_thr) and pd.notna(spread) and spread > spread_thr else "Healthy"

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
        delta=f"thr={spread_thr:.6f}" if pd.notna(spread_thr) else None,
    )
    stance = f"{w_action}" if w_action != "Unknown" else "Unknown"
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
        y1=2,
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
        y=2,
        line_dash="dot",
        line_color="rgba(255, 255, 255, 0.3)",
        annotation_text="2% (Recovery Ramp Entry)",
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
    spread_thr = _compute_spread_danger_threshold(d)
    spread = d["Fragmentation_Spread"]
    if pd.notna(spread_thr):
        bar_colors = ["#EF4444" if (pd.notna(v) and v > spread_thr) else "#64748B" for v in spread.tolist()]
    else:
        bar_colors = ["#64748B"] * len(d)

    fig.add_trace(
        go.Bar(
            x=d["decision_date"],
            y=d["Fragmentation_Spread"],
            marker_color=bar_colors,
            name="Spread (%)",
            hovertemplate="Fragmentation_Spread: %{y:.6f}<br>micro-units: %{customdata:.0f}<extra></extra>",
            customdata=(d["Fragmentation_Spread"] * 1_000_000.0),
        ),
        row=3,
        col=1,
    )
    if pd.notna(spread_thr):
        fig.add_hline(
            y=spread_thr,
            line_dash="dash",
            line_color="#EF4444",
            line_width=1,
            row=3,
            col=1,
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
        title_text="Spread (%)",
        showgrid=True,
        gridcolor=GRID,
        row=3,
        col=1,
    )

    # Regime transition flags: draw faint vertical dotted lines when crossing boundaries.
    env = d["Environment_APR"].astype(float)
    bounds = (2.0, 5.0, 15.0)
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
    badge_spread_color = "#EF4444" if stress_tier == "Critical" else "#F59E0B" if stress_tier == "Elevated" else "#64748B"
    _add_terminal_badge(
        fig,
        "y3",
        f"[ {_format_spread(last_s)} · {stress_tier} ]" if pd.notna(last_s) else "[ NaN ]",
        last_s,
        badge_spread_color,
    )

    st.plotly_chart(fig, width="stretch")


def main() -> None:
    st.set_page_config(page_title="Macro Regime Monitor", layout="wide")

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
        thr_txt = (
            f"Elev>{_format_spread(spread_elev)} · Crit>{_format_spread(spread_crit)}"
            if pd.notna(spread_crit)
            else "Thresholds unavailable (insufficient history)"
        )
        _decision_card(
            "Stress State",
            _format_spread(spread_now),
            f"{spread_sev} ({thr_txt})",
            f"Derived from Fragmentation_Spread vs historical thresholds. {_format_spread_micro_label(spread_now)}.",
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


if __name__ == "__main__":
    main()

