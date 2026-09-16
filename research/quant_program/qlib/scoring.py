"""Alpha Quality Score (AQS) and the standard anomaly scorecard.

Weights (sum 100) and why
-------------------------
mechanism        20  An edge without a counterparty story is the most likely to be noise or
                     to decay silently. It gets the largest weight, and a hard cap (below).
oos_significance 15  Out-of-sample, cost-inclusive evidence is the core statistical test.
regime_robust    10  Crypto regimes (2021 bull, 2022 bear, 2024-26) differ violently.
param_stability  10  Broad plateaus survive; single-point optima do not.
cost_resilience  10  Many crypto anomalies live inside the spread of the assets they need.
capacity         10  A real edge that cannot absorb meaningful size is a hobby.
sample_size       8  Independent (declustered) observations, not raw rows.
decay             7  Evidence the effect is not already fading (recent vs early sample).
implementation    5  Venue access, automation, data latency (higher = easier).
counterparty_risk 5  Exchange/venue/collateral/smart-contract risk (higher = safer).

Each component is scored 0-5. AQS = sum(weight * score / 5).
Hard gates (applied after weighting):
  * mechanism <= 1           -> AQS capped at 40 (no plausible mechanism = higher hurdle);
  * oos_significance <= 1    -> recommendation capped at WATCH;
  * cost_resilience == 0     -> recommendation REJECT (does not survive realistic costs);
  * sample_size <= 1         -> recommendation capped at WATCH.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

import pandas as pd

WEIGHTS = {"mechanism": 20, "oos_significance": 15, "regime_robust": 10, "param_stability": 10,
           "cost_resilience": 10, "capacity": 10, "sample_size": 8, "decay": 7,
           "implementation": 5, "counterparty_risk": 5}
assert sum(WEIGHTS.values()) == 100

RECOMMENDATIONS = ["REJECT", "WATCH", "PAPER TRADE", "SMALL LIVE TEST", "PRODUCTION CANDIDATE"]
CLASSES = ["PERSISTENT RISK PREMIUM", "BEHAVIORAL", "STRUCTURAL ALPHA", "FORCED FLOW",
           "MICROSTRUCTURE", "LIKELY NOISE"]


@dataclass
class Scorecard:
    name: str
    family: str
    mechanism: str = ""
    counterparty: str = ""
    why_they_trade: str = ""
    why_opportunity_exists: str = ""
    why_it_persists: str = ""
    what_kills_it: str = ""
    half_life: str = ""
    capacity: str = ""
    liquidity: str = ""
    execution_difficulty: str = ""
    venue_risk: str = ""
    data_quality: str = ""
    classification: str = "LIKELY NOISE"
    # statistics (filled from results)
    stats: dict = field(default_factory=dict)          # n, mean, median, win, sharpe, mae, mfe, maxdd, ...
    in_sample: dict = field(default_factory=dict)
    out_of_sample: dict = field(default_factory=dict)
    cost_sensitivity: str = ""
    regime_dependence: str = ""
    component_scores: dict = field(default_factory=dict)  # 0-5 per WEIGHTS key
    confidence: str = "LOW"
    notes: str = ""
    # study-specific evidence gates: {reason: max recommendation}; applied after the generic gates
    gates: dict = field(default_factory=dict)

    def aqs(self) -> float:
        s = sum(WEIGHTS[k] * self.component_scores.get(k, 0) / 5 for k in WEIGHTS)
        if self.component_scores.get("mechanism", 0) <= 1:
            s = min(s, 40.0)
        return round(s, 1)

    def recommendation(self) -> str:
        c = self.component_scores
        a = self.aqs()
        if c.get("cost_resilience", 0) == 0 or self.classification == "LIKELY NOISE":
            return "REJECT"
        rec = ("PRODUCTION CANDIDATE" if a >= 80 else "SMALL LIVE TEST" if a >= 68 else
               "PAPER TRADE" if a >= 55 else "WATCH" if a >= 35 else "REJECT")
        cap = "WATCH" if (c.get("oos_significance", 0) <= 1 or c.get("sample_size", 0) <= 1) else None
        caps = ([cap] if cap else []) + list(self.gates.values())
        for cp in caps:
            if RECOMMENDATIONS.index(rec) > RECOMMENDATIONS.index(cp):
                rec = cp
        return rec

    def to_row(self) -> dict:
        d = asdict(self)
        d.update({f"score_{k}": self.component_scores.get(k, 0) for k in WEIGHTS})
        d["AQS"] = self.aqs()
        d["recommendation"] = self.recommendation()
        for k in ("stats", "in_sample", "out_of_sample", "component_scores", "gates"):
            d[k] = str(d[k])
        return d

    def to_markdown(self) -> str:
        s, i, o = self.stats, self.in_sample, self.out_of_sample
        fmt = lambda d: "; ".join(f"{k} {v:.4g}" if isinstance(v, float) else f"{k} {v}" for k, v in d.items()) or "—"
        comp = ", ".join(f"{k} {self.component_scores.get(k, 0)}" for k in WEIGHTS)
        return "\n".join([
            f"### {self.name}",
            f"*Family:* {self.family} · **AQS {self.aqs()}** · **{self.recommendation()}** · "
            f"Classification: {self.classification} · Confidence: {self.confidence}",
            "",
            "| Field | Assessment |", "|---|---|",
            f"| Mechanism | {self.mechanism} |",
            f"| Counterparty | {self.counterparty} |",
            f"| Why they trade | {self.why_they_trade} |",
            f"| Why the opportunity exists | {self.why_opportunity_exists} |",
            f"| Why it persists | {self.why_it_persists} |",
            f"| What would kill it | {self.what_kills_it} |",
            f"| Expected half-life | {self.half_life} |",
            f"| Capacity / liquidity | {self.capacity} / {self.liquidity} |",
            f"| Execution difficulty | {self.execution_difficulty} |",
            f"| Venue / counterparty risk | {self.venue_risk} |",
            f"| Data quality | {self.data_quality} |",
            f"| Statistics | {fmt(s)} |",
            f"| In-sample | {fmt(i)} |",
            f"| Out-of-sample | {fmt(o)} |",
            f"| Cost sensitivity | {self.cost_sensitivity} |",
            f"| Regime dependence | {self.regime_dependence} |",
            f"| Component scores (0-5) | {comp} |",
            f"| Evidence gates | {'; '.join(f'{k} → max {v}' for k, v in self.gates.items()) or 'none triggered'} |",
            "" if not self.notes else f"\n{self.notes}",
        ])


def rank(cards: list[Scorecard]) -> pd.DataFrame:
    df = pd.DataFrame([c.to_row() for c in cards])
    return df.sort_values("AQS", ascending=False).reset_index(drop=True) if len(df) else df


def score_sample_size(n_independent: int) -> int:
    return 0 if n_independent < 10 else 1 if n_independent < 25 else 2 if n_independent < 50 else \
        3 if n_independent < 100 else 4 if n_independent < 300 else 5


def score_oos(oos_t: float, oos_same_sign: bool) -> int:
    if oos_t != oos_t or not oos_same_sign:
        return 0
    return 1 if oos_t < 1 else 2 if oos_t < 1.65 else 3 if oos_t < 2 else 4 if oos_t < 3 else 5
