from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class TickerNormalizationRules:
    strip_suffixes: tuple[str, ...] = ()
    strip_prefixes: tuple[str, ...] = ()


_WS_RE = re.compile(r"\s+")


def normalize_ticker(raw: str, rules: TickerNormalizationRules) -> str:
    """
    Normalize a ticker string for cross-venue matching.

    Intended for mapping venue tickers to panel tickers (case-insensitive),
    not for making assumptions about contract specifications.
    """
    s = (raw or "").strip()
    s = _WS_RE.sub("", s)
    s = s.upper()

    for p in rules.strip_prefixes:
        if not p:
            continue
        p2 = p.strip().upper()
        if p2 and s.startswith(p2):
            s = s[len(p2) :]

    for suf in rules.strip_suffixes:
        if not suf:
            continue
        suf2 = suf.strip().upper()
        if suf2 and s.endswith(suf2):
            s = s[: -len(suf2)]

    # Common separators -> remove for matching (BTC-USD, BTC/USD).
    s = s.replace("-", "").replace("/", "").replace(":", "")

    return s


def build_normalization_rules(
    strip_suffixes: Iterable[str] | None,
    strip_prefixes: Iterable[str] | None,
) -> TickerNormalizationRules:
    return TickerNormalizationRules(
        strip_suffixes=tuple(strip_suffixes or ()),
        strip_prefixes=tuple(strip_prefixes or ()),
    )

