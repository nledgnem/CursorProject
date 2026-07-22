"""
RWA off-hours effort — STEP 2: DARK CALENDAR.

Timestamp-level "is this name genuinely dark now?" = no venue that could form a price
for this name is open. Both money-making avenues REQUIRE the underlying to be dark:
  A) cross-venue discrepancy (needs a live off-venue price -> only meaningful at the
     dark/near-dark edges), and
  B) dark-window peer read-through (needs the traded name itself to be dark so the
     perp cannot converge to a live cash/extended print).

DST-AWARENESS: sessions are defined in each venue's LOCAL time and converted with
`zoneinfo`, which applies DST automatically. UTC offsets are NEVER hardcoded.

HOLIDAYS: weekends handled; US NYSE full-day closures + half-days are handled via the
hardcoded NYSE_HOLIDAYS / NYSE_HALF_DAYS sets (2025-2027; extend yearly). Non-US venue
holidays are NOT modelled, but foreign names are excluded from avenue B anyway.

CME SINGLE-STOCK FUTURES (effective 2026-07-27): CME launches 55 US single-stock
futures on CME Globex. For CME-covered names, Globex trades ~23h/weekday (Sun 17:00 CT
open .. Fri 16:00 CT close, with a daily 16:00-17:00 CT maintenance halt), which LIGHTS
the 01:00-07:00 UTC weekday window that used to be dark. So from the launch date, a
covered name has essentially NO weekday dark window; only weekends (Fri close -> Sun
reopen) and holidays remain dark. Coverage is read from the universe config column
`cme_ssf_covered` (Y). CONSERVATIVE STANCE: covered names are treated as LIT from the
launch date regardless of the (unknown) liquidity ramp of the new contracts.

EU OFF-VENUE (Eurex SSF / Tradegate): recorded per name via the config column
`eu_offvenue_coverage`. Eurex hours (~07:00-21:00 UTC) do NOT intersect the 01:00-07:00
UTC event window — they only touch the 07:00-09:00 UTC shoulder — and liquidity is
UNVERIFIED, so Eurex is NOT wired into the core dark determination; it is surfaced as a
caveat only.
"""
from __future__ import annotations

import csv
import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

# CME single-stock futures go live on this date; Globex only counts on/after it.
# Source: CME official fact card (May-2026, "list subject to change") + 2026-06-30 release.
# Globex equity hours (confirmed): Sun 18:00 ET .. Fri 17:00 ET, daily 17:00-18:00 ET
# maintenance (= Sun 17:00 CT .. Fri 16:00 CT, halt 16:00-17:00 CT, as encoded below).
# CAVEATS: (1) RE-VERIFY the final list at/after launch (fact card is the May version;
# the 6/30 release named SpaceX, absent from the May table -> list may be revised).
# (2) Coverage != liquidity: covered names are treated as LIT from launch regardless, but
# a dead contract forms no price -> log real CME volumes when re-verifying. (3) Contracts
# are quarterly (Mar/Jun/Sep/Dec), cash-settled. (4) SSFs also halt if ES is lock-limited
# overnight (rare; not modelled).
CME_SSF_EFFECTIVE = dt.date(2026, 7, 27)

# --------------------------------------------------------------------------- #
# Venue session definitions (LOCAL time; DST applied via zoneinfo).
# Times are (open_hh, open_mm, close_hh, close_mm) on weekdays (Mon-Fri).
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Venue:
    name: str
    tz: str
    open_hm: tuple[int, int]
    close_hm: tuple[int, int]
    holiday_calendar: str = ""  # "NYSE" -> apply US holiday closures / early closes


# Hardcoded NYSE full-day closures (owner-approved; extend yearly). date objects.
NYSE_HOLIDAYS: set[dt.date] = {
    # 2025
    dt.date(2025, 1, 1), dt.date(2025, 1, 20), dt.date(2025, 2, 17), dt.date(2025, 4, 18),
    dt.date(2025, 5, 26), dt.date(2025, 6, 19), dt.date(2025, 7, 4), dt.date(2025, 9, 1),
    dt.date(2025, 11, 27), dt.date(2025, 12, 25),
    # 2026
    dt.date(2026, 1, 1), dt.date(2026, 1, 19), dt.date(2026, 2, 16), dt.date(2026, 4, 3),
    dt.date(2026, 5, 25), dt.date(2026, 6, 19), dt.date(2026, 7, 3), dt.date(2026, 9, 7),
    dt.date(2026, 11, 26), dt.date(2026, 12, 25),
    # 2027
    dt.date(2027, 1, 1), dt.date(2027, 1, 18), dt.date(2027, 2, 15), dt.date(2027, 3, 26),
    dt.date(2027, 5, 31), dt.date(2027, 6, 18), dt.date(2027, 7, 5), dt.date(2027, 9, 6),
    dt.date(2027, 11, 25), dt.date(2027, 12, 24),
}
# Half-days: NYSE primary closes 13:00 ET; extended after-hours ends ~17:00 ET.
NYSE_HALF_DAYS: set[dt.date] = {
    dt.date(2025, 7, 3), dt.date(2025, 11, 28), dt.date(2025, 12, 24),
    dt.date(2026, 11, 27), dt.date(2026, 12, 24),
    dt.date(2027, 11, 26),
}
_HALF_DAY_CLOSE = {"US_PRIMARY": (13, 0), "US_EXTENDED": (17, 0)}


VENUES = {
    "US_PRIMARY": Venue("US_PRIMARY", "America/New_York", (9, 30), (16, 0), "NYSE"),
    # US extended = pre-market 04:00 ET .. after-hours 20:00 ET.
    "US_EXTENDED": Venue("US_EXTENDED", "America/New_York", (4, 0), (20, 0), "NYSE"),
    # Eurex SSF + Tradegate retail (Frankfurt). Only applied if a liquid contract exists.
    "EUREX_TRADEGATE": Venue("EUREX_TRADEGATE", "Europe/Berlin", (8, 0), (22, 0)),
    # Foreign home markets.
    "TWSE": Venue("TWSE", "Asia/Taipei", (9, 0), (13, 30)),          # TSM
    "COPENHAGEN": Venue("COPENHAGEN", "Europe/Copenhagen", (9, 0), (17, 0)),  # NVO
    "HELSINKI": Venue("HELSINKI", "Europe/Helsinki", (10, 0), (18, 30)),     # NOK
}

# Foreign names: their home market lights the UTC overnight window -> they are rarely
# dark in a useful window. Flagged for likely EXCLUSION from avenue B.
FOREIGN_HOME_VENUE = {"TSM": "TWSE", "NVO": "COPENHAGEN", "NOK": "HELSINKI"}

# Whether a liquid off-venue (Eurex SSF/Tradegate) contract exists per US name.
# None = UNVERIFIED (default). True/False only when confirmed. UNVERIFIED -> caveat.
OFFVENUE_CONTRACT: dict[str, bool | None] = {}

# Populated from the universe config (see load_coverage_from_config).
CME_SSF_COVERED: set[str] = set()     # cme_ssf_covered == Y  -> Globex lights weekday window
CME_SSF_ILLIQUID: set[str] = set()    # cme_ssf_liquid == N   -> covered but forms no price
EU_OFFVENUE: set[str] = set()         # eu_offvenue_coverage truthy -> Eurex shoulder caveat


def load_coverage_from_config(universe_csv: str | Path) -> None:
    """Populate CME_SSF_COVERED / CME_SSF_ILLIQUID / EU_OFFVENUE from the universe config.

    Columns:
      cme_ssf_covered       Y -> listed on CME (Globex could light the weekday window)
      cme_ssf_liquid        (launch-time) blank/Y -> treat covered name as LIT;
                            N -> covered but NOT forming a price (dead volume) -> keep DARK.
                            Coverage != liquidity: a listed-but-illiquid SSF forms no
                            off-hours price, so the name stays in the dark hunting ground.
      eu_offvenue_coverage  metadata only (Eurex shoulder; not wired into core window).
    """
    p = Path(universe_csv)
    if not p.exists():
        return
    CME_SSF_COVERED.clear()
    CME_SSF_ILLIQUID.clear()
    EU_OFFVENUE.clear()
    with p.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            t = (r.get("ticker") or "").strip().upper()
            if not t:
                continue
            if str(r.get("cme_ssf_covered", "")).strip().lower() in ("y", "yes", "true", "1"):
                CME_SSF_COVERED.add(t)
            if str(r.get("cme_ssf_liquid", "")).strip().lower() in ("n", "no", "false", "0"):
                CME_SSF_ILLIQUID.add(t)
            euv = str(r.get("eu_offvenue_coverage", "")).strip().lower()
            if euv and euv not in ("n", "no", "false", "0", "none", "unverified", ""):
                EU_OFFVENUE.add(t)


def _globex_open(ts_utc: dt.datetime) -> bool:
    """CME Globex equity session in Chicago local time (DST via zoneinfo).
    Sun 17:00 CT open .. Fri 16:00 CT close, daily 16:00-17:00 CT maintenance halt.
    Closed on full NYSE holidays (covered names stay dark on holidays, per spec)."""
    ct = ts_utc.astimezone(ZoneInfo("America/Chicago"))
    if ct.date() in NYSE_HOLIDAYS:
        return False
    wd = ct.weekday()          # Mon=0 .. Sun=6
    t = ct.time()
    halt_start, halt_end = dt.time(16, 0), dt.time(17, 0)
    if wd == 5:                # Saturday: closed all day
        return False
    if wd == 6:                # Sunday: opens 17:00 CT
        return t >= halt_end
    if wd == 4:                # Friday: open until 16:00 CT, then weekend
        return t < halt_start
    return not (halt_start <= t < halt_end)  # Mon-Thu: open except maintenance halt


@dataclass
class DarkState:
    ticker: str
    ts_utc: dt.datetime
    is_dark: bool
    open_venues: list[str]
    caveats: list[str] = field(default_factory=list)


def _venue_open(venue: Venue, ts_utc: dt.datetime) -> bool:
    """Is `venue` open at UTC instant ts_utc? Weekday + local session window, with
    NYSE holiday/half-day closures applied for US venues. Non-US venue holidays are
    NOT modelled (foreign names are excluded from avenue B anyway)."""
    local = ts_utc.astimezone(ZoneInfo(venue.tz))
    if local.weekday() >= 5:  # Sat/Sun in local time
        return False
    close_hm = venue.close_hm
    if venue.holiday_calendar == "NYSE":
        if local.date() in NYSE_HOLIDAYS:
            return False
        if local.date() in NYSE_HALF_DAYS:
            close_hm = _HALF_DAY_CLOSE.get(venue.name, venue.close_hm)
    o = local.replace(hour=venue.open_hm[0], minute=venue.open_hm[1], second=0, microsecond=0)
    c = local.replace(hour=close_hm[0], minute=close_hm[1], second=0, microsecond=0)
    return o <= local < c


def applicable_venues(ticker: str) -> list[str]:
    """Venues that could form a price for this name."""
    t = ticker.upper()
    venues = ["US_PRIMARY", "US_EXTENDED"]
    if t in FOREIGN_HOME_VENUE:
        venues.append(FOREIGN_HOME_VENUE[t])
    # Off-venue EU contract: include only if confirmed True.
    if OFFVENUE_CONTRACT.get(t) is True:
        venues.append("EUREX_TRADEGATE")
    return venues


def is_dark(ticker: str, ts_utc: dt.datetime) -> DarkState:
    """Return DarkState for a name at a UTC instant. dark == no applicable venue open."""
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=dt.timezone.utc)
    t = ticker.upper()
    open_now = [v for v in applicable_venues(t) if _venue_open(VENUES[v], ts_utc)]
    # CME Globex single-stock future, effective 2026-07-27, for covered names only.
    # A covered name flagged illiquid (cme_ssf_liquid=N) forms no price -> stays dark.
    if (t in CME_SSF_COVERED and t not in CME_SSF_ILLIQUID
            and ts_utc.date() >= CME_SSF_EFFECTIVE and _globex_open(ts_utc)):
        open_now.append("CME_GLOBEX")
    caveats: list[str] = []
    if t in FOREIGN_HOME_VENUE:
        caveats.append(f"foreign name: home market {FOREIGN_HOME_VENUE[t]} lights the "
                       f"overnight window -> likely EXCLUDE from avenue B")
    if t in CME_SSF_COVERED and t in CME_SSF_ILLIQUID:
        caveats.append("CME-listed but flagged illiquid (cme_ssf_liquid=N) -> forms no "
                       "off-hours price, treated as STILL DARK on weekdays")
    elif t in CME_SSF_COVERED:
        caveats.append("CME single-stock future covered (Globex lights weekday window from "
                       "2026-07-27); liquidity treated as LIT until volumes say otherwise")
    if t in EU_OFFVENUE:
        caveats.append("Eurex SSF exists (liquidity unverified); affects only the "
                       "07:00-09:00 UTC shoulder, not the core 01:00-07:00 UTC window")
    return DarkState(t, ts_utc, is_dark=(len(open_now) == 0), open_venues=open_now, caveats=caveats)


def dark_intervals_utc(ticker: str, date_utc: dt.date, step_minutes: int = 5
                       ) -> list[tuple[dt.datetime, dt.datetime]]:
    """Scan a UTC calendar day at `step_minutes` resolution; return dark [start,end) UTC
    intervals. Coarse by construction (step-resolution); fine for measurement, not fills."""
    start = dt.datetime.combine(date_utc, dt.time(0, 0), tzinfo=dt.timezone.utc)
    intervals: list[tuple[dt.datetime, dt.datetime]] = []
    cur = start
    end_of_day = start + dt.timedelta(days=1)
    run_start = None
    step = dt.timedelta(minutes=step_minutes)
    while cur < end_of_day:
        d = is_dark(ticker, cur).is_dark
        if d and run_start is None:
            run_start = cur
        elif not d and run_start is not None:
            intervals.append((run_start, cur))
            run_start = None
        cur += step
    if run_start is not None:
        intervals.append((run_start, end_of_day))
    return intervals


def total_dark_hours(ticker: str, date_utc: dt.date, step_minutes: int = 5) -> float:
    return round(sum((b - a).total_seconds() for a, b in
                     dark_intervals_utc(ticker, date_utc, step_minutes)) / 3600.0, 2)
