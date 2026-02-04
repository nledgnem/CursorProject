#!/usr/bin/env python3
"""
Run data lake updates: (1) fact_global_market_history, (2) extend price/mcap/volume/OHLC/exchange volume to latest.

Usage:
    python scripts/run_data_updates.py
    python scripts/run_data_updates.py --skip-global-history
    python scripts/run_data_updates.py --skip-incremental
"""

import sys
import argparse
import subprocess
from pathlib import Path

repo_root = Path(__file__).parent.parent
data_lake_dir = repo_root / "data" / "curated" / "data_lake"


def main():
    parser = argparse.ArgumentParser(description="Run data lake updates")
    parser.add_argument("--skip-global-history", action="store_true", help="Skip global market cap history fetch")
    parser.add_argument("--skip-incremental", action="store_true", help="Skip price/mcap/volume incremental update")
    parser.add_argument("--skip-ohlc", action="store_true", help="Skip OHLC backfill")
    parser.add_argument("--skip-exchange-volume-history", action="store_true", help="Skip exchange volume history")
    parser.add_argument("--global-days", type=int, default=3650, help="Days for global market cap history (default 3650)")
    parser.add_argument("--days-back", type=int, default=None, help="Days back for incremental update (default: from latest date)")
    args = parser.parse_args()

    scripts_dir = repo_root / "scripts"
    py = sys.executable

    # 1. Global market cap history
    if not args.skip_global_history:
        print("\n" + "=" * 80)
        print("[1/4] FACT_GLOBAL_MARKET_HISTORY")
        print("=" * 80)
        r = subprocess.run(
            [py, str(scripts_dir / "fetch_global_market_data.py"), "--history", "--days", str(args.global_days)],
            cwd=str(repo_root),
        )
        if r.returncode != 0:
            print("[WARN] Global market history fetch failed, continuing.")
    else:
        print("\n[1/4] Skipping global market history (--skip-global-history)")

    # 2. Incremental update (price, marketcap, volume)
    if not args.skip_incremental:
        print("\n" + "=" * 80)
        print("[2/4] INCREMENTAL UPDATE (fact_price, fact_marketcap, fact_volume)")
        print("=" * 80)
        cmd = [py, str(scripts_dir / "incremental_update.py")]
        if args.days_back is not None:
            cmd.extend(["--days-back", str(args.days_back)])
        r = subprocess.run(cmd, cwd=str(repo_root))
        if r.returncode != 0:
            print("[ERROR] Incremental update failed.")
            sys.exit(1)
    else:
        print("\n[2/4] Skipping incremental update (--skip-incremental)")

    # 3. OHLC backfill (extends to latest date in fact_price)
    if not args.skip_ohlc:
        print("\n" + "=" * 80)
        print("[3/4] OHLC BACKFILL (fact_ohlc)")
        print("=" * 80)
        r = subprocess.run(
            [py, str(scripts_dir / "fetch_analyst_tier_data.py"), "--ohlc"],
            cwd=str(repo_root),
        )
        if r.returncode != 0:
            print("[WARN] OHLC backfill failed, continuing.")
    else:
        print("\n[3/4] Skipping OHLC (--skip-ohlc)")

    # 4. Exchange volume history (extends to latest)
    if not args.skip_exchange_volume_history:
        print("\n" + "=" * 80)
        print("[4/4] EXCHANGE VOLUME HISTORY (fact_exchange_volume_history)")
        print("=" * 80)
        # fetch_high_priority_data runs trending, categories, markets, and exchange volume history.
        # We only need exchange volume history; the script doesn't support --exchange-volume-only.
        # Running full script is OK (adds/refreshes other daily data too).
        r = subprocess.run(
            [py, str(scripts_dir / "fetch_high_priority_data.py")],
            cwd=str(repo_root),
        )
        if r.returncode != 0:
            print("[WARN] High-priority fetch (exchange volume history etc.) failed, continuing.")
    else:
        print("\n[4/4] Skipping exchange volume history (--skip-exchange-volume-history)")

    print("\n" + "=" * 80)
    print("UPDATES COMPLETE")
    print("=" * 80)
    print("\nFunding (fact_funding) is from Coinglass, not CoinGecko.")
    print("To extend funding to latest, run:")
    print("  python scripts/fetch_coinglass_funding.py --incremental")
    print("  (Requires Coinglass API key: --api-key YOUR_KEY)")


if __name__ == "__main__":
    main()
