#!/usr/bin/env python3
"""
BTCDOM Assumption Ledger — Audit report (terminal only).
Answers: Data path, execution engine, physics formula, and pipeline (Silver vs Bronze).
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_LAKE = REPO_ROOT / "data" / "curated" / "data_lake"

def main():
    print("=" * 70)
    print("BTCDOM ASSUMPTION LEDGER — AUDIT REPORT")
    print("=" * 70)

    # 1. Data Sourcing
    binance_path = DATA_LAKE / "binance_btcdom.csv"
    print("\n1. DATA SOURCING: Official Binance BTCDOM historical baseline")
    print(f"   File path: {binance_path}")
    print(f"   Exists: {binance_path.exists()}")

    # 2. Execution Engine
    backfill_script = REPO_ROOT / "scripts" / "data_ingestion" / "btcdom_backfill.py"
    print("\n2. EXECUTION ENGINE: Script that calculates our custom BTCDOM")
    print(f"   Script: {backfill_script}")
    print(f"   Exists: {backfill_script.exists()}")

    # 3. The Physics (from index_calculator.py + btcdom_backfill.py)
    print("\n3. THE PHYSICS: Exact mathematical formula for custom BTCDOM")
    print("   - Type: Fixed-quantity, price-weighted index (not a simple market cap ratio).")
    print("   - Basket: Top 20 altcoins by market cap on each Thursday rebalance;")
    print("             exclusions: BTC, stables, wrapped/staked, inverse/leveraged.")
    print("   - Weights: Market-cap proportional — w_i = (MC_i / sum(MC_j)) normalized to sum 1.")
    print("   - Rebalance price per alt i: P_i = BTC_price / alt_price_i (BTC per unit of alt).")
    print("   - Quantities: q_i = w_i / P_i so that at rebalance sum(q_i * P_i) = 1.")
    print("   - Divisor: At rebalance, divisor = (sum q_i * P_i) / index_target = 1 / index_target;")
    print("             index_target = base_index_level on first period, else last index value.")
    print("   - Daily index: I(t) = sum_i [ q_i * clamp(P_i(t), P_i*(1-delta), P_i*(1+delta)) ] / divisor;")
    print("             delta = 0.3 (30% band); clamp uses rebalance-day P_i.")
    print("   - Base divisor / level: base_index_level = 2448.02529635 (Binance BTCDOM close 2024-07-04).")
    print("   - Forward-fill: missing alt prices ffilled up to max_ffill_days=3 within segment.")

    # 4. The Pipeline
    data_loader_ref = REPO_ROOT / "scripts" / "data_ingestion" / "data_loader.py"
    print("\n4. THE PIPELINE: Silver vs Bronze")
    print("   DataLoader (data_loader.py) loads:")
    print("     - fact_price.parquet")
    print("     - fact_marketcap.parquet")
    silver_price = DATA_LAKE / "silver_fact_price.parquet"
    silver_mcap = DATA_LAKE / "silver_fact_marketcap.parquet"
    bronze_price = DATA_LAKE / "fact_price.parquet"
    bronze_mcap = DATA_LAKE / "fact_marketcap.parquet"
    print("   Bronze (current source): fact_price.parquet, fact_marketcap.parquet")
    print(f"     fact_price exists: {bronze_price.exists()}")
    print(f"     fact_marketcap exists: {bronze_mcap.exists()}")
    print("   Silver (pristine): silver_fact_price.parquet, silver_fact_marketcap.parquet")
    print(f"     silver_fact_price exists: {silver_price.exists()}")
    print(f"     silver_fact_marketcap exists: {silver_mcap.exists()}")
    print("   Conclusion: Custom BTCDOM calculator is still using the BRONZE layer")
    print("              (fact_price.parquet, fact_marketcap.parquet). It is NOT wired to")
    print("              silver_fact_price.parquet or silver_fact_marketcap.parquet.")

    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
