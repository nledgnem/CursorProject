#!/usr/bin/env python3
"""
Quick summary of funding rate data length results.
Shows top 10 and bottom 10 based on current results file.
"""

import json
from datetime import datetime

results_file = "funding_data_length_results.json"

try:
    with open(results_file, 'r') as f:
        results = json.load(f)
except FileNotFoundError:
    print(f"Results file {results_file} not found. Run analyze_funding_data_length.py first.")
    exit(1)

if not results:
    print("No results found in file.")
    exit(1)

# Sort by data length
results_sorted = sorted(results, key=lambda x: x["data_length"], reverse=True)

print("=" * 80)
print("FUNDING RATE DATA LENGTH SUMMARY")
print("=" * 80)
print(f"\nTotal coins analyzed: {len(results)}")
print(f"Results file: {results_file}\n")

# Top 10
print("=" * 80)
print("TOP 10 COINS BY DATA LENGTH (Longest History)")
print("=" * 80)
print(f"{'Rank':<6} {'Symbol':<10} {'Days':<8} {'First Date':<12} {'Last Date':<12}")
print("-" * 80)

for i, r in enumerate(results_sorted[:10], 1):
    symbol = r["symbol"]
    length = r["data_length"]
    first = r["first_date"] if r["first_date"] else "N/A"
    last = r["last_date"] if r["last_date"] else "N/A"
    print(f"{i:<6} {symbol:<10} {length:<8} {str(first):<12} {str(last):<12}")

# Bottom 10 (non-zero)
print("\n" + "=" * 80)
print("BOTTOM 10 COINS BY DATA LENGTH (Shortest History)")
print("=" * 80)
print(f"{'Rank':<6} {'Symbol':<10} {'Days':<8} {'First Date':<12} {'Last Date':<12}")
print("-" * 80)

non_zero = [r for r in results_sorted if r["data_length"] > 0]
if non_zero:
    bottom_10 = non_zero[-10:] if len(non_zero) >= 10 else non_zero
    for i, r in enumerate(reversed(bottom_10), 1):
        symbol = r["symbol"]
        length = r["data_length"]
        first = r["first_date"] if r["first_date"] else "N/A"
        last = r["last_date"] if r["last_date"] else "N/A"
        print(f"{i:<6} {symbol:<10} {length:<8} {str(first):<12} {str(last):<12}")
else:
    print("No coins with data found.")

# Zero data coins
zero_data = [r for r in results_sorted if r["data_length"] == 0]
if zero_data:
    print("\n" + "=" * 80)
    print(f"COINS WITH NO DATA ({len(zero_data)} coins)")
    print("=" * 80)
    symbols = [r["symbol"] for r in zero_data]
    print(", ".join(symbols))

# Summary stats
zero_count = sum(1 for r in results if r["data_length"] == 0)
avg_length = sum(r["data_length"] for r in results) / len(results) if results else 0

print("\n" + "=" * 80)
print("SUMMARY STATISTICS")
print("=" * 80)
print(f"Total coins analyzed: {len(results)}")
print(f"Coins with data: {len(results) - zero_count}")
print(f"Coins with no data: {zero_count}")
if results_sorted:
    print(f"Max data length: {results_sorted[0]['data_length']} days ({results_sorted[0]['symbol']})")
    if non_zero:
        print(f"Min data length (non-zero): {non_zero[-1]['data_length']} days ({non_zero[-1]['symbol']})")
    print(f"Average data length: {avg_length:.1f} days")
