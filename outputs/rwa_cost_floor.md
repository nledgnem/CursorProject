# RWA Off-Hours — Step 1: Cost Floor (indicative spreads)

- Source: `perps_variational.csv` (73 daily snapshots, 2026-04-21 -> 2026-07-02) via Variational public `/metadata/stats`.
- Round-trip taker cost floor (optimistic) ~= one quoted spread; break-even move % ~= spread_bps / 100. Excludes funding + slippage beyond the clip.
- Realistic-clip basis: **size_100k** tier. Thresholds: PASS <= 100 bps, MARGINAL <= 200 bps, else FAIL. n<8 => INSUFFICIENT_DATA (not scored).
- Verdicts: PASS 18, MARGINAL 7, FAIL 1, INSUFFICIENT 3.

> CAVEAT: equity names appear in only a fraction of snapshots (coverage is recent/thin); low-n rows are not reliable. Base tier is tight for all names; the cost problem is at size (size_100k).

| Ticker | Co. | n | base med | s1k med | s100k med | breakeven % @100k | Verdict |
|---|---|--:|--:|--:|--:|--:|---|
| HIMS | Hims & Hers Health | 9 | 41.6 | 53.1 | 233.4 | 2.33 | FAIL_TOO_WIDE |
| USAR | USA Rare Earth | 10 | 12.4 | 22.3 | 199.5 | 2.0 | MARGINAL |
| AAOI | Applied Optoelectronics | 15 | 9.1 | 13.6 | 160.7 | 1.61 | MARGINAL |
| NVO | Novo Nordisk | 9 | 14.2 | 25.5 | 141.7 | 1.42 | MARGINAL |
| NOK | Nokia | 13 | 16.7 | 17.3 | 136.3 | 1.36 | MARGINAL |
| LLY | Eli Lilly | 9 | 21.2 | 34.0 | 127.5 | 1.27 | MARGINAL |
| NFLX | Netflix | 7 | 10.2 | 20.9 | 123.3 | 1.23 | INSUFFICIENT_DATA |
| NBIS | Nebius Group | 21 | 9.2 | 13.0 | 106.9 | 1.07 | MARGINAL |
| QCOM | Qualcomm | 23 | 6.7 | 11.9 | 102.1 | 1.02 | MARGINAL |
| ARM | Arm Holdings | 16 | 6.5 | 9.8 | 90.7 | 0.91 | PASS |
| LITE | Lumentum Holdings | 15 | 6.5 | 8.9 | 81.9 | 0.82 | PASS |
| TSM | Taiwan Semiconductor Manufacturing | 24 | 6.3 | 8.9 | 80.6 | 0.81 | PASS |
| COIN | Coinbase Global | 14 | 5.6 | 11.3 | 70.5 | 0.7 | PASS |
| AMD | Advanced Micro Devices | 20 | 5.2 | 8.4 | 67.0 | 0.67 | PASS |
| RKLB | Rocket Lab | 22 | 6.9 | 10.5 | 65.6 | 0.66 | PASS |
| CBRS | VERIFY - Chain Bridge Bancorp? | 21 | 6.1 | 9.7 | 63.7 | 0.64 | PASS |
| MSFT | Microsoft | 8 | 7.1 | 9.0 | 49.9 | 0.5 | PASS |
| HOOD | Robinhood Markets | 2 | 5.6 | 7.8 | 49.3 | 0.49 | INSUFFICIENT_DATA |
| PLTR | Palantir Technologies | 3 | 6.5 | 7.2 | 47.5 | 0.47 | INSUFFICIENT_DATA |
| MRVL | Marvell Technology | 21 | 4.5 | 7.8 | 45.3 | 0.45 | PASS |
| META | Meta Platforms | 8 | 6.8 | 9.4 | 39.7 | 0.4 | PASS |
| CRCL | Circle Internet Group | 14 | 6.2 | 7.6 | 38.3 | 0.38 | PASS |
| MSTR | Strategy (MicroStrategy) | 14 | 5.7 | 7.1 | 35.2 | 0.35 | PASS |
| GOOGL | Alphabet | 8 | 6.4 | 8.8 | 31.5 | 0.32 | PASS |
| SNDK | SanDisk | 17 | 4.0 | 5.0 | 28.1 | 0.28 | PASS |
| INTC | Intel | 27 | 4.1 | 6.1 | 27.2 | 0.27 | PASS |
| NVDA | Nvidia | 20 | 4.8 | 5.2 | 24.4 | 0.24 | PASS |
| TSLA | Tesla | 27 | 3.4 | 5.3 | 23.9 | 0.24 | PASS |
| MU | Micron Technology | 23 | 3.2 | 3.8 | 19.1 | 0.19 | PASS |
