# Basket Composition: Which Coins Are Long vs Short

This document explains what makes up the Method A basket: which assets are held long, which are held short, and how the basket is structured.

---

## 1. Structure Overview

The basket is **dollar-neutral** and **highly diversified**:

- **~186 assets LONG** — you profit when these go up
- **~193 assets SHORT** — you profit when these go down
- **Equal weight per asset:** ~0.27% of notional per position (weight ≈ 0.00266)
- **Gross exposure:** ~50% long + ~50% short ≈ 100% (constrained by G=1.0)
- **Net exposure:** 0 (sum of weights = 0)

---

## 2. How the Split Was Chosen

The minimum-variance QP does not assign long vs short by fundamentals (e.g., “good” vs “bad”). It uses **return covariances** to minimize portfolio variance. Given:

- Dollar neutrality: sum(weights) = 0
- Per-asset cap: |weight| ≤ 10%
- Gross exposure cap: sum(|weights|) ≤ 1.0

the solver picks the combination of long/short positions that, in combination, produces the **lowest forecast volatility** of the basket’s daily P&L.

Assets that contribute to lower variance (e.g., via correlation structure) end up long; others end up short. The split is driven by the covariance matrix, not by alpha signals.

---

## 3. LONG Positions (186 assets)

You are **long** these assets. Example names (full list in `rank1_A_weights.csv`):

** majors:** BTC, ETH, BNB, ADA, DOT, ATOM, ARB, APT, etc.

** sample mid-caps:** AAVE, APT, ARB, AXS, CRV, FET, FIL, ICP, IMX, INJ, OP, SAND, etc.

** sample smaller names:** 1INCH, AGIX, AGLD, APE, BLUR, DYDX, GALA, GRT, etc.

The long leg spans majors, L1/L2 chains, DeFi, gaming, and other categories. The optimizer chooses them for covariance properties, not narrative.

---

## 4. SHORT Positions (193 assets)

You are **short** these assets. Example names:

** majors:** LINK, LTC, NEAR, OP, RENDER, SEI, SUI, UNI, XLM, XRP, STETH, etc.

** sample mid-caps:** KAS, KAVA, LDO, MANA, MINA, PENDLE, RUNE, SNX, STX, TIA, WLD, etc.

** sample smaller names:** JUV, LAZIO, PSG, SANTOS (fan tokens), MAGIC, PYTH, STRK, etc.

The short leg also includes majors and alts. Again, the choice is driven by covariance, not directional views.

---

## 5. Notable Pairs

Because the split is purely variance-driven, you end up with counterintuitive setups, e.g.:

- **LONG** BTC, ETH, ADA, DOT, ATOM, ARB — **SHORT** LINK, LTC, NEAR, OP, UNI, XRP, XLM
- Same sectors on both sides: e.g. LONG APT vs SHORT SUI, SEI

This shows that the same “sector” can appear on both sides; the optimizer cares about correlation, not labels.

---

## 6. Weight Per Asset

Each position has weight ≈ **0.00266** (≈0.27% of notional):

- Long leg: 186 × 0.00266 ≈ 49.5% gross
- Short leg: 193 × 0.00266 ≈ 51.3% gross  
- Net: 0 (dollar-neutral)

The per-asset cap of 10% is not binding; the optimizer prefers many small positions for diversification.

---

## 7. What Drives the Long/Short Split?

In practice, the scipy fallback QP uses:

1. Ledoit–Wolf covariance from 90-day returns
2. Initial guess: first half of assets long, second half short (by ordering)
3. Iteration to minimize variance subject to constraints

The exact assignment depends on:

- Asset order in the universe
- Covariance structure over the lookback window
- Feasibility under constraints (liquidity, caps, etc.)

So the split is **statistical**, not discretionary. Re-running with different data or parameters can change which assets are long vs short.

---

## 8. Full Lists

### Long (186 assets)

$MYRO, 1INCH, AAVE, ABT, ACM, ADA, AEVO, AGETH, AGI, AGIX, AGLD, AI16Z, AIOZ, AKT, ALCX, ALGO, ALICE, ALPH, ALPINE, ALT, AMPL, ANKRETH, ANT, ANYONE, APE, APEX, API3, APT, AQT, AR, ARB, ARG, ARK, ARKM, ARPA, ASR, ASTR, ATH, ATM, ATOM, AUCTION, AUDIO, AUSD, AXL, AXS, AZERO, BADGER, BAKE, BAL, BAN, BANANA, BAND, BAR, BAT, BCH, BEL, BGB, BICO, BIFI, BIGTIME, BLUR, BLZ, BNB, BNBX, BNSOL, BNT, BONE, BSV, BTC, BTC.B, BTG, C98, CAKE, CBETH, CBK, CELO, CEUR, CFX, CGPT, CHR, CHZ, CITY, CLOUD, COMP, CORE, COTI, CRO, CRV, CRVUSD, CTC, CTK, CTSI, CUSD, CVC, CVX, CYBER, DAO, DASH, DCR, DEEP, DEGO, DESO, DEXE, DIA, DODO, DOLA, DOT, DPI, DUSK, DYDX, EDU, EGLD, EIGEN, ELA, ELF, EMP, ENA, ENJ, ENS, ETC, ETH, ETH2X-FLI, ETHDYDX, ETHFI, ETHW, ETHX, EUL, EURC, EURS, EURT, EWT, FARM, FB, FDUSD, FET, FIDA, FIL, FIRO, FIS, FLIP, FLOW, FLUX, FORTH, FRXETH, FTN, FWOG, G, GAFI, GAL, GALA, GAS, GFI, GHO, GHST, GLM, GLMR, GMT, GMX, GNO, GNS, GOMINING, GRAIL, GRASS, GRS, GRT, GT, GTC, GUSD, HBAR, HFT, HIFI, HNT, HOOK, HUNT, ICP, ID, ILV, IMX, INDEX, INF, INJ, INTER, INV, IO, IOTA, JASMY, JTO, JUP

### Short (193 assets)

JUV, KAG, KAS, KAU, KAVA, KCS, KDA, KSM, KUB, KWENTA, LAZIO, LDO, LEO, LINK, LISTA, LISUSD, LIT, LMWR, LPT, LQTY, LRC, LSK, LTC, LUSD, LYX, MAGIC, MANA, MANTA, MASK, MAV, MAVIA, MBOX, MEME, METIS, MINA, MLK, MLN, MOCA, MOVR, MSOL, MTL, MX, NAKA, NEAR, NEO, NEON, NEXO, NMR, OCEAN, OETH, OG, OGN, OHM, OKB, OLAS, OM, OMG, OMNI, ONG, ONT, OP, ORAI, ORCA, ORDI, ORN, OSETH, OSMO, PAAL, PENDLE, PEOPLE, PERP, PHA, PIXEL, PNUT, POLS, POLYX, PORTAL, POWR, PRIME, PRO, PROM, PROPC, PSG, PUFFER, PUNDIX, PYR, PYTH, PYUSD, QNT, QTUM, RAD, RARE, RARI, RAY, RDNT, RENDER, RETH, RLC, RON, ROSE, RPL, RUNE, SAND, SANTOS, SAVAX, SBD, SCR, SD, SEI, SETH, SETH2, SFP, SFRXETH, SFUND, SKL, SNX, SSV, STATOM, STEEM, STETH, STG, STORJ, STRK, STSOL, STX, SUI, SUNDOG, SUPER, SUSHI, SWELL, SWETH, SXP, SYN, TAO, TBTC, TET, THETA, TIA, TIME, TKO, TKX, TLOS, TOMI, TON, TRAC, TRB, TRIAS, TRU, TWT, ULTIMA, UMA, UNI, UQC, USD+, USDD, USDE, VCNT, VET, VIC, VOXEL, W, WAVES, WBETH, WBT, WEMIX, WHITE, WIF, WLD, WNXM, WOO, XAUT, XCH, XLM, XMR, XNO, XRP, XSGD, XTZ, XVS, YFI, YFII, YGG, ZEN, ZEPH, ZETA, ZK, ZRO, ZRX

---

For exact weights, market cap, and ADV at rebalance, see `rank1_A_weights.csv`.
