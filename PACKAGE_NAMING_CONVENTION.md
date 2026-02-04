# Package Naming Convention

## Format
`verification_package_<purpose>_<timestamp>`

## Examples
- `verification_package_chatgpt_fixes_20260126_144056` - For verifying ChatGPT fixes
- `verification_package_phase_0_1_2_20260126_120000` - For Phase 0/1/2 verification
- `verification_package_msm_baseline_20260126_150000` - For MSM baseline verification

## Purpose Tags
- `chatgpt_fixes` - Verification of fixes based on ChatGPT feedback
- `phase_0_1_2` - Verification of Phase 0/1/2 implementation
- `msm_baseline` - MSM baseline experiment verification
- `regime_eval` - Regime evaluation verification
- `funding_fix` - Funding calculation fix verification

## Timestamp Format
`YYYYMMDD_HHMMSS` (e.g., 20260126_144056)

## Usage
Run the appropriate packaging script:
- `package_verification_chatgpt_fixes.py` - For ChatGPT fixes verification
- Future scripts can follow the same pattern with different purpose tags
