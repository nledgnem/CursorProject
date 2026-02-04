# Experiments Registry

This directory contains experiment specifications organized by category.

## Structure

- `experiments/msm/` - Market State Monitor experiments (pure MSM, selection-independent)
- `experiments/alpha/` - Coin selection / alpha experiments
- `experiments/exec/` - Execution/gating/sizing experiments

## Experiment Spec Format

Each experiment is a YAML file with:

- `title`: Human-readable name
- `experiment_id`: Machine-readable ID (unique)
- `category_path`: Category (e.g., "msm", "alpha", "exec")
- `features`: List of feature IDs and parameters
- `target`: Portfolio definition (alts index vs BTC, exclusions, N, weighting, horizon)
- `state_mapping`: Regime thresholds, hysteresis, persistence, smoothing
- `backtest`: Backtest parameters (rebalance frequency, costs on/off)

## Running Experiments

```bash
# Run single experiment
python -m majors_alts_monitor.run --experiment experiments/msm/msm_v1.yaml

# Run batch of experiments
python -m majors_alts_monitor.sweep --glob "experiments/msm/*.yaml"
```
