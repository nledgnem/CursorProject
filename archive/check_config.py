#!/usr/bin/env python3
"""Check baseline config is valid."""

import yaml

config = yaml.safe_load(open('majors_alts_monitor/config_baseline.yaml'))

print('Config validation:')
print(f'  walk_forward: {config["backtest"]["walk_forward"]}')
print(f'  risk_management present: {"risk_management" in config["backtest"]}')
print(f'  weights: {list(config["regime"]["composite"]["default_weights"].keys())}')
print(f'  funding_skew in weights: {"funding_skew" in config["regime"]["composite"]["default_weights"]}')
print(f'  oi_risk in weights: {"oi_risk" in config["regime"]["composite"]["default_weights"]}')
print(f'  Total weight: {sum(config["regime"]["composite"]["default_weights"].values())}')
