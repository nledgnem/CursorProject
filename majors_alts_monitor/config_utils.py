"""Configuration utilities for deep merging experiment specs."""

from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries, with override taking precedence.
    
    Rules:
    - If both values are dicts, recursively merge
    - If override value is a list, replace base list (no merging)
    - Otherwise, override value replaces base value
    
    Args:
        base: Base configuration dictionary
        override: Override configuration dictionary (takes precedence)
    
    Returns:
        Merged configuration dictionary
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            # Both are dicts: recursively merge
            result[key] = deep_merge(result[key], value)
        elif isinstance(value, list):
            # Lists are replaced, not merged
            result[key] = value
        else:
            # Override takes precedence
            result[key] = value
    
    return result


def apply_msm_config_overrides(config: Dict[str, Any], experiment_spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply MSM-specific config overrides, forcing non-MSM knobs to be disabled.
    
    In MSM mode, we want to:
    - Disable alt_selection filters
    - Ignore neutrality_mode (use fixed weights)
    - Use MSM-specific target config
    - Override universe settings from experiment spec
    
    Args:
        config: Base configuration
        experiment_spec: Experiment specification
    
    Returns:
        Modified configuration with MSM overrides applied
    """
    # Start with a copy
    config = config.copy()
    
    # Get MSM target config
    target_config = experiment_spec.get("target", {})
    short_leg = target_config.get("short_leg", {})
    long_leg = target_config.get("long_leg", {})
    
    # Override universe settings for MSM
    if "universe" not in config:
        config["universe"] = {}
    
    # Update basket size from MSM config
    config["universe"]["basket_size"] = short_leg.get("n", 20)
    
    # Override min_volume_usd if specified in MSM config
    if "min_volume_usd" in short_leg:
        config["universe"]["min_volume_usd"] = short_leg["min_volume_usd"]
    
    # Force-disable alt_selection for MSM (pure market cap selection)
    config["universe"]["alt_selection"] = {"enabled": False}
    
    # Ignore neutrality_mode for MSM (we use fixed major weights)
    # Keep it in config but it won't be used
    
    # Merge backtest config
    if "backtest" in experiment_spec:
        if "backtest" not in config:
            config["backtest"] = {}
        config["backtest"] = deep_merge(config["backtest"], experiment_spec["backtest"])
    
    # Merge state_mapping into regime config
    if "state_mapping" in experiment_spec:
        if "regime" not in config:
            config["regime"] = {}
        if "composite" not in config["regime"]:
            config["regime"]["composite"] = {}
        
        state_mapping = experiment_spec["state_mapping"]
        if "n_regimes" in state_mapping:
            config["regime"]["n_regimes"] = state_mapping["n_regimes"]
        if "thresholds" in state_mapping:
            thresholds = state_mapping["thresholds"]
            config["regime"]["composite"]["threshold_low"] = thresholds.get("low", -0.5)
            config["regime"]["composite"]["threshold_high"] = thresholds.get("high", 0.5)
            config["regime"]["composite"]["threshold_strong_low"] = thresholds.get("strong_low", -1.5)
            config["regime"]["composite"]["threshold_strong_high"] = thresholds.get("strong_high", 1.5)
        if "hysteresis" in state_mapping:
            hyst = state_mapping["hysteresis"]
            config["regime"]["composite"]["hysteresis_low"] = hyst.get("low_band", -0.3)
            config["regime"]["composite"]["hysteresis_high"] = hyst.get("high_band", 0.3)
        if "persistence" in state_mapping:
            persist = state_mapping["persistence"]
            config["regime"]["composite"]["min_duration_days"] = persist.get("min_duration_days", 3)
            config["regime"]["composite"]["requires_stronger_signal"] = persist.get("requires_stronger_signal", True)
    
    return config
