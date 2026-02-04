"""Label mapping for MSM v0: percentile bins."""

import polars as pl
import numpy as np
from typing import List, Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


def compute_percentile_bins(
    values: List[float],
    bin_ranges: List[List[int]] = [[0, 20], [20, 40], [40, 60], [60, 80], [80, 100]],
    bin_names: List[str] = ["Red", "Orange", "Yellow", "YellowGreen", "Green"],
) -> Dict[str, Tuple[float, float]]:
    """
    Compute percentile thresholds for bins.
    
    Args:
        values: List of feature values
        bin_ranges: List of [min_pct, max_pct] for each bin
        bin_names: List of bin names
    
    Returns:
        Dict mapping bin_name to (min_threshold, max_threshold)
    """
    if len(values) == 0:
        return {}
    
    # Collect all unique percentile points needed
    percentile_points = set()
    for min_pct, max_pct in bin_ranges:
        percentile_points.add(min_pct)
        percentile_points.add(max_pct)
    
    # Compute percentiles at all needed points
    percentile_values = np.percentile(values, sorted(percentile_points))
    percentile_dict = dict(zip(sorted(percentile_points), percentile_values))
    
    # Create bin thresholds
    bins = {}
    for name, (min_pct, max_pct) in zip(bin_names, bin_ranges):
        bins[name] = (float(percentile_dict[min_pct]), float(percentile_dict[max_pct]))
    
    return bins


def assign_label(
    value: float,
    bins: Dict[str, Tuple[float, float]],
) -> str:
    """
    Assign label to a feature value based on percentile bins.
    
    Uses half-open intervals [min, max) except the last bin [min, max] to avoid overlaps.
    
    Args:
        value: Feature value
        bins: Dict mapping bin_name to (min_threshold, max_threshold)
    
    Returns:
        Bin name (label)
    """
    bin_names = list(bins.keys())
    
    for i, (bin_name, (min_thresh, max_thresh)) in enumerate(bins.items()):
        is_last_bin = (i == len(bin_names) - 1)
        
        if is_last_bin:
            # Last bin: inclusive on both ends [min, max]
            if min_thresh <= value <= max_thresh:
                return bin_name
        else:
            # Other bins: half-open [min, max)
            if min_thresh <= value < max_thresh:
                return bin_name
    
    # Edge case: value outside all bins (shouldn't happen, but handle gracefully)
    if value < min(bins.values(), key=lambda x: x[0])[0]:
        return bin_names[0]  # Return first bin
    else:
        return bin_names[-1]  # Return last bin


def compute_percentile_rank(
    value: float,
    values: List[float],
) -> float:
    """
    Compute percentile rank of a value within a list of values.
    
    Args:
        value: Value to rank
        values: List of values to rank against
    
    Returns:
        Percentile rank (0-100)
    """
    if len(values) == 0:
        return float('nan')
    
    # Count values <= value
    n_below_or_equal = sum(1 for v in values if v <= value)
    
    # Percentile rank: (n_below_or_equal / n_total) * 100
    percentile_rank = (n_below_or_equal / len(values)) * 100.0
    
    return percentile_rank


def compute_labels_v0_0(
    feature_values: List[float],
    bin_ranges: List[List[int]] = [[0, 20], [20, 40], [40, 60], [60, 80], [80, 100]],
    bin_names: List[str] = ["Red", "Orange", "Yellow", "YellowGreen", "Green"],
) -> Tuple[List[str], List[float]]:
    """
    Compute labels using v0.0 mode: FULL_SAMPLE.
    
    Percentiles computed vs all weeks in the dataset (exploratory; allows look-ahead).
    
    Args:
        feature_values: List of all feature values in dataset
        bin_ranges: Percentile ranges for each bin
        bin_names: Names for each bin
    
    Returns:
        Tuple of (labels, percentile_ranks)
        - labels: List of labels (one per feature value)
        - percentile_ranks: List of percentile ranks (0-100, one per feature value)
    """
    if len(feature_values) == 0:
        return [], []
    
    # Compute bins from all values
    bins = compute_percentile_bins(feature_values, bin_ranges, bin_names)
    
    # Assign labels and compute percentile ranks
    labels = []
    percentile_ranks = []
    for v in feature_values:
        labels.append(assign_label(v, bins))
        percentile_ranks.append(compute_percentile_rank(v, feature_values))
    
    return labels, percentile_ranks


def compute_labels_v0_1(
    feature_values: List[float],
    window_weeks: int = 52,
    bin_ranges: List[List[int]] = [[0, 20], [20, 40], [40, 60], [60, 80], [80, 100]],
    bin_names: List[str] = ["Red", "Orange", "Yellow", "YellowGreen", "Green"],
) -> Tuple[List[Optional[str]], List[Optional[float]]]:
    """
    Compute labels using v0.1 mode: ROLLING_PAST_52W.
    
    Percentiles computed vs prior 52 valid weekly observations only.
    If <52 prior weeks, label = NA.
    
    Args:
        feature_values: List of feature values (ordered by time)
        window_weeks: Number of prior weeks to use for percentile computation
        bin_ranges: Percentile ranges for each bin
        bin_names: Names for each bin
    
    Returns:
        Tuple of (labels, percentile_ranks)
        - labels: List of labels (one per feature value, None if insufficient history)
        - percentile_ranks: List of percentile ranks (0-100, None if insufficient history)
    """
    if len(feature_values) == 0:
        return [], []
    
    labels = []
    percentile_ranks = []
    
    for i, value in enumerate(feature_values):
        if value is None:
            labels.append(None)
            percentile_ranks.append(None)
            continue
        
        # Get prior window (exclusive of current observation)
        prior_start = max(0, i - window_weeks)
        prior_values = [v for v in feature_values[prior_start:i] if v is not None]
        
        if len(prior_values) < window_weeks:
            # Insufficient history
            labels.append(None)
            percentile_ranks.append(None)
            continue
        
        # Compute bins from prior window
        bins = compute_percentile_bins(prior_values, bin_ranges, bin_names)
        
        # Assign label and compute percentile rank
        label = assign_label(value, bins)
        labels.append(label)
        
        percentile_rank = compute_percentile_rank(value, prior_values)
        percentile_ranks.append(percentile_rank)
    
    return labels, percentile_ranks
