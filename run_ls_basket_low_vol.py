#!/usr/bin/env python
"""Entry point for LS basket low-vol pipeline."""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO))

from scripts.ls_basket_low_vol.run_pipeline import main

if __name__ == "__main__":
    main()
