"""
Main runner script for CHZ World Cup event study analysis.
Runs all analysis steps in sequence.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def main():
    """Run complete analysis pipeline."""
    print("=" * 80)
    print("CHZ WORLD CUP EVENT STUDY - COMPLETE ANALYSIS PIPELINE")
    print("=" * 80)
    
    # Step 1: Data fetching and event study
    print("\n" + "=" * 80)
    print("STEP 1: Running Event Study Analysis")
    print("=" * 80)
    try:
        from chz_worldcup_analysis.chz_event_study import main as run_event_study
        run_event_study()
    except Exception as e:
        print(f"\n[ERROR] Event study failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 2: Visualization
    print("\n" + "=" * 80)
    print("STEP 2: Generating Visualizations")
    print("=" * 80)
    try:
        from chz_worldcup_analysis.visualize_results import main as run_visualizations
        run_visualizations()
    except Exception as e:
        print(f"\n[ERROR] Visualization failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: Generate memo and playbook
    print("\n" + "=" * 80)
    print("STEP 3: Generating Research Memo and Playbook")
    print("=" * 80)
    try:
        from chz_worldcup_analysis.generate_memo import main as generate_docs
        generate_docs()
    except Exception as e:
        print(f"\n[ERROR] Memo generation failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n" + "=" * 80)
    print("ANALYSIS PIPELINE COMPLETE")
    print("=" * 80)
    print("\nAll outputs saved to: chz_worldcup_analysis/outputs/")
    print("\nGenerated files:")
    print("  - research_memo.md")
    print("  - tradeable_playbook.md")
    print("  - window_metrics.csv")
    print("  - abnormal_returns.csv")
    print("  - statistical_tests.csv")
    print("  - summary_table.csv")
    print("  - price_chart_with_events.png")
    print("  - window_returns_barchart.png")
    print("  - car_by_event.png")
    print("  - rolling_beta.png")
    print("  - drawdown_curves.png")


if __name__ == "__main__":
    main()
