#!/usr/bin/env python3
"""
Enhanced Daily MLB Pipeline with Kelly Criterion Bet Sizing

This script runs the complete daily MLB prediction pipeline and automatically
applies Kelly Criterion bet sizing to generate actionable betting recommendations.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import subprocess

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the complete daily pipeline with Kelly sizing."""
    
    logger.info("🚀 Starting Enhanced Daily MLB Pipeline with Kelly Criterion")
    
    # Configuration
    BANKROLL = float(os.getenv('MLB_BANKROLL', '1000.0'))  # Default $1000 bankroll
    DEFAULT_ODDS = float(os.getenv('MLB_DEFAULT_ODDS', '1.91'))  # -110 American odds
    MAX_BET_FRACTION = float(os.getenv('MLB_MAX_BET_FRACTION', '0.25'))  # Max 25% per bet
    
    today_str = datetime.today().strftime('%Y-%m-%d')
    base_dir = Path(__file__).parent
    
    # Step 1: Run the original daily pipeline
    logger.info("📊 Step 1: Running original prediction pipeline...")
    try:
        result = subprocess.run([
            sys.executable, 
            str(base_dir / 'run_daily_pipeline.py')
        ], capture_output=True, text=True, timeout=300)
        
        if result.returncode != 0:
            logger.error(f"Original pipeline failed: {result.stderr}")
            return 1
        else:
            logger.info("✅ Original pipeline completed successfully")
    
    except subprocess.TimeoutExpired:
        logger.error("❌ Original pipeline timed out (5 minutes)")
        return 1
    except Exception as e:
        logger.error(f"❌ Error running original pipeline: {e}")
        return 1
    
    # Step 2: Find the latest prediction file
    logger.info("🔍 Step 2: Looking for latest prediction file...")
    
    processed_dir = base_dir / 'data' / 'processed'
    prediction_files = list(processed_dir.glob(f'readable_win_predictions_for_{today_str}_using_*.csv'))
    
    if not prediction_files:
        # Try alternative naming pattern
        prediction_files = list(processed_dir.glob(f'readable_win_predictions_for_{today_str}.csv'))
    
    if not prediction_files:
        logger.error(f"❌ No prediction files found for {today_str}")
        logger.info("Available files:")
        for file in processed_dir.glob('readable_win_predictions_*.csv'):
            logger.info(f"  - {file.name}")
        return 1
    
    # Use the most recent file
    latest_prediction = max(prediction_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"📁 Found prediction file: {latest_prediction.name}")
    
    # Step 3: Apply Kelly Criterion sizing
    logger.info("💰 Step 3: Applying Kelly Criterion bet sizing...")
    
    try:
        kelly_result = subprocess.run([
            sys.executable, 
            str(base_dir / 'modeling' / 'predict_with_kelly.py'),
            '--predictions', str(latest_prediction),
            '--bankroll', str(BANKROLL),
            '--odds', str(DEFAULT_ODDS),
            '--max-fraction', str(MAX_BET_FRACTION)
        ], capture_output=True, text=True, timeout=120)
        
        if kelly_result.returncode != 0:
            logger.error(f"Kelly sizing failed: {kelly_result.stderr}")
            return 1
        else:
            logger.info("✅ Kelly sizing completed successfully")
            # Print Kelly output to console
            print("\n" + "="*80)
            print("DAILY MLB BETTING RECOMMENDATIONS")
            print("="*80)
            print(kelly_result.stdout)
    
    except subprocess.TimeoutExpired:
        logger.error("❌ Kelly sizing timed out (2 minutes)")
        return 1
    except Exception as e:
        logger.error(f"❌ Error in Kelly sizing: {e}")
        return 1
    
    # Step 4: Summary
    logger.info("📈 Pipeline Summary:")
    logger.info(f"  - Bankroll: ${BANKROLL:,.2f}")
    logger.info(f"  - Default Odds: {DEFAULT_ODDS} ({-110 if DEFAULT_ODDS == 1.91 else 'custom'})")
    logger.info(f"  - Max Bet Fraction: {MAX_BET_FRACTION:.0%}")
    logger.info(f"  - Date: {today_str}")
    
    logger.info("🎉 Enhanced Daily Pipeline completed successfully!")
    
    return 0


if __name__ == "__main__":
    
    print("""
    ⚾ Enhanced MLB Daily Pipeline with Kelly Criterion ⚾
    
    Environment Variables (optional):
    - MLB_BANKROLL: Available bankroll (default: $1000)
    - MLB_DEFAULT_ODDS: Default decimal odds (default: 1.91 = -110)
    - MLB_MAX_BET_FRACTION: Max fraction per bet (default: 0.25)
    
    Example:
    export MLB_BANKROLL=5000
    export MLB_DEFAULT_ODDS=2.0
    python run_daily_pipeline_with_kelly.py
    """)
    
    sys.exit(main())