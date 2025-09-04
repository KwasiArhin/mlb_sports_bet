# run_daily_pipeline.py

import os
import logging
from datetime import datetime
from pathlib import Path

# === Component imports ===
from scraping.daily_betting_pipeline import DailyBettingPipeline


# === Logging setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Directory setup ===
BASE_DIR = Path(__file__).resolve().parent

def run_pipeline():
    logger.info("Starting daily MLB prediction pipeline...")
    
    pipeline = DailyBettingPipeline()
    
    # Use the working pipeline to collect and integrate data
    try:
        results = pipeline.collect_daily_data()
        pipeline.print_pipeline_summary(results)
        
        if results['integrated_data'] is not None:
            logger.info("✅ Pipeline completed successfully!")
            return results['files_created'][-1]  # Return the integrated data file path
        else:
            logger.error("❌ Pipeline failed to create integrated data")
            return None
            
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        return None

if __name__ == "__main__":
    run_pipeline()

# cd C:\Users\roman\baseball_forecast_project
# python run_daily_pipeline.py
