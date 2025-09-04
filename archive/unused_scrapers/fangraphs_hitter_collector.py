# fangraphs_hitter_collector.py

import pandas as pd
from pybaseball import batting_stats
from pathlib import Path
import logging
from datetime import datetime
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def collect_fangraphs_hitter_data(season=2025, min_pa=100):
    """
    Collect hitter data from Fangraphs using pybaseball
    
    Metrics collected:
    - OPS (On-base Plus Slugging)
    - wOBA (Weighted On-Base Average)
    - wRC+ (Weighted Runs Created Plus)
    - K% (Strikeout Rate)
    - BB% (Walk Rate)
    - Plus other standard hitting stats
    """
    try:
        logger.info(f"Collecting Fangraphs hitter data for {season} season...")
        logger.info(f"Minimum plate appearances: {min_pa}")
        
        # Get batting stats from Fangraphs
        hitter_data = batting_stats(season, season, qual=min_pa)
        
        if hitter_data is None or hitter_data.empty:
            logger.error("No hitter data returned from Fangraphs")
            return None
            
        logger.info(f"Retrieved data for {len(hitter_data)} hitters")
        logger.info(f"Available columns: {list(hitter_data.columns)}")
        
        # Select the columns we need
        required_columns = ['Name', 'Team', 'G', 'PA', 'AB', 'H', 'HR', 'R', 'RBI', 
                           'SB', 'BB%', 'K%', 'ISO', 'BABIP', 'AVG', 'OBP', 'SLG', 
                           'OPS', 'wOBA', 'wRC+']
        
        # Check which columns are available
        available_columns = [col for col in required_columns if col in hitter_data.columns]
        missing_columns = [col for col in required_columns if col not in hitter_data.columns]
        
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns}")
        
        logger.info(f"Using columns: {available_columns}")
        
        # Filter to available columns
        hitter_data = hitter_data[available_columns].copy()
        
        # Calculate K:BB ratio
        if 'K%' in hitter_data.columns and 'BB%' in hitter_data.columns:
            # Handle division by zero
            hitter_data['K:BB'] = hitter_data['K%'] / hitter_data['BB%'].replace(0, 0.1)
            hitter_data['K:BB'] = hitter_data['K:BB'].round(2)
            logger.info("Calculated K:BB ratio")
        
        # Clean up data
        hitter_data = hitter_data.dropna(subset=['Name', 'Team'])
        hitter_data = hitter_data.sort_values('wRC+', ascending=False)
        
        # Save the data
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = PROCESSED_DIR / f"fangraphs_hitter_data_{date_str}.csv"
        hitter_data.to_csv(output_path, index=False)
        logger.info(f"Saved hitter data to: {output_path}")
        
        # Print sample data
        print(f"\n🏏 FANGRAPHS HITTER DATA COLLECTED")
        print(f"Season: {season} | Min PA: {min_pa} | Total Hitters: {len(hitter_data)}")
        print("\n📊 TOP 10 HITTERS (by wRC+):")
        print("-" * 100)
        
        top_10 = hitter_data.head(10)
        for idx, hitter in top_10.iterrows():
            name = hitter['Name'][:18].ljust(18)
            team = hitter['Team']
            ops = hitter.get('OPS', 'N/A')
            woba = hitter.get('wOBA', 'N/A')
            wrc_plus = hitter.get('wRC+', 'N/A')
            k_rate = hitter.get('K%', 'N/A')
            bb_rate = hitter.get('BB%', 'N/A')
            k_bb = hitter.get('K:BB', 'N/A')
            
            print(f"{name} ({team:3s}) | OPS: {ops:>5} | wOBA: {woba:>5} | wRC+: {wrc_plus:>5} | "
                  f"K%: {k_rate:>5} | BB%: {bb_rate:>5} | K:BB: {k_bb}")
        
        return output_path, hitter_data
        
    except Exception as e:
        logger.error(f"Error collecting hitter data: {e}")
        return None, None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect hitter data from Fangraphs")
    parser.add_argument("--season", help="MLB season year", type=int, default=2025)
    parser.add_argument("--min-pa", help="Minimum plate appearances", type=int, default=100)
    args = parser.parse_args()
    
    output_path, hitter_data = collect_fangraphs_hitter_data(args.season, args.min_pa)
    if output_path:
        print(f"\n💾 Data saved to: {output_path}")
    else:
        print("❌ Data collection failed")