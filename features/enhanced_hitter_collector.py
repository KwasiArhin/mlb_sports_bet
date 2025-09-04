# enhanced_hitter_collector.py

import pandas as pd
from pybaseball import batting_stats
from pathlib import Path
import logging
from datetime import datetime
import argparse
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def collect_enhanced_hitter_data(season=2025, min_pa=300):
    """
    Collect comprehensive hitter data from Fangraphs
    
    Target metrics:
    - OPS (On-base Plus Slugging)
    - wOBA (Weighted On-Base Average)  
    - wRC+ (Weighted Runs Created Plus)
    - xwOBA (Expected Weighted On-Base Average)
    - xBA (Expected Batting Average)
    - xSLG (Expected Slugging)
    - Hard-Hit % (Hard Hit Percentage)
    - Barrel % (Barrel Percentage)
    - K:BB ratio (Strikeout to Walk Ratio)
    """
    try:
        logger.info(f"Collecting enhanced hitter data for {season} season...")
        logger.info(f"Minimum plate appearances: {min_pa}")
        
        # Get comprehensive batting stats from Fangraphs
        hitter_data = batting_stats(season, season, qual=min_pa)
        
        if hitter_data is None or hitter_data.empty:
            logger.error("No hitter data returned from Fangraphs")
            return None, None
            
        logger.info(f"Retrieved data for {len(hitter_data)} hitters")
        logger.info(f"Total columns available: {len(hitter_data.columns)}")
        
        # Target columns for our analysis
        target_columns = [
            # Basic info
            'Name', 'Team', 'G', 'PA', 'AB', 'H', 'HR', 'R', 'RBI', 'SB',
            # Traditional stats
            'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
            # Advanced stats
            'wOBA', 'wRC+', 'BB%', 'K%',
            # Expected stats (from Statcast)
            'xBA', 'xSLG', 'xwOBA',
            # Batted ball metrics
            'HardHit%', 'Barrel%', 'EV', 'LA', 'maxEV',
            # Additional useful metrics
            'Pull%', 'Cent%', 'Oppo%', 'Soft%', 'Med%', 'Hard%'
        ]
        
        # Check which columns are available
        available_columns = []
        missing_columns = []
        
        for col in target_columns:
            if col in hitter_data.columns:
                available_columns.append(col)
            else:
                missing_columns.append(col)
        
        if missing_columns:
            logger.warning(f"Missing columns: {missing_columns}")
        
        logger.info(f"Available target columns ({len(available_columns)}): {available_columns}")
        
        # Select available columns
        hitter_data = hitter_data[available_columns].copy()
        
        # Calculate K:BB ratio
        if 'K%' in hitter_data.columns and 'BB%' in hitter_data.columns:
            # Handle division by zero by replacing 0 BB% with very small number
            hitter_data['K:BB'] = hitter_data['K%'] / hitter_data['BB%'].replace(0, 0.001)
            hitter_data['K:BB'] = hitter_data['K:BB'].round(2)
            logger.info("Calculated K:BB ratio")
        
        # Convert percentage columns to proper format
        percentage_columns = ['BB%', 'K%', 'HardHit%', 'Barrel%', 'Pull%', 'Cent%', 'Oppo%', 'Soft%', 'Med%', 'Hard%']
        for col in percentage_columns:
            if col in hitter_data.columns:
                # Check if the data is already in percentage format (0-100) or decimal format (0-1)
                max_val = hitter_data[col].max()
                if max_val <= 1.0:  # Data is in decimal format, convert to percentage
                    hitter_data[col] = hitter_data[col] * 100
                    logger.info(f"Converted {col} from decimal to percentage format")
        
        # Clean up data
        hitter_data = hitter_data.dropna(subset=['Name', 'Team'])
        
        # Sort by wRC+ (best overall hitter metric)
        if 'wRC+' in hitter_data.columns:
            hitter_data = hitter_data.sort_values('wRC+', ascending=False)
        
        # Add rank column
        hitter_data['Rank'] = range(1, len(hitter_data) + 1)
        
        # Save the data
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = PROCESSED_DIR / f"enhanced_hitter_data_{date_str}.csv"
        hitter_data.to_csv(output_path, index=False)
        logger.info(f"Saved enhanced hitter data to: {output_path}")
        
        # Print comprehensive report
        print(f"\n🏏 ENHANCED HITTER DATA COLLECTED")
        print(f"Season: {season} | Min PA: {min_pa} | Total Hitters: {len(hitter_data)}")
        print(f"Metrics included: {len(available_columns)} of {len(target_columns)} target metrics")
        
        # Display sample of key metrics
        print("\n📊 TOP 10 HITTERS (by wRC+):")
        print("-" * 140)
        
        top_10 = hitter_data.head(10)
        for idx, hitter in top_10.iterrows():
            name = str(hitter['Name'])[:18].ljust(18)
            team = hitter['Team']
            
            # Core metrics
            ops = hitter.get('OPS', 'N/A')
            woba = hitter.get('wOBA', 'N/A') 
            wrc_plus = hitter.get('wRC+', 'N/A')
            
            # Expected stats
            xwoba = hitter.get('xwOBA', 'N/A')
            xba = hitter.get('xBA', 'N/A') 
            xslg = hitter.get('xSLG', 'N/A')
            
            # Batted ball
            hard_hit = hitter.get('HardHit%', 'N/A')
            barrel = hitter.get('Barrel%', 'N/A')
            
            # Discipline
            k_bb = hitter.get('K:BB', 'N/A')
            
            print(f"{name} ({team:3s}) | OPS: {ops:>5} | wOBA: {woba:>5} | wRC+: {wrc_plus:>3} | "
                  f"xwOBA: {xwoba:>5} | xBA: {xba:>5} | xSLG: {xslg:>5}")
            print(f"{' ':23} HardHit%: {hard_hit:>5} | Barrel%: {barrel:>5} | K:BB: {k_bb:>5}")
            print("-" * 140)
        
        # Summary statistics
        print(f"\n📈 SUMMARY STATISTICS:")
        print(f"Average wRC+: {hitter_data['wRC+'].mean():.1f}")
        print(f"Average OPS: {hitter_data['OPS'].mean():.3f}")
        if 'xwOBA' in hitter_data.columns:
            print(f"Average xwOBA: {hitter_data['xwOBA'].mean():.3f}")
        if 'HardHit%' in hitter_data.columns:
            print(f"Average Hard-Hit%: {hitter_data['HardHit%'].mean():.1f}%")
        if 'Barrel%' in hitter_data.columns:
            print(f"Average Barrel%: {hitter_data['Barrel%'].mean():.1f}%")
        
        return output_path, hitter_data
        
    except Exception as e:
        logger.error(f"Error collecting enhanced hitter data: {e}")
        return None, None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect enhanced hitter data from Fangraphs")
    parser.add_argument("--season", help="MLB season year", type=int, default=2025)
    parser.add_argument("--min-pa", help="Minimum plate appearances", type=int, default=300)
    args = parser.parse_args()
    
    output_path, hitter_data = collect_enhanced_hitter_data(args.season, args.min_pa)
    if output_path:
        print(f"\n💾 Enhanced hitter data saved to: {output_path}")
    else:
        print("❌ Enhanced data collection failed")