# fangraphs_pitcher_collector.py
# Clean, working solution for getting pitcher data from Fangraphs via pybaseball

from pybaseball import pitching_stats
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import argparse
from unidecode import unidecode

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def get_fangraphs_pitcher_data(season=2024, min_ip=20):
    """
    Get comprehensive pitcher data from Fangraphs using pybaseball
    This is the WORKING solution that uses official Fangraphs data via pybaseball
    """
    try:
        logger.info(f"Fetching Fangraphs pitcher data for {season} season (min {min_ip} IP)...")
        
        # Get pitching stats from Fangraphs via pybaseball
        # This includes all the advanced metrics you want: WHIP, FIP, SIERA, CSW%, xERA, xFIP, etc.
        pitcher_data = pitching_stats(season, season, qual=min_ip)
        
        if pitcher_data is not None and not pitcher_data.empty:
            logger.info(f"Successfully retrieved data for {len(pitcher_data)} pitchers")
            logger.info(f"Available columns: {list(pitcher_data.columns)}")
        else:
            logger.warning("No data returned from Fangraphs")
            
        return pitcher_data
        
    except Exception as e:
        logger.error(f"Error fetching Fangraphs data: {e}")
        logger.info("Consider using --sample flag for testing with sample data")
        return None

def create_sample_fangraphs_data():
    """
    Create sample data with the metrics we need for testing
    """
    logger.info("Creating sample Fangraphs data for testing...")
    
    # Sample data for pitchers from our matchups
    sample_data = {
        'Name': ['Paul Skenes', 'Shane Baz', 'Brady Singer', 'Andre Pallante', 'Joe Ryan',
                'Edward Cabrera', 'Nick Pivetta', 'Jack Flaherty', 'Kevin Gausman', 'Freddy Peralta',
                'Tyler Anderson', 'Jeffrey Springs', 'Clay Holmes', 'Max Fried', 'Richard Fitts'],
        'Team': ['PIT', 'TB', 'KC', 'STL', 'MIN', 'MIA', 'BOS', 'DET', 'TOR', 'MIL', 
                'LAA', 'TB', 'NYY', 'ATL', 'BOS'],
        'IP': [133.0, 98.3, 179.2, 121.0, 135.1, 111.2, 145.0, 106.3, 181.0, 173.2,
              179.1, 123.1, 67.0, 174.1, 89.1],
        'WHIP': [0.947, 1.334, 1.067, 1.355, 1.199, 1.265, 1.103, 1.150, 1.216, 1.063,
                1.538, 1.293, 1.179, 1.164, 1.348],
        'FIP': [2.12, 4.48, 3.71, 4.66, 4.25, 3.97, 4.14, 2.95, 3.81, 3.56,
               4.99, 4.33, 3.06, 3.62, 4.83],
        'SIERA': [2.79, 4.18, 3.98, 4.44, 4.32, 3.85, 4.01, 3.15, 3.89, 3.67,
                 4.85, 4.26, 3.45, 3.71, 4.59],
        'CSW%': [32.1, 28.4, 28.9, 25.1, 29.8, 30.5, 29.1, 31.2, 27.8, 30.4,
                26.3, 28.7, 27.9, 30.1, 28.2],
        'xERA': [2.89, 4.12, 3.85, 4.33, 4.18, 3.76, 3.92, 3.08, 3.71, 3.54,
                4.71, 4.19, 3.38, 3.65, 4.47],
        'xFIP': [2.34, 4.22, 3.68, 4.52, 4.14, 3.89, 4.08, 3.12, 3.75, 3.61,
                4.87, 4.28, 3.18, 3.58, 4.73],
        'xwOBA': [0.256, 0.334, 0.312, 0.351, 0.328, 0.305, 0.318, 0.289, 0.321, 0.298,
                 0.371, 0.336, 0.295, 0.315, 0.349],
        'xBA': [0.201, 0.279, 0.251, 0.288, 0.263, 0.247, 0.259, 0.229, 0.268, 0.235,
               0.301, 0.271, 0.241, 0.254, 0.283],
        'xSLG': [0.312, 0.489, 0.441, 0.521, 0.467, 0.429, 0.453, 0.378, 0.456, 0.401,
                0.558, 0.498, 0.402, 0.438, 0.518],
        'Stuff+': [134, 95, 102, 87, 101, 108, 105, 118, 99, 112,
                  89, 96, 121, 107, 93],
        'ERA': [1.96, 3.92, 3.71, 3.78, 3.60, 3.55, 4.01, 2.95, 3.84, 3.73,
               5.87, 3.67, 3.10, 3.25, 4.14],
        'K%': [33.1, 26.8, 24.2, 21.7, 26.9, 28.4, 25.6, 30.1, 24.1, 27.8,
              19.3, 24.5, 26.2, 25.4, 22.9],
        'BB%': [6.8, 10.2, 7.1, 11.8, 8.9, 9.7, 8.4, 6.9, 7.3, 7.9,
               10.9, 9.1, 7.8, 8.2, 9.6]
    }
    
    df = pd.DataFrame(sample_data)
    return df

def collect_pitcher_data_for_evaluation(matchup_file_path=None, season=2024, min_ip=20, use_sample=False):
    """
    Main function to collect pitcher data for evaluation
    
    Args:
        matchup_file_path: Path to CSV with pitcher matchups (optional)
        season: MLB season year (default: 2024) 
        min_ip: Minimum innings pitched filter (default: 20)
        use_sample: Use sample data instead of live Fangraphs data (for testing)
    
    Returns:
        Path to saved CSV file with pitcher data
    """
    
    if use_sample:
        logger.info("Using sample data for testing...")
        pitcher_data = create_sample_fangraphs_data()
    else:
        logger.info("Fetching live data from Fangraphs...")
        pitcher_data = get_fangraphs_pitcher_data(season=season, min_ip=min_ip)
    
    if pitcher_data is None or pitcher_data.empty:
        logger.error("Failed to get pitcher data")
        return None
    
    # Filter to specific pitchers if we have a matchup file
    if matchup_file_path:
        try:
            matchups = pd.read_csv(matchup_file_path)
            home_pitchers = matchups['home_pitcher'].dropna().tolist()
            away_pitchers = matchups['away_pitcher'].dropna().tolist()
            target_pitchers = list(set(home_pitchers + away_pitchers))
            target_pitchers = [name for name in target_pitchers if name != 'TBD']
            
            logger.info(f"Filtering for {len(target_pitchers)} pitchers from matchups")
            
            # Try to match pitcher names
            target_pitchers_clean = [unidecode(name.strip()) for name in target_pitchers]
            pitcher_data_clean = pitcher_data.copy()
            pitcher_data_clean['Name_clean'] = pitcher_data_clean['Name'].apply(lambda x: unidecode(x.strip()))
            
            filtered_data = pitcher_data_clean[pitcher_data_clean['Name_clean'].isin(target_pitchers_clean)]
            
            if len(filtered_data) > 0:
                logger.info(f"Found {len(filtered_data)} matching pitchers")
                final_data = filtered_data.drop(columns=['Name_clean'])
            else:
                logger.warning("No specific pitchers found, using all data")
                final_data = pitcher_data
                
        except Exception as e:
            logger.error(f"Error filtering pitchers: {e}")
            final_data = pitcher_data
    else:
        final_data = pitcher_data
    
    # Save the data
    date_str = datetime.now().strftime('%Y-%m-%d')
    output_path = PROCESSED_DIR / f"fangraphs_pitcher_data_{date_str}.csv"
    
    final_data.to_csv(output_path, index=False)
    logger.info(f"Saved pitcher data to: {output_path}")
    
    # Print summary
    print("\nFANGRAPHS PITCHER DATA COLLECTED")
    print("=" * 70)
    print(f"Total pitchers: {len(final_data)}")
    print(f"Season: {season}")
    print(f"Minimum IP: {min_ip}")
    
    # Show key metrics for top pitchers by FIP
    if 'FIP' in final_data.columns:
        print(f"\nTOP 10 PITCHERS BY FIP:")
        top_fip = final_data.nsmallest(10, 'FIP')[['Name', 'Team', 'IP', 'FIP', 'WHIP', 'ERA', 'K%']]
        print(top_fip.to_string(index=False))
    
    return output_path

def find_latest_matchup_file():
    """Find the most recent matchup file"""
    files = list(RAW_DIR.glob("mlb_probable_pitchers_*.csv"))
    if not files:
        return None
    return max(files, key=lambda x: x.stat().st_mtime)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect pitcher data from Fangraphs")
    parser.add_argument("--matchups", help="Path to matchups CSV file", type=str)
    parser.add_argument("--season", help="MLB season year", type=int, default=2024)
    parser.add_argument("--min-ip", help="Minimum innings pitched", type=int, default=20)
    parser.add_argument("--sample", help="Use sample data for testing", action="store_true")
    
    args = parser.parse_args()
    
    matchup_file = None
    if args.matchups:
        matchup_file = Path(args.matchups)
        if not matchup_file.exists():
            logger.error(f"Matchup file not found: {matchup_file}")
            exit(1)
    else:
        matchup_file = find_latest_matchup_file()
        if matchup_file:
            logger.info(f"Using latest matchup file: {matchup_file}")
    
    result_path = collect_pitcher_data_for_evaluation(
        matchup_file_path=matchup_file,
        season=args.season,
        min_ip=args.min_ip,
        use_sample=args.sample
    )
    
    if result_path:
        print(f"\n💾 Data saved to: {result_path}")
    else:
        print("❌ Failed to collect pitcher data")