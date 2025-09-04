# daily_betting_pipeline.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime, timedelta
import argparse
import sys

# Add project root to path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from scraping.daily_games_collector import DailyGamesCollector
from scraping.odds_api_collector import OddsAPICollector
from scraping.game_results_collector import GameResultsCollector
from features.enhanced_pitcher_collector import collect_enhanced_pitcher_data
from features.enhanced_hitter_collector import collect_enhanced_hitter_data

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

class DailyBettingPipeline:
    def __init__(self):
        self.games_collector = DailyGamesCollector()
        self.odds_collector = OddsAPICollector()
        self.results_collector = GameResultsCollector()
        
        # Name matching for pitcher integration
        self.name_variants = {
            'Jr.': '', 'Sr.': '', 'III': '', 'II': '',
            'José': 'Jose', 'Jesús': 'Jesus', 'Andrés': 'Andres',
            'Martín': 'Martin', 'Alejandro': 'Alex'
        }
    
    def normalize_pitcher_name(self, name):
        """Normalize pitcher name for matching"""
        if not name or pd.isna(name):
            return ''
        
        # Remove suffixes and normalize accents
        normalized = str(name).strip()
        for variant, replacement in self.name_variants.items():
            normalized = normalized.replace(variant, replacement)
        
        return normalized.strip()
    
    def collect_daily_data(self, date_str=None):
        """Collect all daily betting data"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"🚀 Starting daily betting pipeline for {date_str}")
        
        results = {
            'date': date_str,
            'games_data': None,
            'odds_data': None,
            'pitcher_data': None,
            'hitter_data': None,
            'integrated_data': None,
            'files_created': []
        }
        
        # 1. Collect today's games
        logger.info("📅 Collecting daily games...")
        try:
            if date_str == datetime.now().strftime('%Y-%m-%d'):
                games_file, games_data = self.games_collector.get_current_games()
            else:
                games_data = self.games_collector.get_games_for_date(date_str)
                games_file = self.games_collector.save_games_data(games_data, date_str)
            
            if games_data:
                results['games_data'] = games_data
                results['files_created'].append(games_file)
                logger.info(f"✅ Collected {len(games_data)} games")
            else:
                logger.warning("❌ No games data collected")
        
        except Exception as e:
            logger.error(f"❌ Error collecting games: {e}")
        
        # 2. Collect odds data
        logger.info("💰 Collecting odds data...")
        try:
            odds_data = self.odds_collector.get_current_odds()
            if odds_data:
                odds_file = self.odds_collector.save_odds_data(odds_data)
                results['odds_data'] = odds_data
                results['files_created'].append(odds_file)
                logger.info(f"✅ Collected odds for {len(odds_data)} games")
            else:
                logger.warning("❌ No odds data collected")
        
        except Exception as e:
            logger.error(f"❌ Error collecting odds: {e}")
        
        # 3. Collect pitcher data (current season)
        logger.info("⚾ Collecting pitcher data...")
        try:
            pitcher_file, pitcher_data = collect_enhanced_pitcher_data(season=2025, min_ip=50)
            if pitcher_data is not None:
                results['pitcher_data'] = pitcher_data
                results['files_created'].append(pitcher_file)
                logger.info(f"✅ Collected data for {len(pitcher_data)} pitchers")
            else:
                logger.warning("❌ No pitcher data collected")
        
        except Exception as e:
            logger.error(f"❌ Error collecting pitcher data: {e}")
        
        # 4. Collect hitter data (current season)
        logger.info("🏏 Collecting hitter data...")
        try:
            hitter_file, hitter_data = collect_enhanced_hitter_data(season=2025, min_pa=200)
            if hitter_data is not None:
                results['hitter_data'] = hitter_data
                results['files_created'].append(hitter_file)
                logger.info(f"✅ Collected data for {len(hitter_data)} hitters")
            else:
                logger.warning("❌ No hitter data collected")
        
        except Exception as e:
            logger.error(f"❌ Error collecting hitter data: {e}")
        
        # 5. Integrate all data
        logger.info("🔗 Integrating all data sources...")
        try:
            integrated_data = self.integrate_all_data(results)
            if integrated_data is not None:
                integrated_file = self.save_integrated_data(integrated_data, date_str)
                results['integrated_data'] = integrated_data
                results['files_created'].append(integrated_file)
                logger.info(f"✅ Created integrated dataset with {len(integrated_data)} games")
            else:
                logger.warning("❌ No integrated data created")
        
        except Exception as e:
            logger.error(f"❌ Error integrating data: {e}")
        
        return results
    
    def integrate_all_data(self, results):
        """Integrate games, odds, pitcher, and hitter data"""
        games_data = results.get('games_data')
        odds_data = results.get('odds_data')
        pitcher_data = results.get('pitcher_data')
        hitter_data = results.get('hitter_data')
        
        if not games_data:
            logger.error("No games data to integrate")
            return None
        
        # Start with games data
        integrated_df = pd.DataFrame(games_data)
        
        # Add odds data
        if odds_data:
            odds_df = pd.DataFrame(odds_data)
            
            # Match games by team names
            merged_odds = []
            for _, game in integrated_df.iterrows():
                game_odds = None
                
                # Find matching odds by team abbreviations
                for _, odds_game in odds_df.iterrows():
                    if (game['away_team'] == odds_game['away_team'] and 
                        game['home_team'] == odds_game['home_team']):
                        game_odds = odds_game
                        break
                
                if game_odds is not None:
                    # Add key odds columns
                    for col in ['fd_home_moneyline', 'fd_away_moneyline', 'fd_total_points', 
                               'fd_over_odds', 'fd_under_odds', 'fd_home_spread', 'fd_away_spread']:
                        integrated_df.loc[_, col] = game_odds.get(col)
                else:
                    # Fill with NaN if no odds found
                    for col in ['fd_home_moneyline', 'fd_away_moneyline', 'fd_total_points', 
                               'fd_over_odds', 'fd_under_odds', 'fd_home_spread', 'fd_away_spread']:
                        integrated_df.loc[_, col] = None
        
        # Add pitcher data
        if pitcher_data is not None:
            pitcher_df = pitcher_data.copy()
            
            # Add pitcher stats for away and home pitchers
            for _, game in integrated_df.iterrows():
                away_pitcher = self.normalize_pitcher_name(game.get('away_pitcher', ''))
                home_pitcher = self.normalize_pitcher_name(game.get('home_pitcher', ''))
                
                # Find away pitcher stats
                if away_pitcher:
                    pitcher_match = pitcher_df[
                        pitcher_df['Name'].str.contains(away_pitcher.split()[-1], case=False, na=False)
                    ]
                    if not pitcher_match.empty:
                        pitcher_stats = pitcher_match.iloc[0]
                        integrated_df.loc[_, 'away_pitcher_score'] = pitcher_stats.get('composite_score')
                        integrated_df.loc[_, 'away_pitcher_grade'] = pitcher_stats.get('grade')
                        integrated_df.loc[_, 'away_pitcher_era'] = pitcher_stats.get('era')
                        integrated_df.loc[_, 'away_pitcher_whip'] = pitcher_stats.get('whip')
                        integrated_df.loc[_, 'away_pitcher_fip'] = pitcher_stats.get('fip')
                
                # Find home pitcher stats
                if home_pitcher:
                    pitcher_match = pitcher_df[
                        pitcher_df['Name'].str.contains(home_pitcher.split()[-1], case=False, na=False)
                    ]
                    if not pitcher_match.empty:
                        pitcher_stats = pitcher_match.iloc[0]
                        integrated_df.loc[_, 'home_pitcher_score'] = pitcher_stats.get('composite_score')
                        integrated_df.loc[_, 'home_pitcher_grade'] = pitcher_stats.get('grade')
                        integrated_df.loc[_, 'home_pitcher_era'] = pitcher_stats.get('era')
                        integrated_df.loc[_, 'home_pitcher_whip'] = pitcher_stats.get('whip')
                        integrated_df.loc[_, 'home_pitcher_fip'] = pitcher_stats.get('fip')
        
        # Add team hitter averages (if available)
        if hitter_data is not None:
            team_hitting_stats = self.calculate_team_hitting_averages(hitter_data)
            
            for _, game in integrated_df.iterrows():
                away_team = game['away_team']
                home_team = game['home_team']
                
                # Away team hitting stats
                if away_team in team_hitting_stats:
                    stats = team_hitting_stats[away_team]
                    integrated_df.loc[_, 'away_team_avg_wrc_plus'] = stats.get('avg_wrc_plus')
                    integrated_df.loc[_, 'away_team_avg_ops'] = stats.get('avg_ops')
                    integrated_df.loc[_, 'away_team_hitters_count'] = stats.get('hitters_count')
                
                # Home team hitting stats
                if home_team in team_hitting_stats:
                    stats = team_hitting_stats[home_team]
                    integrated_df.loc[_, 'home_team_avg_wrc_plus'] = stats.get('avg_wrc_plus')
                    integrated_df.loc[_, 'home_team_avg_ops'] = stats.get('avg_ops')
                    integrated_df.loc[_, 'home_team_hitters_count'] = stats.get('hitters_count')
        
        return integrated_df
    
    def calculate_team_hitting_averages(self, hitter_data):
        """Calculate team-level hitting averages"""
        team_stats = {}
        
        for team in hitter_data['Team'].unique():
            team_hitters = hitter_data[hitter_data['Team'] == team]
            
            team_stats[team] = {
                'avg_wrc_plus': team_hitters['wRC+'].mean(),
                'avg_ops': team_hitters['OPS'].mean(),
                'hitters_count': len(team_hitters)
            }
        
        return team_stats
    
    def save_integrated_data(self, integrated_data, date_str):
        """Save integrated data to CSV"""
        filename = f"integrated_betting_data_{date_str}.csv"
        filepath = PROCESSED_DIR / filename
        
        integrated_data.to_csv(filepath, index=False)
        logger.info(f"Saved integrated data to {filepath}")
        
        return filepath
    
    def collect_historical_results(self, start_date, end_date):
        """Collect historical game results for model training"""
        logger.info(f"📊 Collecting historical results from {start_date} to {end_date}")
        
        results = self.results_collector.get_date_range_results(start_date, end_date)
        
        if results:
            filepath = self.results_collector.save_results_data(results)
            logger.info(f"✅ Collected {len(results)} historical games")
            return filepath, results
        
        return None, []
    
    def print_pipeline_summary(self, results):
        """Print summary of pipeline execution"""
        date_str = results['date']
        print(f"\n🚀 DAILY BETTING PIPELINE SUMMARY - {date_str}")
        print("="*80)
        
        # Data collection status
        games_count = len(results['games_data']) if results['games_data'] else 0
        odds_count = len(results['odds_data']) if results['odds_data'] else 0
        pitcher_count = len(results['pitcher_data']) if results['pitcher_data'] is not None else 0
        hitter_count = len(results['hitter_data']) if results['hitter_data'] is not None else 0
        integrated_count = len(results['integrated_data']) if results['integrated_data'] is not None else 0
        
        print(f"📅 Games collected: {games_count}")
        print(f"💰 Games with odds: {odds_count}")
        print(f"⚾ Pitchers analyzed: {pitcher_count}")
        print(f"🏏 Hitters analyzed: {hitter_count}")
        print(f"🔗 Integrated games: {integrated_count}")
        
        print(f"\n💾 Files created: {len(results['files_created'])}")
        for file_path in results['files_created']:
            if file_path:
                print(f"   - {file_path.name}")
        
        # Show sample integrated data
        if results['integrated_data'] is not None and not results['integrated_data'].empty:
            print(f"\n📊 SAMPLE INTEGRATED DATA:")
            print("-"*80)
            sample_df = results['integrated_data'].head(3)
            
            for _, game in sample_df.iterrows():
                away_ml = game.get('fd_away_moneyline', 'N/A')
                home_ml = game.get('fd_home_moneyline', 'N/A')
                away_score = game.get('away_pitcher_score', 'N/A')
                home_score = game.get('home_pitcher_score', 'N/A')
                
                print(f"{game['away_team']:3} @ {game['home_team']:3} | "
                      f"ML: {away_ml:>4}/{home_ml:>4} | "
                      f"Pitcher Scores: {away_score}/{home_score}")

def main():
    parser = argparse.ArgumentParser(description="Run daily betting data pipeline")
    parser.add_argument("--date", help="Date in YYYY-MM-DD format (default: today)", type=str)
    parser.add_argument("--historical", help="Collect historical results", action="store_true")
    parser.add_argument("--start-date", help="Start date for historical collection", type=str)
    parser.add_argument("--end-date", help="End date for historical collection", type=str)
    args = parser.parse_args()
    
    pipeline = DailyBettingPipeline()
    
    if args.historical and args.start_date and args.end_date:
        # Historical results collection
        filepath, results = pipeline.collect_historical_results(args.start_date, args.end_date)
        if filepath:
            print(f"💾 Historical results saved to: {filepath}")
    
    else:
        # Daily pipeline
        date_str = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
        results = pipeline.collect_daily_data(date_str)
        pipeline.print_pipeline_summary(results)

if __name__ == "__main__":
    main()