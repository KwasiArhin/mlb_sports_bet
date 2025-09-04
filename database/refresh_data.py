#!/usr/bin/env python3
"""
Incremental Data Refresh Script
Only fetches and updates data that's newer than last refresh
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta
import logging
import pandas as pd

# Add paths
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR / "scraping"))
sys.path.append(str(BASE_DIR / "database"))

from mlb_database import MLBDatabase
from migrate_data import DataMigrator

# For fresh data fetching
from odds_api_collector import OddsAPICollector
import requests
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IncrementalDataRefresher:
    def __init__(self):
        self.db = MLBDatabase()
        self.migrator = DataMigrator()
        self.base_dir = BASE_DIR
        self.processed_dir = self.base_dir / "data" / "processed"
        self.odds_collector = OddsAPICollector()
    
    def needs_refresh(self, data_type: str, target_date: date = None) -> bool:
        """Check if data type needs refresh for target date"""
        if target_date is None:
            target_date = date.today()
        
        last_refresh = self.db.get_last_refresh_date(data_type)
        
        if last_refresh is None:
            logger.info(f"No previous refresh found for {data_type}")
            return True
        
        if last_refresh < target_date:
            logger.info(f"Data type {data_type} last refreshed on {last_refresh}, need refresh for {target_date}")
            return True
        
        logger.info(f"Data type {data_type} is up to date (last refresh: {last_refresh})")
        return False
    
    def refresh_betting_odds(self, target_date: date = None) -> bool:
        """Refresh betting odds data"""
        if target_date is None:
            target_date = date.today()
        
        if not self.needs_refresh('betting_odds', target_date):
            return True
        
        try:
            logger.info("Fetching fresh betting odds...")
            
            # Get odds from API
            odds_data = self.odds_collector.get_current_odds()
            
            if not odds_data:
                logger.error("No odds data retrieved")
                return False
            
            # Process each game
            games_processed = 0
            for game_odds in odds_data:
                try:
                    # Create game record
                    game_data = {
                        'game_id': game_odds.get('game_id'),
                        'game_date': target_date,
                        'home_team': game_odds.get('home_team'),
                        'away_team': game_odds.get('away_team'),
                        'venue': 'TBD',  # Odds API doesn't provide venue
                        'commence_time': game_odds.get('commence_time'),
                        'game_status': 'scheduled'
                    }
                    
                    game_db_id = self.db.upsert_game(game_data)
                    
                    if game_db_id:
                        # Add FanDuel odds
                        fanduel_odds = {
                            'home_moneyline': game_odds.get('fd_home_moneyline'),
                            'away_moneyline': game_odds.get('fd_away_moneyline'),
                            'home_spread': game_odds.get('fd_home_spread'),
                            'away_spread': game_odds.get('fd_away_spread'),
                            'total_points': game_odds.get('fd_total_points'),
                            'over_odds': game_odds.get('fd_over_odds'),
                            'under_odds': game_odds.get('fd_under_odds'),
                        }
                        
                        if self.db.upsert_betting_odds(game_db_id, 'fanduel', fanduel_odds):
                            games_processed += 1
                
                except Exception as e:
                    logger.error(f"Error processing game odds: {e}")
                    continue
            
            # Log the refresh
            self.db.log_data_refresh('betting_odds', target_date, games_processed)
            logger.info(f"Refreshed betting odds for {games_processed} games")
            
            return games_processed > 0
            
        except Exception as e:
            logger.error(f"Error refreshing betting odds: {e}")
            self.db.log_data_refresh('betting_odds', target_date, 0, 'error', str(e))
            return False
    
    def refresh_player_stats(self, target_date: date = None) -> bool:
        """Refresh player stats if newer data is available"""
        if target_date is None:
            target_date = date.today()
        
        # Check if we need to refresh pitcher stats
        pitcher_refresh_needed = self.needs_refresh('pitcher_stats', target_date)
        hitter_refresh_needed = self.needs_refresh('hitter_stats', target_date)
        
        if not pitcher_refresh_needed and not hitter_refresh_needed:
            logger.info("Player stats are up to date")
            return True
        
        # Look for newer CSV files that might have been generated
        success = True
        
        if pitcher_refresh_needed:
            pitcher_files = list(self.processed_dir.glob(f"fangraphs_pitcher_data_{target_date}.csv"))
            if pitcher_files:
                logger.info(f"Found new pitcher data file for {target_date}")
                # Re-run migration for new data
                count = self.migrator.migrate_pitcher_data()
                logger.info(f"Refreshed {count} pitcher records")
            else:
                logger.warning(f"No new pitcher data found for {target_date}")
                success = False
        
        if hitter_refresh_needed:
            hitter_files = list(self.processed_dir.glob(f"fangraphs_hitter_data_{target_date}.csv"))
            if hitter_files:
                logger.info(f"Found new hitter data file for {target_date}")
                count = self.migrator.migrate_hitter_data()
                logger.info(f"Refreshed {count} hitter records")
            else:
                logger.warning(f"No new hitter data found for {target_date}")
                success = False
        
        return success
    
    def refresh_all_data(self, target_date: date = None) -> dict:
        """Refresh all data types"""
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"Starting incremental data refresh for {target_date}")
        
        results = {
            'target_date': str(target_date),
            'betting_odds': False,
            'player_stats': False,
            'timestamp': datetime.now().isoformat()
        }
        
        # Refresh betting odds (most important for daily operation)
        try:
            results['betting_odds'] = self.refresh_betting_odds(target_date)
        except Exception as e:
            logger.error(f"Failed to refresh betting odds: {e}")
        
        # Refresh player stats if available
        try:
            results['player_stats'] = self.refresh_player_stats(target_date)
        except Exception as e:
            logger.error(f"Failed to refresh player stats: {e}")
        
        # Get final database stats
        db_stats = self.db.get_database_stats()
        results['database_stats'] = db_stats
        
        logger.info("Incremental refresh completed")
        logger.info(f"Results: {results}")
        
        return results
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old data to keep database size manageable"""
        cutoff_date = date.today() - timedelta(days=days_to_keep)
        
        logger.info(f"Cleaning up data older than {cutoff_date}")
        
        try:
            conn = self.db.get_connection()
            
            # Clean up old betting odds
            result = conn.execute("""
                DELETE FROM betting_odds 
                WHERE game_id IN (
                    SELECT id FROM games WHERE game_date < ?
                )
            """, (cutoff_date,))
            
            odds_deleted = result.rowcount
            
            # Clean up old games
            result = conn.execute("""
                DELETE FROM games WHERE game_date < ?
            """, (cutoff_date,))
            
            games_deleted = result.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Cleaned up {games_deleted} old games and {odds_deleted} old odds records")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def get_refresh_status(self) -> dict:
        """Get status of all data types"""
        data_types = ['betting_odds', 'pitcher_stats', 'hitter_stats', 'statcast_stats']
        
        status = {}
        for data_type in data_types:
            last_refresh = self.db.get_last_refresh_date(data_type)
            needs_refresh = self.needs_refresh(data_type)
            
            status[data_type] = {
                'last_refresh': str(last_refresh) if last_refresh else 'Never',
                'needs_refresh': needs_refresh,
                'days_since_refresh': (date.today() - last_refresh).days if last_refresh else None
            }
        
        # Add database stats
        status['database_stats'] = self.db.get_database_stats()
        
        return status

def main():
    """Main refresh function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Refresh MLB data incrementally")
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD)', default=None)
    parser.add_argument('--odds-only', action='store_true', help='Refresh only betting odds')
    parser.add_argument('--status', action='store_true', help='Show refresh status')
    parser.add_argument('--cleanup', type=int, help='Clean up data older than N days', default=None)
    
    args = parser.parse_args()
    
    refresher = IncrementalDataRefresher()
    
    # Parse target date
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD")
            return
    
    # Show status
    if args.status:
        status = refresher.get_refresh_status()
        print("\n" + "=" * 50)
        print("MLB DATA REFRESH STATUS")
        print("=" * 50)
        
        for data_type, info in status.items():
            if data_type != 'database_stats':
                print(f"\n{data_type.upper()}:")
                print(f"  Last refresh: {info['last_refresh']}")
                print(f"  Needs refresh: {info['needs_refresh']}")
                if info['days_since_refresh'] is not None:
                    print(f"  Days since refresh: {info['days_since_refresh']}")
        
        print(f"\nDATABASE STATISTICS:")
        for table, count in status['database_stats'].items():
            print(f"  {table}: {count} records")
        
        print("=" * 50)
        return
    
    # Clean up old data
    if args.cleanup:
        refresher.cleanup_old_data(args.cleanup)
        return
    
    # Run refresh
    if args.odds_only:
        success = refresher.refresh_betting_odds(target_date)
        print(f"Betting odds refresh: {'Success' if success else 'Failed'}")
    else:
        results = refresher.refresh_all_data(target_date)
        
        print("\n" + "=" * 50)
        print("INCREMENTAL REFRESH RESULTS")
        print("=" * 50)
        print(f"Target date: {results['target_date']}")
        print(f"Betting odds: {'✅ Success' if results['betting_odds'] else '❌ Failed'}")
        print(f"Player stats: {'✅ Success' if results['player_stats'] else '❌ Failed'}")
        
        print(f"\nDatabase Statistics:")
        for table, count in results['database_stats'].items():
            print(f"  {table}: {count} records")
        print("=" * 50)

if __name__ == "__main__":
    main()