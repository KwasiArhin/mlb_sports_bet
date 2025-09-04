# odds_api_collector.py

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
import argparse
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

class OddsAPICollector:
    def __init__(self):
        self.api_key = "a74383d66d314cc2fc96f1e54931d6a4"
        self.base_url = "https://api.the-odds-api.com/v4"
        self.sport = "baseball_mlb"
        
        # Bookmakers to prioritize (FanDuel first)
        self.preferred_bookmakers = ['fanduel', 'draftkings', 'betmgm', 'caesars', 'unibet']
        
        # Team name mapping
        self.team_mapping = {
            'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL',
            'Boston Red Sox': 'BOS', 'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS',
            'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE', 'Colorado Rockies': 'COL',
            'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
            'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA',
            'Milwaukee Brewers': 'MIL', 'Minnesota Twins': 'MIN', 'New York Mets': 'NYM',
            'New York Yankees': 'NYY', 'Oakland Athletics': 'OAK', 'Philadelphia Phillies': 'PHI',
            'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD', 'San Francisco Giants': 'SF',
            'Seattle Mariners': 'SEA', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB',
            'Texas Rangers': 'TEX', 'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WAS'
        }
    
    def get_team_abbreviation(self, team_name):
        """Convert full team name to abbreviation"""
        return self.team_mapping.get(team_name, team_name)
    
    def get_current_odds(self):
        """Get current MLB odds from the Odds API"""
        url = f"{self.base_url}/sports/{self.sport}/odds"
        
        params = {
            'api_key': self.api_key,
            'regions': 'us',
            'markets': 'h2h,spreads,totals',  # moneyline, spreads, totals
            'oddsFormat': 'american',
            'dateFormat': 'iso'
        }
        
        try:
            logger.info("Fetching odds from The Odds API...")
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Retrieved odds for {len(data)} games")
            
            # Check API usage
            remaining_requests = response.headers.get('x-requests-remaining')
            used_requests = response.headers.get('x-requests-used')
            logger.info(f"API Usage - Used: {used_requests}, Remaining: {remaining_requests}")
            
            odds_data = []
            for game in data:
                game_odds = self.process_game_odds(game)
                if game_odds:
                    odds_data.append(game_odds)
            
            return odds_data
            
        except requests.RequestException as e:
            logger.error(f"Error fetching odds: {e}")
            return []
    
    def process_game_odds(self, game):
        """Process individual game odds data"""
        try:
            # Basic game info
            game_id = game.get('id', '')
            commence_time = game.get('commence_time', '')
            home_team_full = game.get('home_team', '')
            away_team_full = game.get('away_team', '')
            
            # Convert to abbreviations
            home_team = self.get_team_abbreviation(home_team_full)
            away_team = self.get_team_abbreviation(away_team_full)
            
            game_data = {
                'game_id': game_id,
                'commence_time': commence_time,
                'home_team': home_team,
                'away_team': away_team,
                'home_team_full': home_team_full,
                'away_team_full': away_team_full,
                
                # FanDuel odds (prioritized)
                'fd_home_moneyline': None,
                'fd_away_moneyline': None,
                'fd_home_spread': None,
                'fd_away_spread': None,
                'fd_home_spread_odds': None,
                'fd_away_spread_odds': None,
                'fd_total_points': None,
                'fd_over_odds': None,
                'fd_under_odds': None,
                
                # Best available odds (from any book)
                'best_home_moneyline': None,
                'best_away_moneyline': None,
                'best_home_spread': None,
                'best_away_spread': None,
                'best_home_spread_odds': None,
                'best_away_spread_odds': None,
                'best_total_points': None,
                'best_over_odds': None,
                'best_under_odds': None,
                
                'bookmakers_count': len(game.get('bookmakers', [])),
                'scraped_at': datetime.now().isoformat()
            }
            
            # Process bookmaker odds
            bookmakers = game.get('bookmakers', [])
            
            # Find FanDuel odds first
            fanduel_data = None
            for bookmaker in bookmakers:
                if bookmaker.get('key') == 'fanduel':
                    fanduel_data = bookmaker
                    break
            
            if fanduel_data:
                self.extract_bookmaker_odds(fanduel_data, game_data, 'fd_')
            
            # Find best odds across all bookmakers
            best_odds = self.find_best_odds(bookmakers)
            for key, value in best_odds.items():
                game_data[f'best_{key}'] = value
            
            return game_data
            
        except Exception as e:
            logger.error(f"Error processing game odds: {e}")
            return None
    
    def extract_bookmaker_odds(self, bookmaker, game_data, prefix):
        """Extract odds from a specific bookmaker"""
        markets = bookmaker.get('markets', [])
        
        for market in markets:
            market_key = market.get('key')
            outcomes = market.get('outcomes', [])
            
            if market_key == 'h2h':  # Moneyline
                for outcome in outcomes:
                    team_name = outcome.get('name', '')
                    price = outcome.get('price')
                    
                    if team_name == game_data['home_team_full']:
                        game_data[f'{prefix}home_moneyline'] = price
                    elif team_name == game_data['away_team_full']:
                        game_data[f'{prefix}away_moneyline'] = price
            
            elif market_key == 'spreads':  # Point spread
                for outcome in outcomes:
                    team_name = outcome.get('name', '')
                    price = outcome.get('price')
                    point = outcome.get('point')
                    
                    if team_name == game_data['home_team_full']:
                        game_data[f'{prefix}home_spread'] = point
                        game_data[f'{prefix}home_spread_odds'] = price
                    elif team_name == game_data['away_team_full']:
                        game_data[f'{prefix}away_spread'] = point
                        game_data[f'{prefix}away_spread_odds'] = price
            
            elif market_key == 'totals':  # Over/Under
                for outcome in outcomes:
                    name = outcome.get('name', '').lower()
                    price = outcome.get('price')
                    point = outcome.get('point')
                    
                    if name == 'over':
                        game_data[f'{prefix}total_points'] = point
                        game_data[f'{prefix}over_odds'] = price
                    elif name == 'under':
                        game_data[f'{prefix}under_odds'] = price
    
    def find_best_odds(self, bookmakers):
        """Find the best odds across all bookmakers"""
        best_odds = {
            'home_moneyline': None,
            'away_moneyline': None,
            'home_spread_odds': None,
            'away_spread_odds': None,
            'over_odds': None,
            'under_odds': None,
            'home_spread': None,
            'away_spread': None,
            'total_points': None
        }
        
        # Track best values
        best_values = {
            'home_moneyline': -float('inf'),
            'away_moneyline': -float('inf'),
            'home_spread_odds': -float('inf'),
            'away_spread_odds': -float('inf'),
            'over_odds': -float('inf'),
            'under_odds': -float('inf')
        }
        
        for bookmaker in bookmakers:
            markets = bookmaker.get('markets', [])
            
            for market in markets:
                market_key = market.get('key')
                outcomes = market.get('outcomes', [])
                
                if market_key == 'h2h':
                    for outcome in outcomes:
                        price = outcome.get('price', 0)
                        name = outcome.get('name', '')
                        
                        # Higher moneyline is better for bettors
                        if 'home' in name.lower() or name == best_odds.get('home_team_full', ''):
                            if price > best_values['home_moneyline']:
                                best_values['home_moneyline'] = price
                                best_odds['home_moneyline'] = price
                        else:
                            if price > best_values['away_moneyline']:
                                best_values['away_moneyline'] = price
                                best_odds['away_moneyline'] = price
        
        return best_odds
    
    def save_odds_data(self, odds_data):
        """Save odds data to CSV file"""
        if not odds_data:
            logger.warning("No odds data to save")
            return None
        
        df = pd.DataFrame(odds_data)
        
        # Save to CSV
        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = f"mlb_odds_{date_str}.csv"
        filepath = DATA_DIR / filename
        
        df.to_csv(filepath, index=False)
        logger.info(f"Saved odds for {len(odds_data)} games to {filepath}")
        
        return filepath
    
    def print_odds_summary(self, odds_data):
        """Print summary of collected odds"""
        if not odds_data:
            print("\n❌ No odds data found")
            return
        
        print(f"\n💰 MLB ODDS COLLECTED (The Odds API)")
        print("="*100)
        print(f"Total games: {len(odds_data)}")
        
        fanduel_games = len([g for g in odds_data if g.get('fd_home_moneyline') is not None])
        print(f"Games with FanDuel odds: {fanduel_games}")
        
        print("\n🎯 FANDUEL MONEYLINE ODDS:")
        print("-"*100)
        
        for game in odds_data:
            away_team = game.get('away_team', 'Unknown')
            home_team = game.get('home_team', 'Unknown')
            
            # FanDuel odds
            away_ml = game.get('fd_away_moneyline', 'N/A')
            home_ml = game.get('fd_home_moneyline', 'N/A')
            
            # Spread
            away_spread = game.get('fd_away_spread', 'N/A')
            home_spread = game.get('fd_home_spread', 'N/A')
            
            # Total
            total = game.get('fd_total_points', 'N/A')
            over_odds = game.get('fd_over_odds', 'N/A')
            under_odds = game.get('fd_under_odds', 'N/A')
            
            # Game time
            commence_time = game.get('commence_time', '')
            if commence_time:
                try:
                    game_time = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                    time_str = game_time.strftime('%I:%M %p ET')
                except:
                    time_str = commence_time[:16]
            else:
                time_str = 'TBD'
            
            print(f"{away_team:3} @ {home_team:3} | {time_str:>10} | "
                  f"ML: {away_ml:>4}/{home_ml:>4} | "
                  f"Spread: {away_spread:>4}/{home_spread:>4} | "
                  f"Total: {total:>4} O:{over_odds:>4}/U:{under_odds:>4}")

def main():
    parser = argparse.ArgumentParser(description="Collect MLB odds using The Odds API")
    args = parser.parse_args()
    
    collector = OddsAPICollector()
    
    logger.info("Starting MLB odds collection...")
    odds_data = collector.get_current_odds()
    
    if odds_data:
        filepath = collector.save_odds_data(odds_data)
        collector.print_odds_summary(odds_data)
        
        if filepath:
            print(f"\n💾 Odds data saved to: {filepath}")
    else:
        print("❌ No odds data could be collected")

if __name__ == "__main__":
    main()