# daily_games_collector.py

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

class DailyGamesCollector:
    def __init__(self):
        self.base_url = "https://statsapi.mlb.com/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Team abbreviation mapping
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
    
    def get_games_for_date(self, date_str=None):
        """
        Get MLB games for a specific date
        date_str format: 'YYYY-MM-DD' (defaults to today)
        """
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Fetching games for {date_str}")
        
        url = f"{self.base_url}/v1/schedule"
        params = {
            'sportId': 1,  # MLB
            'date': date_str,
            'hydrate': 'team,venue,probablePitcher,weather,broadcasts'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            games = []
            if 'dates' in data and len(data['dates']) > 0:
                for game in data['dates'][0]['games']:
                    game_info = self.extract_game_info(game)
                    if game_info:
                        games.append(game_info)
            
            logger.info(f"Found {len(games)} games for {date_str}")
            return games
            
        except requests.RequestException as e:
            logger.error(f"Error fetching games: {e}")
            return []
    
    def extract_game_info(self, game):
        """Extract relevant game information from API response"""
        try:
            # Basic game info
            game_id = game.get('gamePk')
            game_date = game.get('gameDate', '')[:10]  # Just the date part
            game_time = game.get('gameDate', '')
            status = game.get('status', {}).get('detailedState', 'Unknown')
            
            # Teams
            away_team = game.get('teams', {}).get('away', {})
            home_team = game.get('teams', {}).get('home', {})
            
            away_team_name = away_team.get('team', {}).get('name', '')
            home_team_name = home_team.get('team', {}).get('name', '')
            away_team_abbr = self.team_mapping.get(away_team_name, away_team_name)
            home_team_abbr = self.team_mapping.get(home_team_name, home_team_name)
            
            # Probable pitchers
            away_pitcher = ''
            home_pitcher = ''
            
            if 'probablePitcher' in away_team:
                away_pitcher_data = away_team['probablePitcher']
                away_pitcher = away_pitcher_data.get('fullName', '')
            
            if 'probablePitcher' in home_team:
                home_pitcher_data = home_team['probablePitcher']
                home_pitcher = home_pitcher_data.get('fullName', '')
            
            # Venue
            venue = game.get('venue', {}).get('name', '')
            
            # Weather (if available)
            weather = ''
            if 'weather' in game:
                weather_data = game['weather']
                temp = weather_data.get('temp', '')
                condition = weather_data.get('condition', '')
                wind = weather_data.get('wind', '')
                weather = f"{temp}°F, {condition}, {wind}"
            
            # Game scores (if completed)
            away_score = away_team.get('score', '')
            home_score = home_team.get('score', '')
            
            return {
                'game_id': game_id,
                'game_date': game_date,
                'game_time': game_time,
                'status': status,
                'away_team': away_team_abbr,
                'home_team': home_team_abbr,
                'away_team_full': away_team_name,
                'home_team_full': home_team_name,
                'away_pitcher': away_pitcher,
                'home_pitcher': home_pitcher,
                'venue': venue,
                'weather': weather,
                'away_score': away_score,
                'home_score': home_score
            }
            
        except Exception as e:
            logger.error(f"Error extracting game info: {e}")
            return None
    
    def save_games_data(self, games, date_str):
        """Save games data to CSV file"""
        if not games:
            logger.warning(f"No games to save for {date_str}")
            return None
        
        df = pd.DataFrame(games)
        
        # Save to CSV
        filename = f"mlb_games_{date_str}.csv"
        filepath = DATA_DIR / filename
        df.to_csv(filepath, index=False)
        
        logger.info(f"Saved {len(games)} games to {filepath}")
        return filepath
    
    def get_current_games(self):
        """Get today's games"""
        today = datetime.now().strftime('%Y-%m-%d')
        games = self.get_games_for_date(today)
        
        if games:
            filepath = self.save_games_data(games, today)
            self.print_games_summary(games, today)
            return filepath, games
        else:
            logger.warning("No games found for today")
            return None, []
    
    def print_games_summary(self, games, date_str):
        """Print a summary of today's games"""
        print(f"\n⚾ MLB GAMES FOR {date_str}")
        print("="*80)
        
        scheduled_games = [g for g in games if 'Scheduled' in g.get('status', '')]
        in_progress_games = [g for g in games if 'In Progress' in g.get('status', '') or 'Live' in g.get('status', '')]
        completed_games = [g for g in games if 'Final' in g.get('status', '')]
        
        print(f"📊 SUMMARY: {len(games)} total games | "
              f"{len(scheduled_games)} scheduled | "
              f"{len(in_progress_games)} in progress | "
              f"{len(completed_games)} completed")
        
        if scheduled_games:
            print(f"\n🕒 SCHEDULED GAMES ({len(scheduled_games)}):")
            print("-"*80)
            for game in scheduled_games:
                game_time = datetime.fromisoformat(game['game_time'].replace('Z', '+00:00'))
                local_time = game_time.strftime('%I:%M %p ET')
                
                print(f"{game['away_team']:3} @ {game['home_team']:3} | "
                      f"{local_time} | {game['venue']}")
                if game['away_pitcher'] and game['home_pitcher']:
                    print(f"     Pitchers: {game['away_pitcher']} vs {game['home_pitcher']}")
                print()
        
        if completed_games:
            print(f"\n✅ COMPLETED GAMES ({len(completed_games)}):")
            print("-"*80)
            for game in completed_games:
                winner = game['home_team'] if int(game.get('home_score', 0)) > int(game.get('away_score', 0)) else game['away_team']
                print(f"{game['away_team']:3} {game.get('away_score', '-'):2} @ "
                      f"{game['home_team']:3} {game.get('home_score', '-'):2} | "
                      f"Winner: {winner}")

def main():
    parser = argparse.ArgumentParser(description="Collect daily MLB games")
    parser.add_argument("--date", help="Date in YYYY-MM-DD format (default: today)", type=str)
    args = parser.parse_args()
    
    collector = DailyGamesCollector()
    
    if args.date:
        games = collector.get_games_for_date(args.date)
        if games:
            filepath = collector.save_games_data(games, args.date)
            collector.print_games_summary(games, args.date)
            print(f"\n💾 Games data saved to: {filepath}")
    else:
        filepath, games = collector.get_current_games()
        if filepath:
            print(f"\n💾 Today's games saved to: {filepath}")

if __name__ == "__main__":
    main()