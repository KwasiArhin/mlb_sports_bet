# lineup_collector.py

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

class LineupCollector:
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
        """Get games for a specific date to get game IDs"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Fetching games for {date_str} to collect lineups")
        
        url = f"{self.base_url}/v1/schedule"
        params = {
            'sportId': 1,  # MLB
            'date': date_str,
            'hydrate': 'team,venue'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            games = []
            if 'dates' in data and len(data['dates']) > 0:
                for game in data['dates'][0]['games']:
                    game_info = {
                        'game_id': game.get('gamePk'),
                        'game_date': date_str,
                        'away_team': self.team_mapping.get(game.get('teams', {}).get('away', {}).get('team', {}).get('name', ''), 'Unknown'),
                        'home_team': self.team_mapping.get(game.get('teams', {}).get('home', {}).get('team', {}).get('name', ''), 'Unknown'),
                        'away_team_full': game.get('teams', {}).get('away', {}).get('team', {}).get('name', ''),
                        'home_team_full': game.get('teams', {}).get('home', {}).get('team', {}).get('name', ''),
                        'venue': game.get('venue', {}).get('name', ''),
                        'game_time': game.get('gameDate', ''),
                        'status': game.get('status', {}).get('detailedState', '')
                    }
                    games.append(game_info)
            
            logger.info(f"Found {len(games)} games for lineup collection")
            return games
            
        except requests.RequestException as e:
            logger.error(f"Error fetching games: {e}")
            return []
    
    def get_pregame_lineups(self, game_id):
        """Get pregame starting lineups from schedule endpoint"""
        from datetime import datetime
        
        # Get today's date to find the game in schedule
        date_str = datetime.now().strftime('%Y-%m-%d')
        url = f"{self.base_url}/v1/schedule"
        params = {
            'sportId': 1,
            'date': date_str,
            'hydrate': 'lineups,probablePitcher'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Find the specific game
            target_game = None
            if 'dates' in data and len(data['dates']) > 0:
                for game in data['dates'][0]['games']:
                    if game.get('gamePk') == game_id:
                        target_game = game
                        break
            
            if not target_game or 'lineups' not in target_game:
                return {'lineups_available': False, 'away_lineup': [], 'home_lineup': []}
            
            lineups = target_game['lineups']
            away_players = lineups.get('awayPlayers', [])
            home_players = lineups.get('homePlayers', [])
            
            # Process away lineup
            away_lineup = []
            for i, player in enumerate(away_players):
                batter_info = {
                    'batting_order': i + 1,
                    'player_id': player.get('id', f'unknown_{i}'),
                    'name': player.get('fullName', 'Unknown'),
                    'position': player.get('primaryPosition', {}).get('abbreviation', 'Unknown'),
                    'position_name': player.get('primaryPosition', {}).get('name', 'Unknown'),
                    'avg': 'N/A',  # Not available in pregame data
                    'ops': 'N/A',
                    'hr': 'N/A',
                    'rbi': 'N/A'
                }
                away_lineup.append(batter_info)
            
            # Process home lineup
            home_lineup = []
            for i, player in enumerate(home_players):
                batter_info = {
                    'batting_order': i + 1,
                    'player_id': player.get('id', f'unknown_{i}'),
                    'name': player.get('fullName', 'Unknown'),
                    'position': player.get('primaryPosition', {}).get('abbreviation', 'Unknown'),
                    'position_name': player.get('primaryPosition', {}).get('name', 'Unknown'),
                    'avg': 'N/A',  # Not available in pregame data
                    'ops': 'N/A',
                    'hr': 'N/A',
                    'rbi': 'N/A'
                }
                home_lineup.append(batter_info)
            
            # Get probable pitchers
            away_pitcher = None
            home_pitcher = None
            
            if 'teams' in target_game:
                teams = target_game['teams']
                if 'away' in teams and 'probablePitcher' in teams['away']:
                    away_pitcher = teams['away']['probablePitcher'].get('fullName', 'TBD')
                if 'home' in teams and 'probablePitcher' in teams['home']:
                    home_pitcher = teams['home']['probablePitcher'].get('fullName', 'TBD')
            
            return {
                'game_id': game_id,
                'away_lineup': away_lineup,
                'home_lineup': home_lineup,
                'away_pitcher': away_pitcher,
                'home_pitcher': home_pitcher,
                'lineups_available': len(away_lineup) > 0 and len(home_lineup) > 0
            }
            
        except requests.RequestException as e:
            logger.error(f"Error fetching pregame lineups for game {game_id}: {e}")
            return {'lineups_available': False, 'away_lineup': [], 'home_lineup': []}
    
    def get_lineup_for_game(self, game_id):
        """Get pregame lineup information for a specific game"""
        # First try to get pregame lineups from schedule
        pregame_lineups = self.get_pregame_lineups(game_id)
        if pregame_lineups['lineups_available']:
            return pregame_lineups
        
        # Fallback to boxscore method for in-progress games
        url = f"{self.base_url}/v1/game/{game_id}/boxscore"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            
            lineup_data = {
                'game_id': game_id,
                'away_lineup': [],
                'home_lineup': [],
                'away_pitcher': None,
                'home_pitcher': None,
                'lineups_available': False
            }
            
            # Check if boxscore data exists
            if 'teams' not in data:
                return lineup_data
            
            teams = data['teams']
            
            # Process away team lineup
            if 'away' in teams and 'batters' in teams['away']:
                away_batters = teams['away']['batters']
                away_batting_order = teams['away'].get('battingOrder', [])
                
                for order_num, batter_id in enumerate(away_batting_order):
                    player_key = f"ID{batter_id}"
                    if player_key in teams['away']['players']:
                        player = teams['away']['players'][player_key]
                        person = player.get('person', {})
                        position = player.get('position', {})
                        stats = player.get('stats', {}).get('batting', {})
                        
                        batter_info = {
                            'batting_order': order_num + 1,
                            'player_id': batter_id,
                            'name': person.get('fullName', 'Unknown'),
                            'position': position.get('abbreviation', 'Unknown'),
                            'position_name': position.get('name', 'Unknown'),
                            'avg': stats.get('avg', 'N/A'),
                            'ops': stats.get('ops', 'N/A'),
                            'hr': stats.get('homeRuns', 'N/A'),
                            'rbi': stats.get('rbi', 'N/A')
                        }
                        lineup_data['away_lineup'].append(batter_info)
                
                lineup_data['lineups_available'] = len(lineup_data['away_lineup']) > 0
            
            # Process home team lineup
            if 'home' in teams and 'batters' in teams['home']:
                home_batters = teams['home']['batters']
                home_batting_order = teams['home'].get('battingOrder', [])
                
                for order_num, batter_id in enumerate(home_batting_order):
                    player_key = f"ID{batter_id}"
                    if player_key in teams['home']['players']:
                        player = teams['home']['players'][player_key]
                        person = player.get('person', {})
                        position = player.get('position', {})
                        stats = player.get('stats', {}).get('batting', {})
                        
                        batter_info = {
                            'batting_order': order_num + 1,
                            'player_id': batter_id,
                            'name': person.get('fullName', 'Unknown'),
                            'position': position.get('abbreviation', 'Unknown'),
                            'position_name': position.get('name', 'Unknown'),
                            'avg': stats.get('avg', 'N/A'),
                            'ops': stats.get('ops', 'N/A'),
                            'hr': stats.get('homeRuns', 'N/A'),
                            'rbi': stats.get('rbi', 'N/A')
                        }
                        lineup_data['home_lineup'].append(batter_info)
                
                if len(lineup_data['home_lineup']) > 0:
                    lineup_data['lineups_available'] = True
            
            # Get starting pitchers
            if 'away' in teams and 'pitchers' in teams['away']:
                away_pitchers = teams['away']['pitchers']
                if away_pitchers:
                    # Usually the starting pitcher is first, but let's check stats
                    for pitcher_id in away_pitchers:
                        player_key = f"ID{pitcher_id}"
                        if player_key in teams['away']['players']:
                            player = teams['away']['players'][player_key]
                            pitching_stats = player.get('stats', {}).get('pitching', {})
                            # Starting pitcher typically has innings pitched
                            if pitching_stats.get('inningsPitched', '0') != '0' or not lineup_data['away_pitcher']:
                                person = player.get('person', {})
                                lineup_data['away_pitcher'] = person.get('fullName', 'Unknown')
                                break
            
            if 'home' in teams and 'pitchers' in teams['home']:
                home_pitchers = teams['home']['pitchers']
                if home_pitchers:
                    for pitcher_id in home_pitchers:
                        player_key = f"ID{pitcher_id}"
                        if player_key in teams['home']['players']:
                            player = teams['home']['players'][player_key]
                            pitching_stats = player.get('stats', {}).get('pitching', {})
                            if pitching_stats.get('inningsPitched', '0') != '0' or not lineup_data['home_pitcher']:
                                person = player.get('person', {})
                                lineup_data['home_pitcher'] = person.get('fullName', 'Unknown')
                                break
            
            return lineup_data
            
        except requests.RequestException as e:
            logger.error(f"Error fetching lineup for game {game_id}: {e}")
            return {
                'game_id': game_id,
                'away_lineup': [],
                'home_lineup': [],
                'away_pitcher': None,
                'home_pitcher': None,
                'lineups_available': False
            }
    
    def get_probable_lineups_alternative(self, game):
        """Alternative method to get probable lineups using game info"""
        # This creates a placeholder lineup structure when actual lineups aren't available
        lineup_data = {
            'game_id': game['game_id'],
            'away_lineup': [],
            'home_lineup': [],
            'away_pitcher': None,
            'home_pitcher': None,
            'lineups_available': False,
            'status': 'Not Available - Lineups typically released 2-3 hours before game time'
        }
        
        # Create placeholder lineup positions
        positions = ['C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF', 'DH']
        
        for i in range(9):
            pos = positions[i] if i < len(positions) else 'UTIL'
            
            away_batter = {
                'batting_order': i + 1,
                'player_id': f'tbd_away_{i}',
                'name': 'TBD',
                'position': pos,
                'position_name': pos,
                'avg': 'N/A',
                'ops': 'N/A',
                'hr': 'N/A',
                'rbi': 'N/A'
            }
            
            home_batter = {
                'batting_order': i + 1,
                'player_id': f'tbd_home_{i}',
                'name': 'TBD',
                'position': pos,
                'position_name': pos,
                'avg': 'N/A',
                'ops': 'N/A',
                'hr': 'N/A',
                'rbi': 'N/A'
            }
            
            lineup_data['away_lineup'].append(away_batter)
            lineup_data['home_lineup'].append(home_batter)
        
        return lineup_data
    
    def collect_daily_lineups(self, date_str=None):
        """Collect lineups for all games on a given date"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Collecting lineups for {date_str}")
        
        # Get games for the date
        games = self.get_games_for_date(date_str)
        
        if not games:
            logger.warning(f"No games found for {date_str}")
            return []
        
        all_lineups = []
        
        for game in games:
            game_id = game['game_id']
            logger.info(f"Fetching lineup for {game['away_team']} @ {game['home_team']} (Game ID: {game_id})")
            
            # Try to get actual lineups
            lineup_data = self.get_lineup_for_game(game_id)
            
            # If no lineups available, create placeholder
            if not lineup_data['lineups_available']:
                lineup_data = self.get_probable_lineups_alternative(game)
            
            # Add game information to lineup data
            lineup_data.update({
                'away_team': game['away_team'],
                'home_team': game['home_team'],
                'away_team_full': game['away_team_full'],
                'home_team_full': game['home_team_full'],
                'venue': game['venue'],
                'game_time': game['game_time'],
                'game_date': game['game_date'],
                'game_status': game['status']
            })
            
            all_lineups.append(lineup_data)
        
        # Save lineup data
        self.save_lineup_data(all_lineups, date_str)
        
        return all_lineups
    
    def save_lineup_data(self, lineups, date_str):
        """Save lineup data to JSON and CSV files"""
        if not lineups:
            logger.warning("No lineup data to save")
            return None
        
        # Save as JSON for detailed structure
        json_filename = f"mlb_lineups_{date_str}.json"
        json_filepath = DATA_DIR / json_filename
        
        with open(json_filepath, 'w') as f:
            json.dump(lineups, f, indent=2, default=str)
        
        logger.info(f"Saved lineup data to {json_filepath}")
        
        # Also create a simplified CSV for quick viewing
        csv_data = []
        for lineup in lineups:
            for i in range(max(len(lineup['away_lineup']), len(lineup['home_lineup']))):
                row = {
                    'game_id': lineup['game_id'],
                    'away_team': lineup['away_team'],
                    'home_team': lineup['home_team'],
                    'batting_order': i + 1,
                    'away_player': lineup['away_lineup'][i]['name'] if i < len(lineup['away_lineup']) else '',
                    'away_position': lineup['away_lineup'][i]['position'] if i < len(lineup['away_lineup']) else '',
                    'home_player': lineup['home_lineup'][i]['name'] if i < len(lineup['home_lineup']) else '',
                    'home_position': lineup['home_lineup'][i]['position'] if i < len(lineup['home_lineup']) else '',
                    'lineups_available': lineup['lineups_available']
                }
                csv_data.append(row)
        
        if csv_data:
            csv_df = pd.DataFrame(csv_data)
            csv_filename = f"mlb_lineups_{date_str}.csv"
            csv_filepath = DATA_DIR / csv_filename
            csv_df.to_csv(csv_filepath, index=False)
            logger.info(f"Saved simplified lineup CSV to {csv_filepath}")
        
        return json_filepath
    
    def print_lineup_summary(self, lineups, date_str):
        """Print a summary of collected lineups"""
        print(f"\n⚾ MLB LINEUPS FOR {date_str}")
        print("="*80)
        
        if not lineups:
            print("No games found for this date.")
            return
        
        available_lineups = sum(1 for l in lineups if l['lineups_available'])
        total_games = len(lineups)
        
        print(f"📊 SUMMARY: {total_games} games | {available_lineups} with confirmed lineups")
        
        for lineup in lineups:
            print(f"\n🏟️ {lineup['away_team']} @ {lineup['home_team']}")
            
            if lineup['lineups_available']:
                print("✅ Lineups Available")
                
                # Show first few batters for each team
                print(f"   {lineup['away_team']} Batting Order:")
                for i, batter in enumerate(lineup['away_lineup'][:3]):
                    print(f"     {batter['batting_order']}. {batter['name']} ({batter['position']})")
                
                print(f"   {lineup['home_team']} Batting Order:")
                for i, batter in enumerate(lineup['home_lineup'][:3]):
                    print(f"     {batter['batting_order']}. {batter['name']} ({batter['position']})")
            else:
                print("⏳ Lineups Not Yet Available")
                if 'status' in lineup:
                    print(f"     {lineup['status']}")

def main():
    parser = argparse.ArgumentParser(description="Collect MLB starting lineups")
    parser.add_argument("--date", help="Date in YYYY-MM-DD format (default: today)", type=str)
    args = parser.parse_args()
    
    collector = LineupCollector()
    
    date_str = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    lineups = collector.collect_daily_lineups(date_str)
    
    if lineups:
        collector.print_lineup_summary(lineups, date_str)
        print(f"\n💾 Lineup data saved to data/raw/ directory")
    else:
        print("❌ No lineup data collected")

if __name__ == "__main__":
    main()