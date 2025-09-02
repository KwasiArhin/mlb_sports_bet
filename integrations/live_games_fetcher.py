#!/usr/bin/env python3
"""
Live MLB Games Fetcher

Fetches today's MLB schedule and game information from multiple sources
with fallback options for reliability.
"""

import os
import requests
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import pandas as pd
from pathlib import Path
import time

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data' / 'raw'
DATA_DIR.mkdir(parents=True, exist_ok=True)


class MLBGamesFetcher:
    """Fetches live MLB games from multiple sources with fallback options."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.team_mapping = {
            # MLB API to standard abbreviations
            'LAA': 'LAA', 'HOU': 'HOU', 'OAK': 'OAK', 'TEX': 'TEX', 'SEA': 'SEA',
            'NYY': 'NYY', 'BOS': 'BOS', 'TB': 'TB', 'TOR': 'TOR', 'BAL': 'BAL',
            'CLE': 'CLE', 'DET': 'DET', 'KC': 'KC', 'CWS': 'CHW', 'MIN': 'MIN',
            'ATL': 'ATL', 'MIA': 'MIA', 'NYM': 'NYM', 'PHI': 'PHI', 'WSH': 'WSH',
            'CHC': 'CHC', 'MIL': 'MIL', 'STL': 'STL', 'PIT': 'PIT', 'CIN': 'CIN',
            'LAD': 'LAD', 'SD': 'SD', 'SF': 'SF', 'COL': 'COL', 'ARI': 'ARI'
        }
    
    def fetch_games_from_mlb_api(self, date: str) -> List[Dict]:
        """
        Fetch games from official MLB API.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            List of game dictionaries
        """
        try:
            # MLB Stats API endpoint
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
            
            logger.info(f"Fetching games from MLB API for {date}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            games = []
            
            if 'dates' in data and len(data['dates']) > 0:
                for game in data['dates'][0].get('games', []):
                    try:
                        home_team = game['teams']['home']['team']['abbreviation']
                        away_team = game['teams']['away']['team']['abbreviation']
                        
                        # Map team names to our standard format
                        home_team = self.team_mapping.get(home_team, home_team)
                        away_team = self.team_mapping.get(away_team, away_team)
                        
                        game_info = {
                            'game_date': date,
                            'home_team': home_team,
                            'away_team': away_team,
                            'game_time': game.get('gameDate', ''),
                            'status': game.get('status', {}).get('abstractGameState', 'Unknown'),
                            'venue': game.get('venue', {}).get('name', ''),
                            'game_id': game.get('gamePk', ''),
                            'home_probable_pitcher': self._extract_pitcher(game, 'home'),
                            'away_probable_pitcher': self._extract_pitcher(game, 'away'),
                            'source': 'mlb_api'
                        }
                        
                        games.append(game_info)
                        
                    except KeyError as e:
                        logger.warning(f"Missing key in game data: {e}")
                        continue
            
            logger.info(f"Successfully fetched {len(games)} games from MLB API")
            return games
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from MLB API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in MLB API fetch: {e}")
            return []
    
    def _extract_pitcher(self, game_data: Dict, team_type: str) -> str:
        """Extract probable pitcher name from game data."""
        try:
            pitcher_info = game_data['teams'][team_type].get('probablePitcher', {})
            if pitcher_info:
                full_name = pitcher_info.get('fullName', '')
                return full_name if full_name else 'TBD'
            return 'TBD'
        except:
            return 'TBD'
    
    def fetch_games_from_espn(self, date: str) -> List[Dict]:
        """
        Fetch games from ESPN API as backup.
        
        Args:
            date: Date in YYYY-MM-DD format
            
        Returns:
            List of game dictionaries
        """
        try:
            # Convert date format for ESPN API
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            espn_date = date_obj.strftime('%Y%m%d')
            
            url = f"http://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates={espn_date}"
            
            logger.info(f"Fetching games from ESPN API for {date}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            games = []
            
            for event in data.get('events', []):
                try:
                    competitions = event.get('competitions', [])
                    if not competitions:
                        continue
                    
                    competition = competitions[0]
                    competitors = competition.get('competitors', [])
                    
                    if len(competitors) != 2:
                        continue
                    
                    # ESPN has home/away in specific order
                    home_team = None
                    away_team = None
                    
                    for comp in competitors:
                        if comp.get('homeAway') == 'home':
                            home_team = comp['team'].get('abbreviation', '')
                        elif comp.get('homeAway') == 'away':
                            away_team = comp['team'].get('abbreviation', '')
                    
                    if not home_team or not away_team:
                        continue
                    
                    # Map to our standard format
                    home_team = self.team_mapping.get(home_team, home_team)
                    away_team = self.team_mapping.get(away_team, away_team)
                    
                    game_info = {
                        'game_date': date,
                        'home_team': home_team,
                        'away_team': away_team,
                        'game_time': event.get('date', ''),
                        'status': competition.get('status', {}).get('type', {}).get('name', 'Unknown'),
                        'venue': competition.get('venue', {}).get('fullName', ''),
                        'game_id': event.get('id', ''),
                        'home_probable_pitcher': 'TBD',  # ESPN doesn't always have pitcher info
                        'away_probable_pitcher': 'TBD',
                        'source': 'espn_api'
                    }
                    
                    games.append(game_info)
                    
                except KeyError as e:
                    logger.warning(f"Missing key in ESPN game data: {e}")
                    continue
            
            logger.info(f"Successfully fetched {len(games)} games from ESPN API")
            return games
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching from ESPN API: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in ESPN API fetch: {e}")
            return []
    
    def fetch_today_games(self, date: Optional[str] = None) -> List[Dict]:
        """
        Fetch today's games with fallback sources.
        
        Args:
            date: Date in YYYY-MM-DD format, defaults to today
            
        Returns:
            List of game dictionaries
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        logger.info(f"Fetching games for {date}")
        
        # Try MLB API first (most reliable for MLB data)
        games = self.fetch_games_from_mlb_api(date)
        
        if not games:
            logger.warning("MLB API failed, trying ESPN API")
            games = self.fetch_games_from_espn(date)
        
        if not games:
            logger.error("All APIs failed to return games")
            return []
        
        # Filter out games that are not regular season or are postponed
        filtered_games = []
        for game in games:
            status = game.get('status', '').lower()
            if status not in ['postponed', 'cancelled', 'suspended']:
                filtered_games.append(game)
        
        logger.info(f"Successfully fetched {len(filtered_games)} games for {date}")
        return filtered_games
    
    def save_games_to_csv(self, games: List[Dict], filename: Optional[str] = None) -> str:
        """
        Save games data to CSV file.
        
        Args:
            games: List of game dictionaries
            filename: Optional custom filename
            
        Returns:
            Path to saved file
        """
        if not games:
            logger.warning("No games to save")
            return ""
        
        if filename is None:
            date = games[0].get('game_date', datetime.now().strftime('%Y-%m-%d'))
            filename = f"mlb_games_{date}.csv"
        
        filepath = DATA_DIR / filename
        
        try:
            df = pd.DataFrame(games)
            df.to_csv(filepath, index=False)
            
            logger.info(f"Saved {len(games)} games to {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error saving games to CSV: {e}")
            return ""
    
    def get_live_games_with_status(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get live games with additional status information.
        
        Returns:
            Dictionary with games and metadata
        """
        games = self.fetch_today_games(date)
        
        if not games:
            return {
                'games': [],
                'count': 0,
                'date': date or datetime.now().strftime('%Y-%m-%d'),
                'last_updated': datetime.now().isoformat(),
                'status': 'no_games'
            }
        
        # Categorize games by status
        status_counts = {}
        for game in games:
            status = game.get('status', 'Unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return {
            'games': games,
            'count': len(games),
            'date': date or datetime.now().strftime('%Y-%m-%d'),
            'last_updated': datetime.now().isoformat(),
            'status': 'success',
            'status_breakdown': status_counts,
            'sources_used': list(set([g.get('source', 'unknown') for g in games]))
        }


def main():
    """Main function for standalone usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch today's MLB games")
    parser.add_argument('--date', '-d', help='Date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--save', '-s', action='store_true', help='Save to CSV file')
    parser.add_argument('--output', '-o', help='Output filename for CSV')
    parser.add_argument('--json', '-j', action='store_true', help='Output as JSON instead of table')
    
    args = parser.parse_args()
    
    fetcher = MLBGamesFetcher()
    result = fetcher.get_live_games_with_status(args.date)
    
    if result['status'] == 'no_games':
        print(f"❌ No games found for {result['date']}")
        return 1
    
    games = result['games']
    
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"\n⚾ MLB Games for {result['date']}")
        print(f"📅 Found {result['count']} games")
        print(f"🕐 Last updated: {result['last_updated']}")
        print(f"📊 Sources: {', '.join(result['sources_used'])}")
        print("\n" + "="*80)
        
        for i, game in enumerate(games, 1):
            print(f"{i:2d}. {game['away_team']} @ {game['home_team']}")
            print(f"    Time: {game['game_time']}")
            print(f"    Status: {game['status']}")
            print(f"    Venue: {game['venue']}")
            if game.get('home_probable_pitcher', 'TBD') != 'TBD':
                print(f"    Pitchers: {game['away_probable_pitcher']} vs {game['home_probable_pitcher']}")
            print()
    
    if args.save:
        filepath = fetcher.save_games_to_csv(games, args.output)
        if filepath:
            print(f"💾 Games saved to: {filepath}")
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())