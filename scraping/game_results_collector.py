# game_results_collector.py

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

class GameResultsCollector:
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
    
    def get_completed_games(self, date_str=None):
        """
        Get completed MLB games for a specific date
        date_str format: 'YYYY-MM-DD' (defaults to yesterday)
        """
        if date_str is None:
            # Default to yesterday since today's games likely aren't finished
            yesterday = datetime.now() - timedelta(days=1)
            date_str = yesterday.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching completed games for {date_str}")
        
        url = f"{self.base_url}/v1/schedule"
        params = {
            'sportId': 1,  # MLB
            'date': date_str,
            'hydrate': 'team,venue,decisions,stats'
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            games = []
            if 'dates' in data and len(data['dates']) > 0:
                for game in data['dates'][0]['games']:
                    # Only process completed games
                    game_status = game.get('status', {}).get('detailedState', '')
                    if 'Final' in game_status or 'Completed' in game_status:
                        game_result = self.extract_game_result(game)
                        if game_result:
                            games.append(game_result)
            
            logger.info(f"Found {len(games)} completed games for {date_str}")
            return games
            
        except requests.RequestException as e:
            logger.error(f"Error fetching game results: {e}")
            return []
    
    def extract_game_result(self, game):
        """Extract game result information"""
        try:
            # Basic game info
            game_id = game.get('gamePk')
            game_date = game.get('gameDate', '')[:10]
            status = game.get('status', {}).get('detailedState', '')
            
            # Teams and scores
            teams = game.get('teams', {})
            away_team = teams.get('away', {})
            home_team = teams.get('home', {})
            
            away_team_name = away_team.get('team', {}).get('name', '')
            home_team_name = home_team.get('team', {}).get('name', '')
            away_team_abbr = self.team_mapping.get(away_team_name, away_team_name)
            home_team_abbr = self.team_mapping.get(home_team_name, home_team_name)
            
            away_score = away_team.get('score', 0)
            home_score = home_team.get('score', 0)
            
            # Determine winner
            if home_score > away_score:
                winner = home_team_abbr
                loser = away_team_abbr
                winning_score = home_score
                losing_score = away_score
                home_win = 1
            else:
                winner = away_team_abbr
                loser = home_team_abbr
                winning_score = away_score
                losing_score = home_score
                home_win = 0
            
            # Score differential
            run_differential = abs(home_score - away_score)
            
            # Total runs
            total_runs = home_score + away_score
            
            # Game details
            venue = game.get('venue', {}).get('name', '')
            inning = game.get('linescore', {}).get('currentInning', 9)
            
            # Pitching decisions (if available)
            decisions = game.get('decisions', {})
            winning_pitcher = ''
            losing_pitcher = ''
            save_pitcher = ''
            
            if 'winner' in decisions:
                winning_pitcher = decisions['winner'].get('fullName', '')
            if 'loser' in decisions:
                losing_pitcher = decisions['loser'].get('fullName', '')
            if 'save' in decisions:
                save_pitcher = decisions['save'].get('fullName', '')
            
            return {
                'game_id': game_id,
                'game_date': game_date,
                'away_team': away_team_abbr,
                'home_team': home_team_abbr,
                'away_team_full': away_team_name,
                'home_team_full': home_team_name,
                'away_score': away_score,
                'home_score': home_score,
                'winner': winner,
                'loser': loser,
                'home_win': home_win,
                'run_differential': run_differential,
                'total_runs': total_runs,
                'winning_score': winning_score,
                'losing_score': losing_score,
                'venue': venue,
                'innings_played': inning,
                'status': status,
                'winning_pitcher': winning_pitcher,
                'losing_pitcher': losing_pitcher,
                'save_pitcher': save_pitcher,
                'collected_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error extracting game result: {e}")
            return None
    
    def get_date_range_results(self, start_date, end_date):
        """Get game results for a date range"""
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        all_results = []
        current_date = start_dt
        
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            games = self.get_completed_games(date_str)
            all_results.extend(games)
            current_date += timedelta(days=1)
        
        logger.info(f"Collected {len(all_results)} completed games from {start_date} to {end_date}")
        return all_results
    
    def save_results_data(self, results, date_str=None):
        """Save game results to CSV file"""
        if not results:
            logger.warning("No game results to save")
            return None
        
        df = pd.DataFrame(results)
        
        # Save to CSV
        if date_str:
            filename = f"game_results_{date_str}.csv"
        else:
            filename = f"game_results_{datetime.now().strftime('%Y-%m-%d')}.csv"
        
        filepath = DATA_DIR / filename
        df.to_csv(filepath, index=False)
        
        logger.info(f"Saved {len(results)} game results to {filepath}")
        return filepath
    
    def print_results_summary(self, results, date_str=None):
        """Print summary of game results"""
        if not results:
            print("\n❌ No completed games found")
            return
        
        print(f"\n⚾ COMPLETED GAMES RESULTS")
        if date_str:
            print(f"Date: {date_str}")
        print("="*80)
        print(f"Total completed games: {len(results)}")
        
        # Home vs Away wins
        home_wins = sum(1 for game in results if game['home_win'] == 1)
        away_wins = len(results) - home_wins
        home_win_pct = (home_wins / len(results)) * 100 if results else 0
        
        print(f"Home team wins: {home_wins} ({home_win_pct:.1f}%)")
        print(f"Away team wins: {away_wins} ({100-home_win_pct:.1f}%)")
        
        # Score statistics
        total_runs_list = [game['total_runs'] for game in results]
        avg_total_runs = sum(total_runs_list) / len(total_runs_list) if total_runs_list else 0
        
        run_diff_list = [game['run_differential'] for game in results]
        avg_run_diff = sum(run_diff_list) / len(run_diff_list) if run_diff_list else 0
        
        print(f"Average total runs per game: {avg_total_runs:.1f}")
        print(f"Average run differential: {avg_run_diff:.1f}")
        
        print(f"\n📊 INDIVIDUAL GAME RESULTS:")
        print("-"*80)
        
        for game in results[:10]:  # Show first 10 games
            winner_score = game['winning_score']
            loser_score = game['losing_score']
            winner = game['winner']
            loser = game['loser']
            
            print(f"{game['away_team']:3} {game['away_score']:2} @ "
                  f"{game['home_team']:3} {game['home_score']:2} | "
                  f"Winner: {winner:3} ({winner_score}-{loser_score}) | "
                  f"Total: {game['total_runs']:2}")
        
        if len(results) > 10:
            print(f"... and {len(results) - 10} more games")

def main():
    parser = argparse.ArgumentParser(description="Collect MLB game results")
    parser.add_argument("--date", help="Date in YYYY-MM-DD format (default: yesterday)", type=str)
    parser.add_argument("--start-date", help="Start date for range collection", type=str)
    parser.add_argument("--end-date", help="End date for range collection", type=str)
    args = parser.parse_args()
    
    collector = GameResultsCollector()
    
    if args.start_date and args.end_date:
        # Collect date range
        results = collector.get_date_range_results(args.start_date, args.end_date)
        if results:
            filepath = collector.save_results_data(results)
            collector.print_results_summary(results)
            print(f"\n💾 Results saved to: {filepath}")
    
    elif args.date:
        # Collect specific date
        results = collector.get_completed_games(args.date)
        if results:
            filepath = collector.save_results_data(results, args.date)
            collector.print_results_summary(results, args.date)
            print(f"\n💾 Results saved to: {filepath}")
    
    else:
        # Collect yesterday's games (default)
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        results = collector.get_completed_games(yesterday)
        if results:
            filepath = collector.save_results_data(results, yesterday)
            collector.print_results_summary(results, yesterday)
            print(f"\n💾 Results saved to: {filepath}")

if __name__ == "__main__":
    main()