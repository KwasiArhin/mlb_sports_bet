#!/usr/bin/env python3
"""
MLB Odds Service
Fetches live odds from multiple sportsbooks and calculates implied probabilities.
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class OddsService:
    """Service to fetch and process MLB betting odds."""
    
    def __init__(self):
        """Initialize the odds service."""
        # The Odds API configuration
        self.odds_api_key = "a74383d66d314cc2fc96f1e54931d6a4"
        self.odds_api_base_url = "https://api.the-odds-api.com/v4"
        self.mlb_sport_key = "baseball_mlb"
        
        # ESPN odds endpoint (backup)
        self.espn_odds_url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        
        # Team abbreviation to full name mapping
        self.team_mapping = {
            'ARI': 'Arizona Diamondbacks', 'ATL': 'Atlanta Braves', 'BAL': 'Baltimore Orioles',
            'BOS': 'Boston Red Sox', 'CHC': 'Chicago Cubs', 'CWS': 'Chicago White Sox',
            'CIN': 'Cincinnati Reds', 'CLE': 'Cleveland Guardians', 'COL': 'Colorado Rockies',
            'DET': 'Detroit Tigers', 'HOU': 'Houston Astros', 'KC': 'Kansas City Royals',
            'LAA': 'Los Angeles Angels', 'LAD': 'Los Angeles Dodgers', 'MIA': 'Miami Marlins',
            'MIL': 'Milwaukee Brewers', 'MIN': 'Minnesota Twins', 'NYM': 'New York Mets',
            'NYY': 'New York Yankees', 'OAK': 'Oakland Athletics', 'PHI': 'Philadelphia Phillies',
            'PIT': 'Pittsburgh Pirates', 'SD': 'San Diego Padres', 'SF': 'San Francisco Giants',
            'SEA': 'Seattle Mariners', 'STL': 'St. Louis Cardinals', 'TB': 'Tampa Bay Rays',
            'TEX': 'Texas Rangers', 'TOR': 'Toronto Blue Jays', 'WSH': 'Washington Nationals'
        }
        
        # Reverse mapping for lookups
        self.name_to_abbr = {v: k for k, v in self.team_mapping.items()}
        
        # Backup mock odds for demo purposes
        self.mock_odds = {
            'New York Yankees': {'moneyline': -150, 'spread': 1.5},
            'Boston Red Sox': {'moneyline': 130, 'spread': -1.5},
            'Los Angeles Dodgers': {'moneyline': -180, 'spread': 1.5},
            'San Francisco Giants': {'moneyline': 150, 'spread': -1.5},
            'Houston Astros': {'moneyline': -120, 'spread': 1.5},
            'Oakland Athletics': {'moneyline': 100, 'spread': -1.5},
        }
    
    def american_to_decimal(self, american_odds: int) -> float:
        """Convert American odds to decimal odds."""
        if american_odds > 0:
            return (american_odds / 100) + 1
        else:
            return (100 / abs(american_odds)) + 1
    
    def decimal_to_implied_probability(self, decimal_odds: float) -> float:
        """Convert decimal odds to implied probability."""
        if decimal_odds <= 1:
            return 0.5  # Default to 50/50 if odds are invalid
        return 1 / decimal_odds
    
    def american_to_implied_probability(self, american_odds: int) -> float:
        """Convert American odds directly to implied probability."""
        decimal_odds = self.american_to_decimal(american_odds)
        return self.decimal_to_implied_probability(decimal_odds)
    
    def normalize_team_name(self, team_name: str) -> str:
        """Convert team name/abbreviation to full name for API matching."""
        # If it's already a full name, return as is
        if team_name in self.name_to_abbr:
            return team_name
        
        # If it's an abbreviation, convert to full name
        if team_name.upper() in self.team_mapping:
            return self.team_mapping[team_name.upper()]
        
        # Try to find partial matches
        team_upper = team_name.upper()
        for abbr, full_name in self.team_mapping.items():
            if abbr in team_upper or team_upper in full_name.upper():
                return full_name
        
        # Return as is if no match found
        return team_name
    
    def fetch_odds_api_data(self) -> Dict[str, Dict]:
        """Fetch live MLB odds from The Odds API."""
        try:
            # Construct the API URL for MLB odds
            url = f"{self.odds_api_base_url}/sports/{self.mlb_sport_key}/odds"
            
            params = {
                'api_key': self.odds_api_key,
                'regions': 'us',  # US sportsbooks
                'markets': 'h2h',  # Head-to-head (moneyline) odds
                'oddsFormat': 'american',  # American odds format (+150, -110)
                'dateFormat': 'iso'
            }
            
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            odds_data = {}
            
            logger.info(f"Fetched {len(data)} games from The Odds API")
            
            for game in data:
                away_team = game.get('away_team', '')
                home_team = game.get('home_team', '')
                
                if not away_team or not home_team:
                    continue
                
                # Extract odds from bookmakers
                game_odds = self.extract_best_odds_from_bookmakers(game.get('bookmakers', []), away_team, home_team)
                
                matchup_key = f"{away_team} @ {home_team}"
                odds_data[matchup_key] = {
                    'away_team': away_team,
                    'home_team': home_team,
                    'away_odds': game_odds['away_odds'],
                    'home_odds': game_odds['home_odds'],
                    'away_implied_prob': self.american_to_implied_probability(game_odds['away_odds']),
                    'home_implied_prob': self.american_to_implied_probability(game_odds['home_odds']),
                    'commence_time': game.get('commence_time'),
                    'best_bookmaker': game_odds['bookmaker'],
                    'source': 'The Odds API'
                }
            
            logger.info(f"Processed odds for {len(odds_data)} MLB games from The Odds API")
            return odds_data
            
        except Exception as e:
            logger.error(f"Failed to fetch from The Odds API: {e}")
            return {}
    
    def extract_best_odds_from_bookmakers(self, bookmakers: List[Dict], away_team: str, home_team: str) -> Dict:
        """Extract the best odds from available bookmakers."""
        best_odds = {'away_odds': -110, 'home_odds': -110, 'bookmaker': 'Default'}
        
        if not bookmakers:
            return best_odds
        
        # Use the first available bookmaker (typically DraftKings, FanDuel, etc.)
        for bookmaker in bookmakers:
            if 'markets' in bookmaker:
                for market in bookmaker['markets']:
                    if market.get('key') == 'h2h':  # Head-to-head (moneyline)
                        outcomes = market.get('outcomes', [])
                        if len(outcomes) >= 2:
                            # Match outcomes to teams by name
                            for outcome in outcomes:
                                team_name = outcome.get('name', '')
                                odds_price = outcome.get('price', -110)
                                
                                # Match team name to away or home
                                if team_name == away_team:
                                    best_odds['away_odds'] = odds_price
                                elif team_name == home_team:
                                    best_odds['home_odds'] = odds_price
                            
                            best_odds['bookmaker'] = bookmaker.get('title', 'Unknown')
                            logger.info(f"Extracted odds from {best_odds['bookmaker']}: {away_team} {best_odds['away_odds']}, {home_team} {best_odds['home_odds']}")
                            return best_odds
        
        return best_odds
    
    def fetch_espn_odds(self) -> Dict[str, Dict]:
        """Fetch odds from ESPN API (if available)."""
        try:
            response = requests.get(self.espn_odds_url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            odds_data = {}
            
            if 'events' in data:
                for event in data['events']:
                    if 'competitions' in event:
                        for competition in event['competitions']:
                            if 'competitors' in competition:
                                competitors = competition['competitors']
                                
                                # Extract team names and odds
                                away_team = competitors[1].get('team', {}).get('displayName', '')
                                home_team = competitors[0].get('team', {}).get('displayName', '')
                                
                                # Try to extract odds if available
                                game_odds = self.extract_odds_from_espn_competition(competition)
                                
                                if away_team and home_team:
                                    odds_data[f"{away_team} @ {home_team}"] = {
                                        'away_team': away_team,
                                        'home_team': home_team,
                                        'away_odds': game_odds.get('away_moneyline', -110),
                                        'home_odds': game_odds.get('home_moneyline', -110),
                                        'away_implied_prob': self.american_to_implied_probability(game_odds.get('away_moneyline', -110)),
                                        'home_implied_prob': self.american_to_implied_probability(game_odds.get('home_moneyline', -110)),
                                        'source': 'ESPN'
                                    }
            
            logger.info(f"Fetched odds for {len(odds_data)} games from ESPN")
            return odds_data
            
        except Exception as e:
            logger.warning(f"Failed to fetch ESPN odds: {e}")
            return {}
    
    def extract_odds_from_espn_competition(self, competition: Dict) -> Dict:
        """Extract odds from ESPN competition data."""
        odds = {'away_moneyline': -110, 'home_moneyline': -110}  # Default
        
        try:
            # ESPN sometimes has odds in different places in their API
            if 'odds' in competition:
                for odd in competition['odds']:
                    if 'awayTeamOdds' in odd:
                        odds['away_moneyline'] = odd['awayTeamOdds'].get('moneyLine', -110)
                    if 'homeTeamOdds' in odd:
                        odds['home_moneyline'] = odd['homeTeamOdds'].get('moneyLine', -110)
        except Exception:
            pass  # Use defaults
        
        return odds
    
    def generate_demo_odds(self, away_team: str, home_team: str) -> Dict:
        """Generate realistic demo odds based on team matchup."""
        # Use mock odds if available, otherwise generate realistic odds
        away_odds = self.mock_odds.get(away_team, {}).get('moneyline', -110)
        home_odds = self.mock_odds.get(home_team, {}).get('moneyline', -110)
        
        # Ensure odds are opposite (if one team is favored, other is underdog)
        if away_odds < 0:  # Away team favored
            home_odds = abs(away_odds) + 20
        elif home_odds < 0:  # Home team favored
            away_odds = abs(home_odds) + 20
        else:  # Generate balanced odds
            away_odds = -105
            home_odds = -105
        
        return {
            'away_team': away_team,
            'home_team': home_team,
            'away_odds': away_odds,
            'home_odds': home_odds,
            'away_implied_prob': self.american_to_implied_probability(away_odds),
            'home_implied_prob': self.american_to_implied_probability(home_odds),
            'source': 'Demo'
        }
    
    def get_game_odds(self, away_team: str, home_team: str) -> Dict:
        """Get odds for a specific game matchup."""
        # Normalize team names (convert abbreviations to full names)
        away_full_name = self.normalize_team_name(away_team)
        home_full_name = self.normalize_team_name(home_team)
        
        logger.info(f"Looking for odds: {away_team} ({away_full_name}) @ {home_team} ({home_full_name})")
        
        # First try to fetch from The Odds API
        odds_api_data = self.fetch_odds_api_data()
        
        if not odds_api_data:
            logger.warning("No data from Odds API")
        else:
            logger.info(f"Got {len(odds_api_data)} games from Odds API")
            for key in list(odds_api_data.keys())[:3]:  # Log first few games
                logger.info(f"Available game: {key}")
        
        # Try to match by normalized team names
        for key, value in odds_api_data.items():
            if value['away_team'] == away_full_name and value['home_team'] == home_full_name:
                logger.info(f"Found exact match: {key}")
                return value
            # Also try original team names in case abbreviations are used
            if value['away_team'] == away_team and value['home_team'] == home_team:
                logger.info(f"Found abbreviation match: {key}")
                return value
        
        # Fallback to ESPN if Odds API fails
        logger.info("No match in Odds API, trying ESPN")
        espn_odds = self.fetch_espn_odds()
        matchup_key = f"{away_team} @ {home_team}"
        if matchup_key in espn_odds:
            return espn_odds[matchup_key]
        
        # Try reverse matchup format for ESPN
        for key, value in espn_odds.items():
            if value['away_team'] == away_team and value['home_team'] == home_team:
                return value
        
        # Final fallback to demo odds
        logger.info(f"Using demo odds for {away_team} @ {home_team}")
        return self.generate_demo_odds(away_team, home_team)
    
    def get_all_mlb_odds(self) -> Dict[str, Dict]:
        """Get odds for all current MLB games."""
        # First try The Odds API
        odds_data = self.fetch_odds_api_data()
        
        # If Odds API fails or returns insufficient data, try ESPN as backup
        if len(odds_data) < 3:
            logger.info("Odds API returned insufficient data, trying ESPN backup")
            espn_odds = self.fetch_espn_odds()
            odds_data.update(espn_odds)
        
        # If still insufficient data, supplement with demo data
        if len(odds_data) < 3:  # If we have fewer than 3 games, add demo data
            demo_matchups = [
                ('New York Yankees', 'Boston Red Sox'),
                ('Los Angeles Dodgers', 'San Francisco Giants'),
                ('Houston Astros', 'Oakland Athletics'),
                ('Tampa Bay Rays', 'Toronto Blue Jays'),
                ('Chicago Cubs', 'Milwaukee Brewers'),
                ('Atlanta Braves', 'Philadelphia Phillies'),
                ('Texas Rangers', 'Seattle Mariners'),
                ('St. Louis Cardinals', 'Pittsburgh Pirates'),
            ]
            
            for away, home in demo_matchups:
                matchup_key = f"{away} @ {home}"
                if matchup_key not in odds_data:
                    odds_data[matchup_key] = self.generate_demo_odds(away, home)
        
        return odds_data
    
    def calculate_edge(self, model_prob: float, implied_prob: float) -> float:
        """Calculate betting edge as percentage."""
        return (model_prob - implied_prob) * 100
    
    def get_best_odds_comparison(self, away_team: str, home_team: str, 
                                model_away_prob: float, model_home_prob: float) -> Dict:
        """Get comprehensive odds comparison for a matchup."""
        odds_data = self.get_game_odds(away_team, home_team)
        
        return {
            'away_team': away_team,
            'home_team': home_team,
            'market_data': odds_data,
            'model_probabilities': {
                'away_prob': model_away_prob,
                'home_prob': model_home_prob
            },
            'edges': {
                'away_edge': self.calculate_edge(model_away_prob, odds_data['away_implied_prob']),
                'home_edge': self.calculate_edge(model_home_prob, odds_data['home_implied_prob'])
            },
            'recommendations': {
                'away_bet': model_away_prob > odds_data['away_implied_prob'],
                'home_bet': model_home_prob > odds_data['home_implied_prob']
            }
        }

# Global odds service instance
odds_service = OddsService()

def get_odds_for_matchup(away_team: str, home_team: str) -> Dict:
    """Convenience function to get odds for a matchup."""
    return odds_service.get_game_odds(away_team, home_team)

def get_implied_probability(american_odds: int) -> float:
    """Convenience function to get implied probability from American odds."""
    return odds_service.american_to_implied_probability(american_odds)