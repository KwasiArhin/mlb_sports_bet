#!/usr/bin/env python3
"""
Team Head-to-Head Comparison Service

Provides detailed statistical comparisons between MLB teams including:
- Current season stats
- Recent form (L10, L20 games)
- Head-to-head historical records
- Advanced metrics and trends
"""

import pandas as pd
import numpy as np
import requests
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

logger = logging.getLogger(__name__)

class TeamComparisonService:
    """Service for comparing MLB teams head-to-head."""
    
    def __init__(self, base_dir: Path = None):
        self.base_dir = base_dir or Path(__file__).parent.parent
        self.data_dir = self.base_dir / 'data'
        self.processed_dir = self.data_dir / 'processed'
        
        # MLB team mapping for consistency
        self.team_mapping = {
            'ARI': {'name': 'Arizona Diamondbacks', 'abbr': 'ARI', 'division': 'NL West'},
            'ATL': {'name': 'Atlanta Braves', 'abbr': 'ATL', 'division': 'NL East'},
            'BAL': {'name': 'Baltimore Orioles', 'abbr': 'BAL', 'division': 'AL East'},
            'BOS': {'name': 'Boston Red Sox', 'abbr': 'BOS', 'division': 'AL East'},
            'CHC': {'name': 'Chicago Cubs', 'abbr': 'CHC', 'division': 'NL Central'},
            'CWS': {'name': 'Chicago White Sox', 'abbr': 'CWS', 'division': 'AL Central'},
            'CHW': {'name': 'Chicago White Sox', 'abbr': 'CHW', 'division': 'AL Central'},
            'CIN': {'name': 'Cincinnati Reds', 'abbr': 'CIN', 'division': 'NL Central'},
            'CLE': {'name': 'Cleveland Guardians', 'abbr': 'CLE', 'division': 'AL Central'},
            'COL': {'name': 'Colorado Rockies', 'abbr': 'COL', 'division': 'NL West'},
            'DET': {'name': 'Detroit Tigers', 'abbr': 'DET', 'division': 'AL Central'},
            'HOU': {'name': 'Houston Astros', 'abbr': 'HOU', 'division': 'AL West'},
            'KC': {'name': 'Kansas City Royals', 'abbr': 'KC', 'division': 'AL Central'},
            'LAA': {'name': 'Los Angeles Angels', 'abbr': 'LAA', 'division': 'AL West'},
            'LAD': {'name': 'Los Angeles Dodgers', 'abbr': 'LAD', 'division': 'NL West'},
            'MIA': {'name': 'Miami Marlins', 'abbr': 'MIA', 'division': 'NL East'},
            'MIL': {'name': 'Milwaukee Brewers', 'abbr': 'MIL', 'division': 'NL Central'},
            'MIN': {'name': 'Minnesota Twins', 'abbr': 'MIN', 'division': 'AL Central'},
            'NYM': {'name': 'New York Mets', 'abbr': 'NYM', 'division': 'NL East'},
            'NYY': {'name': 'New York Yankees', 'abbr': 'NYY', 'division': 'AL East'},
            'OAK': {'name': 'Oakland Athletics', 'abbr': 'OAK', 'division': 'AL West'},
            'PHI': {'name': 'Philadelphia Phillies', 'abbr': 'PHI', 'division': 'NL East'},
            'PIT': {'name': 'Pittsburgh Pirates', 'abbr': 'PIT', 'division': 'NL Central'},
            'SD': {'name': 'San Diego Padres', 'abbr': 'SD', 'division': 'NL West'},
            'SF': {'name': 'San Francisco Giants', 'abbr': 'SF', 'division': 'NL West'},
            'SEA': {'name': 'Seattle Mariners', 'abbr': 'SEA', 'division': 'AL West'},
            'STL': {'name': 'St. Louis Cardinals', 'abbr': 'STL', 'division': 'NL Central'},
            'TB': {'name': 'Tampa Bay Rays', 'abbr': 'TB', 'division': 'AL East'},
            'TEX': {'name': 'Texas Rangers', 'abbr': 'TEX', 'division': 'AL West'},
            'TOR': {'name': 'Toronto Blue Jays', 'abbr': 'TOR', 'division': 'AL East'},
            'WSH': {'name': 'Washington Nationals', 'abbr': 'WSH', 'division': 'NL East'}
        }
        
        # Initialize with demo data - in production, you'd fetch from APIs
        self._initialize_demo_stats()
    
    def _initialize_demo_stats(self):
        """Initialize with realistic demo team statistics."""
        # Demo team stats based on realistic 2024 MLB performance
        self.team_stats = {
            'LAD': {
                'wins': 98, 'losses': 64, 'win_pct': 0.605, 'runs_scored': 842, 'runs_allowed': 648,
                'team_era': 3.89, 'team_batting_avg': 0.258, 'home_record': '52-29', 'away_record': '46-35',
                'last_10': '7-3', 'streak': 'W2', 'games_back': 0, 'wild_card_rank': None
            },
            'NYY': {
                'wins': 94, 'losses': 68, 'win_pct': 0.580, 'runs_scored': 815, 'runs_allowed': 702,
                'team_era': 4.12, 'team_batting_avg': 0.254, 'home_record': '50-31', 'away_record': '44-37',
                'last_10': '6-4', 'streak': 'L1', 'games_back': 0, 'wild_card_rank': None
            },
            'HOU': {
                'wins': 88, 'losses': 74, 'win_pct': 0.543, 'runs_scored': 708, 'runs_allowed': 678,
                'team_era': 4.05, 'team_batting_avg': 0.246, 'home_record': '48-33', 'away_record': '40-41',
                'last_10': '8-2', 'streak': 'W4', 'games_back': 0, 'wild_card_rank': 2
            },
            'ATL': {
                'wins': 89, 'losses': 73, 'win_pct': 0.549, 'runs_scored': 724, 'runs_allowed': 651,
                'team_era': 3.85, 'team_batting_avg': 0.251, 'home_record': '47-34', 'away_record': '42-39',
                'last_10': '5-5', 'streak': 'W1', 'games_back': 6, 'wild_card_rank': 1
            },
            'TB': {
                'wins': 80, 'losses': 82, 'win_pct': 0.494, 'runs_scored': 672, 'runs_allowed': 691,
                'team_era': 4.18, 'team_batting_avg': 0.244, 'home_record': '43-38', 'away_record': '37-44',
                'last_10': '4-6', 'streak': 'L2', 'games_back': 14, 'wild_card_rank': None
            },
            'BOS': {
                'wins': 81, 'losses': 81, 'win_pct': 0.500, 'runs_scored': 745, 'runs_allowed': 715,
                'team_era': 4.21, 'team_batting_avg': 0.258, 'home_record': '44-37', 'away_record': '37-44',
                'last_10': '6-4', 'streak': 'W2', 'games_back': 13, 'wild_card_rank': None
            }
        }
        
        # Add remaining teams with varied realistic stats
        additional_teams = ['ARI', 'BAL', 'CHC', 'CWS', 'CHW', 'CIN', 'CLE', 'COL', 'DET', 'KC', 
                          'LAA', 'MIA', 'MIL', 'MIN', 'NYM', 'OAK', 'PHI', 'PIT', 'SD', 'SF', 
                          'SEA', 'STL', 'TEX', 'TOR', 'WSH']
        
        for team in additional_teams:
            if team not in self.team_stats:
                # Generate realistic random stats
                wins = np.random.randint(65, 95)
                losses = 162 - wins
                self.team_stats[team] = {
                    'wins': wins, 'losses': losses, 'win_pct': wins/162,
                    'runs_scored': np.random.randint(650, 850),
                    'runs_allowed': np.random.randint(650, 850),
                    'team_era': np.random.uniform(3.50, 5.00),
                    'team_batting_avg': np.random.uniform(0.235, 0.275),
                    'home_record': f"{np.random.randint(30, 50)}-{np.random.randint(31, 51)}",
                    'away_record': f"{np.random.randint(30, 50)}-{np.random.randint(31, 51)}",
                    'last_10': f"{np.random.randint(3, 8)}-{np.random.randint(2, 7)}",
                    'streak': np.random.choice(['W1', 'W2', 'W3', 'L1', 'L2', 'L3']),
                    'games_back': np.random.randint(0, 25),
                    'wild_card_rank': np.random.choice([None, 1, 2, 3], p=[0.7, 0.1, 0.1, 0.1])
                }
    
    def get_team_stats(self, team_abbr: str) -> Dict:
        """Get comprehensive stats for a specific team."""
        team_abbr = team_abbr.upper()
        
        if team_abbr not in self.team_mapping:
            return {}
        
        base_stats = self.team_stats.get(team_abbr, {})
        team_info = self.team_mapping[team_abbr]
        
        # Calculate additional metrics
        if base_stats:
            base_stats.update({
                'full_name': team_info['name'],
                'division': team_info['division'],
                'run_differential': base_stats.get('runs_scored', 0) - base_stats.get('runs_allowed', 0),
                'pythag_wins': self._calculate_pythagorean_wins(
                    base_stats.get('runs_scored', 0), 
                    base_stats.get('runs_allowed', 0)
                ),
                'strength_rating': self._calculate_strength_rating(base_stats)
            })
        
        return base_stats
    
    def _calculate_pythagorean_wins(self, runs_scored: int, runs_allowed: int) -> float:
        """Calculate Pythagorean expected wins."""
        if runs_allowed == 0:
            return 162.0
        return 162 * (runs_scored ** 2) / (runs_scored ** 2 + runs_allowed ** 2)
    
    def _calculate_strength_rating(self, stats: Dict) -> float:
        """Calculate overall team strength rating (0-100)."""
        if not stats:
            return 50.0
        
        # Weighted combination of key metrics
        win_pct_score = stats.get('win_pct', 0.5) * 100
        run_diff_score = min(max((stats.get('runs_scored', 700) - stats.get('runs_allowed', 700)) / 10, -20), 20) + 50
        era_score = max(min((5.0 - stats.get('team_era', 4.5)) * 20 + 50, 100), 0)
        
        return (win_pct_score * 0.5 + run_diff_score * 0.3 + era_score * 0.2)
    
    def get_head_to_head_record(self, team1: str, team2: str) -> Dict:
        """Get historical head-to-head record between two teams."""
        # In a real implementation, this would query historical game data
        # For now, generate realistic demo data
        
        # Simulate historical matchups (teams play 6-19 times per year depending on division)
        same_division = (self.team_mapping.get(team1, {}).get('division') == 
                        self.team_mapping.get(team2, {}).get('division'))
        
        if same_division:
            games_played = np.random.randint(15, 19)  # Division rivals play more
        else:
            games_played = np.random.randint(6, 10)   # Inter-division play less
        
        team1_wins = np.random.randint(0, games_played)
        team2_wins = games_played - team1_wins
        
        return {
            'team1': team1,
            'team2': team2,
            'team1_wins': team1_wins,
            'team2_wins': team2_wins,
            'games_played': games_played,
            'team1_win_pct': team1_wins / games_played if games_played > 0 else 0,
            'last_meeting': f"2024-{np.random.randint(4, 9):02d}-{np.random.randint(1, 28):02d}",
            'season_series': f"{team1_wins}-{team2_wins}"
        }
    
    def compare_teams(self, team1: str, team2: str) -> Dict:
        """Comprehensive head-to-head team comparison."""
        team1 = team1.upper()
        team2 = team2.upper()
        
        team1_stats = self.get_team_stats(team1)
        team2_stats = self.get_team_stats(team2)
        h2h_record = self.get_head_to_head_record(team1, team2)
        
        if not team1_stats or not team2_stats:
            return {'error': 'Team not found'}
        
        # Calculate advantages
        advantages = self._calculate_advantages(team1_stats, team2_stats)
        
        comparison = {
            'team1': {
                'abbr': team1,
                'stats': team1_stats,
                'advantages': advantages['team1']
            },
            'team2': {
                'abbr': team2,
                'stats': team2_stats,
                'advantages': advantages['team2']
            },
            'head_to_head': h2h_record,
            'key_matchups': self._generate_key_matchups(team1_stats, team2_stats),
            'prediction': self._generate_matchup_prediction(team1_stats, team2_stats, h2h_record)
        }
        
        return comparison
    
    def _calculate_advantages(self, team1_stats: Dict, team2_stats: Dict) -> Dict:
        """Calculate which team has advantages in various categories."""
        advantages = {'team1': [], 'team2': []}
        
        comparisons = [
            ('win_pct', 'Better Record', 'higher'),
            ('run_differential', 'Run Differential', 'higher'),
            ('team_era', 'Pitching (ERA)', 'lower'),
            ('team_batting_avg', 'Batting Average', 'higher'),
            ('strength_rating', 'Overall Strength', 'higher')
        ]
        
        for stat, label, comparison_type in comparisons:
            val1 = team1_stats.get(stat, 0)
            val2 = team2_stats.get(stat, 0)
            
            if comparison_type == 'higher':
                if val1 > val2:
                    advantages['team1'].append(label)
                elif val2 > val1:
                    advantages['team2'].append(label)
            else:  # lower is better
                if val1 < val2:
                    advantages['team1'].append(label)
                elif val2 < val1:
                    advantages['team2'].append(label)
        
        return advantages
    
    def _generate_key_matchups(self, team1_stats: Dict, team2_stats: Dict) -> List[Dict]:
        """Generate key matchup factors to watch."""
        return [
            {
                'category': 'Offense vs Pitching',
                'description': f"{team1_stats.get('full_name', 'Team 1')} batting ({team1_stats.get('team_batting_avg', 0):.3f}) vs {team2_stats.get('full_name', 'Team 2')} pitching ({team2_stats.get('team_era', 0):.2f} ERA)"
            },
            {
                'category': 'Recent Form',
                'description': f"{team1_stats.get('full_name', 'Team 1')}: {team1_stats.get('last_10', 'N/A')} L10, {team2_stats.get('full_name', 'Team 2')}: {team2_stats.get('last_10', 'N/A')} L10"
            },
            {
                'category': 'Home/Road Performance',
                'description': f"Home vs away performance could be decisive in this matchup"
            }
        ]
    
    def _generate_matchup_prediction(self, team1_stats: Dict, team2_stats: Dict, h2h: Dict) -> Dict:
        """Generate a simple matchup prediction based on stats."""
        team1_score = (
            team1_stats.get('strength_rating', 50) * 0.6 +
            (h2h.get('team1_win_pct', 0.5) * 100) * 0.2 +
            (1 if team1_stats.get('streak', '').startswith('W') else 0) * 10 * 0.2
        )
        
        team2_score = (
            team2_stats.get('strength_rating', 50) * 0.6 +
            (h2h.get('team2_win_pct', 0.5) * 100) * 0.2 +
            (1 if team2_stats.get('streak', '').startswith('W') else 0) * 10 * 0.2
        )
        
        total_score = team1_score + team2_score
        
        return {
            'team1_probability': team1_score / total_score if total_score > 0 else 0.5,
            'team2_probability': team2_score / total_score if total_score > 0 else 0.5,
            'confidence': min(abs(team1_score - team2_score) / 20, 1.0),
            'recommended_bet': team1_stats.get('abbr') if team1_score > team2_score else team2_stats.get('abbr')
        }
    
    def get_all_teams(self) -> List[Dict]:
        """Get list of all teams for dropdown/selection."""
        teams = []
        for abbr, info in self.team_mapping.items():
            stats = self.team_stats.get(abbr, {})
            teams.append({
                'abbr': abbr,
                'name': info['name'],
                'division': info['division'],
                'wins': stats.get('wins', 0),
                'losses': stats.get('losses', 0),
                'win_pct': stats.get('win_pct', 0)
            })
        
        # Sort by win percentage
        teams.sort(key=lambda x: x['win_pct'], reverse=True)
        return teams

# Global service instance
team_comparison_service = TeamComparisonService()

def compare_teams(team1: str, team2: str) -> Dict:
    """Convenience function for team comparison."""
    return team_comparison_service.compare_teams(team1, team2)

def get_all_teams() -> List[Dict]:
    """Convenience function to get all teams."""
    return team_comparison_service.get_all_teams()