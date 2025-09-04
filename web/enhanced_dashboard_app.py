#!/usr/bin/env python3
"""
Enhanced MLB Betting Dashboard Web Application
Displays integrated betting data: games, odds, pitcher/hitter analysis
"""

from flask import Flask, render_template, jsonify, send_file, request
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import csv
import io
import sys
import pytz

# Setup paths
BASE_DIR = Path(__file__).resolve().parents[1]

# Add scraping and database modules to path
sys.path.append(str(BASE_DIR / "scraping"))
sys.path.append(str(BASE_DIR / "database"))

# Import lineup collector and database
from lineup_collector import LineupCollector
from mlb_database import MLBDatabase

PROCESSED_DIR = BASE_DIR / "data" / "processed"
RAW_DIR = BASE_DIR / "data" / "raw"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mlb-enhanced-dashboard-2025'

class EnhancedMLBDashboard:
    def __init__(self):
        self.integrated_data = None
        self.pitcher_data = None
        self.hitter_data = None
        self.statcast_data = None
        self.current_date = None
        self.lineup_collector = LineupCollector()
        self.lineup_data = None
        
        # Initialize database
        try:
            self.db = MLBDatabase()
            self.use_database = True
            print("✅ Database connected successfully")
        except Exception as e:
            print(f"⚠️ Database connection failed, falling back to CSV: {e}")
            self.db = None
            self.use_database = False
        
    def load_latest_data(self):
        """Load data for current date, falling back to most recent data"""
        try:
            # Get today's date
            today_str = datetime.now().strftime("%Y-%m-%d")
            today_file = PROCESSED_DIR / f"integrated_betting_data_{today_str}.csv"
            
            # Try to load today's data first
            if today_file.exists():
                self.integrated_data = pd.read_csv(today_file)
                self.current_date = datetime.now().strftime("%B %d, %Y")
                print(f"✅ Loaded today's data: {today_str}")
            else:
                # Try to generate today's data by fetching fresh games
                print(f"🔄 Today's data not found, attempting to fetch fresh games for {today_str}")
                fresh_games = self.fetch_fresh_games(today_str)
                
                if fresh_games is not None and len(fresh_games) > 0:
                    self.integrated_data = fresh_games
                    self.current_date = datetime.now().strftime("%B %d, %Y")
                    print(f"✅ Generated fresh data with {len(fresh_games)} games for {today_str}")
                else:
                    # Final fallback to most recent file
                    integrated_files = list(PROCESSED_DIR.glob("integrated_betting_data_*.csv"))
                    if integrated_files:
                        latest_integrated = max(integrated_files, key=lambda x: x.stat().st_mtime)
                        self.integrated_data = pd.read_csv(latest_integrated)
                        
                        # Parse date from filename
                        date_str = latest_integrated.name.replace("integrated_betting_data_", "").replace(".csv", "")
                        try:
                            self.current_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")
                        except:
                            self.current_date = datetime.now().strftime("%B %d, %Y")
                        print(f"⚠️ Using fallback data: {date_str} (could not fetch fresh games)")
                    else:
                        self.current_date = datetime.now().strftime("%B %d, %Y")
                        print("❌ No integrated betting data found")
            
            # Load comprehensive pitcher data from Enhanced collector (prefer today's data)
            today_pitcher_file = PROCESSED_DIR / f"enhanced_pitcher_data_{today_str}.csv"
            if today_pitcher_file.exists():
                self.pitcher_data = pd.read_csv(today_pitcher_file)
                print(f"✅ Loaded today's enhanced pitcher data: {today_str}")
            else:
                # Try fangraphs data as fallback
                fangraphs_today = PROCESSED_DIR / f"fangraphs_pitcher_data_{today_str}.csv"
                if fangraphs_today.exists():
                    self.pitcher_data = pd.read_csv(fangraphs_today)
                    print(f"✅ Loaded today's Fangraphs pitcher data: {today_str}")
                else:
                    # Get most recent enhanced data
                    enhanced_files = list(PROCESSED_DIR.glob("enhanced_pitcher_data_*.csv"))
                    if enhanced_files:
                        latest_enhanced = max(enhanced_files, key=lambda x: x.stat().st_mtime)
                        self.pitcher_data = pd.read_csv(latest_enhanced)
                        print(f"✅ Using latest enhanced pitcher data from: {latest_enhanced.name}")
                    else:
                        # Final fallback to fangraphs data
                        pitcher_files = list(PROCESSED_DIR.glob("fangraphs_pitcher_data_*.csv"))
                        if pitcher_files:
                            latest_pitcher = max(pitcher_files, key=lambda x: x.stat().st_mtime)
                            self.pitcher_data = pd.read_csv(latest_pitcher)
                            print(f"⚠️ Using fallback Fangraphs pitcher data from: {latest_pitcher.name}")
            
            # Load comprehensive hitter data from Enhanced collector (prefer today's data)
            today_hitter_file = PROCESSED_DIR / f"enhanced_hitter_data_{today_str}.csv"
            if today_hitter_file.exists():
                self.hitter_data = pd.read_csv(today_hitter_file)
                print(f"✅ Loaded today's enhanced hitter data: {today_str}")
            else:
                # Try fangraphs data as fallback
                fangraphs_today = PROCESSED_DIR / f"fangraphs_hitter_data_{today_str}.csv"
                if fangraphs_today.exists():
                    self.hitter_data = pd.read_csv(fangraphs_today)
                    print(f"✅ Loaded today's Fangraphs hitter data: {today_str}")
                else:
                    # Get most recent enhanced data
                    enhanced_files = list(PROCESSED_DIR.glob("enhanced_hitter_data_*.csv"))
                    if enhanced_files:
                        latest_enhanced = max(enhanced_files, key=lambda x: x.stat().st_mtime)
                        self.hitter_data = pd.read_csv(latest_enhanced)
                        print(f"✅ Using latest enhanced hitter data from: {latest_enhanced.name}")
                    else:
                        # Final fallback to fangraphs data
                        hitter_files = list(PROCESSED_DIR.glob("fangraphs_hitter_data_*.csv"))
                        if hitter_files:
                            latest_hitter = max(hitter_files, key=lambda x: x.stat().st_mtime)
                            self.hitter_data = pd.read_csv(latest_hitter)
                            print(f"⚠️ Using fallback Fangraphs hitter data from: {latest_hitter.name}")
            
            # Load Statcast expected stats data (prefer today's data)
            today_statcast_file = PROCESSED_DIR / f"baseball_savant_expected_stats_{today_str}.csv"
            if today_statcast_file.exists():
                self.statcast_data = pd.read_csv(today_statcast_file)
                print(f"✅ Loaded today's Statcast data: {today_str}")
            else:
                statcast_files = list(PROCESSED_DIR.glob("baseball_savant_expected_stats_*.csv"))
                if statcast_files:
                    latest_statcast = max(statcast_files, key=lambda x: x.stat().st_mtime)
                    self.statcast_data = pd.read_csv(latest_statcast)
                    print(f"⚠️ Using fallback Statcast data from: {latest_statcast.name}")
                else:
                    self.statcast_data = pd.DataFrame()
                    print("❌ No Statcast data found")
                
            return True
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return False
    
    def get_dashboard_data(self):
        """Get all data needed for the dashboard"""
        if not self.load_latest_data():
            return None
            
        result = {
            'current_date': self.current_date or datetime.now().strftime("%B %d, %Y"),
            'games': self.get_games_data(),
            'pitcher_summary': self.get_pitcher_summary(),
            'hitter_summary': self.get_hitter_summary(),
            'top_matchups': self.get_top_matchups()
        }
        
        return result
    
    def get_games_data(self):
        """Get today's games with odds and analysis"""
        # Use database if available
        if self.use_database and self.db:
            try:
                from datetime import date
                games_df = self.db.get_games_by_date(date.today())
                
                if games_df.empty:
                    return []
                
                games = []
                for _, game in games_df.iterrows():
                    game_info = {
                        'game_id': game.get('game_id', 'N/A'),
                        'away_team': game.get('away_team_abbr', 'N/A'),
                        'home_team': game.get('home_team_abbr', 'N/A'),
                        'venue': game.get('venue', 'N/A'),
                        'game_status': game.get('game_status', 'scheduled'),
                        
                        # Betting odds
                        'away_moneyline': game.get('away_moneyline', 'N/A'),
                        'home_moneyline': game.get('home_moneyline', 'N/A'),
                        'total': game.get('total_points', 'N/A'),
                        'over_odds': game.get('over_odds', 'N/A'),
                        'under_odds': game.get('under_odds', 'N/A'),
                        
                        # Placeholder for pitcher data - will be enhanced later
                        'away_pitcher': 'TBD',
                        'home_pitcher': 'TBD',
                        'away_pitcher_score': 'N/A',
                        'home_pitcher_score': 'N/A',
                        'away_pitcher_grade': 'N/A',
                        'home_pitcher_grade': 'N/A',
                        'away_team_wrc_plus': 'N/A',
                        'home_team_wrc_plus': 'N/A',
                        'away_team_ops': 'N/A',
                        'home_team_ops': 'N/A'
                    }
                    
                    # Format game time
                    if game.get('commence_time'):
                        try:
                            dt_str = str(game['commence_time'])
                            if 'T' in dt_str:
                                dt_str = dt_str.replace('Z', '+00:00')
                                dt_utc = datetime.fromisoformat(dt_str)
                                
                                # Convert to Eastern Time
                                eastern = pytz.timezone('US/Eastern')
                                if dt_utc.tzinfo is None:
                                    dt_utc = dt_utc.replace(tzinfo=pytz.UTC)
                                
                                dt_eastern = dt_utc.astimezone(eastern)
                                game_info['game_time'] = dt_eastern.strftime('%I:%M %p ET')
                            else:
                                game_info['game_time'] = dt_str
                        except Exception as e:
                            game_info['game_time'] = 'TBD'
                    else:
                        game_info['game_time'] = 'TBD'
                    
                    games.append(game_info)
                
                return games
                
            except Exception as e:
                print(f"Database query failed, falling back to CSV: {e}")
        
        # Fallback to CSV data
        if self.integrated_data is None:
            return []
            
        games = []
        for _, game in self.integrated_data.iterrows():
            game_info = {
                'away_team': game.get('away_team', 'N/A'),
                'home_team': game.get('home_team', 'N/A'),
                'away_pitcher': game.get('away_pitcher', 'N/A'),
                'home_pitcher': game.get('home_pitcher', 'N/A'),
                'game_time': game.get('game_time', 'TBD'),
                'venue': game.get('venue', 'N/A'),
                
                # Odds
                'away_moneyline': game.get('fd_away_moneyline', 'N/A'),
                'home_moneyline': game.get('fd_home_moneyline', 'N/A'),
                'total': game.get('fd_total_points', 'N/A'),
                'over_odds': game.get('fd_over_odds', 'N/A'),
                'under_odds': game.get('fd_under_odds', 'N/A'),
                'away_spread': game.get('fd_away_spread', 'N/A'),
                'home_spread': game.get('fd_home_spread', 'N/A'),
                
                # Pitcher analysis
                'away_pitcher_score': game.get('away_pitcher_score', 'N/A'),
                'home_pitcher_score': game.get('home_pitcher_score', 'N/A'),
                'away_pitcher_grade': game.get('away_pitcher_grade', 'N/A'),
                'home_pitcher_grade': game.get('home_pitcher_grade', 'N/A'),
                
                # Team hitting
                'away_team_wrc_plus': game.get('away_team_avg_wrc_plus', 'N/A'),
                'home_team_wrc_plus': game.get('home_team_avg_wrc_plus', 'N/A'),
                'away_team_ops': game.get('away_team_avg_ops', 'N/A'),
                'home_team_ops': game.get('home_team_avg_ops', 'N/A')
            }
            
            # Format game time if available
            if game_info['game_time'] != 'TBD' and 'T' in str(game_info['game_time']):
                try:
                    # Parse the ISO datetime string
                    dt_str = str(game_info['game_time']).replace('Z', '+00:00')
                    dt_utc = datetime.fromisoformat(dt_str)
                    
                    # Convert to UTC timezone aware object if not already
                    if dt_utc.tzinfo is None:
                        dt_utc = dt_utc.replace(tzinfo=pytz.UTC)
                    elif dt_utc.tzinfo != pytz.UTC:
                        dt_utc = dt_utc.astimezone(pytz.UTC)
                    
                    # Convert to Eastern Time
                    eastern = pytz.timezone('US/Eastern')
                    dt_eastern = dt_utc.astimezone(eastern)
                    
                    # Format as 12-hour time with ET suffix
                    game_info['formatted_time'] = dt_eastern.strftime('%I:%M %p ET')
                except Exception as e:
                    print(f"Error formatting time {game_info['game_time']}: {e}")
                    game_info['formatted_time'] = 'TBD'
            else:
                game_info['formatted_time'] = 'TBD'
            
            games.append(game_info)
        
        return games
    
    def merge_pitcher_statcast_data(self):
        """Merge pitcher data with Statcast expected stats"""
        if self.pitcher_data is None or self.statcast_data is None or self.statcast_data.empty:
            return self.pitcher_data
        
        # Merge on pitcher name
        merged = self.pitcher_data.merge(
            self.statcast_data,
            left_on='Name',
            right_on='matched_name',
            how='left'
        )
        
        return merged
    
    def get_pitcher_summary(self):
        """Get pitcher summary with your specific metrics"""
        # Use database if available
        if self.use_database and self.db:
            try:
                df = self.db.get_latest_pitcher_stats(limit=15)
                
                if df.empty:
                    return {
                        'total_pitchers': 0,
                        'top_pitchers': [],
                        'whip_distribution': {},
                        'has_statcast': False
                    }
                
                # WHIP tiers for categorization
                whip_tiers = {
                    'Elite': len(df[df['whip'] <= 1.10]),
                    'Good': len(df[(df['whip'] > 1.10) & (df['whip'] <= 1.30)]),
                    'Average': len(df[(df['whip'] > 1.30) & (df['whip'] <= 1.50)]),
                    'Poor': len(df[df['whip'] > 1.50])
                }
                
                # K/BB ratio tiers
                kbb_tiers = {
                    'Elite': len(df[df['k_bb'] >= 3.0]),
                    'Good': len(df[(df['k_bb'] >= 2.0) & (df['k_bb'] < 3.0)]),
                    'Average': len(df[(df['k_bb'] >= 1.5) & (df['k_bb'] < 2.0)]),
                    'Poor': len(df[df['k_bb'] < 1.5])
                }
                
                # Top pitchers with your specific metrics
                top_pitchers = []
                for _, pitcher in df.iterrows():
                    pitcher_info = {
                        'name': pitcher.get('name', 'Unknown'),
                        'team': pitcher.get('team', 'N/A'),
                        
                        # Your required pitcher metrics
                        'whip': round(pitcher.get('whip', 0), 3) if pd.notna(pitcher.get('whip')) else 'N/A',
                        'fip': round(pitcher.get('fip', 0), 2) if pd.notna(pitcher.get('fip')) else 'N/A',
                        'siera': round(pitcher.get('siera', 0), 2) if pd.notna(pitcher.get('siera')) else 'N/A',
                        'xfip': round(pitcher.get('xfip', 0), 2) if pd.notna(pitcher.get('xfip')) else 'N/A',
                        'k_bb': round(pitcher.get('k_bb', 0), 2) if pd.notna(pitcher.get('k_bb')) else 'N/A',
                        
                        # Statcast metrics
                        'xwoba': round(pitcher.get('xwoba', 0), 3) if pd.notna(pitcher.get('xwoba')) else 'N/A',
                        'xba': round(pitcher.get('xba', 0), 3) if pd.notna(pitcher.get('xba')) else 'N/A',
                        'xslg': round(pitcher.get('xslg', 0), 3) if pd.notna(pitcher.get('xslg')) else 'N/A',
                        
                        # Stuff+ (if available)
                        'stuff_plus': int(pitcher.get('stuff_plus', 0)) if pd.notna(pitcher.get('stuff_plus')) else 'N/A',
                        
                        # TODO: CSW%, Run value by pitch type - need to add to database schema
                        'csw_pct': 'N/A',  # Coming soon
                        'run_value': 'N/A'  # Coming soon
                    }
                    
                    top_pitchers.append(pitcher_info)
                
                return {
                    'total_pitchers': len(df),
                    'average_whip': round(df['whip'].mean(), 3) if 'whip' in df.columns else 0,
                    'average_fip': round(df['fip'].mean(), 2) if 'fip' in df.columns else 0,
                    'whip_distribution': whip_tiers,
                    'kbb_distribution': kbb_tiers,
                    'top_pitchers': top_pitchers,
                    'has_statcast': any(pd.notna(df.get('xwoba', [])))
                }
                
            except Exception as e:
                print(f"Database query failed for pitchers, falling back to CSV: {e}")
        
        # Fallback to CSV data with limited metrics
        if self.pitcher_data is None:
            return {'total_pitchers': 0, 'top_pitchers': []}
        
        df = self.merge_pitcher_statcast_data()
        
        # Focus on your specific metrics only
        top_pitchers = []
        for _, pitcher in df.head(15).iterrows():
            pitcher_info = {
                'name': pitcher.get('Name', 'Unknown'),
                'team': pitcher.get('Team', 'N/A'),
                'whip': round(pitcher.get('WHIP', 0), 3) if pd.notna(pitcher.get('WHIP')) else 'N/A',
                'fip': round(pitcher.get('FIP', 0), 2) if pd.notna(pitcher.get('FIP')) else 'N/A',
                'siera': round(pitcher.get('SIERA', 0), 2) if pd.notna(pitcher.get('SIERA')) else 'N/A',
                'xfip': round(pitcher.get('xFIP', 0), 2) if pd.notna(pitcher.get('xFIP')) else 'N/A',
                'k_bb': round(pitcher.get('K/BB', 0), 2) if pd.notna(pitcher.get('K/BB')) else 'N/A',
                'xwoba': round(pitcher.get('xwOBA', 0), 3) if pd.notna(pitcher.get('xwOBA')) else 'N/A',
                'xba': round(pitcher.get('xBA', 0), 3) if pd.notna(pitcher.get('xBA')) else 'N/A',
                'xslg': round(pitcher.get('xSLG', 0), 3) if pd.notna(pitcher.get('xSLG')) else 'N/A',
                'stuff_plus': int(pitcher.get('Stuff+', 0)) if pd.notna(pitcher.get('Stuff+')) else 'N/A',
                'csw_pct': 'N/A',  # Need to add to data collection
                'run_value': 'N/A'  # Need to add to data collection
            }
            top_pitchers.append(pitcher_info)
        
        return {
            'total_pitchers': len(df),
            'top_pitchers': top_pitchers,
            'whip_distribution': {},
            'has_statcast': not self.statcast_data.empty if self.statcast_data is not None else False
        }
    
    def get_hitter_summary(self):
        """Get hitter summary with your specific metrics"""
        # Use database if available
        if self.use_database and self.db:
            try:
                df = self.db.get_latest_hitter_stats(limit=15)
                
                if df.empty:
                    return {
                        'total_hitters': 0,
                        'top_hitters': [],
                        'ops_distribution': {},
                        'wrc_distribution': {}
                    }
                
                # OPS tiers for categorization
                ops_tiers = {
                    'Elite': len(df[df['ops'] >= 0.900]),
                    'Good': len(df[(df['ops'] >= 0.800) & (df['ops'] < 0.900)]),
                    'Average': len(df[(df['ops'] >= 0.700) & (df['ops'] < 0.800)]),
                    'Poor': len(df[df['ops'] < 0.700])
                }
                
                # wRC+ tiers
                wrc_tiers = {
                    'Elite': len(df[df['wrc_plus'] >= 140]),
                    'Above Average': len(df[(df['wrc_plus'] >= 110) & (df['wrc_plus'] < 140)]),
                    'Average': len(df[(df['wrc_plus'] >= 90) & (df['wrc_plus'] < 110)]),
                    'Below Average': len(df[df['wrc_plus'] < 90])
                }
                
                # Top hitters with your specific metrics
                top_hitters = []
                for _, hitter in df.iterrows():
                    # Calculate K:BB ratio
                    k_pct = hitter.get('k_pct', 0) if pd.notna(hitter.get('k_pct')) else 0
                    bb_pct = hitter.get('bb_pct', 0) if pd.notna(hitter.get('bb_pct')) else 0
                    k_bb_ratio = k_pct / bb_pct if bb_pct > 0 else 'N/A'
                    
                    hitter_info = {
                        'name': hitter.get('name', 'Unknown'),
                        'team': hitter.get('team', 'N/A'),
                        
                        # Your required hitter metrics
                        'ops': round(hitter.get('ops', 0), 3) if pd.notna(hitter.get('ops')) else 'N/A',
                        'woba': round(hitter.get('woba', 0), 3) if pd.notna(hitter.get('woba')) else 'N/A',
                        'wrc_plus': round(hitter.get('wrc_plus', 0), 0) if pd.notna(hitter.get('wrc_plus')) else 'N/A',
                        
                        # Statcast metrics (xwOBA, xBA, xSLG, Hard Hit %, Barrel %)
                        'xwoba': round(hitter.get('xwoba', 0), 3) if pd.notna(hitter.get('xwoba')) else 'N/A',
                        'xba': round(hitter.get('xba', 0), 3) if pd.notna(hitter.get('xba')) else 'N/A',
                        'xslg': round(hitter.get('xslg', 0), 3) if pd.notna(hitter.get('xslg')) else 'N/A',
                        'hard_hit_pct': round(hitter.get('hard_hit_pct', 0) * 100, 1) if pd.notna(hitter.get('hard_hit_pct')) else 'N/A',
                        'barrel_pct': round(hitter.get('barrel_pct', 0) * 100, 1) if pd.notna(hitter.get('barrel_pct')) else 'N/A',
                        
                        # K:BB ratio
                        'k_bb_ratio': round(k_bb_ratio, 2) if k_bb_ratio != 'N/A' else 'N/A'
                    }
                    
                    top_hitters.append(hitter_info)
                
                return {
                    'total_hitters': len(df),
                    'average_ops': round(df['ops'].mean(), 3) if 'ops' in df.columns else 0,
                    'average_wrc_plus': round(df['wrc_plus'].mean(), 0) if 'wrc_plus' in df.columns else 0,
                    'average_woba': round(df['woba'].mean(), 3) if 'woba' in df.columns else 0,
                    'ops_distribution': ops_tiers,
                    'wrc_distribution': wrc_tiers,
                    'top_hitters': top_hitters,
                    'has_statcast': any(pd.notna(df.get('xwoba', [])))
                }
                
            except Exception as e:
                print(f"Database query failed for hitters, falling back to CSV: {e}")
        
        # Fallback to CSV data with your specific metrics only
        if self.hitter_data is None:
            return {'total_hitters': 0, 'top_hitters': []}
            
        df = self.hitter_data
        df_sorted = df.sort_values('wRC+', ascending=False, na_position='last')
        
        # Focus on your specific metrics only
        top_hitters = []
        for _, hitter in df_sorted.head(15).iterrows():
            # Calculate K:BB ratio
            k_pct = hitter.get('K%', 0) if pd.notna(hitter.get('K%')) else 0
            bb_pct = hitter.get('BB%', 0) if pd.notna(hitter.get('BB%')) else 0
            k_bb_ratio = k_pct / bb_pct if bb_pct > 0 else 'N/A'
            
            hitter_info = {
                'name': hitter.get('Name', 'Unknown'),
                'team': hitter.get('Team', 'N/A'),
                'ops': round(hitter.get('OPS', 0), 3) if pd.notna(hitter.get('OPS')) else 'N/A',
                'woba': round(hitter.get('wOBA', 0), 3) if pd.notna(hitter.get('wOBA')) else 'N/A',
                'wrc_plus': round(hitter.get('wRC+', 0), 0) if pd.notna(hitter.get('wRC+')) else 'N/A',
                'xwoba': 'N/A',  # Need Statcast integration
                'xba': 'N/A',    # Need Statcast integration
                'xslg': 'N/A',   # Need Statcast integration
                'hard_hit_pct': 'N/A',  # Need to add to data collection
                'barrel_pct': 'N/A',    # Need to add to data collection
                'k_bb_ratio': round(k_bb_ratio, 2) if k_bb_ratio != 'N/A' else 'N/A'
            }
            top_hitters.append(hitter_info)
        
        return {
            'total_hitters': len(df),
            'top_hitters': top_hitters,
            'ops_distribution': {},
            'wrc_distribution': {}
        }
    
    def get_top_matchups(self):
        """Identify top betting opportunities based on pitcher analysis"""
        if self.integrated_data is None:
            return []
            
        matchups = []
        for _, game in self.integrated_data.iterrows():
            away_score = pd.to_numeric(game.get('away_pitcher_score', 0), errors='coerce') or 0
            home_score = pd.to_numeric(game.get('home_pitcher_score', 0), errors='coerce') or 0
            
            if away_score > 0 or home_score > 0:  # Only include games with pitcher data
                score_diff = abs(away_score - home_score)
                
                matchup = {
                    'away_team': game.get('away_team', 'N/A'),
                    'home_team': game.get('home_team', 'N/A'),
                    'away_pitcher': game.get('away_pitcher', 'N/A'),
                    'home_pitcher': game.get('home_pitcher', 'N/A'),
                    'away_score': away_score,
                    'home_score': home_score,
                    'score_difference': score_diff,
                    'away_ml': game.get('fd_away_moneyline', 'N/A'),
                    'home_ml': game.get('fd_home_moneyline', 'N/A'),
                    'advantage': 'away' if away_score > home_score else 'home',
                    'confidence': 'High' if score_diff > 15 else 'Medium' if score_diff > 8 else 'Low'
                }
                matchups.append(matchup)
        
        # Sort by score difference (biggest advantages first)
        matchups.sort(key=lambda x: x['score_difference'], reverse=True)
        
        return matchups[:8]  # Top 8 matchups
    
    def get_lineup_data(self, date_str=None, force_refresh=False):
        """Get lineup data for today's games with integrated player stats"""
        if date_str is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
        
        try:
            # Force refresh or load from file
            json_file = RAW_DIR / f"mlb_lineups_{date_str}.json"
            
            if force_refresh or not json_file.exists():
                # Collect fresh lineup data
                print(f"🔄 Collecting fresh lineup data for {date_str}")
                self.lineup_data = self.lineup_collector.collect_daily_lineups(date_str)
            else:
                # Load from file
                with open(json_file, 'r') as f:
                    self.lineup_data = json.load(f)
            
            # Enhance lineup data with database player stats
            if self.lineup_data:
                self.lineup_data = self.enhance_lineups_with_stats(self.lineup_data)
            
            return self.lineup_data
        except Exception as e:
            print(f"Error loading lineup data: {e}")
            return []
    
    def enhance_lineups_with_stats(self, lineup_data):
        """Enhance lineup data with player statistics from database"""
        try:
            if not self.use_database or not self.db:
                return lineup_data
            
            # Get all hitter stats from database
            hitters_df = self.db.get_latest_hitter_stats()
            
            if hitters_df.empty:
                return lineup_data
            
            # Create lookup dictionary by player name (normalized)
            hitter_lookup = {}
            for _, hitter in hitters_df.iterrows():
                name = hitter.get('name', '').strip()
                normalized_name = self.normalize_player_name(name)
                hitter_lookup[normalized_name] = {
                    'ops': round(hitter.get('ops', 0), 3) if pd.notna(hitter.get('ops')) else 'N/A',
                    'wrc_plus': int(hitter.get('wrc_plus', 0)) if pd.notna(hitter.get('wrc_plus')) else 'N/A',
                    'woba': round(hitter.get('woba', 0), 3) if pd.notna(hitter.get('woba')) else 'N/A',
                    'avg': round(hitter.get('avg', 0), 3) if pd.notna(hitter.get('avg')) else 'N/A',
                    'hr': int(hitter.get('home_runs', 0)) if pd.notna(hitter.get('home_runs')) else 'N/A',
                    'rbi': int(hitter.get('rbis', 0)) if pd.notna(hitter.get('rbis')) else 'N/A',
                    'k_pct': round(hitter.get('k_pct', 0) * 100, 1) if pd.notna(hitter.get('k_pct')) else 'N/A',
                    'bb_pct': round(hitter.get('bb_pct', 0) * 100, 1) if pd.notna(hitter.get('bb_pct')) else 'N/A',
                    # Expected stats
                    'xba': round(hitter.get('xba', 0), 3) if pd.notna(hitter.get('xba')) else 'N/A',
                    'xslg': round(hitter.get('xslg', 0), 3) if pd.notna(hitter.get('xslg')) else 'N/A',
                    'xwoba': round(hitter.get('xwoba', 0), 3) if pd.notna(hitter.get('xwoba')) else 'N/A'
                }
            
            # Enhance each game's lineup
            enhanced_lineups = []
            for game in lineup_data:
                if not isinstance(game, dict):
                    continue
                    
                enhanced_game = game.copy()
                
                # Add team logos
                enhanced_game['away_team_logo'] = self.get_team_logo_url(game.get('away_team', ''))
                enhanced_game['home_team_logo'] = self.get_team_logo_url(game.get('home_team', ''))
                
                # Convert game time from UTC to Eastern time
                if 'game_time' in game and game['game_time'] and game['game_time'] != 'TBD':
                    try:
                        # Parse UTC time
                        utc_time = datetime.fromisoformat(game['game_time'].replace('Z', '+00:00'))
                        # Convert to Eastern time
                        eastern = pytz.timezone('US/Eastern')
                        eastern_time = utc_time.astimezone(eastern)
                        # Format as readable string
                        enhanced_game['game_time'] = eastern_time.strftime('%I:%M %p ET')
                    except Exception as e:
                        print(f"⚠️ Error converting game time: {e}")
                        enhanced_game['game_time'] = game.get('game_time', 'TBD')
                
                # Enhance away lineup
                if 'away_lineup' in game:
                    enhanced_game['away_lineup'] = self.enhance_team_lineup(game['away_lineup'], hitter_lookup)
                
                # Enhance home lineup  
                if 'home_lineup' in game:
                    enhanced_game['home_lineup'] = self.enhance_team_lineup(game['home_lineup'], hitter_lookup)
                
                enhanced_lineups.append(enhanced_game)
            
            print(f"✅ Enhanced {len(enhanced_lineups)} game lineups with database stats")
            return enhanced_lineups
            
        except Exception as e:
            print(f"Error enhancing lineups with stats: {e}")
            return lineup_data
    
    def enhance_team_lineup(self, team_lineup, hitter_lookup):
        """Enhance a single team's lineup with player stats"""
        enhanced_lineup = []
        
        for player in team_lineup:
            if not isinstance(player, dict):
                continue
                
            enhanced_player = player.copy()
            player_name = player.get('name', '').strip()
            normalized_name = self.normalize_player_name(player_name)
            
            # Try to find player stats
            if normalized_name in hitter_lookup:
                stats = hitter_lookup[normalized_name]
                enhanced_player.update(stats)
                enhanced_player['data_source'] = 'Database'
            # If not found, try to fetch individual player data
            else:
                print(f"🔍 Player not found in database: {player_name}, attempting individual fetch...")
                individual_data = fetch_individual_player_data(player_name)
                
                if individual_data:
                    # Convert individual data to our enhanced format
                    enhanced_stats = {
                        'ops': round(individual_data.get('OPS', 0), 3) if individual_data.get('OPS') else None,
                        'wrc_plus': int(individual_data.get('wRC+', 0)) if individual_data.get('wRC+') else None,
                        'woba': round(individual_data.get('wOBA', 0), 3) if individual_data.get('wOBA') else None,
                        'avg': round(individual_data.get('AVG', 0), 3) if individual_data.get('AVG') else None,
                        'hr': int(individual_data.get('HR', 0)) if individual_data.get('HR') else 0,
                        'rbi': int(individual_data.get('RBI', 0)) if individual_data.get('RBI') else 0,
                        'k_pct': round(individual_data.get('K%', 0) * 100, 1) if individual_data.get('K%') else None,
                        'bb_pct': round(individual_data.get('BB%', 0) * 100, 1) if individual_data.get('BB%') else None,
                        'xba': round(individual_data.get('xBA', 0), 3) if individual_data.get('xBA') else None,
                        'xslg': round(individual_data.get('xSLG', 0), 3) if individual_data.get('xSLG') else None,
                        'xwoba': round(individual_data.get('xwOBA', 0), 3) if individual_data.get('xwOBA') else None,
                        'data_source': 'Individual Fetch'
                    }
                    enhanced_player.update(enhanced_stats)
                    print(f"✅ Successfully enhanced {player_name} with individual data")
                    
                    # Optionally add to our main hitter dataset for future use
                    if dashboard.hitter_data is not None:
                        new_row = pd.DataFrame([individual_data])
                        dashboard.hitter_data = pd.concat([dashboard.hitter_data, new_row], ignore_index=True)
                        dashboard.hitter_data = dashboard.hitter_data.drop_duplicates(subset=['Name'], keep='last')
                        print(f"📊 Added {player_name} to hitter dataset")
                else:
                    # Fallback to limited data
                    enhanced_player.update({
                        'ops': player.get('ops', 'Limited'),
                        'wrc_plus': 'Limited', 
                        'woba': 'Limited',
                        'k_pct': 'Limited',
                        'bb_pct': 'Limited',
                        'xba': 'Limited',
                        'xslg': 'Limited', 
                        'xwoba': 'Limited',
                        'data_source': 'MLB API'
                    })
                    
                    # If MLB API provided batting average, show it
                    if player.get('avg') and player.get('avg') != 'N/A':
                        enhanced_player['avg'] = player.get('avg')
                    else:
                        enhanced_player['avg'] = 'Limited'
            
            enhanced_lineup.append(enhanced_player)
        
        return enhanced_lineup
    
    def normalize_player_name(self, name):
        """Normalize player name for matching"""
        if not name:
            return ""
        
        # Remove accents and special characters, convert to lowercase
        import unicodedata
        normalized = unicodedata.normalize('NFD', name)
        normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
        
        # Handle common name variations
        normalized = normalized.lower().strip()
        normalized = normalized.replace('.', '')
        normalized = normalized.replace(' jr.', '')
        normalized = normalized.replace(' jr', '')
        normalized = normalized.replace(' sr.', '')
        normalized = normalized.replace(' sr', '')
        normalized = normalized.replace(' iii', '')
        normalized = normalized.replace(' ii', '')
        
        return normalized
    
    def get_team_logo_url(self, team_abbr):
        """Get team logo URL from team abbreviation"""
        team_logos = {
            'ARI': 'https://www.mlbstatic.com/team-logos/109.svg',
            'ATL': 'https://www.mlbstatic.com/team-logos/144.svg', 
            'BAL': 'https://www.mlbstatic.com/team-logos/110.svg',
            'BOS': 'https://www.mlbstatic.com/team-logos/111.svg',
            'CHC': 'https://www.mlbstatic.com/team-logos/112.svg',
            'CWS': 'https://www.mlbstatic.com/team-logos/145.svg',
            'CIN': 'https://www.mlbstatic.com/team-logos/113.svg',
            'CLE': 'https://www.mlbstatic.com/team-logos/114.svg',
            'COL': 'https://www.mlbstatic.com/team-logos/115.svg',
            'DET': 'https://www.mlbstatic.com/team-logos/116.svg',
            'HOU': 'https://www.mlbstatic.com/team-logos/117.svg',
            'KC': 'https://www.mlbstatic.com/team-logos/118.svg',
            'LAA': 'https://www.mlbstatic.com/team-logos/108.svg',
            'LAD': 'https://www.mlbstatic.com/team-logos/119.svg',
            'MIA': 'https://www.mlbstatic.com/team-logos/146.svg',
            'MIL': 'https://www.mlbstatic.com/team-logos/158.svg',
            'MIN': 'https://www.mlbstatic.com/team-logos/142.svg',
            'NYM': 'https://www.mlbstatic.com/team-logos/121.svg',
            'NYY': 'https://www.mlbstatic.com/team-logos/147.svg',
            'OAK': 'https://www.mlbstatic.com/team-logos/133.svg',
            'PHI': 'https://www.mlbstatic.com/team-logos/143.svg',
            'PIT': 'https://www.mlbstatic.com/team-logos/134.svg',
            'SD': 'https://www.mlbstatic.com/team-logos/135.svg',
            'SEA': 'https://www.mlbstatic.com/team-logos/136.svg',
            'SF': 'https://www.mlbstatic.com/team-logos/137.svg',
            'STL': 'https://www.mlbstatic.com/team-logos/138.svg',
            'TB': 'https://www.mlbstatic.com/team-logos/139.svg',
            'TEX': 'https://www.mlbstatic.com/team-logos/140.svg',
            'TOR': 'https://www.mlbstatic.com/team-logos/141.svg',
            'WAS': 'https://www.mlbstatic.com/team-logos/120.svg'
        }
        
        return team_logos.get(team_abbr, 'https://www.mlbstatic.com/team-logos/1.svg')
    
    def fetch_current_odds(self):
        """Fetch current betting odds from The Odds API"""
        try:
            import requests
            
            api_key = "a74383d66d314cc2fc96f1e54931d6a4"
            url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
            params = {
                'apiKey': api_key,
                'regions': 'us',
                'markets': 'h2h,spreads,totals',
                'bookmakers': 'fanduel,draftkings',
                'oddsFormat': 'american'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            odds_data = response.json()
            
            print(f"📊 Fetched odds for {len(odds_data)} games")
            return odds_data
            
        except Exception as e:
            print(f"⚠️ Could not fetch betting odds: {e}")
            return []
    
    def match_odds_to_game(self, game_teams, odds_data):
        """Match betting odds to a specific game"""
        if not odds_data:
            return {}
            
        away_team = game_teams['away']
        home_team = game_teams['home']
        
        for odds_game in odds_data:
            # Try to match teams
            if self.teams_match(away_team, home_team, odds_game):
                return self.extract_odds_from_game(odds_game)
        
        return {}
    
    def teams_match(self, away_team, home_team, odds_game):
        """Check if MLB game teams match odds API teams"""
        odds_teams = [odds_game.get('away_team', ''), odds_game.get('home_team', '')]
        
        # Simple contains check for team matching
        away_match = any(away_team.lower() in team.lower() or team.lower() in away_team.lower() for team in odds_teams)
        home_match = any(home_team.lower() in team.lower() or team.lower() in home_team.lower() for team in odds_teams)
        
        return away_match and home_match
    
    def extract_odds_from_game(self, odds_game):
        """Extract FanDuel odds from odds game data"""
        odds_info = {
            'fd_away_moneyline': 'N/A',
            'fd_home_moneyline': 'N/A',
            'fd_total_points': 'N/A',
            'fd_over_odds': 'N/A',
            'fd_under_odds': 'N/A',
            'fd_away_spread': 'N/A',
            'fd_home_spread': 'N/A'
        }
        
        try:
            bookmakers = odds_game.get('bookmakers', [])
            for bookmaker in bookmakers:
                if bookmaker.get('key') == 'fanduel':
                    markets = bookmaker.get('markets', [])
                    
                    for market in markets:
                        if market.get('key') == 'h2h':  # Moneyline
                            outcomes = market.get('outcomes', [])
                            for outcome in outcomes:
                                if outcome.get('name') == odds_game.get('away_team'):
                                    odds_info['fd_away_moneyline'] = outcome.get('price')
                                elif outcome.get('name') == odds_game.get('home_team'):
                                    odds_info['fd_home_moneyline'] = outcome.get('price')
                        
                        elif market.get('key') == 'totals':  # Over/Under
                            outcomes = market.get('outcomes', [])
                            if outcomes:
                                odds_info['fd_total_points'] = outcomes[0].get('point', 'N/A')
                                for outcome in outcomes:
                                    if outcome.get('name') == 'Over':
                                        odds_info['fd_over_odds'] = outcome.get('price')
                                    elif outcome.get('name') == 'Under':
                                        odds_info['fd_under_odds'] = outcome.get('price')
                        
                        elif market.get('key') == 'spreads':  # Point spread
                            outcomes = market.get('outcomes', [])
                            for outcome in outcomes:
                                if outcome.get('name') == odds_game.get('away_team'):
                                    odds_info['fd_away_spread'] = outcome.get('point')
                                elif outcome.get('name') == odds_game.get('home_team'):
                                    odds_info['fd_home_spread'] = outcome.get('point')
                    break
        except Exception as e:
            print(f"⚠️ Error extracting odds: {e}")
        
        return odds_info
    
    def fetch_fresh_games(self, date_str):
        """Fetch fresh games from MLB API for the given date"""
        try:
            import requests
            
            # Use MLB API to get today's games
            url = "https://statsapi.mlb.com/api/v1/schedule"
            params = {
                'sportId': 1,  # MLB
                'date': date_str,
                'hydrate': 'team,venue,probablePitcher'
            }
            
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'dates' not in data or len(data['dates']) == 0:
                print(f"❌ No games found for {date_str}")
                return None
                
            # Try to get betting odds
            odds_data = self.fetch_current_odds()
            
            games_data = []
            for game in data['dates'][0]['games']:
                # Get basic game info
                away_team_full = game.get('teams', {}).get('away', {}).get('team', {}).get('name', '')
                home_team_full = game.get('teams', {}).get('home', {}).get('team', {}).get('name', '')
                
                game_info = {
                    'away_team': self.lineup_collector.team_mapping.get(away_team_full, 'Unknown'),
                    'home_team': self.lineup_collector.team_mapping.get(home_team_full, 'Unknown'),
                    'game_time': game.get('gameDate', ''),
                    'venue': game.get('venue', {}).get('name', ''),
                    'away_pitcher': '',
                    'home_pitcher': '',
                    'game_status': game.get('status', {}).get('detailedState', ''),
                }
                
                # Try to match and add betting odds
                game_teams = {'away': away_team_full, 'home': home_team_full}
                odds_info = self.match_odds_to_game(game_teams, odds_data)
                
                # Add odds data (real or placeholder)
                game_info.update({
                    'fd_away_moneyline': odds_info.get('fd_away_moneyline', 'N/A'),
                    'fd_home_moneyline': odds_info.get('fd_home_moneyline', 'N/A'),
                    'fd_total_points': odds_info.get('fd_total_points', 'N/A'),
                    'fd_over_odds': odds_info.get('fd_over_odds', 'N/A'),
                    'fd_under_odds': odds_info.get('fd_under_odds', 'N/A'),
                    'fd_away_spread': odds_info.get('fd_away_spread', 'N/A'),
                    'fd_home_spread': odds_info.get('fd_home_spread', 'N/A'),
                    
                    # Add placeholder analysis data
                    'away_pitcher_score': 'N/A',
                    'home_pitcher_score': 'N/A',
                    'away_pitcher_grade': 'N/A',
                    'home_pitcher_grade': 'N/A',
                    'away_team_avg_wrc_plus': 'N/A',
                    'home_team_avg_wrc_plus': 'N/A',
                    'away_team_avg_ops': 'N/A',
                    'home_team_avg_ops': 'N/A'
                })
                
                # Get probable pitchers if available
                teams = game.get('teams', {})
                if 'away' in teams and 'probablePitcher' in teams['away']:
                    game_info['away_pitcher'] = teams['away']['probablePitcher'].get('fullName', '')
                if 'home' in teams and 'probablePitcher' in teams['home']:
                    game_info['home_pitcher'] = teams['home']['probablePitcher'].get('fullName', '')
                
                games_data.append(game_info)
            
            # Convert to DataFrame to match expected format
            df = pd.DataFrame(games_data)
            print(f"🎯 Successfully fetched {len(df)} fresh games for {date_str}")
            return df
            
        except Exception as e:
            print(f"❌ Error fetching fresh games: {e}")
            return None

# Initialize dashboard
dashboard = EnhancedMLBDashboard()

@app.route('/')
def index():
    """Main dashboard page"""
    data = dashboard.get_dashboard_data()
    return render_template('enhanced_dashboard.html', data=data)

@app.route('/api/games')
def api_games():
    """API endpoint for games data"""
    dashboard.load_latest_data()
    games = dashboard.get_games_data()
    return jsonify(games)

@app.route('/api/pitchers')
def api_pitchers():
    """API endpoint for comprehensive pitcher data with deduplication and processing"""
    if dashboard.pitcher_data is None:
        dashboard.load_latest_data()
    
    if dashboard.pitcher_data is not None:
        # Create a copy and ensure no duplicates by Name
        df = dashboard.pitcher_data.copy()
        df = df.drop_duplicates(subset=['Name'], keep='first')
        
        # Filter for qualified pitchers (minimum 30 IP) - same as top10
        df = df[df['IP'] >= 30] if 'IP' in df.columns else df
        
        # Sort by FIP ascending (lower is better)
        df = df.sort_values('FIP', ascending=True, na_position='last')
        
        # Convert to records and clean up data
        pitchers = []
        for _, pitcher in df.iterrows():
            # Calculate composite score based on key metrics (lower FIP, WHIP, SIERA is better)
            fip = pitcher.get('FIP', 5.00) if pd.notna(pitcher.get('FIP')) else 5.00
            whip = pitcher.get('WHIP', 1.50) if pd.notna(pitcher.get('WHIP')) else 1.50
            siera = pitcher.get('SIERA', 5.00) if pd.notna(pitcher.get('SIERA')) else 5.00
            csw_pct = pitcher.get('CSW%', 20.0) if pd.notna(pitcher.get('CSW%')) else 20.0
            
            # Inverse scoring for pitchers (lower is better for most stats)
            fip_score = max(0, 100 - (fip - 2.0) * 20)  # 2.0 FIP = 100 points
            whip_score = max(0, 100 - (whip - 0.8) * 100)  # 0.8 WHIP = 100 points
            siera_score = max(0, 100 - (siera - 2.5) * 20)  # 2.5 SIERA = 100 points
            csw_score = min(100, (csw_pct - 15) * 2.5)  # 55% CSW = 100 points
            
            composite_score = (fip_score + whip_score + siera_score + csw_score) / 4
            
            # Assign grade based on composite score
            if composite_score >= 85:
                grade = 'A+'
            elif composite_score >= 80:
                grade = 'A'
            elif composite_score >= 75:
                grade = 'A-'
            elif composite_score >= 70:
                grade = 'B+'
            elif composite_score >= 65:
                grade = 'B'
            elif composite_score >= 60:
                grade = 'B-'
            elif composite_score >= 55:
                grade = 'C+'
            elif composite_score >= 50:
                grade = 'C'
            elif composite_score >= 45:
                grade = 'C-'
            elif composite_score >= 40:
                grade = 'D+'
            elif composite_score >= 35:
                grade = 'D'
            else:
                grade = 'F'
            
            pitcher_data = {
                'pitcher_name': pitcher.get('Name', 'Unknown'),
                'team': pitcher.get('Team', 'N/A'),
                'games': int(pitcher.get('G', 0)) if pd.notna(pitcher.get('G')) else 0,
                'games_started': int(pitcher.get('GS', 0)) if pd.notna(pitcher.get('GS')) else 0,
                'innings_pitched': round(pitcher.get('IP', 0), 1) if pd.notna(pitcher.get('IP')) else 0,
                'composite_score': round(composite_score, 1),
                'grade': grade,
                
                # Core pitching stats
                'era': round(pitcher.get('ERA', 0), 2) if pd.notna(pitcher.get('ERA')) else None,
                'fip': round(fip, 2) if fip > 0 else None,
                'xfip': round(pitcher.get('xFIP', 0), 2) if pd.notna(pitcher.get('xFIP')) else None,
                'siera': round(siera, 2) if siera > 0 else None,
                'whip': round(whip, 3) if whip > 0 else None,
                
                # Advanced metrics
                'k_9': round(pitcher.get('K/9', 0), 1) if pd.notna(pitcher.get('K/9')) else None,
                'bb_9': round(pitcher.get('BB/9', 0), 1) if pd.notna(pitcher.get('BB/9')) else None,
                'k_bb': round(pitcher.get('K/BB', 0), 2) if pd.notna(pitcher.get('K/BB')) else None,
                'h_9': round(pitcher.get('H/9', 0), 1) if pd.notna(pitcher.get('H/9')) else None,
                'hr_9': round(pitcher.get('HR/9', 0), 2) if pd.notna(pitcher.get('HR/9')) else None,
                
                # Plate discipline
                'k_pct': round(pitcher.get('K%', 0), 3) if pd.notna(pitcher.get('K%')) else None,
                'bb_pct': round(pitcher.get('BB%', 0), 3) if pd.notna(pitcher.get('BB%')) else None,
                'csw_pct': round(csw_pct, 1) if csw_pct > 0 else None,
                
                # Expected stats (if available)
                'xera': round(pitcher.get('xERA', 0), 2) if pd.notna(pitcher.get('xERA')) else None,
                'xwoba_against': round(pitcher.get('xwOBA', 0), 3) if pd.notna(pitcher.get('xwOBA')) else None,
                'xba_against': round(pitcher.get('xBA', 0), 3) if pd.notna(pitcher.get('xBA')) else None,
                'xslg_against': round(pitcher.get('xSLG', 0), 3) if pd.notna(pitcher.get('xSLG')) else None,
                
                # Batted ball stats
                'hard_hit_pct_against': round(pitcher.get('HardHit%', 0), 3) if pd.notna(pitcher.get('HardHit%')) else None,
                'barrel_pct_against': round(pitcher.get('Barrel%', 0), 3) if pd.notna(pitcher.get('Barrel%')) else None,
                'gb_pct': round(pitcher.get('GB%', 0), 3) if pd.notna(pitcher.get('GB%')) else None,
                'fb_pct': round(pitcher.get('FB%', 0), 3) if pd.notna(pitcher.get('FB%')) else None,
                'ld_pct': round(pitcher.get('LD%', 0), 3) if pd.notna(pitcher.get('LD%')) else None,
                
                # Counting stats
                'wins': int(pitcher.get('W', 0)) if pd.notna(pitcher.get('W')) else 0,
                'losses': int(pitcher.get('L', 0)) if pd.notna(pitcher.get('L')) else 0,
                'saves': int(pitcher.get('SV', 0)) if pd.notna(pitcher.get('SV')) else 0,
                'strikeouts': int(pitcher.get('SO', 0)) if pd.notna(pitcher.get('SO')) else 0,
                'walks': int(pitcher.get('BB', 0)) if pd.notna(pitcher.get('BB')) else 0,
                
                # Stuff+ (if available)
                'stuff_plus': int(pitcher.get('Stuff+', 100)) if pd.notna(pitcher.get('Stuff+')) else None,
            }
            
            pitchers.append(pitcher_data)
        
        return jsonify(pitchers)
    
    return jsonify([])

@app.route('/api/hitters')
def api_hitters():
    """API endpoint for comprehensive hitter data with deduplication and processing"""
    if dashboard.hitter_data is None:
        dashboard.load_latest_data()
    
    if dashboard.hitter_data is not None:
        # Create a copy and ensure no duplicates by Name
        df = dashboard.hitter_data.copy()
        df = df.drop_duplicates(subset=['Name'], keep='first')
        
        # Filter for qualified hitters (minimum 150 PA) - same as top10
        df = df[df['PA'] >= 150] if 'PA' in df.columns else df
        
        # Sort by wRC+ descending
        df = df.sort_values('wRC+', ascending=False, na_position='last')
        
        # Convert to records and clean up data
        hitters = []
        for _, hitter in df.iterrows():
            # Calculate K:BB ratio
            k_pct = hitter.get('K%', 0) if pd.notna(hitter.get('K%')) else 0
            bb_pct = hitter.get('BB%', 0) if pd.notna(hitter.get('BB%')) else 0
            k_bb_ratio = k_pct / bb_pct if bb_pct > 0 else None
            
            # Calculate composite score based on key metrics
            ops = hitter.get('OPS', 0) if pd.notna(hitter.get('OPS')) else 0
            wrc_plus = hitter.get('wRC+', 0) if pd.notna(hitter.get('wRC+')) else 0
            woba = hitter.get('wOBA', 0) if pd.notna(hitter.get('wOBA')) else 0
            
            # Simple composite score (normalize and weight)
            ops_norm = min(ops / 1.000, 1.5) * 33.33  # Max 50 points
            wrc_norm = min(wrc_plus / 200, 1.5) * 33.33  # Max 50 points  
            woba_norm = min(woba / 0.400, 1.5) * 33.33  # Max 50 points
            composite_score = ops_norm + wrc_norm + woba_norm
            
            # Assign grade based on composite score
            if composite_score >= 85:
                grade = 'A+'
            elif composite_score >= 80:
                grade = 'A'
            elif composite_score >= 75:
                grade = 'A-'
            elif composite_score >= 70:
                grade = 'B+'
            elif composite_score >= 65:
                grade = 'B'
            elif composite_score >= 60:
                grade = 'B-'
            elif composite_score >= 55:
                grade = 'C+'
            elif composite_score >= 50:
                grade = 'C'
            elif composite_score >= 45:
                grade = 'C-'
            elif composite_score >= 40:
                grade = 'D+'
            elif composite_score >= 35:
                grade = 'D'
            else:
                grade = 'F'
            
            hitter_data = {
                'hitter_name': hitter.get('Name', 'Unknown'),
                'team': hitter.get('Team', 'N/A'),
                'games': int(hitter.get('G', 0)) if pd.notna(hitter.get('G')) else 0,
                'plate_appearances': int(hitter.get('PA', 0)) if pd.notna(hitter.get('PA')) else 0,
                'composite_score': round(composite_score, 1),
                'grade': grade,
                
                # Core hitting stats
                'avg': round(hitter.get('AVG', 0), 3) if pd.notna(hitter.get('AVG')) else None,
                'obp': round(hitter.get('OBP', 0), 3) if pd.notna(hitter.get('OBP')) else None,
                'slg': round(hitter.get('SLG', 0), 3) if pd.notna(hitter.get('SLG')) else None,
                'ops': round(ops, 3) if ops > 0 else None,
                'iso': round(hitter.get('ISO', 0), 3) if pd.notna(hitter.get('ISO')) else None,
                
                # Advanced metrics
                'woba': round(woba, 3) if woba > 0 else None,
                'wrc_plus': int(wrc_plus) if wrc_plus > 0 else None,
                'babip': round(hitter.get('BABIP', 0), 3) if pd.notna(hitter.get('BABIP')) else None,
                
                # Plate discipline
                'bb_pct': round(bb_pct, 1) if bb_pct > 0 else None,
                'k_pct': round(k_pct, 1) if k_pct > 0 else None,
                'k_bb_ratio': round(k_bb_ratio, 2) if k_bb_ratio else None,
                
                # Expected stats (Statcast)
                'xba': round(hitter.get('xBA', 0), 3) if pd.notna(hitter.get('xBA')) else None,
                'xslg': round(hitter.get('xSLG', 0), 3) if pd.notna(hitter.get('xSLG')) else None,
                'xwoba': round(hitter.get('xwOBA', 0), 3) if pd.notna(hitter.get('xwOBA')) else None,
                
                # Batted ball stats
                'hard_hit_pct': round(hitter.get('HardHit%', 0), 1) if pd.notna(hitter.get('HardHit%')) else None,
                'barrel_pct': round(hitter.get('Barrel%', 0), 1) if pd.notna(hitter.get('Barrel%')) else None,
                'ev': round(hitter.get('EV', 0), 1) if pd.notna(hitter.get('EV')) else None,
                'la': round(hitter.get('LA', 0), 1) if pd.notna(hitter.get('LA')) else None,
                'max_ev': round(hitter.get('maxEV', 0), 1) if pd.notna(hitter.get('maxEV')) else None,
                
                # Counting stats
                'home_runs': int(hitter.get('HR', 0)) if pd.notna(hitter.get('HR')) else 0,
                'runs': int(hitter.get('R', 0)) if pd.notna(hitter.get('R')) else 0,
                'rbi': int(hitter.get('RBI', 0)) if pd.notna(hitter.get('RBI')) else 0,
                'stolen_bases': int(hitter.get('SB', 0)) if pd.notna(hitter.get('SB')) else 0,
                
                # Pull/Spray chart
                'pull_pct': round(hitter.get('Pull%', 0), 1) if pd.notna(hitter.get('Pull%')) else None,
                'cent_pct': round(hitter.get('Cent%', 0), 1) if pd.notna(hitter.get('Cent%')) else None,
                'oppo_pct': round(hitter.get('Oppo%', 0), 1) if pd.notna(hitter.get('Oppo%')) else None,
            }
            
            hitters.append(hitter_data)
        
        return jsonify(hitters)
    
    return jsonify([])

@app.route('/api/hitters/top10')
def api_hitters_top10():
    """API endpoint for top 10 hitters in each key category"""
    if dashboard.hitter_data is None:
        dashboard.load_latest_data()
    
    if dashboard.hitter_data is None:
        return jsonify({})
    
    # Create a copy and ensure no duplicates
    df = dashboard.hitter_data.copy()
    df = df.drop_duplicates(subset=['Name'], keep='first')
    
    # Filter for qualified hitters (minimum 150 PA)
    qualified_df = df[df['PA'] >= 150] if 'PA' in df.columns else df
    
    def get_top10(column, ascending=False):
        """Get top 10 players for a specific stat"""
        if column not in qualified_df.columns:
            return []
        
        sorted_df = qualified_df.dropna(subset=[column]).sort_values(column, ascending=ascending).head(10)
        
        players = []
        for rank, (_, player) in enumerate(sorted_df.iterrows(), 1):
            players.append({
                'rank': rank,
                'name': player.get('Name', 'Unknown'),
                'team': player.get('Team', 'N/A'),
                'value': round(player[column], 3) if isinstance(player[column], (int, float)) else player[column]
            })
        return players
    
    top10_categories = {
        'ops': {
            'title': 'OPS Leaders',
            'players': get_top10('OPS', ascending=False)
        },
        'wrc_plus': {
            'title': 'wRC+ Leaders',
            'players': get_top10('wRC+', ascending=False)
        },
        'woba': {
            'title': 'wOBA Leaders', 
            'players': get_top10('wOBA', ascending=False)
        },
        'xwoba': {
            'title': 'xwOBA Leaders',
            'players': get_top10('xwOBA', ascending=False)
        },
        'hard_hit_pct': {
            'title': 'Hard-Hit% Leaders',
            'players': get_top10('HardHit%', ascending=False)
        },
        'barrel_pct': {
            'title': 'Barrel% Leaders',
            'players': get_top10('Barrel%', ascending=False)
        },
        'home_runs': {
            'title': 'Home Run Leaders',
            'players': get_top10('HR', ascending=False)
        },
        'avg': {
            'title': 'Batting Average Leaders',
            'players': get_top10('AVG', ascending=False)
        },
        'xba': {
            'title': 'xBA Leaders',
            'players': get_top10('xBA', ascending=False)
        },
        'xslg': {
            'title': 'xSLG Leaders',
            'players': get_top10('xSLG', ascending=False)
        },
        'bb_pct': {
            'title': 'Walk% Leaders',
            'players': get_top10('BB%', ascending=False)
        },
        'lowest_k_pct': {
            'title': 'Lowest Strikeout% (Best Contact)',
            'players': get_top10('K%', ascending=True)
        }
    }
    
    return jsonify(top10_categories)

@app.route('/api/pitchers/top10')
def api_pitchers_top10():
    """API endpoint for top 10 pitchers in each key category"""
    if dashboard.pitcher_data is None:
        dashboard.load_latest_data()
    
    if dashboard.pitcher_data is None:
        return jsonify({})
    
    # Create a copy and ensure no duplicates
    df = dashboard.pitcher_data.copy()
    df = df.drop_duplicates(subset=['Name'], keep='first')
    
    # Filter for qualified pitchers (minimum 30 IP)
    qualified_df = df[df['IP'] >= 30] if 'IP' in df.columns else df
    
    def get_top10_pitchers(column, ascending=True):
        """Get top 10 pitchers for a specific stat"""
        if column not in qualified_df.columns:
            return []
        
        sorted_df = qualified_df.dropna(subset=[column]).sort_values(column, ascending=ascending).head(10)
        
        players = []
        for rank, (_, pitcher) in enumerate(sorted_df.iterrows(), 1):
            players.append({
                'rank': rank,
                'name': pitcher.get('Name', 'Unknown'),
                'team': pitcher.get('Team', 'N/A'),
                'value': round(pitcher[column], 3) if isinstance(pitcher[column], (int, float)) else pitcher[column]
            })
        return players
    
    top10_categories = {
        'fip': {
            'title': 'Best FIP (Fielding Independent Pitching)',
            'players': get_top10_pitchers('FIP', ascending=True)
        },
        'whip': {
            'title': 'Best WHIP (Walks + Hits per IP)',
            'players': get_top10_pitchers('WHIP', ascending=True)
        },
        'siera': {
            'title': 'Best SIERA (Skill-Interactive ERA)',
            'players': get_top10_pitchers('SIERA', ascending=True)
        },
        'xfip': {
            'title': 'Best xFIP (Expected FIP)',
            'players': get_top10_pitchers('xFIP', ascending=True)
        },
        'csw_pct': {
            'title': 'Best CSW% (Called Strike + Whiff)',
            'players': get_top10_pitchers('CSW%', ascending=False)
        },
        'k_9': {
            'title': 'Highest K/9 (Strikeouts per 9)',
            'players': get_top10_pitchers('K/9', ascending=False)
        },
        'k_bb': {
            'title': 'Best K/BB Ratio',
            'players': get_top10_pitchers('K/BB', ascending=False)
        },
        'k_pct': {
            'title': 'Highest Strikeout%',
            'players': get_top10_pitchers('K%', ascending=False)
        },
        'lowest_bb_pct': {
            'title': 'Lowest Walk% (Best Control)',
            'players': get_top10_pitchers('BB%', ascending=True)
        },
        'lowest_hr_9': {
            'title': 'Lowest HR/9 (Home Run Prevention)',
            'players': get_top10_pitchers('HR/9', ascending=True)
        },
        'xera': {
            'title': 'Best xERA (Expected ERA)',
            'players': get_top10_pitchers('xERA', ascending=True)
        },
        'stuff_plus': {
            'title': 'Best Stuff+ (Pitch Quality)',
            'players': get_top10_pitchers('Stuff+', ascending=False)
        }
    }
    
    return jsonify(top10_categories)

def fetch_individual_player_data(player_name, season=2025):
    """Fetch individual player data from Fangraphs for missing players"""
    try:
        import sys
        sys.path.append(str(BASE_DIR / "features"))
        from pybaseball import playerid_lookup, batting_stats_bref
        
        # Look up player ID
        player_lookup = playerid_lookup(player_name.split()[1], player_name.split()[0])
        if player_lookup.empty:
            print(f"❌ Could not find player: {player_name}")
            return None
            
        # Get the first match (most likely correct player)
        player_id = player_lookup.iloc[0]['key_fangraphs']
        if pd.isna(player_id):
            print(f"❌ No Fangraphs ID found for: {player_name}")
            return None
            
        # Fetch individual player data from Fangraphs
        from pybaseball import batting_stats
        player_data = batting_stats(season, season, ind=1)  # Get individual player data
        player_row = player_data[player_data['playerid'] == int(player_id)]
        
        if player_row.empty:
            print(f"❌ No {season} data found for: {player_name}")
            return None
            
        # Convert to the same format as our existing data
        player_stats = player_row.iloc[0]
        formatted_data = {
            'Name': player_name,
            'Team': player_stats.get('Team', 'N/A'),
            'G': player_stats.get('G', 0),
            'PA': player_stats.get('PA', 0),
            'AB': player_stats.get('AB', 0),
            'H': player_stats.get('H', 0),
            'HR': player_stats.get('HR', 0),
            'R': player_stats.get('R', 0),
            'RBI': player_stats.get('RBI', 0),
            'SB': player_stats.get('SB', 0),
            'AVG': player_stats.get('AVG', 0),
            'OBP': player_stats.get('OBP', 0),
            'SLG': player_stats.get('SLG', 0),
            'OPS': player_stats.get('OPS', 0),
            'wOBA': player_stats.get('wOBA', 0),
            'wRC+': player_stats.get('wRC+', 0),
            'BB%': player_stats.get('BB%', 0),
            'K%': player_stats.get('K%', 0),
            'ISO': player_stats.get('ISO', 0),
            'BABIP': player_stats.get('BABIP', 0),
            # Expected stats might not be available for individual lookups
            'xBA': player_stats.get('xBA', None),
            'xSLG': player_stats.get('xSLG', None),
            'xwOBA': player_stats.get('xwOBA', None),
            'HardHit%': player_stats.get('HardHit%', None),
            'Barrel%': player_stats.get('Barrel%', None),
            'EV': player_stats.get('EV', None),
            'LA': player_stats.get('LA', None),
            'maxEV': player_stats.get('maxEV', None),
            'Pull%': player_stats.get('Pull%', None),
            'Cent%': player_stats.get('Cent%', None),
            'Oppo%': player_stats.get('Oppo%', None),
        }
        
        print(f"✅ Successfully fetched data for: {player_name}")
        return formatted_data
        
    except Exception as e:
        print(f"❌ Error fetching individual player data for {player_name}: {e}")
        return None

@app.route('/api/player/<player_name>')
def fetch_player_data(player_name):
    """API endpoint to fetch individual player data"""
    try:
        player_data = fetch_individual_player_data(player_name)
        if player_data:
            return jsonify({'success': True, 'data': player_data})
        else:
            return jsonify({'success': False, 'error': 'Player not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/summary')
def api_summary():
    """API endpoint for summary statistics"""
    data = dashboard.get_dashboard_data()
    if data:
        return jsonify({
            'pitcher_summary': data.get('pitcher_summary', {}),
            'hitter_summary': data.get('hitter_summary', {}),
            'games_count': len(data.get('games', [])),
            'current_date': data.get('current_date', '')
        })
    return jsonify({})

@app.route('/pitchers')
def pitchers_page():
    """Dedicated pitcher analysis page"""
    data = dashboard.get_dashboard_data()
    return render_template('pitchers.html', data=data)

@app.route('/hitters') 
def hitters_page():
    """Dedicated hitter analysis page"""
    data = dashboard.get_dashboard_data()
    return render_template('hitters.html', data=data)

@app.route('/games')
def games_page():
    """Dedicated games and odds page"""
    data = dashboard.get_dashboard_data()
    return render_template('games.html', data=data)

@app.route('/lineups')
def lineups_page():
    """Dedicated lineups page"""
    data = dashboard.get_dashboard_data()
    lineup_data = dashboard.get_lineup_data()
    return render_template('lineups.html', data=data, lineups=lineup_data)

@app.route('/api/lineups')
def api_lineups():
    """API endpoint for lineup data"""
    # Check if refresh is requested
    force_refresh = request.args.get('refresh', 'false').lower() == 'true'
    
    try:
        lineups = dashboard.get_lineup_data(force_refresh=force_refresh)
        # Get current Eastern time
        eastern = pytz.timezone('US/Eastern')
        eastern_time = datetime.now(eastern)
        
        return jsonify({
            'success': True,
            'lineups': lineups,
            'timestamp': eastern_time.strftime('%Y-%m-%d %I:%M:%S %p ET'),
            'refreshed': force_refresh
        })
    except Exception as e:
        # Get current Eastern time for error case too
        eastern = pytz.timezone('US/Eastern')
        eastern_time = datetime.now(eastern)
        
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': eastern_time.strftime('%Y-%m-%d %I:%M:%S %p ET')
        }), 500

@app.route('/export/games')
def export_games():
    """Export games data as CSV"""
    games = dashboard.get_games_data()
    
    if not games:
        return "No games data available", 404
    
    # Create CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=games[0].keys())
    writer.writeheader()
    writer.writerows(games)
    
    # Create file response
    csv_data = output.getvalue()
    output.close()
    
    response = app.response_class(
        csv_data,
        mimetype='text/csv',
        headers={"Content-disposition": f"attachment; filename=mlb_games_{datetime.now().strftime('%Y-%m-%d')}.csv"}
    )
    
    return response

@app.route('/export/pitchers')
def export_pitchers():
    """Export pitcher data as CSV"""
    if dashboard.pitcher_data is None:
        dashboard.load_latest_data()
    
    if dashboard.pitcher_data is None:
        return "No pitcher data available", 404
    
    # Create CSV response
    output = io.StringIO()
    dashboard.pitcher_data.to_csv(output, index=False)
    csv_data = output.getvalue()
    output.close()
    
    response = app.response_class(
        csv_data,
        mimetype='text/csv',
        headers={"Content-disposition": f"attachment; filename=mlb_pitchers_{datetime.now().strftime('%Y-%m-%d')}.csv"}
    )
    
    return response

@app.route('/export/hitters')
def export_hitters():
    """Export hitter data as CSV"""
    if dashboard.hitter_data is None:
        dashboard.load_latest_data()
    
    if dashboard.hitter_data is None:
        return "No hitter data available", 404
    
    # Create CSV response
    output = io.StringIO()
    dashboard.hitter_data.to_csv(output, index=False)
    csv_data = output.getvalue()
    output.close()
    
    response = app.response_class(
        csv_data,
        mimetype='text/csv',
        headers={"Content-disposition": f"attachment; filename=mlb_hitters_{datetime.now().strftime('%Y-%m-%d')}.csv"}
    )
    
    return response

if __name__ == '__main__':
    print("🚀 Starting Enhanced MLB Betting Dashboard...")
    
    # Load initial data
    if dashboard.load_latest_data():
        print("✅ Data loaded successfully")
        if dashboard.integrated_data is not None:
            print(f"📊 Integrated games: {len(dashboard.integrated_data)}")
        if dashboard.pitcher_data is not None:
            print(f"⚾ Pitchers analyzed: {len(dashboard.pitcher_data)}")
        if dashboard.hitter_data is not None:
            print(f"🏏 Hitters analyzed: {len(dashboard.hitter_data)}")
    else:
        print("⚠️ No data loaded - dashboard will show placeholder content")
    
    print("🌐 Dashboard will be available at: http://localhost:5000")
    print("\n📊 Available Pages:")
    print("   /          - Main dashboard overview")
    print("   /games     - Games and odds analysis")
    print("   /pitchers  - Pitcher analysis")
    print("   /hitters   - Hitter analysis")
    
    app.run(debug=True, host='0.0.0.0', port=5000)