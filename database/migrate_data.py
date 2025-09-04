#!/usr/bin/env python3
"""
Data Migration Script
Migrates existing CSV data to SQLite database
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, date
import logging
import re
from mlb_database import MLBDatabase

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataMigrator:
    def __init__(self):
        self.db = MLBDatabase()
        self.base_dir = Path(__file__).resolve().parents[1]
        self.processed_dir = self.base_dir / "data" / "processed"
        
        # Team name mappings
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
            'Texas Rangers': 'TEX', 'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WAS',
            # Additional mappings for common variations
            'SDP': 'SD', 'TBR': 'TB', 'WSN': 'WAS', 'CHW': 'CWS', 'KCR': 'KC', 'SFG': 'SF'
        }
        
        # Reverse mapping for abbreviations to full names
        self.abbr_to_full = {v: k for k, v in self.team_mapping.items()}
    
    def normalize_team_name(self, team_name):
        """Normalize team name to abbreviation"""
        if not team_name or pd.isna(team_name):
            return None
        
        team_str = str(team_name).strip()
        
        # If it's already an abbreviation
        if team_str in self.abbr_to_full:
            return team_str
        
        # If it's a full name
        if team_str in self.team_mapping:
            return self.team_mapping[team_str]
        
        # Try partial matching
        for full_name, abbr in self.team_mapping.items():
            if team_str.lower() in full_name.lower() or full_name.lower() in team_str.lower():
                return abbr
        
        logger.warning(f"Could not normalize team name: {team_name}")
        return team_str
    
    def setup_teams(self):
        """Initialize teams table with MLB teams"""
        logger.info("Setting up teams...")
        
        # MLB divisions
        teams_data = [
            # AL East
            ('BAL', 'Baltimore Orioles', 'AL', 'East'),
            ('BOS', 'Boston Red Sox', 'AL', 'East'),
            ('NYY', 'New York Yankees', 'AL', 'East'),
            ('TB', 'Tampa Bay Rays', 'AL', 'East'),
            ('TOR', 'Toronto Blue Jays', 'AL', 'East'),
            
            # AL Central
            ('CWS', 'Chicago White Sox', 'AL', 'Central'),
            ('CLE', 'Cleveland Guardians', 'AL', 'Central'),
            ('DET', 'Detroit Tigers', 'AL', 'Central'),
            ('KC', 'Kansas City Royals', 'AL', 'Central'),
            ('MIN', 'Minnesota Twins', 'AL', 'Central'),
            
            # AL West
            ('HOU', 'Houston Astros', 'AL', 'West'),
            ('LAA', 'Los Angeles Angels', 'AL', 'West'),
            ('OAK', 'Oakland Athletics', 'AL', 'West'),
            ('SEA', 'Seattle Mariners', 'AL', 'West'),
            ('TEX', 'Texas Rangers', 'AL', 'West'),
            
            # NL East
            ('ATL', 'Atlanta Braves', 'NL', 'East'),
            ('MIA', 'Miami Marlins', 'NL', 'East'),
            ('NYM', 'New York Mets', 'NL', 'East'),
            ('PHI', 'Philadelphia Phillies', 'NL', 'East'),
            ('WAS', 'Washington Nationals', 'NL', 'East'),
            
            # NL Central
            ('CHC', 'Chicago Cubs', 'NL', 'Central'),
            ('CIN', 'Cincinnati Reds', 'NL', 'Central'),
            ('MIL', 'Milwaukee Brewers', 'NL', 'Central'),
            ('PIT', 'Pittsburgh Pirates', 'NL', 'Central'),
            ('STL', 'St. Louis Cardinals', 'NL', 'Central'),
            
            # NL West
            ('ARI', 'Arizona Diamondbacks', 'NL', 'West'),
            ('COL', 'Colorado Rockies', 'NL', 'West'),
            ('LAD', 'Los Angeles Dodgers', 'NL', 'West'),
            ('SD', 'San Diego Padres', 'NL', 'West'),
            ('SF', 'San Francisco Giants', 'NL', 'West'),
        ]
        
        for abbr, full_name, league, division in teams_data:
            self.db.upsert_team(abbr, full_name, league, division)
        
        logger.info(f"Set up {len(teams_data)} teams")
    
    def extract_date_from_filename(self, filename):
        """Extract date from filename like 'fangraphs_pitcher_data_2025-09-02.csv'"""
        match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if match:
            return datetime.strptime(match.group(1), '%Y-%m-%d').date()
        return date.today()
    
    def migrate_pitcher_data(self):
        """Migrate Fangraphs pitcher data"""
        logger.info("Migrating pitcher data...")
        
        pitcher_files = list(self.processed_dir.glob("fangraphs_pitcher_data_*.csv"))
        total_records = 0
        
        for file_path in pitcher_files:
            logger.info(f"Processing pitcher file: {file_path.name}")
            stat_date = self.extract_date_from_filename(file_path.name)
            
            try:
                df = pd.read_csv(file_path)
                records_processed = 0
                
                for _, row in df.iterrows():
                    player_name = row.get('Name')
                    team_abbr = self.normalize_team_name(row.get('Team'))
                    
                    if not player_name or not team_abbr:
                        continue
                    
                    # Map Fangraphs columns to database columns
                    stats_data = {
                        'games': self._safe_int(row.get('G')),
                        'games_started': self._safe_int(row.get('GS')),
                        'wins': self._safe_int(row.get('W')),
                        'losses': self._safe_int(row.get('L')),
                        'saves': self._safe_int(row.get('SV')),
                        'innings_pitched': self._safe_float(row.get('IP')),
                        'hits_allowed': self._safe_int(row.get('H')),
                        'runs_allowed': self._safe_int(row.get('R')),
                        'earned_runs': self._safe_int(row.get('ER')),
                        'home_runs_allowed': self._safe_int(row.get('HR')),
                        'walks': self._safe_int(row.get('BB')),
                        'strikeouts': self._safe_int(row.get('SO')),
                        'era': self._safe_float(row.get('ERA')),
                        'whip': self._safe_float(row.get('WHIP')),
                        'fip': self._safe_float(row.get('FIP')),
                        'xfip': self._safe_float(row.get('xFIP')),
                        'siera': self._safe_float(row.get('SIERA')),
                        'war': self._safe_float(row.get('WAR')),
                        'k_9': self._safe_float(row.get('K/9')),
                        'bb_9': self._safe_float(row.get('BB/9')),
                        'hr_9': self._safe_float(row.get('HR/9')),
                        'k_bb': self._safe_float(row.get('K/BB')),
                        'avg_against': self._safe_float(row.get('AVG')),
                        'babip': self._safe_float(row.get('BABIP')),
                        'lob_pct': self._safe_float(row.get('LOB%')),
                        'gb_fb_ratio': self._safe_float(row.get('GB/FB')),
                        'stuff_plus': self._safe_int(row.get('Stuff+')),
                        'location_plus': self._safe_int(row.get('Location+')),
                        'pitching_plus': self._safe_int(row.get('Pitching+')),
                        'fastball_pct': self._safe_float(row.get('FB%')),
                        'fastball_velo': self._safe_float(row.get('FBv')),
                        'slider_pct': self._safe_float(row.get('SL%')),
                        'curveball_pct': self._safe_float(row.get('CB%')),
                        'changeup_pct': self._safe_float(row.get('CH%')),
                    }
                    
                    if self.db.upsert_pitcher_stats(player_name, team_abbr, stat_date, stats_data):
                        records_processed += 1
                
                logger.info(f"Processed {records_processed} pitcher records from {file_path.name}")
                total_records += records_processed
                
                # Log the migration
                self.db.log_data_refresh('pitcher_stats', stat_date, records_processed)
                
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")
                self.db.log_data_refresh('pitcher_stats', stat_date, 0, 'error', str(e))
        
        logger.info(f"Migrated {total_records} total pitcher records")
        return total_records
    
    def migrate_hitter_data(self):
        """Migrate Fangraphs hitter data"""
        logger.info("Migrating hitter data...")
        
        hitter_files = list(self.processed_dir.glob("fangraphs_hitter_data_*.csv"))
        total_records = 0
        
        for file_path in hitter_files:
            logger.info(f"Processing hitter file: {file_path.name}")
            stat_date = self.extract_date_from_filename(file_path.name)
            
            try:
                df = pd.read_csv(file_path)
                records_processed = 0
                
                for _, row in df.iterrows():
                    player_name = row.get('Name')
                    team_abbr = self.normalize_team_name(row.get('Team'))
                    
                    if not player_name or not team_abbr:
                        continue
                    
                    # Map Fangraphs columns to database columns
                    stats_data = {
                        'games': self._safe_int(row.get('G')),
                        'plate_appearances': self._safe_int(row.get('PA')),
                        'at_bats': self._safe_int(row.get('AB')),
                        'hits': self._safe_int(row.get('H')),
                        'home_runs': self._safe_int(row.get('HR')),
                        'runs': self._safe_int(row.get('R')),
                        'rbis': self._safe_int(row.get('RBI')),
                        'walks': self._safe_int(row.get('BB')),
                        'strikeouts': self._safe_int(row.get('K')),
                        'stolen_bases': self._safe_int(row.get('SB')),
                        'avg': self._safe_float(row.get('AVG')),
                        'obp': self._safe_float(row.get('OBP')),
                        'slg': self._safe_float(row.get('SLG')),
                        'ops': self._safe_float(row.get('OPS')),
                        'woba': self._safe_float(row.get('wOBA')),
                        'wrc_plus': self._safe_int(row.get('wRC+')),
                        'war': self._safe_float(row.get('WAR')),
                        'babip': self._safe_float(row.get('BABIP')),
                        'iso': self._safe_float(row.get('ISO')),
                        'k_pct': self._safe_float(row.get('K%')),
                        'bb_pct': self._safe_float(row.get('BB%')),
                    }
                    
                    if self.db.upsert_hitter_stats(player_name, team_abbr, stat_date, stats_data):
                        records_processed += 1
                
                logger.info(f"Processed {records_processed} hitter records from {file_path.name}")
                total_records += records_processed
                
                # Log the migration
                self.db.log_data_refresh('hitter_stats', stat_date, records_processed)
                
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")
                self.db.log_data_refresh('hitter_stats', stat_date, 0, 'error', str(e))
        
        logger.info(f"Migrated {total_records} total hitter records")
        return total_records
    
    def migrate_statcast_data(self):
        """Migrate Statcast expected stats data"""
        logger.info("Migrating Statcast data...")
        
        statcast_files = list(self.processed_dir.glob("baseball_savant_expected_stats_*.csv"))
        total_records = 0
        
        for file_path in statcast_files:
            logger.info(f"Processing Statcast file: {file_path.name}")
            stat_date = self.extract_date_from_filename(file_path.name)
            
            try:
                df = pd.read_csv(file_path)
                records_processed = 0
                
                for _, row in df.iterrows():
                    player_name = row.get('matched_name')
                    
                    if not player_name or pd.isna(player_name):
                        continue
                    
                    # Update pitcher stats with Statcast data
                    statcast_data = {
                        'xwoba': self._safe_float(row.get('xwOBA')),
                        'xba': self._safe_float(row.get('xBA')),
                        'xslg': self._safe_float(row.get('xSLG')),
                    }
                    
                    # Find player in database and update
                    conn = self.db.get_connection()
                    try:
                        # Update pitcher stats
                        result = conn.execute("""
                            UPDATE pitcher_stats 
                            SET xwoba = ?, xba = ?, xslg = ?
                            WHERE player_id = (SELECT id FROM players WHERE name = ?)
                            AND stat_date = ?
                        """, (
                            statcast_data['xwoba'],
                            statcast_data['xba'],
                            statcast_data['xslg'],
                            player_name,
                            stat_date
                        ))
                        
                        if result.rowcount > 0:
                            records_processed += 1
                        
                        conn.commit()
                    finally:
                        conn.close()
                
                logger.info(f"Updated {records_processed} records with Statcast data from {file_path.name}")
                total_records += records_processed
                
                # Log the migration
                self.db.log_data_refresh('statcast_stats', stat_date, records_processed)
                
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")
                self.db.log_data_refresh('statcast_stats', stat_date, 0, 'error', str(e))
        
        logger.info(f"Updated {total_records} records with Statcast data")
        return total_records
    
    def migrate_betting_data(self):
        """Migrate integrated betting data"""
        logger.info("Migrating betting data...")
        
        betting_files = list(self.processed_dir.glob("integrated_betting_data_*.csv"))
        total_records = 0
        
        for file_path in betting_files:
            logger.info(f"Processing betting file: {file_path.name}")
            stat_date = self.extract_date_from_filename(file_path.name)
            
            try:
                df = pd.read_csv(file_path)
                records_processed = 0
                
                for _, row in df.iterrows():
                    # Create game record
                    game_data = {
                        'game_id': row.get('game_id'),
                        'game_date': stat_date,
                        'home_team': self.normalize_team_name(row.get('home_team')),
                        'away_team': self.normalize_team_name(row.get('away_team')),
                        'venue': row.get('venue'),
                        'commence_time': row.get('game_time'),
                        'game_status': 'scheduled'
                    }
                    
                    game_db_id = self.db.upsert_game(game_data)
                    
                    if game_db_id:
                        # Add FanDuel odds
                        fanduel_odds = {
                            'home_moneyline': self._safe_int(row.get('fd_home_moneyline')),
                            'away_moneyline': self._safe_int(row.get('fd_away_moneyline')),
                            'home_spread': self._safe_float(row.get('fd_home_spread')),
                            'away_spread': self._safe_float(row.get('fd_away_spread')),
                            'total_points': self._safe_float(row.get('fd_total_points')),
                            'over_odds': self._safe_int(row.get('fd_over_odds')),
                            'under_odds': self._safe_int(row.get('fd_under_odds')),
                        }
                        
                        self.db.upsert_betting_odds(game_db_id, 'fanduel', fanduel_odds)
                        records_processed += 1
                
                logger.info(f"Processed {records_processed} betting records from {file_path.name}")
                total_records += records_processed
                
                # Log the migration
                self.db.log_data_refresh('betting_data', stat_date, records_processed)
                
            except Exception as e:
                logger.error(f"Error processing {file_path.name}: {e}")
                self.db.log_data_refresh('betting_data', stat_date, 0, 'error', str(e))
        
        logger.info(f"Migrated {total_records} total betting records")
        return total_records
    
    def _safe_int(self, value):
        """Safely convert to int"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert to float"""
        if pd.isna(value) or value == '' or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def run_full_migration(self):
        """Run complete data migration"""
        logger.info("=" * 50)
        logger.info("Starting full data migration...")
        logger.info("=" * 50)
        
        # Setup teams first
        self.setup_teams()
        
        # Migrate all data types
        pitcher_count = self.migrate_pitcher_data()
        hitter_count = self.migrate_hitter_data() 
        statcast_count = self.migrate_statcast_data()
        betting_count = self.migrate_betting_data()
        
        # Get final database stats
        db_stats = self.db.get_database_stats()
        
        logger.info("=" * 50)
        logger.info("Migration Summary:")
        logger.info(f"Pitcher records migrated: {pitcher_count}")
        logger.info(f"Hitter records migrated: {hitter_count}")
        logger.info(f"Statcast updates: {statcast_count}")
        logger.info(f"Betting records migrated: {betting_count}")
        logger.info("")
        logger.info("Database Statistics:")
        for table, count in db_stats.items():
            logger.info(f"{table}: {count} records")
        logger.info("=" * 50)
        
        return db_stats

def main():
    """Main migration function"""
    migrator = DataMigrator()
    migrator.run_full_migration()

if __name__ == "__main__":
    main()