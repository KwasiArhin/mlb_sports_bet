#!/usr/bin/env python3
"""
MLB Database Management System
Handles SQLite database operations for MLB betting analytics
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, date
import logging
from typing import Optional, List, Dict, Any
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLBDatabase:
    def __init__(self, db_path: Optional[str] = None):
        """Initialize MLB Database connection"""
        if db_path is None:
            # Default to database directory in project root
            base_dir = Path(__file__).resolve().parents[1]
            db_dir = base_dir / "database"
            db_dir.mkdir(exist_ok=True)
            self.db_path = db_dir / "mlb_data.db"
        else:
            self.db_path = Path(db_path)
        
        logger.info(f"Database path: {self.db_path}")
        self.init_database()
    
    def get_connection(self):
        """Get database connection with foreign key support"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dict-like access
        return conn
    
    def init_database(self):
        """Create all database tables if they don't exist"""
        conn = self.get_connection()
        try:
            # Teams table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY,
                    abbreviation TEXT UNIQUE NOT NULL,
                    full_name TEXT NOT NULL,
                    league TEXT,
                    division TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Players table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    team_id INTEGER,
                    position TEXT,
                    fangraphs_id INTEGER UNIQUE,
                    mlb_id INTEGER UNIQUE,
                    active BOOLEAN DEFAULT TRUE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (team_id) REFERENCES teams (id)
                )
            """)
            
            # Games table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id INTEGER PRIMARY KEY,
                    game_id TEXT UNIQUE,
                    game_date DATE NOT NULL,
                    home_team_id INTEGER,
                    away_team_id INTEGER,
                    home_pitcher_id INTEGER,
                    away_pitcher_id INTEGER,
                    venue TEXT,
                    game_status TEXT DEFAULT 'scheduled',
                    commence_time DATETIME,
                    home_score INTEGER,
                    away_score INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (home_team_id) REFERENCES teams (id),
                    FOREIGN KEY (away_team_id) REFERENCES teams (id),
                    FOREIGN KEY (home_pitcher_id) REFERENCES players (id),
                    FOREIGN KEY (away_pitcher_id) REFERENCES players (id)
                )
            """)
            
            # Pitcher stats table (daily snapshots)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pitcher_stats (
                    id INTEGER PRIMARY KEY,
                    player_id INTEGER NOT NULL,
                    stat_date DATE NOT NULL,
                    season INTEGER NOT NULL,
                    
                    -- Basic stats
                    games INTEGER,
                    games_started INTEGER,
                    wins INTEGER,
                    losses INTEGER,
                    saves INTEGER,
                    innings_pitched REAL,
                    hits_allowed INTEGER,
                    runs_allowed INTEGER,
                    earned_runs INTEGER,
                    home_runs_allowed INTEGER,
                    walks INTEGER,
                    strikeouts INTEGER,
                    
                    -- Advanced stats
                    era REAL,
                    whip REAL,
                    fip REAL,
                    xfip REAL,
                    siera REAL,
                    war REAL,
                    k_9 REAL,
                    bb_9 REAL,
                    hr_9 REAL,
                    k_bb REAL,
                    avg_against REAL,
                    babip REAL,
                    lob_pct REAL,
                    gb_fb_ratio REAL,
                    
                    -- Statcast data
                    xwoba REAL,
                    xba REAL,
                    xslg REAL,
                    exit_velocity REAL,
                    launch_angle REAL,
                    barrel_pct REAL,
                    hard_hit_pct REAL,
                    
                    -- Stuff+ metrics
                    stuff_plus INTEGER,
                    location_plus INTEGER,
                    pitching_plus INTEGER,
                    
                    -- Pitch data
                    fastball_pct REAL,
                    fastball_velo REAL,
                    slider_pct REAL,
                    curveball_pct REAL,
                    changeup_pct REAL,
                    
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players (id),
                    UNIQUE(player_id, stat_date)
                )
            """)
            
            # Hitter stats table (daily snapshots)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS hitter_stats (
                    id INTEGER PRIMARY KEY,
                    player_id INTEGER NOT NULL,
                    stat_date DATE NOT NULL,
                    season INTEGER NOT NULL,
                    
                    -- Basic stats
                    games INTEGER,
                    plate_appearances INTEGER,
                    at_bats INTEGER,
                    hits INTEGER,
                    doubles INTEGER,
                    triples INTEGER,
                    home_runs INTEGER,
                    runs INTEGER,
                    rbis INTEGER,
                    walks INTEGER,
                    strikeouts INTEGER,
                    stolen_bases INTEGER,
                    
                    -- Rate stats
                    avg REAL,
                    obp REAL,
                    slg REAL,
                    ops REAL,
                    woba REAL,
                    wrc_plus INTEGER,
                    war REAL,
                    babip REAL,
                    iso REAL,
                    k_pct REAL,
                    bb_pct REAL,
                    
                    -- Statcast data
                    xwoba REAL,
                    xba REAL,
                    xslg REAL,
                    exit_velocity REAL,
                    launch_angle REAL,
                    barrel_pct REAL,
                    hard_hit_pct REAL,
                    max_exit_velocity REAL,
                    
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (player_id) REFERENCES players (id),
                    UNIQUE(player_id, stat_date)
                )
            """)
            
            # Betting odds table (time series)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS betting_odds (
                    id INTEGER PRIMARY KEY,
                    game_id INTEGER NOT NULL,
                    bookmaker TEXT NOT NULL,
                    odds_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Moneyline odds
                    home_moneyline INTEGER,
                    away_moneyline INTEGER,
                    
                    -- Spread odds
                    home_spread REAL,
                    away_spread REAL,
                    home_spread_odds INTEGER,
                    away_spread_odds INTEGER,
                    
                    -- Total odds
                    total_points REAL,
                    over_odds INTEGER,
                    under_odds INTEGER,
                    
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (game_id) REFERENCES games (id)
                )
            """)
            
            # Data refresh log table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_refresh_log (
                    id INTEGER PRIMARY KEY,
                    data_type TEXT NOT NULL,
                    refresh_date DATE NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'success',
                    error_message TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_players_name ON players (name)",
                "CREATE INDEX IF NOT EXISTS idx_players_team ON players (team_id)",
                "CREATE INDEX IF NOT EXISTS idx_games_date ON games (game_date)",
                "CREATE INDEX IF NOT EXISTS idx_games_teams ON games (home_team_id, away_team_id)",
                "CREATE INDEX IF NOT EXISTS idx_pitcher_stats_player_date ON pitcher_stats (player_id, stat_date)",
                "CREATE INDEX IF NOT EXISTS idx_pitcher_stats_date ON pitcher_stats (stat_date)",
                "CREATE INDEX IF NOT EXISTS idx_hitter_stats_player_date ON hitter_stats (player_id, stat_date)",
                "CREATE INDEX IF NOT EXISTS idx_hitter_stats_date ON hitter_stats (stat_date)",
                "CREATE INDEX IF NOT EXISTS idx_betting_odds_game ON betting_odds (game_id)",
                "CREATE INDEX IF NOT EXISTS idx_betting_odds_time ON betting_odds (odds_time)",
            ]
            
            for index_sql in indexes:
                conn.execute(index_sql)
            
            conn.commit()
            logger.info("Database initialized successfully")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()
    
    def upsert_team(self, abbreviation: str, full_name: str, league: str = None, division: str = None) -> int:
        """Insert or update team data"""
        conn = self.get_connection()
        try:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO teams (abbreviation, full_name, league, division, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (abbreviation, full_name, league, division))
            
            team_id = cursor.lastrowid
            conn.commit()
            return team_id
        finally:
            conn.close()
    
    def upsert_player(self, name: str, team_abbreviation: str, position: str = None, 
                      fangraphs_id: int = None, mlb_id: int = None) -> int:
        """Insert or update player data"""
        conn = self.get_connection()
        try:
            # Get team_id
            team_result = conn.execute(
                "SELECT id FROM teams WHERE abbreviation = ?", (team_abbreviation,)
            ).fetchone()
            
            if not team_result:
                logger.warning(f"Team {team_abbreviation} not found for player {name}")
                return None
            
            team_id = team_result['id']
            
            cursor = conn.execute("""
                INSERT OR REPLACE INTO players (name, team_id, position, fangraphs_id, mlb_id, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (name, team_id, position, fangraphs_id, mlb_id))
            
            player_id = cursor.lastrowid
            conn.commit()
            return player_id
        finally:
            conn.close()
    
    def upsert_game(self, game_data: Dict[str, Any]) -> int:
        """Insert or update game data"""
        conn = self.get_connection()
        try:
            cursor = conn.execute("""
                INSERT OR REPLACE INTO games (
                    game_id, game_date, home_team_id, away_team_id, 
                    venue, game_status, commence_time, updated_at
                ) VALUES (?, ?, 
                    (SELECT id FROM teams WHERE abbreviation = ?),
                    (SELECT id FROM teams WHERE abbreviation = ?),
                    ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                game_data.get('game_id'),
                game_data.get('game_date'),
                game_data.get('home_team'),
                game_data.get('away_team'),
                game_data.get('venue'),
                game_data.get('game_status', 'scheduled'),
                game_data.get('commence_time')
            ))
            
            game_id = cursor.lastrowid
            conn.commit()
            return game_id
        finally:
            conn.close()
    
    def upsert_pitcher_stats(self, player_name: str, team_abbreviation: str, 
                           stat_date: date, stats_data: Dict[str, Any]) -> bool:
        """Insert or update pitcher stats"""
        conn = self.get_connection()
        try:
            # Get or create player
            player_id = self.upsert_player(player_name, team_abbreviation)
            if not player_id:
                return False
            
            # Prepare stats data
            stats_cols = []
            stats_vals = [player_id, stat_date, stat_date.year]
            
            for key, value in stats_data.items():
                if value is not None and value != '':
                    stats_cols.append(key)
                    stats_vals.append(value)
            
            if stats_cols:
                placeholders = ', '.join(['?'] * len(stats_vals))
                columns = 'player_id, stat_date, season, ' + ', '.join(stats_cols)
                
                conn.execute(f"""
                    INSERT OR REPLACE INTO pitcher_stats ({columns})
                    VALUES ({placeholders})
                """, stats_vals)
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error upserting pitcher stats for {player_name}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def upsert_hitter_stats(self, player_name: str, team_abbreviation: str, 
                          stat_date: date, stats_data: Dict[str, Any]) -> bool:
        """Insert or update hitter stats"""
        conn = self.get_connection()
        try:
            # Get or create player
            player_id = self.upsert_player(player_name, team_abbreviation)
            if not player_id:
                return False
            
            # Prepare stats data
            stats_cols = []
            stats_vals = [player_id, stat_date, stat_date.year]
            
            for key, value in stats_data.items():
                if value is not None and value != '':
                    stats_cols.append(key)
                    stats_vals.append(value)
            
            if stats_cols:
                placeholders = ', '.join(['?'] * len(stats_vals))
                columns = 'player_id, stat_date, season, ' + ', '.join(stats_cols)
                
                conn.execute(f"""
                    INSERT OR REPLACE INTO hitter_stats ({columns})
                    VALUES ({placeholders})
                """, stats_vals)
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error upserting hitter stats for {player_name}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def upsert_betting_odds(self, game_id: int, bookmaker: str, odds_data: Dict[str, Any]) -> bool:
        """Insert or update betting odds"""
        conn = self.get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO betting_odds (
                    game_id, bookmaker, home_moneyline, away_moneyline,
                    home_spread, away_spread, home_spread_odds, away_spread_odds,
                    total_points, over_odds, under_odds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game_id, bookmaker,
                odds_data.get('home_moneyline'),
                odds_data.get('away_moneyline'),
                odds_data.get('home_spread'),
                odds_data.get('away_spread'),
                odds_data.get('home_spread_odds'),
                odds_data.get('away_spread_odds'),
                odds_data.get('total_points'),
                odds_data.get('over_odds'),
                odds_data.get('under_odds')
            ))
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error upserting betting odds: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()
    
    def get_latest_pitcher_stats(self, limit: int = None) -> pd.DataFrame:
        """Get latest pitcher stats"""
        conn = self.get_connection()
        try:
            sql = """
                SELECT 
                    p.name,
                    t.abbreviation as team,
                    ps.*
                FROM pitcher_stats ps
                JOIN players p ON ps.player_id = p.id
                JOIN teams t ON p.team_id = t.id
                WHERE ps.stat_date = (
                    SELECT MAX(stat_date) FROM pitcher_stats ps2 WHERE ps2.player_id = ps.player_id
                )
                ORDER BY ps.war DESC
            """
            
            if limit:
                sql += f" LIMIT {limit}"
            
            return pd.read_sql_query(sql, conn)
        finally:
            conn.close()
    
    def get_latest_hitter_stats(self, limit: int = None) -> pd.DataFrame:
        """Get latest hitter stats"""
        conn = self.get_connection()
        try:
            sql = """
                SELECT 
                    p.name,
                    t.abbreviation as team,
                    hs.*
                FROM hitter_stats hs
                JOIN players p ON hs.player_id = p.id
                JOIN teams t ON p.team_id = t.id
                WHERE hs.stat_date = (
                    SELECT MAX(stat_date) FROM hitter_stats hs2 WHERE hs2.player_id = hs.player_id
                )
                ORDER BY hs.wrc_plus DESC
            """
            
            if limit:
                sql += f" LIMIT {limit}"
            
            return pd.read_sql_query(sql, conn)
        finally:
            conn.close()
    
    def get_games_by_date(self, game_date: date) -> pd.DataFrame:
        """Get games for a specific date with odds"""
        conn = self.get_connection()
        try:
            sql = """
                SELECT 
                    g.*,
                    ht.abbreviation as home_team_abbr,
                    ht.full_name as home_team_name,
                    at.abbreviation as away_team_abbr,
                    at.full_name as away_team_name,
                    bo.home_moneyline,
                    bo.away_moneyline,
                    bo.total_points,
                    bo.over_odds,
                    bo.under_odds
                FROM games g
                LEFT JOIN teams ht ON g.home_team_id = ht.id
                LEFT JOIN teams at ON g.away_team_id = at.id
                LEFT JOIN betting_odds bo ON g.id = bo.game_id AND bo.bookmaker = 'fanduel'
                WHERE g.game_date = ?
                ORDER BY g.commence_time
            """
            
            return pd.read_sql_query(sql, conn, params=[game_date])
        finally:
            conn.close()
    
    def log_data_refresh(self, data_type: str, refresh_date: date, 
                        records_processed: int = 0, status: str = 'success', 
                        error_message: str = None):
        """Log data refresh activity"""
        conn = self.get_connection()
        try:
            conn.execute("""
                INSERT INTO data_refresh_log (data_type, refresh_date, records_processed, status, error_message)
                VALUES (?, ?, ?, ?, ?)
            """, (data_type, refresh_date, records_processed, status, error_message))
            
            conn.commit()
        finally:
            conn.close()
    
    def get_last_refresh_date(self, data_type: str) -> Optional[date]:
        """Get the last successful refresh date for a data type"""
        conn = self.get_connection()
        try:
            result = conn.execute("""
                SELECT MAX(refresh_date) as last_refresh
                FROM data_refresh_log
                WHERE data_type = ? AND status = 'success'
            """, (data_type,)).fetchone()
            
            if result and result['last_refresh']:
                return datetime.strptime(result['last_refresh'], '%Y-%m-%d').date()
            return None
        finally:
            conn.close()
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        conn = self.get_connection()
        try:
            stats = {}
            
            # Count records in each table
            tables = ['teams', 'players', 'games', 'pitcher_stats', 'hitter_stats', 'betting_odds']
            for table in tables:
                result = conn.execute(f"SELECT COUNT(*) as count FROM {table}").fetchone()
                stats[table] = result['count']
            
            return stats
        finally:
            conn.close()