# build_advanced_pitcher_features.py

from pybaseball import statcast, playerid_lookup
from unidecode import unidecode
import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime, timedelta
import logging
import argparse
import csv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_HISTORICAL_MATCHUPS_DIR = RAW_DIR / "historical_matchups"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def find_latest_matchup_file(directory: Path) -> Path:
    files = list(directory.glob("mlb_probable_pitchers_*.csv"))
    if not files:
        logger.error("No matchup files found.")
        return None
    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Using matchup file: {latest_file}")
    return latest_file

def extract_game_date_from_filename(filepath: Path) -> datetime.date:
    filename = filepath.name
    date_part = filename.replace("mlb_probable_pitchers_", "").replace(".csv", "")
    try:
        return datetime.strptime(date_part, "%Y-%m-%d").date()
    except Exception as e:
        logger.warning(f"Failed to parse date from filename '{filename}': {e}")
        return datetime.today().date()

def calculate_whip(hits, walks, innings_pitched):
    """Calculate WHIP (Walks + Hits per Innings Pitched)"""
    if innings_pitched == 0:
        return np.nan
    return (hits + walks) / innings_pitched

def calculate_fip(home_runs, walks, hit_by_pitch, strikeouts, innings_pitched, fip_constant=3.10):
    """Calculate FIP (Fielding Independent Pitching)"""
    if innings_pitched == 0:
        return np.nan
    return ((13 * home_runs + 3 * (walks + hit_by_pitch) - 2 * strikeouts) / innings_pitched) + fip_constant

def calculate_siera(strikeouts, walks, home_runs, total_batters_faced):
    """Simplified SIERA calculation (Skill-Interactive ERA)"""
    if total_batters_faced == 0:
        return np.nan
    
    # Simplified SIERA formula - actual formula is much more complex
    k_rate = strikeouts / total_batters_faced
    bb_rate = walks / total_batters_faced
    hr_rate = home_runs / total_batters_faced
    
    # Simplified approximation
    siera = 6.145 - 16.986 * k_rate + 11.434 * bb_rate - 1.858 * hr_rate + 7.653 * k_rate * bb_rate
    return max(0, siera)

def calculate_csw_rate(called_strikes, whiffs, total_pitches):
    """Calculate CSW% (Called Strike + Whiff %)"""
    if total_pitches == 0:
        return np.nan
    return (called_strikes + whiffs) / total_pitches * 100

def calculate_stuff_plus(velocity, spin_rate, movement_x, movement_z, league_avg_velocity=93.0):
    """Simplified Stuff+ calculation based on velocity and spin"""
    if pd.isna(velocity) or pd.isna(spin_rate):
        return np.nan
    
    # Simplified Stuff+ approximation (actual calculation is proprietary)
    velocity_factor = (velocity / league_avg_velocity - 1) * 100
    spin_factor = (spin_rate / 2300 - 1) * 50  # 2300 is approximate average
    movement_factor = 0
    
    if not pd.isna(movement_x) and not pd.isna(movement_z):
        movement_factor = (abs(movement_x) + abs(movement_z)) / 20
    
    stuff_plus = 100 + velocity_factor + spin_factor + movement_factor
    return max(0, stuff_plus)

def calculate_run_value_by_pitch(pitch_data):
    """Calculate run value by pitch type using linear weights"""
    # Simplified run values for different outcomes
    run_values = {
        'single': 0.47,
        'double': 0.77,
        'triple': 1.04,
        'home_run': 1.40,
        'walk': 0.31,
        'hit_by_pitch': 0.33,
        'strikeout': -0.10,
        'field_out': -0.10,
        'force_out': -0.10,
        'fielders_choice_out': -0.10,
        'grounded_into_double_play': -0.18
    }
    
    pitch_run_values = {}
    for pitch_type in pitch_data['pitch_type'].unique():
        if pd.isna(pitch_type):
            continue
            
        pitch_subset = pitch_data[pitch_data['pitch_type'] == pitch_type]
        total_run_value = 0
        
        for event in pitch_subset['events'].dropna():
            if event in run_values:
                total_run_value += run_values[event]
        
        pitch_count = len(pitch_subset)
        if pitch_count > 0:
            pitch_run_values[pitch_type] = total_run_value / pitch_count
        
    return pitch_run_values

def build_advanced_pitcher_features(matchup_path: Path):
    try:
        logger.info(f"Loading matchup file: {matchup_path}")
        matchups = pd.read_csv(matchup_path)

        end_date = extract_game_date_from_filename(matchup_path)
        start_date = end_date - timedelta(days=30)
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        pitcher_names = pd.concat([matchups['home_pitcher'], matchups['away_pitcher']]).dropna().unique()
        pitcher_names = [unidecode(name.strip()) for name in pitcher_names]
        logger.info(f"Found {len(pitcher_names)} unique pitchers: {pitcher_names}")

        pitcher_records = []
        for full_name in pitcher_names:
            if " " not in full_name:
                logger.warning(f"Invalid pitcher name format: {full_name}")
                continue
            first, last = full_name.split(" ", 1)
            lookup = playerid_lookup(last.title(), first.title())
            if lookup.empty or pd.isna(lookup['key_mlbam'].values[0]):
                logger.warning(f"MLBAM ID not found for: {full_name}")
                continue
            pitcher_records.append({'full_name': full_name, 'mlbam_id': int(lookup['key_mlbam'].values[0])})

        pitcher_df = pd.DataFrame(pitcher_records)
        if pitcher_df.empty:
            logger.warning("No valid pitchers with MLBAM IDs.")
            return None

        logger.info(f"Downloading Statcast data from {start_str} to {end_str}...")
        statcast_df = statcast(start_dt=start_str, end_dt=end_str)
        logger.info(f"Total Statcast rows pulled: {len(statcast_df)}")

        filtered_df = statcast_df[statcast_df['pitcher'].isin(pitcher_df['mlbam_id'])].copy()
        logger.info(f"Filtered to {len(filtered_df)} rows for target pitchers")

        filtered_df = filtered_df.merge(pitcher_df, how='left', left_on='pitcher', right_on='mlbam_id')

        # Enhanced pitch outcome categorization
        filtered_df['is_strikeout'] = filtered_df['events'].fillna('').str.lower().str.startswith('strikeout')
        filtered_df['is_walk'] = filtered_df['events'].fillna('').str.lower().str.contains('walk')
        filtered_df['is_hit'] = filtered_df['events'].fillna('').str.lower().str.contains('single|double|triple|home_run')
        filtered_df['is_home_run'] = filtered_df['events'].fillna('').str.lower().str.contains('home_run')
        filtered_df['is_hit_by_pitch'] = filtered_df['events'].fillna('').str.lower().str.contains('hit_by_pitch')
        
        filtered_df['is_whiff'] = filtered_df['description'].isin([
            'swinging_strike', 'swinging_strike_blocked', 'foul_tip'
        ])
        
        filtered_df['is_called_strike'] = filtered_df['description'].isin([
            'called_strike'
        ])

        # Calculate innings pitched (approximation: outs recorded / 3)
        filtered_df['outs_recorded'] = filtered_df['events'].notna() & ~filtered_df['events'].str.contains('walk|hit_by_pitch|single|double|triple|home_run', na=False)

        # Advanced metrics calculation
        pitcher_advanced_metrics = []
        
        for pitcher_id in filtered_df['pitcher'].unique():
            pitcher_data = filtered_df[filtered_df['pitcher'] == pitcher_id]
            
            # Basic counts
            total_pitches = len(pitcher_data)
            total_batters_faced = pitcher_data['at_bat_number'].nunique() if 'at_bat_number' in pitcher_data.columns else total_pitches / 4  # approximation
            
            strikeouts = pitcher_data['is_strikeout'].sum()
            walks = pitcher_data['is_walk'].sum()
            hits = pitcher_data['is_hit'].sum()
            home_runs = pitcher_data['is_home_run'].sum()
            hit_by_pitch = pitcher_data['is_hit_by_pitch'].sum()
            outs_recorded = pitcher_data['outs_recorded'].sum()
            innings_pitched = outs_recorded / 3 if outs_recorded > 0 else 0.1  # minimum to avoid division by zero
            
            whiffs = pitcher_data['is_whiff'].sum()
            called_strikes = pitcher_data['is_called_strike'].sum()
            
            # Advanced metrics
            whip = calculate_whip(hits, walks, innings_pitched)
            fip = calculate_fip(home_runs, walks, hit_by_pitch, strikeouts, innings_pitched)
            siera = calculate_siera(strikeouts, walks, home_runs, total_batters_faced)
            csw_rate = calculate_csw_rate(called_strikes, whiffs, total_pitches)
            
            # xStats (using estimated_ba_using_speedangle and estimated_woba_using_speedangle when available)
            xba = pitcher_data['estimated_ba_using_speedangle'].mean() if 'estimated_ba_using_speedangle' in pitcher_data.columns else np.nan
            xwoba = pitcher_data['estimated_woba_using_speedangle'].mean() if 'estimated_woba_using_speedangle' in pitcher_data.columns else np.nan
            xslg = pitcher_data['estimated_slg_using_speedangle'].mean() if 'estimated_slg_using_speedangle' in pitcher_data.columns else np.nan
            
            # xERA and xFIP approximation (simplified)
            xera = xwoba * 5.4 if not pd.isna(xwoba) else np.nan  # rough approximation
            xfip = fip * 0.9 if not pd.isna(fip) else np.nan  # simplified
            
            # Stuff+ calculation
            avg_velocity = pitcher_data['release_speed'].mean()
            avg_spin_rate = pitcher_data['release_spin_rate'].mean()
            avg_pfx_x = pitcher_data['pfx_x'].mean() if 'pfx_x' in pitcher_data.columns else np.nan
            avg_pfx_z = pitcher_data['pfx_z'].mean() if 'pfx_z' in pitcher_data.columns else np.nan
            stuff_plus = calculate_stuff_plus(avg_velocity, avg_spin_rate, avg_pfx_x, avg_pfx_z)
            
            # Run value by pitch type
            run_values_by_pitch = calculate_run_value_by_pitch(pitcher_data)
            
            pitcher_metrics = {
                'pitcher_id': pitcher_id,
                'full_name': pitcher_data['full_name'].iloc[0],
                'total_pitches': total_pitches,
                'innings_pitched': round(innings_pitched, 1),
                'whip': round(whip, 3) if not pd.isna(whip) else np.nan,
                'fip': round(fip, 3) if not pd.isna(fip) else np.nan,
                'siera': round(siera, 3) if not pd.isna(siera) else np.nan,
                'csw_rate': round(csw_rate, 1) if not pd.isna(csw_rate) else np.nan,
                'xera': round(xera, 3) if not pd.isna(xera) else np.nan,
                'xfip': round(xfip, 3) if not pd.isna(xfip) else np.nan,
                'xwoba': round(xwoba, 3) if not pd.isna(xwoba) else np.nan,
                'xba': round(xba, 3) if not pd.isna(xba) else np.nan,
                'xslg': round(xslg, 3) if not pd.isna(xslg) else np.nan,
                'stuff_plus': round(stuff_plus, 0) if not pd.isna(stuff_plus) else np.nan,
                'strikeout_rate': round((strikeouts / total_batters_faced) * 100, 1) if total_batters_faced > 0 else 0,
                'walk_rate': round((walks / total_batters_faced) * 100, 1) if total_batters_faced > 0 else 0
            }
            
            # Add run values by pitch type as separate columns
            for pitch_type, run_value in run_values_by_pitch.items():
                pitcher_metrics[f'{pitch_type}_run_value'] = round(run_value, 3)
            
            pitcher_advanced_metrics.append(pitcher_metrics)

        metrics_df = pd.DataFrame(pitcher_advanced_metrics)
        logger.info(f"Advanced metrics calculated for {len(metrics_df)} pitchers.")

        output_path = PROCESSED_DIR / f"advanced_pitcher_features_{end_str}.csv"
        metrics_df.to_csv(output_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
        logger.info(f"Saved advanced pitcher features to: {output_path}")
        print("\nAdvanced Pitcher Features:\n", metrics_df.to_string(index=False))
        return output_path

    except Exception as e:
        logger.error(f"Top-level failure: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Optional: Game date (YYYY-MM-DD)", type=str)
    args = parser.parse_args()

    if args.date:
        date_str = args.date
        matchup_path = RAW_HISTORICAL_MATCHUPS_DIR / f"historical_matchups_{date_str}.csv"
        if not matchup_path.exists():
            logger.error(f"Specified matchup file does not exist: {matchup_path}")
            exit(1)
    else:
        matchup_path = find_latest_matchup_file(RAW_DIR)
        if not matchup_path:
            logger.error("No matchup file found.")
            exit(1)

    result_path = build_advanced_pitcher_features(matchup_path)
    if result_path:
        df = pd.read_csv(result_path)
        print("\nReloaded Advanced Features CSV:\n", df.to_string(index=False))