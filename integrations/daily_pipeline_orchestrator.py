#!/usr/bin/env python3
"""
Daily MLB Pipeline Orchestrator

Coordinates the complete daily pipeline from live game fetching through
Kelly Criterion betting recommendations with error handling and monitoring.
"""

import os
import sys
import json
import logging
import subprocess
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import time

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from integrations.live_games_fetcher import MLBGamesFetcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
PROCESSED_DIR = DATA_DIR / 'processed'
RAW_DIR = DATA_DIR / 'raw'
MODELING_DIR = BASE_DIR / 'modeling'
FEATURES_DIR = BASE_DIR / 'features'


class PipelineError(Exception):
    """Custom exception for pipeline errors."""
    pass


class DailyPipelineOrchestrator:
    """Orchestrates the complete daily MLB prediction pipeline."""
    
    def __init__(self, date: Optional[str] = None, bankroll: float = 1000.0):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            date: Date in YYYY-MM-DD format, defaults to today
            bankroll: Available bankroll for betting
        """
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        self.bankroll = bankroll
        self.games_fetcher = MLBGamesFetcher()
        self.pipeline_status = {
            'started': datetime.now().isoformat(),
            'date': self.date,
            'steps_completed': [],
            'steps_failed': [],
            'current_step': None,
            'total_games': 0,
            'recommended_bets': 0,
            'total_bet_amount': 0.0,
            'errors': []
        }
        
        logger.info(f"Initializing daily pipeline for {self.date} with ${bankroll:,.2f} bankroll")
    
    def step(self, step_name: str, description: str = ""):
        """Log and track pipeline step."""
        self.pipeline_status['current_step'] = step_name
        logger.info(f"🔄 Starting: {step_name}" + (f" - {description}" if description else ""))
    
    def step_success(self, step_name: str, details: str = ""):
        """Mark step as successful."""
        self.pipeline_status['steps_completed'].append({
            'step': step_name,
            'completed_at': datetime.now().isoformat(),
            'details': details
        })
        logger.info(f"✅ Completed: {step_name}" + (f" - {details}" if details else ""))
    
    def step_failed(self, step_name: str, error: str):
        """Mark step as failed."""
        self.pipeline_status['steps_failed'].append({
            'step': step_name,
            'failed_at': datetime.now().isoformat(),
            'error': error
        })
        self.pipeline_status['errors'].append(f"{step_name}: {error}")
        logger.error(f"❌ Failed: {step_name} - {error}")
    
    def fetch_todays_games(self) -> List[Dict]:
        """Step 1: Fetch today's MLB games."""
        self.step("fetch_games", "Getting today's MLB schedule")
        
        try:
            result = self.games_fetcher.get_live_games_with_status(self.date)
            
            if result['status'] == 'no_games':
                raise PipelineError(f"No games found for {self.date}")
            
            games = result['games']
            self.pipeline_status['total_games'] = len(games)
            
            # Save games to CSV for other pipeline components
            games_file = RAW_DIR / f"mlb_games_{self.date}.csv"
            df = pd.DataFrame(games)
            df.to_csv(games_file, index=False)
            
            # Also create in expected format for existing pipeline
            matchups_file = RAW_DIR / f"mlb_probable_pitchers_{self.date}.csv"
            matchup_df = df[['game_date', 'home_team', 'away_team', 'home_probable_pitcher', 'away_probable_pitcher']].copy()
            matchup_df.rename(columns={
                'home_probable_pitcher': 'home_pitcher',
                'away_probable_pitcher': 'away_pitcher'
            }, inplace=True)
            matchup_df.to_csv(matchups_file, index=False)
            
            self.step_success("fetch_games", f"Found {len(games)} games, saved to {games_file}")
            return games
            
        except Exception as e:
            self.step_failed("fetch_games", str(e))
            raise PipelineError(f"Failed to fetch games: {e}")
    
    def run_feature_engineering(self) -> bool:
        """Step 2: Run feature engineering pipeline."""
        self.step("feature_engineering", "Generating features for today's games")
        
        try:
            # Run the main features script
            main_features_script = FEATURES_DIR / "main_features.py"
            
            if not main_features_script.exists():
                raise PipelineError(f"Main features script not found: {main_features_script}")
            
            # Execute feature engineering
            cmd = [sys.executable, str(main_features_script)]
            result = subprocess.run(
                cmd, 
                capture_output=True, 
                text=True, 
                timeout=300,  # 5 minutes timeout
                cwd=str(FEATURES_DIR)
            )
            
            if result.returncode != 0:
                error_msg = f"Feature engineering failed: {result.stderr}"
                raise PipelineError(error_msg)
            
            # Check if features file was created
            expected_features_file = PROCESSED_DIR / f"main_features_{self.date}.csv"
            if not expected_features_file.exists():
                raise PipelineError(f"Expected features file not created: {expected_features_file}")
            
            self.step_success("feature_engineering", f"Features saved to {expected_features_file}")
            return True
            
        except subprocess.TimeoutExpired:
            self.step_failed("feature_engineering", "Feature engineering timed out (5 minutes)")
            raise PipelineError("Feature engineering timed out")
        except Exception as e:
            self.step_failed("feature_engineering", str(e))
            raise PipelineError(f"Feature engineering failed: {e}")
    
    def run_model_prediction(self) -> str:
        """Step 3: Run model prediction."""
        self.step("model_prediction", "Running XGBoost model predictions")
        
        try:
            # Use XGBoost model for predictions
            xgb_script = MODELING_DIR / "train_xgb.py"
            
            if not xgb_script.exists():
                raise PipelineError(f"XGBoost script not found: {xgb_script}")
            
            # Execute model prediction
            cmd = [sys.executable, str(xgb_script)]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minutes timeout
                cwd=str(MODELING_DIR)
            )
            
            if result.returncode != 0:
                error_msg = f"Model prediction failed: {result.stderr}"
                raise PipelineError(error_msg)
            
            # Find the generated predictions file
            prediction_files = list(PROCESSED_DIR.glob(f"readable_win_predictions_for_{self.date}_using_*.csv"))
            
            if not prediction_files:
                raise PipelineError(f"No prediction file found for {self.date}")
            
            # Use the most recent prediction file
            predictions_file = max(prediction_files, key=lambda p: p.stat().st_mtime)
            
            self.step_success("model_prediction", f"Predictions saved to {predictions_file}")
            return str(predictions_file)
            
        except subprocess.TimeoutExpired:
            self.step_failed("model_prediction", "Model prediction timed out (10 minutes)")
            raise PipelineError("Model prediction timed out")
        except Exception as e:
            self.step_failed("model_prediction", str(e))
            raise PipelineError(f"Model prediction failed: {e}")
    
    def run_kelly_sizing(self, predictions_file: str) -> str:
        """Step 4: Apply Kelly Criterion bet sizing."""
        self.step("kelly_sizing", "Calculating optimal bet sizes")
        
        try:
            kelly_script = MODELING_DIR / "predict_with_kelly.py"
            
            if not kelly_script.exists():
                raise PipelineError(f"Kelly script not found: {kelly_script}")
            
            # Execute Kelly sizing
            cmd = [
                sys.executable, str(kelly_script),
                '--predictions', predictions_file,
                '--bankroll', str(self.bankroll),
                '--odds', '1.91',  # Standard -110 odds
                '--max-fraction', '0.25'
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,  # 2 minutes timeout
                cwd=str(MODELING_DIR)
            )
            
            if result.returncode != 0:
                error_msg = f"Kelly sizing failed: {result.stderr}"
                raise PipelineError(error_msg)
            
            # Find the generated Kelly file
            kelly_files = list((MODELING_DIR / "data" / "predictions").glob("kelly_predictions_*.csv"))
            
            if not kelly_files:
                raise PipelineError("No Kelly predictions file found")
            
            # Use the most recent Kelly file
            kelly_file = max(kelly_files, key=lambda p: p.stat().st_mtime)
            
            # Extract betting summary from Kelly file
            kelly_df = pd.read_csv(kelly_file)
            recommended_bets = len(kelly_df[kelly_df['Kelly_Edge'] == True])
            total_bet_amount = kelly_df['Bet_Size'].sum()
            
            self.pipeline_status['recommended_bets'] = recommended_bets
            self.pipeline_status['total_bet_amount'] = total_bet_amount
            
            self.step_success("kelly_sizing", f"Kelly sizing completed: {recommended_bets} bets, ${total_bet_amount:.2f} total")
            return str(kelly_file)
            
        except subprocess.TimeoutExpired:
            self.step_failed("kelly_sizing", "Kelly sizing timed out (2 minutes)")
            raise PipelineError("Kelly sizing timed out")
        except Exception as e:
            self.step_failed("kelly_sizing", str(e))
            raise PipelineError(f"Kelly sizing failed: {e}")
    
    def update_dashboard_data(self, kelly_file: str) -> bool:
        """Step 5: Update dashboard with new data."""
        self.step("update_dashboard", "Updating dashboard data")
        
        try:
            # The dashboard automatically picks up the latest files,
            # so we just need to ensure files are in the right locations
            
            # Verify Kelly file exists and has data
            if not Path(kelly_file).exists():
                raise PipelineError(f"Kelly file not found: {kelly_file}")
            
            kelly_df = pd.read_csv(kelly_file)
            if kelly_df.empty:
                raise PipelineError("Kelly predictions file is empty")
            
            # Create a summary file for easy dashboard access
            summary = {
                'date': self.date,
                'total_games': self.pipeline_status['total_games'],
                'recommended_bets': self.pipeline_status['recommended_bets'],
                'total_bet_amount': self.pipeline_status['total_bet_amount'],
                'bankroll': self.bankroll,
                'utilization_pct': (self.pipeline_status['total_bet_amount'] / self.bankroll) * 100,
                'last_updated': datetime.now().isoformat(),
                'kelly_file': kelly_file,
                'pipeline_status': 'completed'
            }
            
            summary_file = PROCESSED_DIR / f"daily_summary_{self.date}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            self.step_success("update_dashboard", f"Dashboard data updated, summary saved to {summary_file}")
            return True
            
        except Exception as e:
            self.step_failed("update_dashboard", str(e))
            raise PipelineError(f"Dashboard update failed: {e}")
    
    def run_complete_pipeline(self) -> Dict[str, Any]:
        """Run the complete daily pipeline."""
        logger.info(f"🚀 Starting complete daily pipeline for {self.date}")
        
        start_time = datetime.now()
        
        try:
            # Step 1: Fetch games
            games = self.fetch_todays_games()
            
            # Step 2: Feature engineering
            self.run_feature_engineering()
            
            # Step 3: Model prediction
            predictions_file = self.run_model_prediction()
            
            # Step 4: Kelly sizing
            kelly_file = self.run_kelly_sizing(predictions_file)
            
            # Step 5: Update dashboard
            self.update_dashboard_data(kelly_file)
            
            # Pipeline completed successfully
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.pipeline_status.update({
                'status': 'completed',
                'completed_at': end_time.isoformat(),
                'duration_seconds': duration,
                'current_step': None
            })
            
            logger.info(f"🎉 Pipeline completed successfully in {duration:.1f} seconds")
            logger.info(f"📊 Summary: {self.pipeline_status['total_games']} games, {self.pipeline_status['recommended_bets']} bets, ${self.pipeline_status['total_bet_amount']:.2f} total")
            
            return self.pipeline_status
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.pipeline_status.update({
                'status': 'failed',
                'failed_at': end_time.isoformat(),
                'duration_seconds': duration,
                'final_error': str(e)
            })
            
            logger.error(f"💥 Pipeline failed after {duration:.1f} seconds: {e}")
            logger.error(f"📊 Completed steps: {len(self.pipeline_status['steps_completed'])}")
            logger.error(f"📊 Failed steps: {len(self.pipeline_status['steps_failed'])}")
            
            return self.pipeline_status
    
    def get_status(self) -> Dict[str, Any]:
        """Get current pipeline status."""
        return self.pipeline_status.copy()


def main():
    """Main function for standalone usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="MLB Daily Pipeline Orchestrator")
    parser.add_argument('--date', '-d', help='Date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--bankroll', '-b', type=float, default=1000.0, help='Available bankroll (default: $1000)')
    parser.add_argument('--status-only', action='store_true', help='Only show status, don\'t run pipeline')
    parser.add_argument('--dry-run', action='store_true', help='Validate setup without running pipeline')
    
    args = parser.parse_args()
    
    orchestrator = DailyPipelineOrchestrator(args.date, args.bankroll)
    
    if args.dry_run:
        print(f"🧪 Dry run mode for {orchestrator.date}")
        print(f"💰 Bankroll: ${args.bankroll:,.2f}")
        print("✅ Pipeline orchestrator initialized successfully")
        return 0
    
    if args.status_only:
        status = orchestrator.get_status()
        print(json.dumps(status, indent=2))
        return 0
    
    # Run complete pipeline
    result = orchestrator.run_complete_pipeline()
    
    # Print final summary
    print("\n" + "="*80)
    print("DAILY PIPELINE SUMMARY")
    print("="*80)
    print(f"Date: {result['date']}")
    print(f"Status: {result['status'].upper()}")
    print(f"Duration: {result.get('duration_seconds', 0):.1f} seconds")
    print(f"Total Games: {result['total_games']}")
    print(f"Recommended Bets: {result['recommended_bets']}")
    print(f"Total Bet Amount: ${result['total_bet_amount']:,.2f}")
    print(f"Bankroll Utilization: {(result['total_bet_amount']/args.bankroll)*100:.1f}%")
    
    if result['status'] == 'failed':
        print(f"Final Error: {result.get('final_error', 'Unknown')}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())