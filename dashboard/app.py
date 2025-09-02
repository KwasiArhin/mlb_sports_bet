#!/usr/bin/env python3
"""
MLB Predictions Dashboard

A Flask web application for monitoring MLB prediction models, viewing betting
recommendations, and tracking performance metrics in real-time.
"""

import os
import json
import logging
import requests
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from flask import Flask, render_template, jsonify, request
from werkzeug.serving import WSGIRequestHandler
from team_logos import get_team_logo_path
from odds_service import odds_service
from fangraphs_service import get_fangraphs_predictions
from model_insights import get_model_insights, get_feature_importance
from team_comparison import compare_teams, get_all_teams

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress Flask request logs
class SilentWSGIRequestHandler(WSGIRequestHandler):
    def log_request(self, code='-', size='-'):
        pass

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'mlb-predictions-dashboard'

# Base directory for data files
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
PROCESSED_DIR = DATA_DIR / 'processed'
MODELS_DIR = BASE_DIR / 'modeling'
PLOTS_DIR = BASE_DIR / 'plots'


def load_todays_live_games():
    """Load today's live games from the pipeline API."""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        response = requests.get(
            f'http://localhost:5001/api/games/today?date={today}',
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and data.get('games', {}).get('games'):
                games_list = data['games']['games']
                
                # Convert to DataFrame for consistent interface
                games_df = pd.DataFrame(games_list)
                
                # Rename columns to match existing format
                games_df = games_df.rename(columns={
                    'home_team': 'Home Team',
                    'away_team': 'Away Team', 
                    'game_date': 'Game Date'
                })
                
                # Add placeholder columns for Kelly data
                games_df['Win Probability'] = 0.50  # Default 50/50
                games_df['Prediction'] = 'No prediction yet'
                games_df['Bet_Size'] = 0.0
                games_df['Team_To_Bet'] = ''
                games_df['Expected_Value'] = 0.0
                games_df['Kelly_Edge'] = False
                games_df['Odds_Used'] = 1.91
                games_df['Bankroll'] = 1000.0
                games_df['Live_Game'] = True  # Flag to identify live games
                
                return games_df
        
        return pd.DataFrame()
        
    except Exception as e:
        logger.warning(f"Could not fetch live games: {e}")
        return pd.DataFrame()


def load_latest_predictions():
    """Load the most recent prediction files, prioritizing today's generated predictions."""
    try:
        # First try to get today's date
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Look for Kelly predictions from today first (prioritize latest)
        kelly_files = list((MODELS_DIR / 'data' / 'predictions').glob('kelly_predictions_*.csv'))
        
        if kelly_files:
            # Get the most recent file
            latest_kelly = max(kelly_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Loading predictions from: {latest_kelly}")
            kelly_df = pd.read_csv(latest_kelly)
            
            # If we have Kelly predictions, use them as both sources
            return kelly_df, kelly_df
        
        # Find regular predictions for today if no Kelly predictions
        pred_files = list(PROCESSED_DIR.glob(f'readable_win_predictions_*{today}*.csv'))
        
        if not pred_files:
            # Fall back to any predictions
            pred_files = list(PROCESSED_DIR.glob('readable_win_predictions_*.csv'))
        
        if pred_files:
            latest_pred = max(pred_files, key=lambda p: p.stat().st_mtime)
            pred_df = pd.read_csv(latest_pred)
            return pred_df, pred_df
        
        # If no predictions found, try FanGraphs as backup
        logger.info("No predictions found, trying FanGraphs backup...")
        try:
            fangraphs_df = get_fangraphs_predictions()
            if not fangraphs_df.empty:
                logger.info(f"Found {len(fangraphs_df)} games from FanGraphs")
                return fangraphs_df, fangraphs_df
        except Exception as e:
            logger.warning(f"FanGraphs backup failed: {e}")
        
        # Final fallback: get live games 
        logger.info("Trying live games as final fallback...")
        live_games = load_todays_live_games()
        if not live_games.empty:
            logger.info(f"Found {len(live_games)} live games for today")
            return live_games, live_games
        
        return pd.DataFrame(), pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error loading predictions: {e}")
        # Try live games as fallback
        try:
            live_games = load_todays_live_games()
            return live_games, live_games
        except:
            return pd.DataFrame(), pd.DataFrame()


def load_model_metrics():
    """Load model performance metrics."""
    try:
        metrics_file = PROCESSED_DIR / 'model_metrics_log.csv'
        if metrics_file.exists():
            return pd.read_csv(metrics_file)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading model metrics: {e}")
        return pd.DataFrame()


def load_feature_importance():
    """Load feature importance data from XGBoost model."""
    try:
        # Check if feature importance plot exists
        importance_plot = PLOTS_DIR / 'feature_importance.png'
        if importance_plot.exists():
            return str(importance_plot)
        return None
    except Exception as e:
        logger.error(f"Error loading feature importance: {e}")
        return None


def get_pipeline_status():
    """Get status of data pipeline components."""
    status = {
        'last_update': 'Unknown',
        'data_freshness': 'Unknown',
        'model_status': 'Unknown',
        'files_status': []
    }
    
    try:
        # Check data freshness
        today = datetime.now().strftime('%Y-%m-%d')
        today_files = list(PROCESSED_DIR.glob(f'*{today}*.csv'))
        
        status['data_freshness'] = 'Fresh' if today_files else 'Stale'
        status['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Check model status
        model_files = list(MODELS_DIR.glob("rf_*.joblib"))
        if model_files:
            latest_model = max(model_files, key=lambda p: p.stat().st_mtime)
            model_age = datetime.now() - datetime.fromtimestamp(latest_model.stat().st_mtime)
            status['model_status'] = 'Fresh' if model_age.days < 7 else 'Outdated'
        else:
            status['model_status'] = 'Missing'
        
        # Check key files
        key_files = [
            'historical_main_features.csv',
            'model_metrics_log.csv',
            'prediction_accuracy_log.csv'
        ]
        
        for file in key_files:
            file_path = PROCESSED_DIR / file
            status['files_status'].append({
                'name': file,
                'exists': file_path.exists(),
                'modified': datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S') if file_path.exists() else None
            })
        
        # Check prediction files
        pred_files = list((MODELS_DIR / 'data' / 'predictions').glob('kelly_predictions_*.csv'))
        if pred_files:
            latest_pred = max(pred_files, key=lambda p: p.stat().st_mtime)
            pred_age = datetime.now() - datetime.fromtimestamp(latest_pred.stat().st_mtime)
            status['files_status'].append({
                'name': 'Latest Predictions',
                'exists': True,
                'modified': datetime.fromtimestamp(latest_pred.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'age_hours': pred_age.total_seconds() / 3600
            })
        
    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        status['error'] = str(e)
    
    return status


@app.route('/')
def home():
    """Dashboard home page with overview."""
    kelly_df, pred_df = load_latest_predictions()
    metrics_df = load_model_metrics()
    pipeline_status = get_pipeline_status()
    
    # Calculate summary stats
    summary_stats = {
        'total_games': len(pred_df) if not pred_df.empty else 0,
        'recommended_bets': len(kelly_df[kelly_df['Kelly_Edge'] == True]) if not kelly_df.empty else 0,
        'total_bet_amount': kelly_df['Bet_Size'].sum() if not kelly_df.empty else 0,
        'avg_win_prob': kelly_df[kelly_df['Kelly_Edge'] == True]['Win Probability'].mean() if not kelly_df.empty else 0,
        'latest_accuracy': metrics_df['accuracy'].iloc[-1] if not metrics_df.empty else 0,
        'bankroll_utilization': (kelly_df['Bet_Size'].sum() / kelly_df['Bankroll'].iloc[0] * 100) if not kelly_df.empty and len(kelly_df) > 0 else 0
    }
    
    return render_template('home.html', 
                         summary_stats=summary_stats,
                         pipeline_status=pipeline_status)


@app.route('/predictions')
def predictions():
    """Today's predictions with Kelly sizing."""
    kelly_df, pred_df = load_latest_predictions()
    
    # Prepare data for display
    if not kelly_df.empty:
        # Sort by bet size (highest first)
        display_df = kelly_df.sort_values('Bet_Size', ascending=False)
        
        # Format for display
        predictions_data = []
        for _, row in display_df.iterrows():
            away_team = row.get('Away Team', 'Unknown')
            home_team = row.get('Home Team', 'Unknown')
            team_to_bet = row.get('Team_To_Bet', 'None')
            win_prob = row.get('Win Probability', 0.5)
            
            # Determine which team the win probability refers to
            # The win probability in the data refers to the team that was predicted to win
            prediction = row.get('Prediction', '')
            
            # Calculate individual team probabilities
            if team_to_bet == away_team:
                away_win_prob = win_prob
                home_win_prob = 1 - win_prob
            elif team_to_bet == home_team:
                home_win_prob = win_prob
                away_win_prob = 1 - win_prob
            else:
                # Fallback if team_to_bet is unclear - assume 50/50
                away_win_prob = 0.5
                home_win_prob = 0.5
            
            predictions_data.append({
                'game_date': row.get('Game Date', 'Unknown'),
                'matchup': f"{away_team} @ {home_team}",
                'away_team': away_team,
                'home_team': home_team,
                'away_team_logo': get_team_logo_path(away_team),
                'home_team_logo': get_team_logo_path(home_team),
                'away_win_prob': f"{away_win_prob:.1%}",
                'home_win_prob': f"{home_win_prob:.1%}",
                'team_to_bet_logo': get_team_logo_path(team_to_bet),
                'win_probability': f"{win_prob:.1%}",  # Keep original for compatibility
                'team_to_bet': team_to_bet,
                'bet_size': f"${row.get('Bet_Size', 0):,.2f}",
                'expected_value': f"${row.get('Expected_Value', 0):+.2f}",
                'kelly_edge': row.get('Kelly_Edge', False),
                'odds_used': row.get('Odds_Used', 1.91)
            })
    else:
        predictions_data = []
    
    return render_template('predictions.html', predictions=predictions_data)


@app.route('/matchup-breakdown')
def matchup_breakdown():
    """Detailed breakdown of each matchup with comprehensive statistics."""
    kelly_df, pred_df = load_latest_predictions()
    
    # Prepare detailed matchup analysis
    matchup_details = []
    summary_stats = {
        'total_games': 0,
        'avg_win_prob': 0,
        'avg_bet_size': 0,
        'total_expected_value': 0,
        'recommended_bets': 0,
        'avg_odds': 0,
        'win_prob_range': {'min': 1, 'max': 0},
        'bet_size_range': {'min': float('inf'), 'max': 0},
        'teams_analysis': {}
    }
    
    if not kelly_df.empty:
        summary_stats['total_games'] = len(kelly_df)
        summary_stats['avg_win_prob'] = kelly_df['Win Probability'].mean()
        summary_stats['avg_bet_size'] = kelly_df['Bet_Size'].mean()
        summary_stats['total_expected_value'] = kelly_df['Expected_Value'].sum()
        summary_stats['recommended_bets'] = len(kelly_df[kelly_df['Kelly_Edge'] == True])
        summary_stats['avg_odds'] = kelly_df['Odds_Used'].mean()
        summary_stats['win_prob_range']['min'] = kelly_df['Win Probability'].min()
        summary_stats['win_prob_range']['max'] = kelly_df['Win Probability'].max()
        
        bet_sizes = kelly_df[kelly_df['Bet_Size'] > 0]['Bet_Size']
        if not bet_sizes.empty:
            summary_stats['bet_size_range']['min'] = bet_sizes.min()
            summary_stats['bet_size_range']['max'] = bet_sizes.max()
        
        # Analyze each matchup in detail
        for _, row in kelly_df.iterrows():
            away_team = row.get('Away Team', 'Unknown')
            home_team = row.get('Home Team', 'Unknown')
            team_to_bet = row.get('Team_To_Bet', 'None')
            win_prob = row.get('Win Probability', 0.5)
            
            # Calculate individual team probabilities
            if team_to_bet == away_team:
                away_win_prob = win_prob
                home_win_prob = 1 - win_prob
            elif team_to_bet == home_team:
                home_win_prob = win_prob
                away_win_prob = 1 - win_prob
            else:
                away_win_prob = 0.5
                home_win_prob = 0.5
            
            # Calculate additional metrics
            bet_size = row.get('Bet_Size', 0)
            expected_value = row.get('Expected_Value', 0)
            kelly_odds = row.get('Odds_Used', 1.91)
            kelly_implied_prob = 1 / kelly_odds if kelly_odds > 0 else 0.5
            kelly_edge = row.get('Kelly_Edge', False)
            
            # Fetch current market odds
            market_odds = odds_service.get_game_odds(away_team, home_team)
            
            matchup_detail = {
                'game_date': row.get('Game Date', 'Unknown'),
                'away_team': away_team,
                'home_team': home_team,
                'away_team_logo': get_team_logo_path(away_team),
                'home_team_logo': get_team_logo_path(home_team),
                'away_win_prob': away_win_prob,
                'home_win_prob': home_win_prob,
                'team_to_bet': team_to_bet,
                'team_to_bet_logo': get_team_logo_path(team_to_bet),
                'bet_size': bet_size,
                'expected_value': expected_value,
                'kelly_odds': kelly_odds,
                'kelly_implied_prob': kelly_implied_prob,
                'kelly_edge': kelly_edge,
                'edge_percentage': (win_prob - kelly_implied_prob) * 100 if kelly_edge else 0,
                'confidence': 'High' if abs(win_prob - 0.5) > 0.2 else 'Medium' if abs(win_prob - 0.5) > 0.1 else 'Low',
                # Market odds data
                'market_odds': market_odds,
                'away_market_odds': market_odds.get('away_odds', -110),
                'home_market_odds': market_odds.get('home_odds', -110),
                'away_market_implied_prob': market_odds.get('away_implied_prob', 0.5),
                'home_market_implied_prob': market_odds.get('home_implied_prob', 0.5),
                'away_edge': (away_win_prob - market_odds.get('away_implied_prob', 0.5)) * 100,
                'home_edge': (home_win_prob - market_odds.get('home_implied_prob', 0.5)) * 100,
                'odds_source': market_odds.get('source', 'Demo')
            }
            
            matchup_details.append(matchup_detail)
            
            # Track team statistics
            for team in [away_team, home_team]:
                if team not in summary_stats['teams_analysis']:
                    summary_stats['teams_analysis'][team] = {
                        'games': 0, 'total_win_prob': 0, 'recommended_bets': 0, 
                        'total_bet_amount': 0, 'total_expected_value': 0
                    }
                
                team_stats = summary_stats['teams_analysis'][team]
                team_stats['games'] += 1
                
                if team == team_to_bet:
                    team_stats['total_win_prob'] += win_prob
                    if kelly_edge:
                        team_stats['recommended_bets'] += 1
                        team_stats['total_bet_amount'] += bet_size
                        team_stats['total_expected_value'] += expected_value
                else:
                    team_stats['total_win_prob'] += (1 - win_prob)
        
        # Calculate team averages
        for team, stats in summary_stats['teams_analysis'].items():
            if stats['games'] > 0:
                stats['avg_win_prob'] = stats['total_win_prob'] / stats['games']
                stats['avg_bet_amount'] = stats['total_bet_amount'] / stats['games'] if stats['games'] > 0 else 0
    
    return render_template('matchup_breakdown.html', 
                         matchups=matchup_details, 
                         summary=summary_stats)


@app.route('/model-analytics')
def model_analytics():
    """Model performance and analytics."""
    metrics_df = load_model_metrics()
    feature_importance_path = load_feature_importance()
    
    # Prepare metrics data for charts
    metrics_data = {}
    if not metrics_df.empty:
        metrics_data = {
            'dates': metrics_df['date'].tolist(),
            'accuracy': metrics_df['accuracy'].tolist(),
            'mae': metrics_df['mae'].tolist(),
            'mse': metrics_df['mse'].tolist(),
            'mape': metrics_df['mape'].tolist()
        }
    
    return render_template('model_analytics.html', 
                         metrics_data=metrics_data,
                         feature_importance=feature_importance_path)


@app.route('/pipeline-status')
def pipeline_status():
    """Data pipeline status and monitoring."""
    status = get_pipeline_status()
    return render_template('pipeline_status.html', status=status)


@app.route('/historical')
def historical():
    """Historical performance and P&L tracking."""
    # Load historical prediction accuracy
    accuracy_files = list(PROCESSED_DIR.glob('prediction_accuracy_log.csv'))
    
    historical_data = []
    if accuracy_files:
        try:
            accuracy_df = pd.read_csv(accuracy_files[0])
            
            # Group by date and aggregate stats
            daily_stats = accuracy_df.groupby('date').agg({
                'correct_prediction': ['count', 'sum'],
                'bet_amount': 'sum',
                'profit_loss': 'sum'
            }).reset_index()
            
            # Flatten column names
            daily_stats.columns = ['date', 'games', 'correct', 'total_bet', 'profit_loss']
            daily_stats['incorrect'] = daily_stats['games'] - daily_stats['correct']
            daily_stats['accuracy'] = (daily_stats['correct'] / daily_stats['games'] * 100).round(1)
            
            historical_data = daily_stats.to_dict('records')
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
    
    return render_template('historical.html', historical_data=historical_data)


@app.route('/model-insights')
def model_insights():
    """Deep model insights and interpretability."""
    try:
        insights = get_model_insights()
        feature_importance = get_feature_importance()
        return render_template('model_insights.html', 
                             insights=insights, 
                             feature_importance=feature_importance)
    except Exception as e:
        logger.error(f"Error loading model insights: {e}")
        return render_template('model_insights.html', 
                             insights={'error': str(e)}, 
                             feature_importance={})


@app.route('/team-comparison')
def team_comparison():
    """Head-to-head team comparison tool."""
    team1 = request.args.get('team1', '').upper()
    team2 = request.args.get('team2', '').upper()
    
    # Get all teams for dropdown
    all_teams = get_all_teams()
    
    comparison_data = {}
    if team1 and team2 and team1 != team2:
        try:
            comparison_data = compare_teams(team1, team2)
            # Add team logos
            if 'team1' in comparison_data:
                comparison_data['team1']['logo'] = get_team_logo_path(team1)
            if 'team2' in comparison_data:
                comparison_data['team2']['logo'] = get_team_logo_path(team2)
        except Exception as e:
            logger.error(f"Error comparing teams {team1} vs {team2}: {e}")
            comparison_data = {'error': str(e)}
    
    return render_template('team_comparison.html', 
                         comparison=comparison_data,
                         all_teams=all_teams,
                         selected_team1=team1,
                         selected_team2=team2)


@app.route('/api/predictions')
def api_predictions():
    """API endpoint for predictions data."""
    kelly_df, pred_df = load_latest_predictions()
    
    if not kelly_df.empty:
        # Convert to JSON-friendly format
        data = kelly_df.to_dict('records')
        return jsonify({'success': True, 'data': data})
    else:
        return jsonify({'success': False, 'error': 'No predictions available'})


@app.route('/api/metrics')
def api_metrics():
    """API endpoint for model metrics."""
    metrics_df = load_model_metrics()
    
    if not metrics_df.empty:
        data = metrics_df.to_dict('records')
        return jsonify({'success': True, 'data': data})
    else:
        return jsonify({'success': False, 'error': 'No metrics available'})


@app.route('/api/status')
def api_status():
    """API endpoint for pipeline status."""
    status = get_pipeline_status()
    return jsonify(status)


@app.errorhandler(404)
def not_found(error):
    """404 error handler."""
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    """500 error handler."""
    logger.error(f"Internal server error: {error}")
    return render_template('500.html'), 500


if __name__ == '__main__':
    print(f"""
    ⚾ MLB Predictions Dashboard Starting ⚾
    
    📊 Dashboard URL: http://localhost:5000
    📁 Data Directory: {DATA_DIR}
    🔧 Models Directory: {MODELS_DIR}
    
    Available Pages:
    - / : Home overview
    - /predictions : Today's betting recommendations  
    - /model-analytics : Model performance metrics
    - /pipeline-status : Data pipeline monitoring
    - /historical : Historical performance tracking
    
    Press Ctrl+C to stop the server
    """)
    
    app.run(
        host='0.0.0.0', 
        port=5000, 
        debug=True,
        request_handler=SilentWSGIRequestHandler
    )