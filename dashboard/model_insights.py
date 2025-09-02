#!/usr/bin/env python3
"""
Model Insights and Analysis Service

Provides deep insights into model performance, feature importance, 
prediction confidence, and areas for improvement.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import joblib
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Set up plotting style
plt.style.use('default')
sns.set_palette("husl")

logger = logging.getLogger(__name__)

class ModelInsightsService:
    """Service for analyzing model performance and generating insights."""
    
    def __init__(self, base_dir: Path = None):
        self.base_dir = base_dir or Path(__file__).parent.parent
        self.models_dir = self.base_dir / 'modeling'
        self.data_dir = self.base_dir / 'data'
        self.processed_dir = self.data_dir / 'processed'
        self.plots_dir = self.base_dir / 'plots'
        self.plots_dir.mkdir(exist_ok=True)
        
        # Load latest model
        self.model = None
        self.feature_names = []
        self.load_latest_model()
        
    def load_latest_model(self) -> bool:
        """Load the most recent trained model."""
        try:
            # Find latest model file
            model_files = list(self.models_dir.glob("rf_*.joblib"))
            if not model_files:
                logger.error("No model files found")
                return False
            
            latest_model = max(model_files, key=lambda p: p.stat().st_mtime)
            self.model = joblib.load(latest_model)
            
            # Load feature names
            feature_file = latest_model.parent / f"{latest_model.stem}_features.txt"
            if feature_file.exists():
                with open(feature_file, 'r') as f:
                    self.feature_names = [line.strip() for line in f.readlines()]
            
            logger.info(f"Loaded model from {latest_model}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False
    
    def get_feature_importance_analysis(self) -> Dict:
        """Analyze feature importance with categorization."""
        if not self.model or not self.feature_names:
            return {}
        
        importances = self.model.feature_importances_
        
        # Categorize features
        categories = {
            'Pitcher Mechanics': ['velocity', 'spin_rate', 'extension', 'total_pitches'],
            'Pitcher Performance': ['strikeouts', 'whiffs', 'games_played'],
            'Batting Metrics': ['bat_speed', 'launch_angle', 'exit_velocity', 'swing_length'],
            'Team Record': ['wins', 'losses', 'win_pct', 'run_diff', 'games_played']
        }
        
        # Group importances by category
        category_importance = {cat: 0 for cat in categories}
        feature_details = []
        
        for i, (feature, importance) in enumerate(zip(self.feature_names, importances)):
            feature_details.append({
                'feature': feature,
                'importance': importance,
                'rank': i + 1
            })
            
            # Assign to category
            for category, keywords in categories.items():
                if any(keyword in feature.lower() for keyword in keywords):
                    category_importance[category] += importance
                    break
        
        # Sort features by importance
        feature_details.sort(key=lambda x: x['importance'], reverse=True)
        
        return {
            'feature_details': feature_details,
            'category_importance': category_importance,
            'top_features': feature_details[:10],
            'model_info': {
                'type': type(self.model).__name__,
                'n_estimators': getattr(self.model, 'n_estimators', 'N/A'),
                'max_depth': getattr(self.model, 'max_depth', 'N/A'),
                'total_features': len(self.feature_names)
            }
        }
    
    def analyze_prediction_confidence(self, predictions_file: str = None) -> Dict:
        """Analyze prediction confidence and edge cases."""
        try:
            if not predictions_file:
                # Find latest predictions
                pred_files = list((self.models_dir / 'data' / 'predictions').glob('kelly_predictions_*.csv'))
                if not pred_files:
                    return {'error': 'No prediction files found'}
                predictions_file = max(pred_files, key=lambda p: p.stat().st_mtime)
            
            df = pd.read_csv(predictions_file)
            
            # Analyze confidence metrics
            analysis = {
                'total_predictions': len(df),
                'avg_win_probability': df['Win Probability'].mean(),
                'confidence_distribution': {
                    'high_confidence': len(df[df['Win Probability'] > 0.65]),  # >65%
                    'medium_confidence': len(df[(df['Win Probability'] >= 0.55) & (df['Win Probability'] <= 0.65)]),  # 55-65%
                    'low_confidence': len(df[df['Win Probability'] < 0.55])  # <55%
                },
                'betting_recommendations': {
                    'recommended_bets': len(df[df['Kelly_Edge'] == True]),
                    'total_bet_amount': df['Bet_Size'].sum(),
                    'avg_bet_size': df['Bet_Size'].mean(),
                    'total_expected_value': df['Expected_Value'].sum()
                }
            }
            
            # Most/least confident predictions
            df_sorted = df.sort_values('Win Probability', ascending=False)
            analysis['most_confident'] = df_sorted.head(3)[['Away Team', 'Home Team', 'Win Probability', 'Team_To_Bet']].to_dict('records')
            analysis['least_confident'] = df_sorted.tail(3)[['Away Team', 'Home Team', 'Win Probability', 'Team_To_Bet']].to_dict('records')
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing prediction confidence: {e}")
            return {'error': str(e)}
    
    def identify_model_weaknesses(self) -> Dict:
        """Identify potential areas for model improvement."""
        weaknesses = {
            'data_limitations': [],
            'feature_gaps': [],
            'model_architecture': [],
            'recommendations': []
        }
        
        # Data limitations
        try:
            df = pd.read_csv(self.processed_dir / 'historical_main_features.csv')
            n_games = len(df)
            
            if n_games < 500:
                weaknesses['data_limitations'].append(f"Limited training data: {n_games} games (recommend >1000)")
            
            # Check date range
            if 'game_date' in df.columns:
                date_range = pd.to_datetime(df['game_date']).max() - pd.to_datetime(df['game_date']).min()
                if date_range.days < 180:
                    weaknesses['data_limitations'].append(f"Short date range: {date_range.days} days (recommend full season+)")
                    
        except Exception as e:
            weaknesses['data_limitations'].append("Could not analyze training data")
        
        # Feature gaps (based on modern ML best practices)
        missing_features = [
            "Weather conditions (temp, humidity, wind)",
            "Ballpark factors (dimensions, altitude)",
            "Recent form (L10 games, streaks)", 
            "Rest days between games",
            "Historical matchup data",
            "Platoon splits (L/R matchups)",
            "Bullpen strength metrics",
            "Injury reports",
            "Vegas line movement",
            "Public betting percentages"
        ]
        weaknesses['feature_gaps'] = missing_features
        
        # Model architecture suggestions
        if self.model:
            if hasattr(self.model, 'n_estimators') and self.model.n_estimators < 200:
                weaknesses['model_architecture'].append("Consider increasing n_estimators to 200-500")
            if hasattr(self.model, 'max_depth') and self.model.max_depth < 15:
                weaknesses['model_architecture'].append("Consider deeper trees (max_depth=15-20)")
                
        # Recommendations
        weaknesses['recommendations'] = [
            "Collect more historical data (3+ seasons)",
            "Add weather API integration",
            "Include ballpark factors dataset",
            "Implement rolling statistics (L10, L20 games)",
            "Add ensemble methods (XGBoost, LightGBM)",
            "Implement cross-validation for better evaluation",
            "Add feature engineering pipeline",
            "Track prediction accuracy over time"
        ]
        
        return weaknesses
    
    def generate_performance_report(self) -> Dict:
        """Generate comprehensive model performance report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'model_overview': {},
            'feature_analysis': {},
            'prediction_analysis': {},
            'improvement_areas': {}
        }
        
        try:
            # Model overview
            if self.model:
                report['model_overview'] = {
                    'model_type': type(self.model).__name__,
                    'n_features': len(self.feature_names),
                    'feature_names': self.feature_names,
                    'model_params': self.model.get_params() if hasattr(self.model, 'get_params') else {}
                }
            
            # Feature analysis
            report['feature_analysis'] = self.get_feature_importance_analysis()
            
            # Prediction analysis
            report['prediction_analysis'] = self.analyze_prediction_confidence()
            
            # Improvement areas
            report['improvement_areas'] = self.identify_model_weaknesses()
            
        except Exception as e:
            logger.error(f"Error generating performance report: {e}")
            report['error'] = str(e)
        
        return report
    
    def create_feature_importance_plot(self) -> str:
        """Create and save feature importance visualization."""
        if not self.model or not self.feature_names:
            return None
            
        try:
            importances = self.model.feature_importances_
            
            # Create DataFrame for plotting
            df = pd.DataFrame({
                'feature': self.feature_names,
                'importance': importances
            }).sort_values('importance', ascending=True)
            
            # Create plot
            plt.figure(figsize=(12, 8))
            plt.barh(range(len(df)), df['importance'])
            plt.yticks(range(len(df)), df['feature'])
            plt.xlabel('Feature Importance')
            plt.title('Model Feature Importance Analysis')
            plt.tight_layout()
            
            # Save plot
            plot_path = self.plots_dir / f'feature_importance_{datetime.now().strftime("%Y%m%d_%H%M")}.png'
            plt.savefig(plot_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(plot_path)
            
        except Exception as e:
            logger.error(f"Error creating feature importance plot: {e}")
            return None

# Global service instance
insights_service = ModelInsightsService()

def get_model_insights() -> Dict:
    """Get comprehensive model insights."""
    return insights_service.generate_performance_report()

def get_feature_importance() -> Dict:
    """Get feature importance analysis."""
    return insights_service.get_feature_importance_analysis()