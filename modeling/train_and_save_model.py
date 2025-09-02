#!/usr/bin/env python3
"""
Train and Save MLB Model

Creates a trained ML model for win predictions and saves it for use by the prediction service.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import logging
import joblib
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Root directory setup
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR = Path(__file__).parent


def create_demo_model():
    """Create a demo model with synthetic data for testing."""
    logger.info("Creating demo model with synthetic data...")
    
    # Generate synthetic training data
    np.random.seed(42)
    n_samples = 1000
    n_features = 20
    
    # Feature names that match what we expect
    feature_names = [
        'home_batting_avg', 'away_batting_avg', 
        'home_era', 'away_era',
        'home_runs_scored', 'away_runs_scored',
        'home_runs_allowed', 'away_runs_allowed',
        'home_wins', 'away_wins',
        'home_losses', 'away_losses',
        'home_on_base_pct', 'away_on_base_pct',
        'home_slugging_pct', 'away_slugging_pct',
        'home_fielding_pct', 'away_fielding_pct',
        'home_stolen_bases', 'away_stolen_bases'
    ]
    
    # Create realistic baseball statistics
    X = np.random.normal(0, 1, (n_samples, len(feature_names)))
    
    # Make some features correlated to create realistic patterns
    # Better batting average = more wins
    X[:, 0] = np.random.normal(0.250, 0.030, n_samples)  # home_batting_avg
    X[:, 1] = np.random.normal(0.250, 0.030, n_samples)  # away_batting_avg
    X[:, 2] = np.random.normal(4.20, 0.80, n_samples)    # home_era
    X[:, 3] = np.random.normal(4.20, 0.80, n_samples)    # away_era
    
    # Create target variable with some logic
    # Home team wins if they have better stats
    home_advantage = X[:, 0] - X[:, 1] + (X[:, 3] - X[:, 2]) * 0.1
    y = (home_advantage + np.random.normal(0, 0.1, n_samples) > 0).astype(int)
    
    # Add home field advantage (slight bias toward home wins)
    home_bias = np.random.normal(0.05, 0.02, n_samples)
    y = ((home_advantage + home_bias) > 0).astype(int)
    
    X_df = pd.DataFrame(X, columns=feature_names)
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X_df, y, test_size=0.2, random_state=42
    )
    
    # Train model
    logger.info(f"Training RandomForest on {len(X_train)} samples...")
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        class_weight='balanced'
    )
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    logger.info(f"Model accuracy: {accuracy:.3f}")
    
    # Save model
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    model_path = MODELS_DIR / f"rf_model_{timestamp}.joblib"
    features_path = MODELS_DIR / f"rf_model_{timestamp}_features.txt"
    
    joblib.dump(model, model_path)
    logger.info(f"Saved model to: {model_path}")
    
    # Save feature names
    with open(features_path, 'w') as f:
        f.write('\n'.join(feature_names))
    logger.info(f"Saved feature names to: {features_path}")
    
    return model_path, feature_names, accuracy


def train_from_historical_data():
    """Train model from actual historical data if available."""
    historical_file = PROCESSED_DIR / "historical_main_features.csv"
    
    if not historical_file.exists():
        logger.warning(f"Historical data not found at {historical_file}")
        return None, None, None
    
    try:
        logger.info(f"Loading historical data from {historical_file}")
        df = pd.read_csv(historical_file)
        
        if df.empty:
            logger.warning("Historical data file is empty")
            return None, None, None
        
        # Define target and features
        if 'actual_winner' not in df.columns:
            logger.error("No 'actual_winner' column found in historical data")
            return None, None, None
        
        # Non-feature columns
        non_feature_cols = [
            "actual_winner", "game_date", "home_team", "away_team",
            "home_pitcher", "away_pitcher", "home_pitcher_full_name", "away_pitcher_full_name"
        ]
        
        # Get numeric feature columns
        numeric_cols = [
            col for col in df.columns 
            if col not in non_feature_cols and pd.api.types.is_numeric_dtype(df[col])
        ]
        
        if len(numeric_cols) < 5:
            logger.warning(f"Too few numeric features found: {len(numeric_cols)}")
            return None, None, None
        
        X = df[numeric_cols].fillna(df[numeric_cols].median())
        y = (df["actual_winner"] == df["home_team"]).astype(int)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Train model
        logger.info(f"Training on {len(X_train)} games with {len(numeric_cols)} features")
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=12,
            random_state=42,
            class_weight='balanced'
        )
        model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"Model accuracy: {accuracy:.3f}")
        logger.info("Classification Report:")
        logger.info(f"\n{classification_report(y_test, y_pred)}")
        
        # Save model
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        model_path = MODELS_DIR / f"rf_historical_{timestamp}.joblib"
        features_path = MODELS_DIR / f"rf_historical_{timestamp}_features.txt"
        
        joblib.dump(model, model_path)
        logger.info(f"Saved model to: {model_path}")
        
        # Save feature names
        with open(features_path, 'w') as f:
            f.write('\n'.join(numeric_cols))
        logger.info(f"Saved feature names to: {features_path}")
        
        return model_path, numeric_cols, accuracy
        
    except Exception as e:
        logger.error(f"Error training from historical data: {e}")
        return None, None, None


def main():
    """Main training function."""
    logger.info("Starting model training...")
    
    # Try to train from historical data first
    model_path, features, accuracy = train_from_historical_data()
    
    # If that fails, create demo model
    if model_path is None:
        logger.info("Historical data training failed, creating demo model...")
        model_path, features, accuracy = create_demo_model()
    
    if model_path:
        logger.info(f"✅ Model training completed successfully!")
        logger.info(f"📁 Model saved to: {model_path}")
        logger.info(f"🎯 Accuracy: {accuracy:.3f}")
        logger.info(f"🔢 Features: {len(features)}")
        return True
    else:
        logger.error("❌ Model training failed!")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)