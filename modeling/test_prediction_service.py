#!/usr/bin/env python3
"""Test the prediction service."""

from prediction_service import MLBPredictionService
import sys

def test_service():
    """Test the prediction service."""
    print("Testing MLB Prediction Service...")
    
    service = MLBPredictionService()
    result = service.predict_live_games()
    
    if result['success']:
        print('✅ Prediction service test successful!')
        print(f"Games: {result['summary']['total_games']}")
        print(f"Recommended bets: {result['summary']['recommended_bets']}")
        print(f"Total bet amount: ${result['summary']['total_bet_amount']:.2f}")
        print(f"Output file: {result['summary']['output_file']}")
        return True
    else:
        print(f'❌ Test failed: {result["error"]}')
        return False

if __name__ == "__main__":
    success = test_service()
    sys.exit(0 if success else 1)