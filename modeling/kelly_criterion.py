#!/usr/bin/env python3
"""
Kelly Criterion Bet Sizing for MLB Predictions

Implements optimal bet sizing using the Kelly Criterion formula to maximize
long-term bankroll growth while managing risk through fractional Kelly betting.

Kelly formula: f = (bp - q) / b
Where:
- f = fraction of bankroll to bet
- b = decimal odds - 1 (net odds received)
- p = probability of winning
- q = probability of losing (1 - p)
"""

import logging
from typing import Union
import numpy as np

# Setup logging
logger = logging.getLogger(__name__)


def calculate_kelly_bet_size(
    win_probability: float,
    decimal_odds: float, 
    bankroll: float,
    max_bet_fraction: float = 0.25
) -> float:
    """
    Calculate optimal bet size using Kelly Criterion with safety constraints.
    
    The Kelly Criterion determines the optimal fraction of bankroll to wager
    to maximize long-term growth while accounting for both win probability
    and payout odds.
    
    Args:
        win_probability: Probability of winning the bet (0.0 < p < 1.0)
        decimal_odds: Decimal odds format (e.g., 2.0 = even money)
        bankroll: Current bankroll amount in currency units
        max_bet_fraction: Maximum fraction of bankroll to risk (default: 0.25)
        
    Returns:
        Optimal bet amount in currency units. Returns 0.0 if Kelly is negative
        or if there's no positive expected value.
        
    Raises:
        ValueError: If input parameters violate preconditions
        
    Examples:
        >>> # Positive edge case: 60% win prob, 2:1 odds
        >>> calculate_kelly_bet_size(0.6, 2.0, 1000.0)
        200.0
        
        >>> # Negative edge case: 40% win prob, 2:1 odds  
        >>> calculate_kelly_bet_size(0.4, 2.0, 1000.0)
        0.0
        
        >>> # Capped by max fraction
        >>> calculate_kelly_bet_size(0.8, 1.5, 1000.0, max_bet_fraction=0.1)
        100.0
    """
    
    # Input validation - enforce preconditions
    if not (0.0 < win_probability < 1.0):
        raise ValueError(f"win_probability must be between 0.0 and 1.0, got {win_probability}")
    
    if not decimal_odds > 1.0:
        raise ValueError(f"decimal_odds must be greater than 1.0, got {decimal_odds}")
    
    if not bankroll > 0.0:
        raise ValueError(f"bankroll must be positive, got {bankroll}")
    
    if not (0.0 < max_bet_fraction <= 1.0):
        raise ValueError(f"max_bet_fraction must be between 0.0 and 1.0, got {max_bet_fraction}")
    
    try:
        # Kelly Criterion calculation
        # f = (bp - q) / b
        # Where b = decimal_odds - 1 (net odds)
        net_odds = decimal_odds - 1.0  # Convert decimal odds to net odds
        lose_probability = 1.0 - win_probability
        
        # Kelly fraction
        kelly_fraction = (net_odds * win_probability - lose_probability) / net_odds
        
        # Safety checks
        if kelly_fraction <= 0.0:
            # Negative expected value - don't bet
            logger.debug(f"Negative Kelly fraction: {kelly_fraction:.4f} - no bet recommended")
            return 0.0
        
        # Apply fractional Kelly for risk management
        safe_kelly_fraction = min(kelly_fraction, max_bet_fraction)
        
        # Calculate bet amount
        bet_amount = bankroll * safe_kelly_fraction
        
        # Final safety bounds check
        bet_amount = max(0.0, min(bet_amount, bankroll * max_bet_fraction))
        
        logger.debug(f"Kelly calculation: prob={win_probability:.3f}, odds={decimal_odds:.2f}, "
                    f"kelly_fraction={kelly_fraction:.4f}, safe_fraction={safe_kelly_fraction:.4f}, "
                    f"bet_amount=${bet_amount:.2f}")
        
        return round(bet_amount, 2)
        
    except (ZeroDivisionError, OverflowError, ValueError) as e:
        logger.error(f"Kelly calculation error: {e}")
        return 0.0


def calculate_kelly_for_predictions(
    predictions_df,
    bankroll: float,
    default_odds: float = 1.91,  # -110 American odds
    max_bet_fraction: float = 0.25
) -> dict:
    """
    Apply Kelly Criterion to a DataFrame of MLB predictions.
    
    Args:
        predictions_df: DataFrame with columns ['Win Probability', 'Home Team', 'Away Team']
        bankroll: Current bankroll amount
        default_odds: Default decimal odds to use if not provided
        max_bet_fraction: Maximum fraction of bankroll per bet
        
    Returns:
        Dictionary with bet recommendations
    """
    
    if predictions_df.empty:
        logger.warning("No predictions provided for Kelly sizing")
        return {}
    
    recommendations = []
    total_recommended_bets = 0.0
    
    for idx, row in predictions_df.iterrows():
        win_prob = row.get('Win Probability', 0.0)
        home_team = row.get('Home Team', 'Unknown')
        away_team = row.get('Away Team', 'Unknown')
        
        # Determine which team to bet on
        if win_prob > 0.5:
            # Bet on home team
            team_to_bet = home_team
            bet_probability = win_prob
        else:
            # Bet on away team  
            team_to_bet = away_team
            bet_probability = 1.0 - win_prob
        
        # Only consider bets with reasonable edge
        if bet_probability > 0.53:  # Minimum 53% confidence
            bet_size = calculate_kelly_bet_size(
                win_probability=bet_probability,
                decimal_odds=default_odds,
                bankroll=bankroll - total_recommended_bets,  # Account for other bets
                max_bet_fraction=max_bet_fraction
            )
            
            if bet_size > 0:
                recommendations.append({
                    'matchup': f"{away_team} @ {home_team}",
                    'team_to_bet': team_to_bet,
                    'win_probability': bet_probability,
                    'bet_size': bet_size,
                    'expected_value': bet_size * (bet_probability * (default_odds - 1) - (1 - bet_probability))
                })
                total_recommended_bets += bet_size
    
    return {
        'recommendations': recommendations,
        'total_bet_amount': total_recommended_bets,
        'remaining_bankroll': bankroll - total_recommended_bets,
        'bankroll_utilization': (total_recommended_bets / bankroll) * 100
    }


# Test cases to verify implementation
if __name__ == "__main__":
    import sys
    
    # Test Case 1: Positive edge case
    result1 = calculate_kelly_bet_size(0.6, 2.0, 1000.0)
    expected1 = 200.0  # (1 * 0.6 - 0.4) / 1 = 0.2 * 1000 = 200
    assert abs(result1 - expected1) < 0.01, f"Test 1 failed: expected {expected1}, got {result1}"
    print(f"✅ Test 1 passed: {result1}")
    
    # Test Case 2: Negative edge case
    result2 = calculate_kelly_bet_size(0.4, 2.0, 1000.0)
    expected2 = 0.0  # Negative Kelly = no bet
    assert result2 == expected2, f"Test 2 failed: expected {expected2}, got {result2}"
    print(f"✅ Test 2 passed: {result2}")
    
    # Test Case 3: Max bet fraction constraint
    result3 = calculate_kelly_bet_size(0.8, 1.5, 1000.0, max_bet_fraction=0.1)
    expected3 = 100.0  # Should be capped at 10% of bankroll
    assert abs(result3 - expected3) < 0.01, f"Test 3 failed: expected {expected3}, got {result3}"
    print(f"✅ Test 3 passed: {result3}")
    
    # Test Case 4: Input validation
    try:
        calculate_kelly_bet_size(-0.1, 2.0, 1000.0)
        assert False, "Should have raised ValueError for negative probability"
    except ValueError:
        print("✅ Test 4 passed: Input validation working")
    
    print("\n🎉 All Kelly Criterion tests passed!")