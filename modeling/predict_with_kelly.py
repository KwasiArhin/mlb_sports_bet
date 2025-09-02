#!/usr/bin/env python3
"""
MLB Predictions with Kelly Criterion Bet Sizing

Enhanced prediction pipeline that integrates optimal bet sizing using Kelly Criterion
with the existing ML prediction models.
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any
import numpy as np

# Import existing components
from kelly_criterion import calculate_kelly_for_predictions
from predict_today_matchups import find_latest_file

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def predict_with_kelly_sizing(
    predictions_path: str,
    bankroll: float = 1000.0,
    default_odds: float = 1.91,  # -110 American odds
    max_bet_fraction: float = 0.25,
    min_edge_threshold: float = 0.53,
    output_dir: Path = Path("modeling/data/predictions")
) -> Optional[str]:
    """
    Load MLB predictions and add Kelly Criterion bet sizing recommendations.
    
    Args:
        predictions_path: Path to CSV file with model predictions
        bankroll: Available bankroll for betting
        default_odds: Default decimal odds to use
        max_bet_fraction: Maximum fraction of bankroll per bet
        min_edge_threshold: Minimum win probability to consider betting
        output_dir: Directory to save enhanced predictions
        
    Returns:
        Path to output file with bet sizing, or None if failed
    """
    
    try:
        # Load existing predictions
        logger.info(f"Loading predictions from {predictions_path}")
        predictions_df = pd.read_csv(predictions_path)
        
        if predictions_df.empty:
            logger.warning("No predictions found in file")
            return None
            
        # Validate required columns
        required_cols = ['Win Probability', 'Home Team', 'Away Team']
        missing_cols = [col for col in required_cols if col not in predictions_df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return None
            
        logger.info(f"Loaded {len(predictions_df)} predictions")
        
        # Apply Kelly Criterion
        kelly_results = calculate_kelly_for_predictions(
            predictions_df=predictions_df,
            bankroll=bankroll,
            default_odds=default_odds,
            max_bet_fraction=max_bet_fraction
        )
        
        # Create enhanced output DataFrame
        enhanced_df = predictions_df.copy()
        
        # Add Kelly sizing columns
        enhanced_df['Bet_Size'] = 0.0
        enhanced_df['Team_To_Bet'] = ''
        enhanced_df['Expected_Value'] = 0.0
        enhanced_df['Kelly_Edge'] = False
        
        # Populate bet sizing information
        for rec in kelly_results['recommendations']:
            # Find matching row
            mask = (
                (enhanced_df['Home Team'].str.contains(rec['matchup'].split(' @ ')[1], na=False)) &
                (enhanced_df['Away Team'].str.contains(rec['matchup'].split(' @ ')[0], na=False))
            )
            
            if mask.any():
                enhanced_df.loc[mask, 'Bet_Size'] = rec['bet_size']
                enhanced_df.loc[mask, 'Team_To_Bet'] = rec['team_to_bet']
                enhanced_df.loc[mask, 'Expected_Value'] = rec['expected_value']
                enhanced_df.loc[mask, 'Kelly_Edge'] = True
        
        # Add summary information
        enhanced_df['Odds_Used'] = default_odds
        enhanced_df['Bankroll'] = bankroll
        
        # Sort by bet size (highest first)
        enhanced_df = enhanced_df.sort_values('Bet_Size', ascending=False)
        
        # Generate output file
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M')
        output_filename = f"kelly_predictions_{timestamp}.csv"
        output_path = output_dir / output_filename
        
        # Save enhanced predictions
        enhanced_df.to_csv(output_path, index=False)
        
        # Generate summary report
        summary = generate_kelly_summary(kelly_results, enhanced_df, bankroll)
        summary_path = output_dir / f"kelly_summary_{timestamp}.txt"
        
        with open(summary_path, 'w') as f:
            f.write(summary)
        
        logger.info(f"Enhanced predictions saved to: {output_path}")
        logger.info(f"Summary report saved to: {summary_path}")
        
        # Print summary to console
        print("\n" + "="*60)
        print("KELLY CRITERION BET SIZING SUMMARY")
        print("="*60)
        print(summary)
        
        return str(output_path)
        
    except Exception as e:
        logger.error(f"Error in Kelly prediction pipeline: {e}")
        return None


def generate_kelly_summary(kelly_results: Dict, enhanced_df: pd.DataFrame, bankroll: float) -> str:
    """Generate human-readable summary of Kelly betting recommendations."""
    
    recommendations = kelly_results['recommendations']
    total_bets = kelly_results['total_bet_amount']
    remaining = kelly_results['remaining_bankroll']
    utilization = kelly_results['bankroll_utilization']
    
    summary_lines = [
        f"Bankroll: ${bankroll:,.2f}",
        f"Total Recommended Bets: ${total_bets:,.2f}",
        f"Remaining Bankroll: ${remaining:,.2f}",
        f"Bankroll Utilization: {utilization:.1f}%",
        f"Number of Recommended Bets: {len(recommendations)}",
        "",
        "BETTING RECOMMENDATIONS:",
        "-" * 40
    ]
    
    if not recommendations:
        summary_lines.append("❌ No bets recommended - insufficient edge detected")
    else:
        for i, rec in enumerate(recommendations, 1):
            summary_lines.extend([
                f"{i}. {rec['matchup']}",
                f"   Bet on: {rec['team_to_bet']}",
                f"   Win Probability: {rec['win_probability']:.1%}",
                f"   Bet Size: ${rec['bet_size']:,.2f}",
                f"   Expected Value: ${rec['expected_value']:+.2f}",
                ""
            ])
        
        # Add risk metrics
        max_bet = max(rec['bet_size'] for rec in recommendations)
        avg_bet = total_bets / len(recommendations) if recommendations else 0
        
        summary_lines.extend([
            "RISK METRICS:",
            "-" * 20,
            f"Largest Single Bet: ${max_bet:,.2f} ({(max_bet/bankroll)*100:.1f}% of bankroll)",
            f"Average Bet Size: ${avg_bet:,.2f}",
            f"Total Games with Predictions: {len(enhanced_df)}",
            f"Games with Kelly Edge: {len(recommendations)}",
            f"Kelly Hit Rate: {(len(recommendations)/len(enhanced_df))*100:.1f}%" if len(enhanced_df) > 0 else "N/A"
        ])
    
    return "\n".join(summary_lines)


def main():
    """Main execution for standalone usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="MLB Predictions with Kelly Criterion Bet Sizing")
    parser.add_argument("--predictions", "-p", required=True, help="Path to predictions CSV file")
    parser.add_argument("--bankroll", "-b", type=float, default=1000.0, help="Available bankroll (default: $1000)")
    parser.add_argument("--odds", "-o", type=float, default=1.91, help="Default decimal odds (default: 1.91 = -110)")
    parser.add_argument("--max-fraction", "-m", type=float, default=0.25, help="Max bet fraction (default: 0.25)")
    parser.add_argument("--min-edge", "-e", type=float, default=0.53, help="Minimum edge threshold (default: 0.53)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if not Path(args.predictions).exists():
        print(f"❌ Error: Predictions file not found: {args.predictions}")
        return 1
    
    if args.bankroll <= 0:
        print(f"❌ Error: Bankroll must be positive, got: {args.bankroll}")
        return 1
    
    # Run Kelly prediction pipeline
    result = predict_with_kelly_sizing(
        predictions_path=args.predictions,
        bankroll=args.bankroll,
        default_odds=args.odds,
        max_bet_fraction=args.max_fraction,
        min_edge_threshold=args.min_edge
    )
    
    if result:
        print(f"\n✅ Kelly predictions completed successfully!")
        print(f"📁 Output file: {result}")
        return 0
    else:
        print(f"\n❌ Kelly predictions failed!")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())