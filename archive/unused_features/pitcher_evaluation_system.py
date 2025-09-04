# pitcher_evaluation_system.py

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Project Paths ===
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

class PitcherEvaluator:
    def __init__(self):
        # Weights for each metric (higher weight = more important)
        self.metric_weights = {
            'whip': 0.15,      # Lower is better
            'fip': 0.15,       # Lower is better  
            'siera': 0.12,     # Lower is better
            'csw_rate': 0.12,  # Higher is better
            'xera': 0.10,      # Lower is better
            'xfip': 0.08,      # Lower is better
            'xwoba': 0.08,     # Lower is better
            'xba': 0.06,       # Lower is better
            'xslg': 0.06,      # Lower is better
            'stuff_plus': 0.08 # Higher is better
        }
        
        # Benchmarks for percentile scoring (approximate MLB averages/ranges)
        self.benchmarks = {
            'whip': {'excellent': 1.00, 'good': 1.20, 'average': 1.35, 'poor': 1.50},
            'fip': {'excellent': 3.00, 'good': 3.50, 'average': 4.20, 'poor': 5.00},
            'siera': {'excellent': 3.20, 'good': 3.80, 'average': 4.40, 'poor': 5.00},
            'csw_rate': {'excellent': 32.0, 'good': 29.0, 'average': 26.0, 'poor': 23.0},
            'xera': {'excellent': 3.00, 'good': 3.70, 'average': 4.40, 'poor': 5.20},
            'xfip': {'excellent': 3.20, 'good': 3.80, 'average': 4.40, 'poor': 5.00},
            'xwoba': {'excellent': 0.300, 'good': 0.320, 'average': 0.340, 'poor': 0.360},
            'xba': {'excellent': 0.220, 'good': 0.250, 'average': 0.280, 'poor': 0.310},
            'xslg': {'excellent': 0.360, 'good': 0.410, 'average': 0.460, 'poor': 0.520},
            'stuff_plus': {'excellent': 110, 'good': 105, 'average': 100, 'poor': 95}
        }
    
    def calculate_percentile_score(self, value, metric):
        """Convert raw metric to 0-100 percentile score"""
        if pd.isna(value):
            return 50  # neutral score for missing data
            
        benchmarks = self.benchmarks[metric]
        
        # For metrics where lower is better (inverse scoring)
        if metric in ['whip', 'fip', 'siera', 'xera', 'xfip', 'xwoba', 'xba', 'xslg']:
            if value <= benchmarks['excellent']:
                return 95
            elif value <= benchmarks['good']:
                return 80
            elif value <= benchmarks['average']:
                return 60
            elif value <= benchmarks['poor']:
                return 40
            else:
                return 20
        
        # For metrics where higher is better
        else:  # csw_rate, stuff_plus
            if value >= benchmarks['excellent']:
                return 95
            elif value >= benchmarks['good']:
                return 80
            elif value >= benchmarks['average']:
                return 60
            elif value >= benchmarks['poor']:
                return 40
            else:
                return 20
    
    def calculate_composite_score(self, pitcher_data):
        """Calculate weighted composite score for a pitcher"""
        total_score = 0
        total_weight = 0
        
        for metric, weight in self.metric_weights.items():
            if metric in pitcher_data and not pd.isna(pitcher_data[metric]):
                percentile_score = self.calculate_percentile_score(pitcher_data[metric], metric)
                total_score += percentile_score * weight
                total_weight += weight
        
        if total_weight == 0:
            return 50  # neutral if no valid metrics
        
        return round(total_score / total_weight, 1)
    
    def get_grade(self, score):
        """Convert numerical score to letter grade"""
        if score >= 90:
            return 'A+'
        elif score >= 85:
            return 'A'
        elif score >= 80:
            return 'A-'
        elif score >= 75:
            return 'B+'
        elif score >= 70:
            return 'B'
        elif score >= 65:
            return 'B-'
        elif score >= 60:
            return 'C+'
        elif score >= 55:
            return 'C'
        elif score >= 50:
            return 'C-'
        elif score >= 45:
            return 'D+'
        elif score >= 40:
            return 'D'
        else:
            return 'F'
    
    def get_tier(self, score):
        """Categorize pitcher into performance tiers"""
        if score >= 85:
            return 'Elite'
        elif score >= 75:
            return 'Above Average'
        elif score >= 60:
            return 'Average'
        elif score >= 45:
            return 'Below Average'
        else:
            return 'Poor'
    
    def analyze_strengths_weaknesses(self, pitcher_data):
        """Identify pitcher's strengths and weaknesses"""
        strengths = []
        weaknesses = []
        
        for metric in self.metric_weights.keys():
            if metric in pitcher_data and not pd.isna(pitcher_data[metric]):
                score = self.calculate_percentile_score(pitcher_data[metric], metric)
                
                if score >= 80:
                    strengths.append(f"{metric.upper()}: {pitcher_data[metric]}")
                elif score <= 40:
                    weaknesses.append(f"{metric.upper()}: {pitcher_data[metric]}")
        
        return strengths, weaknesses
    
    def evaluate_pitchers(self, features_df):
        """Evaluate all pitchers and return ranked results"""
        results = []
        
        for idx, pitcher in features_df.iterrows():
            composite_score = self.calculate_composite_score(pitcher)
            grade = self.get_grade(composite_score)
            tier = self.get_tier(composite_score)
            strengths, weaknesses = self.analyze_strengths_weaknesses(pitcher)
            
            # Calculate run value summary
            run_value_columns = [col for col in pitcher.index if col.endswith('_run_value')]
            best_pitch = None
            worst_pitch = None
            
            if run_value_columns:
                run_values = {col.replace('_run_value', ''): pitcher[col] 
                             for col in run_value_columns if not pd.isna(pitcher[col])}
                if run_values:
                    best_pitch = min(run_values.items(), key=lambda x: x[1])  # Lower run value is better for pitcher
                    worst_pitch = max(run_values.items(), key=lambda x: x[1])
            
            result = {
                'pitcher_name': pitcher['full_name'],
                'composite_score': composite_score,
                'grade': grade,
                'tier': tier,
                'whip': pitcher.get('whip', 'N/A'),
                'fip': pitcher.get('fip', 'N/A'),
                'siera': pitcher.get('siera', 'N/A'),
                'csw_rate': pitcher.get('csw_rate', 'N/A'),
                'xera': pitcher.get('xera', 'N/A'),
                'xfip': pitcher.get('xfip', 'N/A'),
                'xwoba': pitcher.get('xwoba', 'N/A'),
                'xba': pitcher.get('xba', 'N/A'),
                'xslg': pitcher.get('xslg', 'N/A'),
                'stuff_plus': pitcher.get('stuff_plus', 'N/A'),
                'best_pitch': f"{best_pitch[0]} ({best_pitch[1]:.3f})" if best_pitch else 'N/A',
                'worst_pitch': f"{worst_pitch[0]} ({worst_pitch[1]:.3f})" if worst_pitch else 'N/A',
                'strengths': '; '.join(strengths[:3]) if strengths else 'None identified',
                'weaknesses': '; '.join(weaknesses[:3]) if weaknesses else 'None identified'
            }
            
            results.append(result)
        
        # Sort by composite score (descending)
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('composite_score', ascending=False)
        results_df['rank'] = range(1, len(results_df) + 1)
        
        return results_df

def evaluate_pitchers_from_file(features_file: Path):
    """Load features and evaluate pitchers"""
    try:
        logger.info(f"Loading pitcher features from: {features_file}")
        features_df = pd.read_csv(features_file)
        
        evaluator = PitcherEvaluator()
        results_df = evaluator.evaluate_pitchers(features_df)
        
        # Save results
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = PROCESSED_DIR / f"pitcher_evaluation_report_{date_str}.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"Evaluation report saved to: {output_path}")
        
        # Print summary
        print("\n" + "="*100)
        print("PITCHER EVALUATION REPORT")
        print("="*100)
        
        # Top 5 pitchers
        print("\nTOP 5 PITCHERS:")
        print("-" * 100)
        top_5 = results_df.head(5)
        for _, pitcher in top_5.iterrows():
            print(f"{pitcher['rank']:2d}. {pitcher['pitcher_name']:<20} | Score: {pitcher['composite_score']:5.1f} | Grade: {pitcher['grade']:2s} | Tier: {pitcher['tier']}")
        
        # Bottom 5 pitchers
        print("\nBOTTOM 5 PITCHERS:")
        print("-" * 100)
        bottom_5 = results_df.tail(5)
        for _, pitcher in bottom_5.iterrows():
            print(f"{pitcher['rank']:2d}. {pitcher['pitcher_name']:<20} | Score: {pitcher['composite_score']:5.1f} | Grade: {pitcher['grade']:2s} | Tier: {pitcher['tier']}")
        
        # Tier distribution
        print("\nTIER DISTRIBUTION:")
        print("-" * 50)
        tier_counts = results_df['tier'].value_counts()
        for tier, count in tier_counts.items():
            percentage = (count / len(results_df)) * 100
            print(f"{tier:<15}: {count:2d} pitchers ({percentage:4.1f}%)")
        
        # Show detailed view of top 3
        print("\nDETAILED ANALYSIS - TOP 3 PITCHERS:")
        print("=" * 100)
        for _, pitcher in results_df.head(3).iterrows():
            print(f"\n{pitcher['rank']}. {pitcher['pitcher_name']} - {pitcher['grade']} ({pitcher['composite_score']} points)")
            print(f"   WHIP: {pitcher['whip']} | FIP: {pitcher['fip']} | SIERA: {pitcher['siera']} | CSW%: {pitcher['csw_rate']}")
            print(f"   xERA: {pitcher['xera']} | xwOBA: {pitcher['xwoba']} | Stuff+: {pitcher['stuff_plus']}")
            print(f"   Best Pitch: {pitcher['best_pitch']} | Worst Pitch: {pitcher['worst_pitch']}")
            print(f"   Strengths: {pitcher['strengths']}")
            if pitcher['weaknesses'] != 'None identified':
                print(f"   Weaknesses: {pitcher['weaknesses']}")
        
        return output_path, results_df
        
    except Exception as e:
        logger.error(f"Error evaluating pitchers: {e}")
        return None, None

def find_latest_features_file():
    """Find the most recent advanced pitcher features file"""
    files = list(PROCESSED_DIR.glob("advanced_pitcher_features_*.csv"))
    if not files:
        logger.error("No advanced pitcher features files found.")
        return None
    return max(files, key=lambda x: x.stat().st_mtime)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate pitchers using advanced metrics")
    parser.add_argument("--file", help="Path to pitcher features CSV file", type=str)
    args = parser.parse_args()
    
    if args.file:
        features_file = Path(args.file)
        if not features_file.exists():
            logger.error(f"Specified file does not exist: {features_file}")
            exit(1)
    else:
        features_file = find_latest_features_file()
        if not features_file:
            logger.error("No features file found. Run build_advanced_pitcher_features.py first.")
            exit(1)
    
    output_path, results_df = evaluate_pitchers_from_file(features_file)
    if output_path:
        print(f"\nComplete evaluation saved to: {output_path}")