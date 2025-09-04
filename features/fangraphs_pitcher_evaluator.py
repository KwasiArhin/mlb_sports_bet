# fangraphs_pitcher_evaluator.py

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

class FangraphsPitcherEvaluator:
    def __init__(self):
        # Weights for each metric (higher weight = more important)
        # Using your exact specified metrics
        self.metric_weights = {
            'WHIP': 0.15,      # Lower is better
            'FIP': 0.15,       # Lower is better  
            'SIERA': 0.12,     # Lower is better
            'CSW%': 0.12,      # Higher is better (Called Strike + Whiff %)
            'xERA': 0.10,      # Lower is better
            'xFIP': 0.08,      # Lower is better
            'xwOBA': 0.08,     # Lower is better
            'xBA': 0.06,       # Lower is better (Expected Batting Average)
            'xSLG': 0.06,      # Lower is better (Expected Slugging)
            'Stuff+': 0.08     # Higher is better
        }
        
        # Benchmarks for percentile scoring (MLB averages/ranges for 2024-2025)
        self.benchmarks = {
            'WHIP': {'excellent': 1.00, 'good': 1.15, 'average': 1.30, 'poor': 1.45},
            'FIP': {'excellent': 3.20, 'good': 3.70, 'average': 4.20, 'poor': 4.80},
            'SIERA': {'excellent': 3.40, 'good': 3.90, 'average': 4.40, 'poor': 4.90},
            'CSW%': {'excellent': 30.0, 'good': 28.0, 'average': 25.5, 'poor': 23.0},
            'xERA': {'excellent': 3.50, 'good': 4.00, 'average': 4.50, 'poor': 5.00},
            'xFIP': {'excellent': 3.50, 'good': 4.00, 'average': 4.50, 'poor': 5.00},
            'xwOBA': {'excellent': 0.310, 'good': 0.330, 'average': 0.350, 'poor': 0.370},
            'xBA': {'excellent': 0.240, 'good': 0.260, 'average': 0.280, 'poor': 0.300},
            'xSLG': {'excellent': 0.390, 'good': 0.430, 'average': 0.470, 'poor': 0.510},
            'Stuff+': {'excellent': 105, 'good': 100, 'average': 95, 'poor': 90}
        }
        
        # Alternative column names that might appear in Fangraphs data
        self.column_mapping = {
            'WHIP': ['WHIP'],
            'FIP': ['FIP'],
            'SIERA': ['SIERA'],
            'CSW%': ['CSW%', 'CSW'],
            'xERA': ['xERA'],
            'xFIP': ['xFIP'],
            'xwOBA': ['xwOBA'],
            'xBA': ['xBA'],
            'xSLG': ['xSLG'],
            'Stuff+': ['Stuff+', 'Stuff']
        }
    
    def find_column(self, df, metric):
        """Find the correct column name in the DataFrame"""
        possible_names = self.column_mapping.get(metric, [metric])
        for name in possible_names:
            if name in df.columns:
                return name
        return None
    
    def calculate_percentile_score(self, value, metric):
        """Convert raw metric to 0-100 percentile score"""
        if pd.isna(value):
            return 50  # neutral score for missing data
            
        benchmarks = self.benchmarks[metric]
        
        # For metrics where lower is better (inverse scoring)
        if metric in ['WHIP', 'FIP', 'SIERA', 'xERA', 'xFIP', 'xwOBA', 'xBA', 'xSLG']:
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
        else:  # CSW%, Stuff+
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
    
    def calculate_composite_score(self, pitcher_row, df_columns):
        """Calculate weighted composite score for a pitcher"""
        total_score = 0
        total_weight = 0
        score_breakdown = {}
        
        for metric, weight in self.metric_weights.items():
            col_name = self.find_column(pd.DataFrame(columns=df_columns), metric)
            
            if col_name and col_name in pitcher_row.index and not pd.isna(pitcher_row[col_name]):
                percentile_score = self.calculate_percentile_score(pitcher_row[col_name], metric)
                total_score += percentile_score * weight
                total_weight += weight
                score_breakdown[metric] = {
                    'value': pitcher_row[col_name],
                    'percentile_score': percentile_score,
                    'weight': weight,
                    'contribution': percentile_score * weight
                }
        
        if total_weight == 0:
            return 50, score_breakdown  # neutral if no valid metrics
        
        composite_score = total_score / total_weight
        return round(composite_score, 1), score_breakdown
    
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
    
    def analyze_strengths_weaknesses(self, score_breakdown):
        """Identify pitcher's strengths and weaknesses"""
        strengths = []
        weaknesses = []
        
        for metric, details in score_breakdown.items():
            percentile_score = details['percentile_score']
            value = details['value']
            
            if percentile_score >= 80:
                strengths.append(f"{metric}: {value}")
            elif percentile_score <= 40:
                weaknesses.append(f"{metric}: {value}")
        
        return strengths, weaknesses
    
    def evaluate_pitchers(self, fangraphs_df):
        """Evaluate all pitchers and return ranked results"""
        results = []
        
        logger.info(f"Evaluating {len(fangraphs_df)} pitchers...")
        
        for idx, pitcher in fangraphs_df.iterrows():
            composite_score, score_breakdown = self.calculate_composite_score(pitcher, fangraphs_df.columns)
            grade = self.get_grade(composite_score)
            tier = self.get_tier(composite_score)
            strengths, weaknesses = self.analyze_strengths_weaknesses(score_breakdown)
            
            # Get key stats for display
            key_stats = {}
            for metric in self.metric_weights.keys():
                col_name = self.find_column(fangraphs_df, metric)
                if col_name and col_name in pitcher.index:
                    key_stats[metric] = pitcher[col_name]
                else:
                    key_stats[metric] = 'N/A'
            
            result = {
                'pitcher_name': pitcher['Name'] if 'Name' in pitcher.index else 'Unknown',
                'team': pitcher['Team'] if 'Team' in pitcher.index else 'N/A',
                'innings_pitched': pitcher['IP'] if 'IP' in pitcher.index else 'N/A',
                'composite_score': composite_score,
                'grade': grade,
                'tier': tier,
                'whip': key_stats['WHIP'],
                'fip': key_stats['FIP'],
                'siera': key_stats['SIERA'],
                'csw_rate': key_stats['CSW%'],
                'xera': key_stats['xERA'],
                'xfip': key_stats['xFIP'],
                'xwoba': key_stats['xwOBA'],
                'xba': key_stats['xBA'],
                'xslg': key_stats['xSLG'],
                'stuff_plus': key_stats['Stuff+'],
                'era': pitcher['ERA'] if 'ERA' in pitcher.index else 'N/A',
                'k_rate': pitcher['K%'] if 'K%' in pitcher.index else 'N/A',
                'bb_rate': pitcher['BB%'] if 'BB%' in pitcher.index else 'N/A',
                'strengths': '; '.join(strengths[:3]) if strengths else 'None identified',
                'weaknesses': '; '.join(weaknesses[:3]) if weaknesses else 'None identified',
                'score_breakdown': score_breakdown
            }
            
            results.append(result)
        
        # Sort by composite score (descending)
        results_df = pd.DataFrame(results)
        results_df = results_df.sort_values('composite_score', ascending=False)
        results_df['rank'] = range(1, len(results_df) + 1)
        
        return results_df

def evaluate_fangraphs_pitchers(fangraphs_file: Path):
    """Load Fangraphs data and evaluate pitchers"""
    try:
        logger.info(f"Loading Fangraphs pitcher data from: {fangraphs_file}")
        fangraphs_df = pd.read_csv(fangraphs_file)
        
        logger.info(f"Available columns: {list(fangraphs_df.columns)}")
        
        evaluator = FangraphsPitcherEvaluator()
        results_df = evaluator.evaluate_pitchers(fangraphs_df)
        
        # Save results
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = PROCESSED_DIR / f"fangraphs_pitcher_evaluation_{date_str}.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"Evaluation report saved to: {output_path}")
        
        # Print comprehensive report
        print("\n" + "="*120)
        print("FANGRAPHS PITCHER EVALUATION REPORT")
        print("="*120)
        
        # Top 5 pitchers
        print("\n🏆 TOP 5 PITCHERS:")
        print("-" * 120)
        top_5 = results_df.head(5)
        for _, pitcher in top_5.iterrows():
            print(f"{pitcher['rank']:2d}. {pitcher['pitcher_name']:<18} ({pitcher['team']:3s}) | "
                  f"Score: {pitcher['composite_score']:5.1f} | Grade: {pitcher['grade']:2s} | "
                  f"Tier: {pitcher['tier']:13s} | IP: {pitcher['innings_pitched']}")
        
        # Bottom 5 pitchers
        print("\n📉 BOTTOM 5 PITCHERS:")
        print("-" * 120)
        bottom_5 = results_df.tail(5)
        for _, pitcher in bottom_5.iterrows():
            print(f"{pitcher['rank']:2d}. {pitcher['pitcher_name']:<18} ({pitcher['team']:3s}) | "
                  f"Score: {pitcher['composite_score']:5.1f} | Grade: {pitcher['grade']:2s} | "
                  f"Tier: {pitcher['tier']:13s} | IP: {pitcher['innings_pitched']}")
        
        # Tier distribution
        print("\n📊 TIER DISTRIBUTION:")
        print("-" * 60)
        tier_counts = results_df['tier'].value_counts().sort_index()
        total_pitchers = len(results_df)
        for tier, count in tier_counts.items():
            percentage = (count / total_pitchers) * 100
            print(f"{tier:<15}: {count:3d} pitchers ({percentage:4.1f}%)")
        
        # Detailed analysis of top 3
        print("\n🔍 DETAILED ANALYSIS - TOP 3 PITCHERS:")
        print("=" * 120)
        
        for _, pitcher in results_df.head(3).iterrows():
            print(f"\n{pitcher['rank']}. {pitcher['pitcher_name']} ({pitcher['team']}) - {pitcher['grade']} ({pitcher['composite_score']} points)")
            print(f"   📊 Key Stats: WHIP: {pitcher['whip']} | FIP: {pitcher['fip']} | SIERA: {pitcher['siera']} | CSW%: {pitcher['csw_rate']}")
            print(f"   📈 xStats: xERA: {pitcher['xera']} | xwOBA: {pitcher['xwoba']} | xBA: {pitcher['xba']} | Stuff+: {pitcher['stuff_plus']}")
            print(f"   ✅ Strengths: {pitcher['strengths']}")
            if pitcher['weaknesses'] != 'None identified':
                print(f"   ❌ Weaknesses: {pitcher['weaknesses']}")
        
        # Summary statistics
        print(f"\n📈 SUMMARY STATISTICS:")
        print("-" * 60)
        print(f"Average Score: {results_df['composite_score'].mean():.1f}")
        print(f"Median Score: {results_df['composite_score'].median():.1f}")
        print(f"Score Range: {results_df['composite_score'].min():.1f} - {results_df['composite_score'].max():.1f}")
        
        return output_path, results_df
        
    except Exception as e:
        logger.error(f"Error evaluating pitchers: {e}")
        return None, None

def find_latest_fangraphs_file():
    """Find the most recent pitcher data file (enhanced or regular Fangraphs)"""
    # Look for enhanced data first
    enhanced_files = list(PROCESSED_DIR.glob("enhanced_pitcher_data_*.csv"))
    fangraphs_files = list(PROCESSED_DIR.glob("fangraphs_pitcher_data_*.csv"))
    
    all_files = enhanced_files + fangraphs_files
    if not all_files:
        logger.error("No pitcher data files found.")
        return None
    
    latest_file = max(all_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Using pitcher data file: {latest_file}")
    return latest_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate pitchers using Fangraphs data")
    parser.add_argument("--file", help="Path to Fangraphs pitcher data CSV file", type=str)
    args = parser.parse_args()
    
    if args.file:
        fangraphs_file = Path(args.file)
        if not fangraphs_file.exists():
            logger.error(f"Specified file does not exist: {fangraphs_file}")
            exit(1)
    else:
        fangraphs_file = find_latest_fangraphs_file()
        if not fangraphs_file:
            logger.error("No Fangraphs data file found. Run scrape_fangraphs_pitcher_data.py first.")
            exit(1)
    
    output_path, results_df = evaluate_fangraphs_pitchers(fangraphs_file)
    if output_path:
        print(f"\n💾 Complete evaluation saved to: {output_path}")
    else:
        print("❌ Evaluation failed")