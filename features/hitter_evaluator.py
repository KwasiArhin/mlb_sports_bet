# hitter_evaluator.py

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

class HitterEvaluator:
    def __init__(self):
        # Weights for each metric (higher weight = more important)
        # Using your exact specified metrics
        self.metric_weights = {
            'OPS': 0.15,       # On-base Plus Slugging
            'wOBA': 0.15,      # Weighted On-Base Average
            'wRC+': 0.15,      # Weighted Runs Created Plus (era-adjusted)
            'xwOBA': 0.12,     # Expected Weighted On-Base Average
            'xBA': 0.10,       # Expected Batting Average
            'xSLG': 0.10,      # Expected Slugging
            'HardHit%': 0.08,  # Hard Hit Percentage
            'Barrel%': 0.08,   # Barrel Percentage
            'K:BB': 0.07       # Strikeout to Walk Ratio (lower is better)
        }
        
        # Benchmarks for percentile scoring (MLB ranges for 2024-2025)
        self.benchmarks = {
            'OPS': {'excellent': 0.900, 'good': 0.800, 'average': 0.720, 'poor': 0.650},
            'wOBA': {'excellent': 0.370, 'good': 0.340, 'average': 0.320, 'poor': 0.300},
            'wRC+': {'excellent': 130, 'good': 115, 'average': 100, 'poor': 85},
            'xwOBA': {'excellent': 0.370, 'good': 0.340, 'average': 0.320, 'poor': 0.300},
            'xBA': {'excellent': 0.280, 'good': 0.260, 'average': 0.240, 'poor': 0.220},
            'xSLG': {'excellent': 0.480, 'good': 0.420, 'average': 0.380, 'poor': 0.340},
            'HardHit%': {'excellent': 45.0, 'good': 40.0, 'average': 35.0, 'poor': 30.0},
            'Barrel%': {'excellent': 12.0, 'good': 8.0, 'average': 5.5, 'poor': 3.5},
            'K:BB': {'excellent': 1.50, 'good': 2.00, 'average': 2.75, 'poor': 3.50}  # Lower is better
        }
        
        # Alternative column names that might appear in data
        self.column_mapping = {
            'OPS': ['OPS'],
            'wOBA': ['wOBA'],
            'wRC+': ['wRC+', 'wRC_plus'],
            'xwOBA': ['xwOBA'],
            'xBA': ['xBA'],
            'xSLG': ['xSLG'],
            'HardHit%': ['HardHit%', 'Hard%'],
            'Barrel%': ['Barrel%'],
            'K:BB': ['K:BB', 'K_BB']
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
        
        # For K:BB ratio, lower is better (inverse scoring)
        if metric == 'K:BB':
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
        
        # For all other metrics, higher is better
        else:
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
    
    def calculate_composite_score(self, hitter_row, df_columns):
        """Calculate weighted composite score for a hitter"""
        total_score = 0
        total_weight = 0
        score_breakdown = {}
        
        for metric, weight in self.metric_weights.items():
            col_name = self.find_column(pd.DataFrame(columns=df_columns), metric)
            
            if col_name and col_name in hitter_row.index and not pd.isna(hitter_row[col_name]):
                percentile_score = self.calculate_percentile_score(hitter_row[col_name], metric)
                total_score += percentile_score * weight
                total_weight += weight
                score_breakdown[metric] = {
                    'value': hitter_row[col_name],
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
        """Categorize hitter into performance tiers"""
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
        """Identify hitter's strengths and weaknesses"""
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
    
    def evaluate_hitters(self, hitters_df):
        """Evaluate all hitters and return ranked results"""
        results = []
        
        logger.info(f"Evaluating {len(hitters_df)} hitters...")
        
        for idx, hitter in hitters_df.iterrows():
            composite_score, score_breakdown = self.calculate_composite_score(hitter, hitters_df.columns)
            grade = self.get_grade(composite_score)
            tier = self.get_tier(composite_score)
            strengths, weaknesses = self.analyze_strengths_weaknesses(score_breakdown)
            
            # Get key stats for display
            key_stats = {}
            for metric in self.metric_weights.keys():
                col_name = self.find_column(hitters_df, metric)
                if col_name and col_name in hitter.index:
                    key_stats[metric] = hitter[col_name]
                else:
                    key_stats[metric] = 'N/A'
            
            result = {
                'hitter_name': hitter['Name'] if 'Name' in hitter.index else 'Unknown',
                'team': hitter['Team'] if 'Team' in hitter.index else 'N/A',
                'plate_appearances': hitter['PA'] if 'PA' in hitter.index else 'N/A',
                'games': hitter['G'] if 'G' in hitter.index else 'N/A',
                'composite_score': composite_score,
                'grade': grade,
                'tier': tier,
                'ops': key_stats['OPS'],
                'woba': key_stats['wOBA'],
                'wrc_plus': key_stats['wRC+'],
                'xwoba': key_stats['xwOBA'],
                'xba': key_stats['xBA'],
                'xslg': key_stats['xSLG'],
                'hard_hit_pct': key_stats['HardHit%'],
                'barrel_pct': key_stats['Barrel%'],
                'k_bb_ratio': key_stats['K:BB'],
                'avg': hitter['AVG'] if 'AVG' in hitter.index else 'N/A',
                'hr': hitter['HR'] if 'HR' in hitter.index else 'N/A',
                'rbi': hitter['RBI'] if 'RBI' in hitter.index else 'N/A',
                'sb': hitter['SB'] if 'SB' in hitter.index else 'N/A',
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

def evaluate_hitters(hitters_file: Path):
    """Load hitter data and evaluate hitters"""
    try:
        logger.info(f"Loading hitter data from: {hitters_file}")
        hitters_df = pd.read_csv(hitters_file)
        
        logger.info(f"Available columns: {list(hitters_df.columns)}")
        
        evaluator = HitterEvaluator()
        results_df = evaluator.evaluate_hitters(hitters_df)
        
        # Save results
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = PROCESSED_DIR / f"hitter_evaluation_{date_str}.csv"
        results_df.to_csv(output_path, index=False)
        logger.info(f"Evaluation report saved to: {output_path}")
        
        # Print comprehensive report
        print("\n" + "="*120)
        print("HITTER EVALUATION REPORT")
        print("="*120)
        
        # Top 5 hitters
        print("\n🏆 TOP 5 HITTERS:")
        print("-" * 120)
        top_5 = results_df.head(5)
        for _, hitter in top_5.iterrows():
            print(f"{hitter['rank']:2d}. {hitter['hitter_name']:<18} ({hitter['team']:3s}) | "
                  f"Score: {hitter['composite_score']:5.1f} | Grade: {hitter['grade']:2s} | "
                  f"Tier: {hitter['tier']:13s} | PA: {hitter['plate_appearances']}")
        
        # Bottom 5 hitters
        print("\n📉 BOTTOM 5 HITTERS:")
        print("-" * 120)
        bottom_5 = results_df.tail(5)
        for _, hitter in bottom_5.iterrows():
            print(f"{hitter['rank']:2d}. {hitter['hitter_name']:<18} ({hitter['team']:3s}) | "
                  f"Score: {hitter['composite_score']:5.1f} | Grade: {hitter['grade']:2s} | "
                  f"Tier: {hitter['tier']:13s} | PA: {hitter['plate_appearances']}")
        
        # Tier distribution
        print("\n📊 TIER DISTRIBUTION:")
        print("-" * 60)
        tier_counts = results_df['tier'].value_counts().sort_index()
        total_hitters = len(results_df)
        for tier, count in tier_counts.items():
            percentage = (count / total_hitters) * 100
            print(f"{tier:<15}: {count:3d} hitters ({percentage:4.1f}%)")
        
        # Detailed analysis of top 3
        print("\n🔍 DETAILED ANALYSIS - TOP 3 HITTERS:")
        print("=" * 120)
        
        for _, hitter in results_df.head(3).iterrows():
            print(f"\n{hitter['rank']}. {hitter['hitter_name']} ({hitter['team']}) - {hitter['grade']} ({hitter['composite_score']} points)")
            print(f"   📊 Traditional: OPS: {hitter['ops']} | wOBA: {hitter['woba']} | wRC+: {hitter['wrc_plus']}")
            print(f"   📈 Expected: xwOBA: {hitter['xwoba']} | xBA: {hitter['xba']} | xSLG: {hitter['xslg']}")
            print(f"   🚀 Batted Ball: Hard-Hit%: {hitter['hard_hit_pct']} | Barrel%: {hitter['barrel_pct']} | K:BB: {hitter['k_bb_ratio']}")
            print(f"   ✅ Strengths: {hitter['strengths']}")
            if hitter['weaknesses'] != 'None identified':
                print(f"   ❌ Weaknesses: {hitter['weaknesses']}")
        
        # Summary statistics
        print(f"\n📈 SUMMARY STATISTICS:")
        print("-" * 60)
        print(f"Average Score: {results_df['composite_score'].mean():.1f}")
        print(f"Median Score: {results_df['composite_score'].median():.1f}")
        print(f"Score Range: {results_df['composite_score'].min():.1f} - {results_df['composite_score'].max():.1f}")
        
        return output_path, results_df
        
    except Exception as e:
        logger.error(f"Error evaluating hitters: {e}")
        return None, None

def find_latest_hitter_file():
    """Find the most recent hitter data file"""
    # Look for enhanced data first
    enhanced_files = list(PROCESSED_DIR.glob("enhanced_hitter_data_*.csv"))
    fangraphs_files = list(PROCESSED_DIR.glob("fangraphs_hitter_data_*.csv"))
    
    all_files = enhanced_files + fangraphs_files
    if not all_files:
        logger.error("No hitter data files found.")
        return None
    
    latest_file = max(all_files, key=lambda x: x.stat().st_mtime)
    logger.info(f"Using hitter data file: {latest_file}")
    return latest_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate hitters using comprehensive data")
    parser.add_argument("--file", help="Path to hitter data CSV file", type=str)
    args = parser.parse_args()
    
    if args.file:
        hitters_file = Path(args.file)
        if not hitters_file.exists():
            logger.error(f"Specified file does not exist: {hitters_file}")
            exit(1)
    else:
        hitters_file = find_latest_hitter_file()
        if not hitters_file:
            logger.error("No hitter data file found. Run enhanced_hitter_collector.py first.")
            exit(1)
    
    output_path, results_df = evaluate_hitters(hitters_file)
    if output_path:
        print(f"\n💾 Complete evaluation saved to: {output_path}")
    else:
        print("❌ Evaluation failed")