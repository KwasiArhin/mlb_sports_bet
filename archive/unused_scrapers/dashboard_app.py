#!/usr/bin/env python3
"""
MLB Pitcher Dashboard Web Application
Displays pitcher evaluation data in an interactive web dashboard
"""

from flask import Flask, render_template, jsonify, send_file
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import csv
import io

# Setup paths
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'mlb-pitcher-dashboard-2025'

class PitcherDashboard:
    def __init__(self):
        self.data = None
        self.current_date = None
        
    def load_latest_evaluation_data(self):
        """Load the most recent pitcher evaluation data"""
        try:
            # Find the latest evaluation file
            eval_files = list(PROCESSED_DIR.glob("fangraphs_pitcher_evaluation_*.csv"))
            if not eval_files:
                return None
                
            latest_file = max(eval_files, key=lambda x: x.stat().st_mtime)
            
            # Load the data
            df = pd.read_csv(latest_file)
            
            # Parse the date from filename
            date_str = latest_file.name.replace("fangraphs_pitcher_evaluation_", "").replace(".csv", "")
            try:
                self.current_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")
            except:
                self.current_date = datetime.now().strftime("%B %d, %Y")
            
            # Clean and prepare data
            df = self.clean_data(df)
            self.data = df
            
            return df
            
        except Exception as e:
            print(f"Error loading evaluation data: {e}")
            return None
    
    def clean_data(self, df):
        """Clean and prepare the data for display"""
        # Handle NaN values
        numeric_columns = ['whip', 'fip', 'siera', 'csw_rate', 'xera', 'xfip', 'xwoba', 'xba', 'xslg', 'stuff_plus', 'innings_pitched']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle missing strengths/weaknesses
        df['strengths'] = df['strengths'].fillna('None identified')
        df['weaknesses'] = df['weaknesses'].fillna('None identified')
        
        # Ensure rank column exists
        if 'rank' not in df.columns:
            df = df.sort_values('composite_score', ascending=False)
            df['rank'] = range(1, len(df) + 1)
        
        return df
    
    def get_summary_stats(self):
        """Calculate summary statistics for the dashboard"""
        if self.data is None:
            return {}
        
        df = self.data
        
        # Count by tier
        tier_counts = df['tier'].value_counts()
        
        # Grade mappings for elite count
        elite_grades = ['A+', 'A', 'A-']
        above_avg_grades = ['B+', 'B', 'B-']
        average_grades = ['C+', 'C', 'C-']
        poor_grades = ['D+', 'D', 'F']
        
        elite_count = len(df[df['grade'].isin(elite_grades)])
        above_average_count = len(df[df['grade'].isin(above_avg_grades)])
        average_count = len(df[df['grade'].isin(average_grades)])
        poor_count = len(df[df['grade'].isin(poor_grades)])
        
        return {
            'total_pitchers': len(df),
            'elite_count': elite_count,
            'above_average_count': above_average_count,
            'average_count': average_count,
            'poor_count': poor_count,
            'avg_score': round(df['composite_score'].mean(), 1),
            'tier_distribution': tier_counts.to_dict()
        }
    
    def get_top_performers(self, n=6):
        """Get top N performing pitchers"""
        if self.data is None:
            return []
        
        return self.data.head(n).to_dict('records')
    
    def get_unique_teams(self):
        """Get list of unique teams"""
        if self.data is None:
            return []
        
        return sorted(self.data['team'].unique().tolist())

# Initialize dashboard
dashboard = PitcherDashboard()

@app.route('/')
def index():
    """Main dashboard page"""
    # Load latest data
    data = dashboard.load_latest_evaluation_data()
    
    if data is None:
        return render_template('error.html', 
                             error="No pitcher evaluation data found. Please run the evaluation system first.")
    
    # Get dashboard data
    summary = dashboard.get_summary_stats()
    top_performers = dashboard.get_top_performers()
    teams = dashboard.get_unique_teams()
    pitchers = data.to_dict('records')
    
    return render_template('pitcher_dashboard.html',
                         pitchers=pitchers,
                         top_performers=top_performers,
                         summary=summary,
                         teams=teams,
                         current_date=dashboard.current_date)

@app.route('/api/pitchers')
def api_pitchers():
    """API endpoint for pitcher data"""
    if dashboard.data is None:
        return jsonify({'error': 'No data available'}), 404
    
    return jsonify(dashboard.data.to_dict('records'))

@app.route('/api/summary')
def api_summary():
    """API endpoint for summary statistics"""
    if dashboard.data is None:
        return jsonify({'error': 'No data available'}), 404
    
    return jsonify(dashboard.get_summary_stats())

@app.route('/export/csv')
def export_csv():
    """Export pitcher data as CSV"""
    if dashboard.data is None:
        return "No data available", 404
    
    # Create CSV in memory
    output = io.StringIO()
    dashboard.data.to_csv(output, index=False)
    output.seek(0)
    
    # Convert to BytesIO for sending
    mem = io.BytesIO()
    mem.write(output.getvalue().encode('utf-8'))
    mem.seek(0)
    
    return send_file(
        mem,
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'pitcher_evaluation_{datetime.now().strftime("%Y%m%d")}.csv'
    )

@app.route('/refresh')
def refresh_data():
    """Refresh the dashboard data"""
    data = dashboard.load_latest_evaluation_data()
    if data is None:
        return jsonify({'success': False, 'error': 'Failed to load data'}), 500
    
    return jsonify({'success': True, 'message': 'Data refreshed successfully'})

@app.errorhandler(404)
def not_found(error):
    return render_template('error.html', error="Page not found"), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error="Internal server error"), 500

if __name__ == '__main__':
    print("🚀 Starting MLB Pitcher Dashboard...")
    print("📊 Loading latest pitcher evaluation data...")
    
    # Load data on startup
    data = dashboard.load_latest_evaluation_data()
    if data is not None:
        print(f"✅ Loaded data for {len(data)} pitchers")
        print(f"📅 Data date: {dashboard.current_date}")
        print("🌐 Dashboard will be available at: http://localhost:5000")
    else:
        print("⚠️  No evaluation data found. Please run the evaluation system first:")
        print("    python features/fangraphs_pitcher_collector.py --sample")
        print("    python features/fangraphs_pitcher_evaluator.py")
    
    # Run the app
    app.run(debug=True, host='0.0.0.0', port=5002)