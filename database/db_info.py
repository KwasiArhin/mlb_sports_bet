#!/usr/bin/env python3
"""
Database Information Script
Shows key information about the MLB database
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from mlb_database import MLBDatabase
import pandas as pd

def show_database_info():
    """Show comprehensive database information"""
    db = MLBDatabase()
    
    print("🏟️  MLB BETTING ANALYTICS DATABASE")
    print("=" * 60)
    
    # Database statistics
    stats = db.get_database_stats()
    print("📊 DATABASE STATISTICS:")
    for table, count in stats.items():
        print(f"   {table:20}: {count:,} records")
    
    print("\n🏆 TOP PITCHERS BY WAR:")
    print("-" * 60)
    try:
        pitchers = db.get_latest_pitcher_stats(limit=10)
        for _, pitcher in pitchers.iterrows():
            war = pitcher.get('war', 0)
            era = pitcher.get('era', 0)
            name = pitcher.get('name', 'Unknown')
            team = pitcher.get('team', 'N/A')
            
            if pd.notna(war) and pd.notna(era):
                print(f"   {name:25} {team:3} | WAR: {war:4.1f} | ERA: {era:4.2f}")
    except Exception as e:
        print(f"   Error loading pitcher data: {e}")
    
    print("\n🏏 TOP HITTERS BY wRC+:")
    print("-" * 60)
    try:
        hitters = db.get_latest_hitter_stats(limit=10)
        for _, hitter in hitters.iterrows():
            wrc_plus = hitter.get('wrc_plus', 0)
            ops = hitter.get('ops', 0)
            name = hitter.get('name', 'Unknown')
            team = hitter.get('team', 'N/A')
            
            if pd.notna(wrc_plus) and pd.notna(ops):
                print(f"   {name:25} {team:3} | wRC+: {wrc_plus:3.0f} | OPS: {ops:5.3f}")
    except Exception as e:
        print(f"   Error loading hitter data: {e}")
    
    print("\n🎯 TODAY'S GAMES:")
    print("-" * 60)
    try:
        from datetime import date
        games = db.get_games_by_date(date.today())
        
        if games.empty:
            print("   No games found for today")
        else:
            for _, game in games.iterrows():
                away = game.get('away_team_abbr', 'N/A')
                home = game.get('home_team_abbr', 'N/A') 
                away_ml = game.get('away_moneyline', 'N/A')
                home_ml = game.get('home_moneyline', 'N/A')
                total = game.get('total_points', 'N/A')
                
                print(f"   {away:3} @ {home:3} | ML: {away_ml:>4}/{home_ml:>4} | Total: {total}")
                
    except Exception as e:
        print(f"   Error loading games: {e}")
    
    # Last refresh dates
    print(f"\n🔄 DATA FRESHNESS:")
    print("-" * 60)
    data_types = ['pitcher_stats', 'hitter_stats', 'betting_odds', 'statcast_stats']
    
    for data_type in data_types:
        last_refresh = db.get_last_refresh_date(data_type)
        if last_refresh:
            days_ago = (date.today() - last_refresh).days
            status = "🟢 Fresh" if days_ago == 0 else f"🟡 {days_ago} days old" if days_ago <= 2 else f"🔴 {days_ago} days old"
            print(f"   {data_type:20}: {last_refresh} ({status})")
        else:
            print(f"   {data_type:20}: Never refreshed (🔴 Needs update)")
    
    print("=" * 60)
    
    # Show database file info
    db_file = Path(__file__).parent / "mlb_data.db"
    if db_file.exists():
        size_mb = db_file.stat().st_size / (1024 * 1024)
        print(f"📁 Database file: {db_file}")
        print(f"💾 File size: {size_mb:.2f} MB")
    
    print("\n🚀 Quick Start:")
    print("   • Start database dashboard: python web/database_dashboard_app.py")
    print("   • Refresh data: python database/refresh_data.py --odds-only")
    print("   • Check status: python database/refresh_data.py --status")

if __name__ == "__main__":
    show_database_info()