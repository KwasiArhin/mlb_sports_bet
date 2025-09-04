#!/usr/bin/env python3
"""
Quick starter script for the MLB Predictions Dashboard
"""

import os
import sys
import subprocess
import webbrowser
from pathlib import Path

def main():
    """Start the dashboard and optionally open browser."""
    
    print("""
    ⚾ Starting MLB Predictions Dashboard ⚾
    
    This will start the Flask web server and open your browser.
    """)
    
    # Change to web directory for enhanced dashboard
    dashboard_dir = Path(__file__).parent / 'web'
    os.chdir(dashboard_dir)
    
    print(f"📁 Working directory: {dashboard_dir}")
    print("🚀 Starting Enhanced Flask server...")
    
    try:
        # Start the Enhanced Flask server
        subprocess.Popen([sys.executable, 'enhanced_dashboard_app.py'])
        
        print("✅ Dashboard server started successfully!")
        print(f"📊 Dashboard URL: http://localhost:5000")
        print(f"📈 View predictions: http://localhost:5000/predictions")
        print(f"🧠 Model analytics: http://localhost:5000/model-analytics")
        print(f"⚙️  Pipeline status: http://localhost:5000/pipeline-status")
        print()
        print("Press Ctrl+C to stop the dashboard when finished.")
        print()
        
        # Ask if user wants to open browser
        try:
            open_browser = input("Open dashboard in browser? (y/n): ").lower().strip()
            if open_browser in ['y', 'yes', '']:
                print("🌐 Opening browser...")
                webbrowser.open('http://localhost:5000')
        except KeyboardInterrupt:
            print("\nSkipping browser open.")
        
    except Exception as e:
        print(f"❌ Error starting dashboard: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())