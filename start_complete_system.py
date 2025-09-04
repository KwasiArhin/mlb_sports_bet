#!/usr/bin/env python3
"""
Complete MLB System Launcher

Starts both the dashboard and pipeline API for a complete MLB prediction system.
"""

import os
import sys
import subprocess
import time
import webbrowser
from pathlib import Path


def main():
    """Start the complete MLB prediction system."""
    
    print("""
    ⚾ MLB Complete Prediction System ⚾
    
    🚀 Starting all components...
    """)
    
    base_dir = Path(__file__).parent
    
    # Start Pipeline API (port 5001)
    print("🔌 Starting Pipeline API server...")
    api_process = subprocess.Popen([
        sys.executable, 
        str(base_dir / 'integrations' / 'pipeline_api.py')
    ])
    
    # Wait a moment for API to start
    time.sleep(3)
    
    # Start Enhanced Dashboard (port 5000)
    print("📊 Starting Enhanced Dashboard server...")
    dashboard_process = subprocess.Popen([
        sys.executable,
        str(base_dir / 'web' / 'enhanced_dashboard_app.py')
    ])
    
    # Wait for servers to start
    time.sleep(5)
    
    print("""
    ✅ System Started Successfully!
    
    🌐 Access URLs:
    ├── 📊 Dashboard: http://localhost:5000
    ├── 🔌 Pipeline API: http://localhost:5001
    └── 🔗 Health Check: http://localhost:5001/api/health
    
    📋 Available Features:
    ├── View today's games and betting recommendations
    ├── Monitor model performance and accuracy
    ├── Trigger manual pipeline runs
    ├── Track historical performance
    └── Kelly Criterion optimal bet sizing
    
    🎯 Quick Actions:
    1. Open dashboard to view current predictions
    2. Use "Pipeline Status" to trigger daily update
    3. Check "Model Analytics" for performance metrics
    4. Review "Today's Games" for betting recommendations
    
    Press Ctrl+C to stop all servers
    """)
    
    # Ask about opening browser
    try:
        open_browser = input("Open dashboard in browser? (y/n): ").lower().strip()
        if open_browser in ['y', 'yes', '']:
            print("🌐 Opening dashboard...")
            webbrowser.open('http://localhost:5000')
    except KeyboardInterrupt:
        print("\\nSkipping browser open.")
    
    try:
        # Keep the processes running
        print("\\n⏳ System is running. Press Ctrl+C to stop all servers.")
        while True:
            time.sleep(1)
            
            # Check if processes are still running
            if api_process.poll() is not None:
                print("⚠️  Pipeline API has stopped unexpectedly")
                break
                
            if dashboard_process.poll() is not None:
                print("⚠️  Dashboard has stopped unexpectedly")
                break
                
    except KeyboardInterrupt:
        print("\\n\\n🛑 Stopping all servers...")
        
        # Terminate processes
        api_process.terminate()
        dashboard_process.terminate()
        
        # Wait for clean shutdown
        time.sleep(2)
        
        # Force kill if still running
        try:
            api_process.kill()
            dashboard_process.kill()
        except:
            pass
        
        print("✅ All servers stopped successfully!")
        print("🏆 Thanks for using the MLB Prediction System!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())