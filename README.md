# MLB Sports Betting Analytics Platform

## Overview

A comprehensive, production-ready system for MLB sports betting analysis that combines advanced player metrics, real-time odds, and game data into actionable insights. The platform leverages Fangraphs advanced statistics, Baseball Savant expected metrics, and The Odds API to provide daily betting recommendations with sophisticated pitcher and hitter evaluations.

This system is designed for serious sports betting analysis, providing institutional-quality data processing with a focus on advanced sabermetrics and expected performance metrics that traditional systems overlook.

---

## Key Features

### 🎯 **Advanced Player Analytics**
- **Pitcher Analysis**: WHIP, FIP, SIERA, CSW%, xERA, xFIP, xwOBA, xBA, xSLG, Stuff+
- **Hitter Analysis**: OPS, wOBA, wRC+, xwOBA, xBA, xSLG, Hard-Hit%, Barrel%, K:BB ratio
- **Weighted Evaluation System**: Composite scores with letter grades and performance tiers

### 💰 **Real-Time Odds Integration**
- **FanDuel Odds** (prioritized via The Odds API)
- **Multiple Sportsbooks** for best line shopping
- **Complete Markets**: Moneyline, spreads, totals (over/under)
- **Live Updates** with API rate limiting management

### 📊 **Comprehensive Data Pipeline**
- **Daily Game Collection** from MLB's official API
- **Historical Results** for model training
- **Data Integration** combining all sources into unified datasets
- **Web Dashboard** for interactive analysis

### 🌐 **Interactive Web Interface**
- **Pitcher Dashboard** with filtering and search
- **Performance Metrics** visualization
- **Grade-based Color Coding** for quick insights
- **Export Functionality** for further analysis

---

## Project Structure

```
mlb_sports_bet/
├── scraping/                          # Data Collection
│   ├── daily_betting_pipeline.py      # 🎯 MAIN PIPELINE
│   ├── daily_games_collector.py       # MLB games via API
│   ├── odds_api_collector.py          # FanDuel odds via API
│   └── game_results_collector.py      # Historical results
│
├── features/                          # Player Analytics
│   ├── enhanced_pitcher_collector.py  # 🏆 Best pitcher data
│   ├── enhanced_hitter_collector.py   # 🏆 Best hitter data
│   ├── fangraphs_pitcher_evaluator.py # Pitcher evaluation
│   └── hitter_evaluator.py           # Hitter evaluation
│
├── web/                              # Dashboard
│   ├── dashboard_app.py              # Flask web app
│   ├── templates/                    # HTML templates
│   └── static/                       # CSS/JS files
│
├── data/
│   ├── raw/                          # Daily collected data
│   └── processed/                    # Analyzed datasets
│
└── archive/                          # Unused files (documented)
```

---

## Quick Start

### 1. **Run Complete Daily Pipeline**
```bash
# Collect everything: games + odds + player analysis
python scraping/daily_betting_pipeline.py
```

### 2. **Launch Web Dashboard**
```bash
# Start interactive pitcher dashboard
python web/dashboard_app.py
# Visit: http://localhost:5000
```

### 3. **Individual Components**
```bash
# Get today's games
python scraping/daily_games_collector.py

# Get FanDuel odds
python scraping/odds_api_collector.py  

# Collect pitcher data
python features/enhanced_pitcher_collector.py --season 2025

# Collect hitter data
python features/enhanced_hitter_collector.py --season 2025
```

---

## Data Sources & APIs

### **Primary Sources**
- **Fangraphs**: Advanced sabermetrics (FIP, SIERA, wRC+, etc.)
- **Baseball Savant**: Expected stats (xwOBA, xBA, xSLG) and batted ball metrics
- **MLB API**: Official game schedules, results, and basic stats
- **The Odds API**: Real-time sportsbook odds (FanDuel prioritized)

### **Key Metrics Explained**

#### **Pitcher Metrics**
- **WHIP**: Walks + Hits per Inning Pitched
- **FIP**: Fielding Independent Pitching (era-adjusted)
- **SIERA**: Skill-Interactive ERA (batted ball outcomes)
- **CSW%**: Called Strike + Whiff % (plate discipline)
- **xERA/xFIP**: Expected versions based on quality of contact
- **Stuff+**: Pitch quality rating (100 = league average)

#### **Hitter Metrics**
- **wOBA**: Weighted On-Base Average (comprehensive offensive metric)
- **wRC+**: Weighted Runs Created Plus (park/era adjusted, 100 = average)
- **xwOBA/xBA/xSLG**: Expected stats based on quality of contact
- **Hard-Hit%**: Percentage of contact over 95 MPH
- **Barrel%**: Optimal launch angle + exit velocity combinations

---

## System Architecture

### **Daily Pipeline Flow**
1. **Game Collection**: Scrape today's MLB schedule with pitchers
2. **Odds Integration**: Fetch FanDuel lines via The Odds API
3. **Player Analysis**: Collect & evaluate current season pitcher/hitter data
4. **Data Integration**: Merge all sources into unified betting dataset
5. **Output Generation**: Create analysis-ready CSV files

### **Evaluation System**
- **Weighted Scoring**: Each metric has assigned importance
- **Percentile Benchmarks**: Performance tiers based on MLB averages
- **Letter Grades**: A+ to F scale for quick assessment
- **Composite Scores**: Single number representing overall quality

### **Web Dashboard Features**
- **Real-time Data**: Auto-refreshing pitcher analysis
- **Interactive Filtering**: By team, grade, tier, specific metrics
- **Export Options**: CSV download for external analysis
- **Mobile Responsive**: Works on all device sizes

---

## Sample Output

### **Daily Pipeline Results**
```
🚀 DAILY BETTING PIPELINE SUMMARY - 2025-09-02
================================================================
📅 Games collected: 14
💰 Games with odds: 21  
⚾ Pitchers analyzed: 295
🏏 Hitters analyzed: 319
🔗 Integrated games: 14

📊 SAMPLE INTEGRATED DATA:
----------------------------------------------------------------
NYM @ DET | ML: -130/+110 | Pitcher Scores: 85.4/72.1
LAD @ PIT | ML: -168/+142 | Pitcher Scores: 91.2/68.7
TOR @ CIN | ML: -146/+124 | Pitcher Scores: 78.9/82.3
```

### **Pitcher Evaluation Example**
```
🏆 TOP 5 PITCHERS:
----------------------------------------------------------------
1. Paul Skenes     (PIT) | Score: 93.2 | Grade: A+ | Elite
2. Tarik Skubal    (DET) | Score: 91.8 | Grade: A+ | Elite  
3. Chris Sale      (ATL) | Score: 89.4 | Grade: A  | Elite
```

---

## Performance & Accuracy

### **Data Quality**
- **295 Pitchers** analyzed with comprehensive metrics
- **319 Hitters** with advanced sabermetrics
- **Real-time Odds** from 9+ major sportsbooks
- **Historical Integration** for model training datasets

### **System Reliability** 
- **API Rate Limiting**: Respectful data collection
- **Error Handling**: Graceful failures with logging
- **Data Validation**: Automatic format corrections (e.g., CSW% percentages)
- **Backup Sources**: Multiple endpoints for critical data

### **Web Dashboard Performance**
- **Sub-second Load Times** with optimized queries
- **Responsive Design** for mobile and desktop
- **Real-time Updates** without page refreshes
- **Export Capabilities** for analysis workflows

---

## Configuration & Setup

### **Dependencies**
```bash
pip install pandas requests selenium pybaseball flask
```

### **API Keys Required**
- **The Odds API**: Already configured (`a74383d66d314cc2fc96f1e54931d6a4`)
- No other API keys needed (Fangraphs/Baseball Savant are free)

### **System Requirements**
- **Python 3.8+**
- **Chrome WebDriver** (for any future web scraping needs)
- **5GB Storage** for historical data accumulation

---

## Advanced Usage

### **Historical Data Collection**
```bash
# Collect game results for model training
python scraping/daily_betting_pipeline.py --historical \
  --start-date 2025-08-01 --end-date 2025-09-01
```

### **Custom Analysis**
```bash
# Specific date analysis
python scraping/daily_betting_pipeline.py --date 2025-09-01

# Minimum qualification thresholds
python features/enhanced_pitcher_collector.py --season 2025 --min-ip 50
python features/enhanced_hitter_collector.py --season 2025 --min-pa 200
```

### **Dashboard Customization**
- **Modify Templates**: `web/templates/pitcher_dashboard.html`
- **Update Styling**: `web/static/css/dashboard.css`
- **Add Features**: `web/static/js/dashboard.js`

---

## Evolution & Architecture Decisions

This system represents the evolution from complex manual feature engineering to leveraging established sabermetric sources. Key architectural decisions:

1. **Fangraphs Integration**: Switched from manual Statcast processing to proven metrics
2. **Dual Data Sources**: Combined Fangraphs + Baseball Savant for comprehensive analysis  
3. **API-First Approach**: Eliminated fragile web scraping in favor of stable APIs
4. **Weighted Evaluation**: Created sophisticated scoring system for player quality
5. **Clean Architecture**: Separated concerns into logical modules with clear interfaces

### **Why This Approach Works**
- **Proven Metrics**: Uses same advanced stats as professional analysts
- **Data Quality**: Sources are maintained by baseball analytics experts
- **Reliability**: APIs are more stable than web scraping
- **Extensibility**: Easy to add new metrics or modify evaluation weights
- **Performance**: Efficient data processing with minimal external dependencies

---

## Future Enhancements

### **Model Integration**
- **Machine Learning Pipeline**: Train models on integrated datasets
- **Predictive Analytics**: Win probability models using advanced metrics
- **Betting Strategy**: Kelly criterion and bankroll management
- **Real-time Updates**: Live game adjustments and in-game betting

### **Advanced Features**
- **Weather Integration**: Park factors and environmental conditions
- **Injury Tracking**: Player availability and impact analysis
- **Line Movement**: Historical odds tracking and value identification
- **Portfolio Management**: Multi-game betting strategy optimization

### **Platform Expansion**
- **Mobile App**: iOS/Android native applications
- **API Endpoints**: RESTful API for third-party integration
- **Alerting System**: Push notifications for value opportunities  
- **Social Features**: Community predictions and leaderboards

---

## Contributing

This is a personal sports betting analytics project. The codebase is organized with clear separation of concerns and extensive documentation for future development.

### **Code Organization**
- **Active Code**: Current working solution in main directories
- **Archive**: `archive/` contains evolution history and unused approaches
- **Documentation**: Comprehensive README files explain decisions and usage

---

## Author

**Kwasi Arhin**  
Machine Learning Engineer | Data Scientist 

📧 **Email**: kwasiarhin@gmail.com 
🔗 **LinkedIn**: https://www.linkedin.com/in/roman-esquibel-75b994223/

---

*This system combines institutional-quality baseball analytics with practical betting applications, providing a professional-grade platform for MLB sports betting analysis.*