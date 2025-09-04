# MLB Sports Betting Analytics - Claude Memory

## Project Context
This is a comprehensive MLB sports betting analytics platform that combines advanced sabermetrics with real-time odds data to provide betting insights. The system focuses on pitcher and hitter evaluation using Fangraphs, Baseball Savant, and The Odds API.

## Key System Commands
- **Main Pipeline**: `python scraping/daily_betting_pipeline.py`
- **Web Dashboard**: `python web/dashboard_app.py`
- **Test Command**: Check for pytest, unittest, or custom test scripts
- **Lint Command**: Check for flake8, pylint, or black formatting

## Project Architecture
- `scraping/`: Data collection pipeline (games, odds, results)
- `features/`: Player analytics and evaluation systems  
- `web/`: Flask dashboard for interactive analysis
- `data/raw/`: Daily collected raw data files
- `data/processed/`: Analyzed and integrated datasets
- `archive/`: Deprecated/unused files (well documented)

## Data Sources & APIs
- **Fangraphs**: Advanced pitcher/hitter sabermetrics
- **Baseball Savant**: Expected stats and batted ball data
- **MLB API**: Official schedules and game results
- **The Odds API**: Real-time sportsbook odds (key: `a74383d66d314cc2fc96f1e54931d6a4`)

## Key Metrics Focus
**Pitchers**: WHIP, FIP, SIERA, CSW%, xERA, xFIP, xwOBA, Stuff+
**Hitters**: OPS, wOBA, wRC+, xwOBA, xBA, xSLG, Hard-Hit%, Barrel%

## Development Preferences
- **File Organization**: Always prefer editing existing files over creating new ones
- **Code Style**: Follow existing patterns in each module, no unnecessary comments
- **Data Processing**: Use pandas for data manipulation, requests for API calls
- **Error Handling**: Graceful failures with informative logging
- **Testing**: Run validation after major changes

## Quality Standards
- Validate data types and formats before processing
- Handle API rate limits respectfully
- Use weighted evaluation systems for player analysis
- Maintain separation between raw data collection and processed analysis
- Prioritize FanDuel odds when available from multiple sportsbooks

## Active Development Areas
- Daily pipeline orchestration and reliability
- Advanced player evaluation algorithms
- Web dashboard interactivity and performance
- Historical data integration for model training

## Archive Policy
The `archive/` directory contains evolution history - reference for context but avoid modifying archived code. Current working solution is in main directories.