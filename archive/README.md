# Archive - Unused Files

This directory contains files that were developed during the evolution of the MLB sports betting project but are no longer actively used in the current solution.

## Current Active Solution (NOT in archive)

### Data Collection (scraping/)
- `daily_games_collector.py` - Gets daily MLB games from MLB API
- `odds_api_collector.py` - Collects FanDuel odds via The Odds API
- `game_results_collector.py` - Collects completed game results
- `daily_betting_pipeline.py` - **Main pipeline integrating everything**

### Player Analysis (features/)
- `enhanced_pitcher_collector.py` - **Best pitcher data** (Fangraphs + Baseball Savant)
- `enhanced_hitter_collector.py` - **Best hitter data** (comprehensive metrics)
- `fangraphs_pitcher_evaluator.py` - Pitcher evaluation with weighted scoring
- `hitter_evaluator.py` - Hitter evaluation with weighted scoring

### Web Dashboard (web/)
- `dashboard_app.py` - Flask web application
- `templates/pitcher_dashboard.html` - Pitcher dashboard template
- `static/css/dashboard.css` - Dashboard styling
- `static/js/dashboard.js` - Dashboard interactivity

## Archived Files

### unused_scrapers/
**Individual collectors replaced by enhanced versions:**
- `fangraphs_pitcher_collector.py` - Replaced by enhanced_pitcher_collector.py
- `baseball_savant_collector.py` - Integrated into enhanced_pitcher_collector.py
- `fangraphs_hitter_collector.py` - Replaced by enhanced_hitter_collector.py

**Old scraping approaches:**
- `scrape_game_results.py` - Replaced by game_results_collector.py
- `scrape_historical_matchups.py` - Old matchup scraping
- `scrape_matchups.py` - Old matchup scraping
- `scrape_statcast.py` - Direct Statcast scraping (complex)
- `scrape_team_form_mlb.py` - Team form scraping
- `scrapetest.py` - Testing file

### unused_features/
**Manual feature engineering (replaced by Fangraphs data):**
- `build_advanced_pitcher_features.py` - Manual pitcher metric calculation
- `build_batter_stat_features.py` - Manual batter stats
- `build_pitcher_event_features.py` - Event-based pitcher features
- `build_pitcher_stat_features.py` - Basic pitcher stats
- `build_player_event_features.py` - Player event features

**Historical data generators:**
- `generate_historical_batter_stats.py` - Historical batter analysis
- `generate_historical_features.py` - Historical feature generation
- `generate_historical_pitcher_stats.py` - Historical pitcher analysis
- `generate_historical_team_form.py` - Historical team form

**Old evaluation systems:**
- `pitcher_evaluation_system.py` - Replaced by fangraphs_pitcher_evaluator.py
- `main_features.py` - Old main feature system
- `historical_main_features.py` - Historical main features
- `engineer_features.py` - General feature engineering

**Utilities:**
- `build_batter_team_lookup.py` - Team lookup utility

### unused_data/
**Old data files:**
- `historical_main_features_raw.csv` - Old feature data
- `team_form_2025-06-30.csv` - Old team form data
- `pitcher_stat_features_2025-07-01.csv` - Old pitcher features
- `main_features_2025-07-01.csv` - Old main features

## Evolution Summary

1. **Started with** manual Statcast data processing and complex feature engineering
2. **User feedback**: "I think it would be better if we pulled all this information from fangraphs instead"
3. **Pivoted to** Fangraphs + Baseball Savant integration for cleaner, more accurate data
4. **Final solution**: Enhanced collectors with proper CSW% formatting and dual data sources

## Key Improvements Made

- **Simplified data collection** using established sources (Fangraphs, Baseball Savant)
- **Fixed CSW% formatting** to display as percentages (32.1% vs 0.321)
- **Dual data source integration** for expected stats
- **Comprehensive evaluation system** with weighted metrics
- **Complete betting pipeline** integrating games, odds, and player analysis

The current solution is much cleaner, more reliable, and provides better data quality than the archived approaches.