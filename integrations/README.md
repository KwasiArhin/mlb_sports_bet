# MLB Live Integration System

Complete integration layer that automatically fetches today's MLB games and runs the prediction pipeline with Kelly Criterion bet sizing.

## Components

### 🎯 Live Games Fetcher (`live_games_fetcher.py`)
Fetches today's MLB schedule from multiple APIs with fallback support:
- **Primary**: MLB Stats API (official)
- **Fallback**: ESPN API
- **Features**: Automatic team name mapping, game status filtering, CSV export

### 🔄 Pipeline Orchestrator (`daily_pipeline_orchestrator.py`)
Coordinates the complete daily workflow:
1. **Fetch Games** → Get today's MLB schedule
2. **Feature Engineering** → Generate model features
3. **Model Prediction** → Run XGBoost predictions
4. **Kelly Sizing** → Calculate optimal bet amounts
5. **Dashboard Update** → Refresh web interface

### 🔌 Pipeline API (`pipeline_api.py`)
RESTful API server for triggering and monitoring the pipeline:
- Trigger pipeline runs remotely
- Monitor pipeline status in real-time
- View run history and errors
- Integration with dashboard

## Quick Start

### Option 1: Complete System (Recommended)
```bash
python start_complete_system.py
```

### Option 2: Individual Components
```bash
# Start Pipeline API
python integrations/pipeline_api.py

# Fetch today's games
python integrations/live_games_fetcher.py

# Run complete pipeline
python integrations/daily_pipeline_orchestrator.py
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/pipeline/trigger` | Start daily pipeline |
| `GET` | `/api/pipeline/status` | Get current status |
| `GET` | `/api/pipeline/history` | View run history |
| `POST` | `/api/pipeline/stop` | Stop running pipeline |
| `GET` | `/api/games/today` | Get today's games |
| `GET` | `/api/health` | Health check |

## Usage Examples

### Fetch Today's Games
```bash
# Get today's games
python integrations/live_games_fetcher.py

# Get specific date
python integrations/live_games_fetcher.py --date 2025-08-31

# Save to CSV
python integrations/live_games_fetcher.py --save
```

### Run Daily Pipeline
```bash
# Run for today with default bankroll ($1000)
python integrations/daily_pipeline_orchestrator.py

# Run for specific date and bankroll
python integrations/daily_pipeline_orchestrator.py --date 2025-08-31 --bankroll 5000

# Dry run (test without execution)
python integrations/daily_pipeline_orchestrator.py --dry-run
```

### API Usage
```bash
# Trigger pipeline via API
curl -X POST http://localhost:5001/api/pipeline/trigger \
     -H "Content-Type: application/json" \
     -d '{"date": "2025-08-31", "bankroll": 1000}'

# Check pipeline status
curl http://localhost:5001/api/pipeline/status

# Get today's games
curl "http://localhost:5001/api/games/today?date=2025-08-31"
```

## Configuration

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `MLB_BANKROLL` | 1000.0 | Default bankroll amount |
| `MLB_DEFAULT_ODDS` | 1.91 | Default odds (-110) |
| `MLB_MAX_BET_FRACTION` | 0.25 | Max bet per game |

### Data Flow
```
Live MLB APIs
    ↓
Game Schedule Fetch
    ↓
Feature Engineering Pipeline
    ↓
XGBoost Model Prediction
    ↓
Kelly Criterion Bet Sizing
    ↓
Dashboard Update
```

## Error Handling

### Automatic Failover
- **MLB API fails** → Automatically tries ESPN API
- **Feature engineering fails** → Detailed error logging and graceful exit
- **Model prediction fails** → Pipeline stops with clear error message
- **Kelly sizing fails** → Falls back to basic predictions

### Monitoring
- All pipeline steps are logged with timestamps
- Failed steps are tracked with specific error messages
- Pipeline status is available via API
- Run history is maintained for debugging

## Integration with Dashboard

The system seamlessly integrates with the web dashboard:

1. **Live Data**: Dashboard automatically shows latest predictions
2. **Manual Triggers**: Pipeline can be started from dashboard interface
3. **Status Updates**: Real-time pipeline status in dashboard
4. **Error Reporting**: Failed runs are displayed with details

## Scheduling

### Manual Execution
Run the pipeline manually when needed:
```bash
python integrations/daily_pipeline_orchestrator.py
```

### Automated Scheduling (macOS/Linux)
Add to crontab for daily execution:
```bash
# Run daily at 8 AM
0 8 * * * cd /path/to/project && python integrations/daily_pipeline_orchestrator.py
```

### Automated Scheduling (Windows)
Use Windows Task Scheduler to run:
```
python integrations/daily_pipeline_orchestrator.py
```

## Production Considerations

### Performance
- Pipeline typically completes in 2-5 minutes
- API responses are typically < 100ms
- Memory usage stays under 500MB during execution

### Reliability
- Multiple API fallbacks for game data
- Comprehensive error handling at each step
- Transaction-like pipeline execution (fail fast)
- Detailed logging for troubleshooting

### Security
- API runs on localhost by default
- No sensitive data exposed in logs
- CORS enabled for dashboard integration

## Troubleshooting

### Common Issues

**No games found**
```bash
# Check if date has games
python integrations/live_games_fetcher.py --date 2025-08-31
```

**Feature engineering fails**
```bash
# Check if required data files exist
ls data/processed/historical_main_features.csv
```

**Model prediction fails**
```bash
# Verify model training data exists
ls data/processed/historical_main_features.csv
```

**API server won't start**
```bash
# Check if port 5001 is available
lsof -i :5001
```

### Logs
- Pipeline logs: `daily_pipeline.log`
- API logs: Console output
- Individual component logs: Component-specific files

---

Built for automated MLB prediction analysis with Kelly Criterion optimal bet sizing 🎯