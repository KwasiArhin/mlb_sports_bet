# MLB Predictions Dashboard

A comprehensive web dashboard for monitoring MLB betting predictions, model performance, and Kelly Criterion bet sizing recommendations.

## Features

### 📊 Dashboard Overview
- Real-time summary of predictions and betting recommendations
- Key performance metrics and pipeline status
- Quick navigation to all sections

### ⚾ Today's Games
- Current game predictions with win probabilities
- Kelly Criterion optimal bet sizing
- Expected value calculations
- Risk management summaries

### 🧠 Model Analytics
- XGBoost model performance metrics over time
- Feature importance visualization
- Model configuration details
- Accuracy trends and error metrics

### ⚙️ Pipeline Status
- Data freshness monitoring
- File status and health checks
- Manual pipeline actions
- System information

### 📈 Historical Performance
- Daily prediction accuracy tracking
- Performance trends over time
- Success/failure analysis

## Quick Start

### Option 1: Use the starter script
```bash
python start_dashboard.py
```

### Option 2: Run directly
```bash
cd dashboard
python app.py
```

Then open your browser to: **http://localhost:5000**

## Pages

| URL | Description |
|-----|-------------|
| `/` | Dashboard home with overview |
| `/predictions` | Today's betting recommendations |
| `/model-analytics` | Model performance metrics |
| `/pipeline-status` | System status monitoring |
| `/historical` | Historical performance data |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/predictions` | JSON prediction data |
| `/api/metrics` | Model metrics data |
| `/api/status` | Pipeline status |

## Dependencies

- **Flask**: Web framework
- **Pandas**: Data processing
- **Bootstrap 5**: UI framework
- **Chart.js**: Data visualization
- **Font Awesome**: Icons

## Configuration

The dashboard automatically detects data files in:
- `../data/processed/` - Processed prediction data
- `../modeling/data/predictions/` - Kelly sizing outputs
- `../plots/` - Model visualization files

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLASK_ENV` | development | Flask environment |
| `FLASK_PORT` | 5000 | Server port |

## Security Note

This dashboard is designed for local development use. For production deployment, implement proper authentication and use a production WSGI server.

## Troubleshooting

### Dashboard won't start
- Check Python version (3.7+ required)
- Install Flask: `pip install flask pandas`

### No data showing
- Run the prediction pipeline first
- Check that data files exist in the expected directories

### Permission errors
- Ensure read access to data directories
- Check file permissions for CSV files

## Development

To extend the dashboard:

1. Add new routes in `app.py`
2. Create templates in `templates/`
3. Add static assets in `static/`
4. Update navigation in `base.html`

---

Built with ❤️ for MLB prediction analysis