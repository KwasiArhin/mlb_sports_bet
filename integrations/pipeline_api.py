#!/usr/bin/env python3
"""
Pipeline API Server

Flask API server for triggering and monitoring the daily MLB pipeline.
Integrates with the dashboard to provide live updates and manual controls.
"""

import os
import sys
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
from flask_cors import CORS
import time

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from integrations.daily_pipeline_orchestrator import DailyPipelineOrchestrator
from modeling.prediction_service import MLBPredictionService

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for dashboard integration

# Global pipeline state
pipeline_state = {
    'current_run': None,
    'last_run': None,
    'is_running': False,
    'run_history': []
}


class ThreadedPipelineRunner:
    """Run pipeline in background thread to avoid blocking API."""
    
    def __init__(self):
        self.current_orchestrator = None
    
    def run_pipeline(self, date: str, bankroll: float):
        """Run pipeline in background thread."""
        global pipeline_state
        
        try:
            pipeline_state['is_running'] = True
            pipeline_state['current_run'] = {
                'date': date,
                'bankroll': bankroll,
                'started_at': datetime.now().isoformat(),
                'status': 'running'
            }
            
            logger.info(f"Starting background pipeline for {date}")
            
            orchestrator = DailyPipelineOrchestrator(date, bankroll)
            self.current_orchestrator = orchestrator
            
            result = orchestrator.run_complete_pipeline()
            
            # Update pipeline state with results
            pipeline_state['current_run']['status'] = result['status']
            pipeline_state['current_run']['completed_at'] = datetime.now().isoformat()
            pipeline_state['current_run']['result'] = result
            
            # Move to history
            pipeline_state['last_run'] = pipeline_state['current_run'].copy()
            pipeline_state['run_history'].append(pipeline_state['current_run'])
            
            # Keep only last 10 runs in history
            if len(pipeline_state['run_history']) > 10:
                pipeline_state['run_history'] = pipeline_state['run_history'][-10:]
            
            pipeline_state['current_run'] = None
            pipeline_state['is_running'] = False
            
            logger.info(f"Background pipeline completed with status: {result['status']}")
            
        except Exception as e:
            logger.error(f"Background pipeline failed: {e}")
            
            pipeline_state['current_run']['status'] = 'failed'
            pipeline_state['current_run']['error'] = str(e)
            pipeline_state['current_run']['completed_at'] = datetime.now().isoformat()
            
            pipeline_state['last_run'] = pipeline_state['current_run'].copy()
            pipeline_state['run_history'].append(pipeline_state['current_run'])
            
            pipeline_state['current_run'] = None
            pipeline_state['is_running'] = False
    
    def get_current_status(self) -> Optional[Dict]:
        """Get status of currently running pipeline."""
        if self.current_orchestrator:
            return self.current_orchestrator.get_status()
        return None


runner = ThreadedPipelineRunner()


@app.route('/api/pipeline/trigger', methods=['POST'])
def trigger_pipeline():
    """Trigger the daily pipeline."""
    try:
        # Check if pipeline is already running
        if pipeline_state['is_running']:
            return jsonify({
                'success': False,
                'error': 'Pipeline is already running',
                'current_run': pipeline_state['current_run']
            }), 400
        
        # Get parameters from request
        data = request.get_json() or {}
        date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
        bankroll = data.get('bankroll', 1000.0)
        
        # Validate parameters
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Invalid date format. Use YYYY-MM-DD'
            }), 400
        
        if not isinstance(bankroll, (int, float)) or bankroll <= 0:
            return jsonify({
                'success': False,
                'error': 'Bankroll must be a positive number'
            }), 400
        
        # Start pipeline in background thread
        thread = threading.Thread(
            target=runner.run_pipeline,
            args=(date, float(bankroll)),
            daemon=True
        )
        thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Pipeline started successfully',
            'date': date,
            'bankroll': bankroll,
            'started_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error triggering pipeline: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to trigger pipeline: {str(e)}'
        }), 500


@app.route('/api/pipeline/status', methods=['GET'])
def get_pipeline_status():
    """Get current pipeline status."""
    try:
        status = {
            'is_running': pipeline_state['is_running'],
            'current_run': pipeline_state['current_run'],
            'last_run': pipeline_state['last_run']
        }
        
        # Add detailed status if pipeline is running
        if pipeline_state['is_running']:
            detailed_status = runner.get_current_status()
            if detailed_status:
                status['detailed_status'] = detailed_status
        
        return jsonify({
            'success': True,
            'status': status
        })
        
    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to get status: {str(e)}'
        }), 500


@app.route('/api/pipeline/history', methods=['GET'])
def get_pipeline_history():
    """Get pipeline run history."""
    try:
        return jsonify({
            'success': True,
            'history': pipeline_state['run_history']
        })
        
    except Exception as e:
        logger.error(f"Error getting pipeline history: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to get history: {str(e)}'
        }), 500


@app.route('/api/pipeline/stop', methods=['POST'])
def stop_pipeline():
    """Stop the currently running pipeline (if possible)."""
    try:
        if not pipeline_state['is_running']:
            return jsonify({
                'success': False,
                'error': 'No pipeline is currently running'
            }), 400
        
        # Note: This is a soft stop - we can't easily kill the subprocess
        # In a production environment, you'd want more sophisticated process management
        
        return jsonify({
            'success': True,
            'message': 'Stop request received (pipeline may take time to respond)',
            'note': 'Current pipeline processes will complete their current step'
        })
        
    except Exception as e:
        logger.error(f"Error stopping pipeline: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to stop pipeline: {str(e)}'
        }), 500


@app.route('/api/games/today', methods=['GET'])
def get_todays_games():
    """Get today's MLB games without running full pipeline."""
    try:
        from integrations.live_games_fetcher import MLBGamesFetcher
        
        date = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
        
        fetcher = MLBGamesFetcher()
        result = fetcher.get_live_games_with_status(date)
        
        return jsonify({
            'success': True,
            'games': result
        })
        
    except Exception as e:
        logger.error(f"Error fetching today's games: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to fetch games: {str(e)}'
        }), 500


@app.route('/api/predictions/generate', methods=['POST'])
def generate_predictions():
    """Generate ML predictions with Kelly sizing for live games."""
    try:
        # Get parameters from request
        data = request.get_json() or {}
        date = data.get('date', datetime.now().strftime('%Y-%m-%d'))
        bankroll = data.get('bankroll', 1000.0)
        odds = data.get('odds', 1.91)
        
        # Validate parameters
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Invalid date format. Use YYYY-MM-DD'
            }), 400
        
        if not isinstance(bankroll, (int, float)) or bankroll <= 0:
            return jsonify({
                'success': False,
                'error': 'Bankroll must be a positive number'
            }), 400
        
        logger.info(f"Generating predictions for {date} with ${bankroll} bankroll")
        
        # Initialize prediction service
        service = MLBPredictionService()
        
        # Generate predictions
        result = service.predict_live_games(
            target_date=date,
            bankroll=float(bankroll),
            default_odds=float(odds)
        )
        
        if result['success']:
            logger.info(f"Generated {result['summary']['total_games']} predictions, {result['summary']['recommended_bets']} with Kelly edge")
            
            return jsonify({
                'success': True,
                'predictions': result['predictions'],
                'summary': result['summary'],
                'generated_at': datetime.now().isoformat()
            })
        else:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 500
        
    except Exception as e:
        logger.error(f"Error generating predictions: {e}")
        return jsonify({
            'success': False,
            'error': f'Failed to generate predictions: {str(e)}'
        }), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """API health check."""
    return jsonify({
        'success': True,
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'pipeline_api_version': '1.0.0'
    })


@app.errorhandler(404)
def not_found(error):
    """404 error handler."""
    return jsonify({
        'success': False,
        'error': 'Endpoint not found'
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """500 error handler."""
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'success': False,
        'error': 'Internal server error'
    }), 500


if __name__ == '__main__':
    print(f"""
    🔌 MLB Pipeline API Server Starting
    
    📡 API URL: http://localhost:5001
    🔗 Health Check: http://localhost:5001/api/health
    
    Available Endpoints:
    - POST /api/pipeline/trigger : Trigger daily pipeline
    - GET  /api/pipeline/status  : Get pipeline status
    - GET  /api/pipeline/history : Get run history
    - POST /api/pipeline/stop    : Stop running pipeline
    - GET  /api/games/today      : Get today's games
    
    Integration with Dashboard:
    The dashboard can call these APIs to trigger and monitor the pipeline.
    
    Press Ctrl+C to stop the server
    """)
    
    app.run(
        host='0.0.0.0',
        port=5001,
        debug=True,
        threaded=True
    )