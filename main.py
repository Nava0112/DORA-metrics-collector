import os
import threading
import time
import logging
import requests
from flask import Flask
from webhook_server import app as webhook_app
from metrics_processor import process_metrics
from db_utils import initialize_db
import schedule

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dora_metrics.log')
    ]
)
logger = logging.getLogger(__name__)

def refresh_grafana_dashboard():
    """Check Grafana dashboard availability instead of trying a non-existent refresh API."""
    try:
        dashboard_uid = os.getenv('GRAFANA_DASHBOARD_UID', 'dora-metrics')
        grafana_url = os.getenv('GRAFANA_URL')
        api_key = os.getenv('GRAFANA_API_KEY')

        if not all([grafana_url, api_key, dashboard_uid]):
            logger.warning("Grafana configuration incomplete, skipping dashboard check.")
            return

        url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            logger.info(f"✅ Grafana dashboard '{dashboard_uid}' is reachable.")
        else:
            logger.error(f"Grafana dashboard check failed: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error connecting to Grafana: {str(e)}")


def metrics_job():
    """Job to process metrics and refresh Grafana"""
    try:
        logger.info("Starting scheduled metrics processing...")
        results = process_metrics()
        logger.info(f"Processed {len(results)} repositories")
        
        # Refresh Grafana dashboard
        refresh_grafana_dashboard()
        
    except Exception as e:
        logger.error(f"Metrics job failed: {str(e)}")

def start_scheduler():
    """Start the background scheduler"""
    # Schedule daily at 00:05 UTC
    schedule.every().day.at("00:05").do(metrics_job)
    
    logger.info("Scheduler started. Next run at 00:05 UTC")
    
    # Run the scheduler in a background thread
    def scheduler_loop():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    return thread

def setup_application():
    """Initialize the application"""
    # Initialize database
    logger.info("Initializing database...")
    initialize_db()
    
    # Start scheduler
    scheduler_thread = start_scheduler()
    
    # Initial metrics processing
    logger.info("Running initial metrics processing...")
    metrics_job()
    
    return webhook_app, scheduler_thread

if __name__ == '__main__':
    # Setup application components
    app, scheduler_thread = setup_application()
    
    try:
        logger.info("Starting DORA Metrics Monitor")
        logger.info(f"Webhook server running on port 5000")
        app.run(host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")