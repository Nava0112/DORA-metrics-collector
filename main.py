# main.py
import os
import threading
import time
import logging
import requests
import schedule
from flask import Flask
from webhook_server import app as webhook_app
from db_utils import initialize_db
from metrics_processor import process_metrics

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
    """Ping Grafana dashboard to confirm it's up (optional)."""
    try:
        dashboard_uid = os.getenv('GRAFANA_DASHBOARD_UID', 'dora-metrics')
        grafana_url = os.getenv('GRAFANA_URL')
        api_key = os.getenv('GRAFANA_API_KEY')

        if not all([grafana_url, api_key, dashboard_uid]):
            logger.warning("Grafana config incomplete, skipping dashboard check.")
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
        logger.error(f"Error checking Grafana: {str(e)}")

def metrics_job():
    """Process DORA metrics and optionally refresh Grafana."""
    try:
        logger.info("📊 Running scheduled DORA metrics processing...")
        results = process_metrics()
        logger.info(f"✅ Processed {len(results)} repositories")
        refresh_grafana_dashboard()
    except Exception as e:
        logger.error(f"❌ Metrics job failed: {str(e)}")

def start_scheduler():
    """Start background scheduler for daily metric processing."""
    schedule.every().day.at("00:05").do(metrics_job)
    logger.info("📅 Scheduler started. First job at 00:05 UTC.")

    def scheduler_loop():
        while True:
            schedule.run_pending()
            time.sleep(60)

    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    return thread

def setup_application():
    """Initialize DB, run first metrics, and start scheduler."""
    logger.info("🚀 Initializing application...")
    initialize_db()
    scheduler_thread = start_scheduler()
    metrics_job()  # Run once immediately
    return webhook_app, scheduler_thread

if __name__ == '__main__':
    app, scheduler_thread = setup_application()
    try:
        logger.info("🧩 Starting DORA Metrics Webhook Server on port 5000...")
        app.run(host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down gracefully...")
    except Exception as e:
        logger.critical(f"🔥 Fatal error: {str(e)}")
