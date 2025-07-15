import os
import logging
import requests
from flask import Flask
from webhook_server import app as webhook_app
from db_utils import initialize_db
from metrics_processor import process_metrics
from github_backfill import backfill

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

FIRST_RUN_FLAG_FILE = ".first_run_complete"

def refresh_grafana_dashboard():
    try:
        dashboard_uid = os.getenv('GRAFANA_DASHBOARD_UID', 'dora-metrics')
        grafana_url = os.getenv('GRAFANA_URL')
        api_key = os.getenv('GRAFANA_API_KEY')

        if not all([grafana_url, api_key, dashboard_uid]):
            logger.warning("Grafana config incomplete. Skipping dashboard check.")
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
        logger.error(f"Error checking Grafana dashboard: {str(e)}")

def metrics_job():
    try:
        logger.info("📊 Running DORA metrics processing...")
        results = process_metrics()
        logger.info(f"✅ Processed metrics for {len(results)} dates.")
        refresh_grafana_dashboard()
    except Exception as e:
        logger.error(f"❌ Metrics job failed: {str(e)}")

def setup_application():
    logger.info("🚀 Starting application setup...")

    is_first_run = not os.path.exists(FIRST_RUN_FLAG_FILE)

    if is_first_run:
        logger.info("🆕 First-time setup detected: initializing DB and running GitHub backfill...")
        initialize_db()
        backfill()
        with open(FIRST_RUN_FLAG_FILE, "w") as f:
            f.write("true")
    else:
        logger.info("✅ DB and data already initialized. Skipping backfill.")

    # Run metrics once on start
    logger.info("⚡ Running initial metrics calculation...")
    metrics_job()

    return webhook_app

if __name__ == '__main__':
    app = setup_application()
    try:
        logger.info("🧩 Starting DORA Metrics Webhook Server on port 5000...")
        app.run(host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("🛑 Shutdown requested by user (CTRL+C)")
    except Exception as e:
        logger.critical(f"🔥 Fatal error: {str(e)}")
