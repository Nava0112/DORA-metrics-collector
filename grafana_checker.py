import os
import requests
import logging
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("GrafanaChecker")

def check_grafana_dashboard():
    grafana_url = os.getenv('GRAFANA_URL')
    api_key = os.getenv('GRAFANA_API_KEY')
    dashboard_uid = os.getenv('GRAFANA_DASHBOARD_UID')

    if not all([grafana_url, api_key, dashboard_uid]):
        logger.error("Missing Grafana configuration in .env. Please set GRAFANA_URL, GRAFANA_API_KEY, GRAFANA_DASHBOARD_UID.")
        return

    # Build URL
    url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        logger.info(f"Connecting to Grafana at {grafana_url}")
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            logger.info(f"✅ Dashboard UID '{dashboard_uid}' found in Grafana.")
        elif response.status_code == 401:
            logger.error("❌ Unauthorized. Check your GRAFANA_API_KEY.")
        elif response.status_code == 404:
            logger.error(f"❌ Dashboard UID '{dashboard_uid}' not found. Check your GRAFANA_DASHBOARD_UID.")
        else:
            logger.error(f"❌ Unexpected response: {response.status_code} - {response.text}")

    except Exception as e:
        logger.critical(f"Failed to connect to Grafana: {str(e)}")

if __name__ == "__main__":
    check_grafana_dashboard()
