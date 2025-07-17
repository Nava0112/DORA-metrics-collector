import os
import requests
from flask import Flask
from webhook_server import app as webhook_app
from db_utils import initialize_db , get_db_connection
from github_auth import get_installation_token
from metrics_processor import process_metrics
from github_backfill import backfill

def refresh_grafana_dashboard():
    dashboard_uid = os.getenv('GRAFANA_DASHBOARD_UID', 'dora-metrics')
    grafana_url = os.getenv('GRAFANA_URL')
    api_key = os.getenv('GRAFANA_API_KEY')

    if not all([grafana_url, api_key, dashboard_uid]):
        print("Grafana config incomplete. Skipping dashboard check.")
        return

    url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print(f"Grafana dashboard '{dashboard_uid}' is reachable.")
        else:
            print(f"Grafana dashboard check failed: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error checking Grafana dashboard: {e}")

def metrics_job():
    try:
        print(" Running DORA metrics processing...")

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT last_webhook_at FROM sync_state WHERE id = 1")
        row = cursor.fetchone()
        last_webhook_at = row[0].date() if row and row[0] else None
        cursor.close()
        conn.close()

        results = process_metrics(start_date=last_webhook_at)

        print(f" Processed metrics for {len(results)} dates.")
        refresh_grafana_dashboard()
    except Exception as e:
        print(f" Metrics job failed: {str(e)}")


def setup_application():
    print("Starting application setup...")
    print("Running DB initialization and GitHub backfill...")
    initialize_db()
    backfill()

    print("Running initial metrics calculation...")
    metrics_job()

    return webhook_app

if __name__ == '__main__':
    app = setup_application()
    try:
        print("Starting DORA Metrics Webhook Server on port 5000...")
        app.run(host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        print("Shutdown requested by user (CTRL+C)")
    except Exception as e:
        print(f"Fatal error: {e}")
