import os
import requests
from dotenv import load_dotenv

# Load .env
load_dotenv()

grafana_url = os.getenv("GRAFANA_URL")
api_key = os.getenv("GRAFANA_API_KEY")

headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

def list_dashboards():
    if not grafana_url or not api_key:
        print("❌ Missing GRAFANA_URL or GRAFANA_API_KEY in your .env")
        return

    try:
        print(f"Connecting to Grafana at {grafana_url}")
        response = requests.get(f"{grafana_url}/api/search", headers=headers)

        if response.status_code == 200:
            dashboards = response.json()
            if dashboards:
                print("✅ Found the following dashboards:")
                for d in dashboards:
                    print(f"- Title: {d.get('title')} | UID: {d.get('uid')} | URI: {d.get('uri')}")
            else:
                print("⚠️ No dashboards found in Grafana.")
        elif response.status_code == 401:
            print("❌ Unauthorized. Check your GRAFANA_API_KEY.")
        else:
            print(f"❌ Failed to list dashboards: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"❌ Error connecting to Grafana: {str(e)}")

if __name__ == "__main__":
    list_dashboards()
