import os
import psycopg2
from dotenv import load_dotenv
from db_utils import get_db_connection  # uses your existing function

load_dotenv()

REPO_ID = 1010258660  # set your repo_id here

def calculate_mttr_per_day():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                DATE(created_at) AS day,
                ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600.0), 2) AS mttr_hours,
                COUNT(*) AS incidents_count
            FROM incidents
            WHERE 
                repo_id = %s 
                AND is_incident = TRUE 
                AND closed_at IS NOT NULL
            GROUP BY DATE(created_at)
            ORDER BY day;
        """, (REPO_ID,))
        
        results = cursor.fetchall()
        
        print("\n📊 MTTR Per Day (by incident created_at date):")
        print("------------------------------------------------")
        if results:
            for day, mttr_hours, incidents_count in results:
                print(f"📅 {day} | 🛠 Incidents: {incidents_count} | ⏱️ MTTR: {mttr_hours} hrs")
        else:
            print("No incidents found for this repo.")
        
    except Exception as e:
        print(f"❌ Error calculating MTTR: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    calculate_mttr_per_day()
