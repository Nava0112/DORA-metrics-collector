import logging
from db_utils import get_db_connection

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def clear_all_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        logger.info("⚠️  Deleting all rows from tables...")
        
        # Delete in dependency order to satisfy FK constraints
        cursor.execute("DELETE FROM deployment_prs;")
        cursor.execute("DELETE FROM incidents;")
        cursor.execute("DELETE FROM pull_requests;")
        cursor.execute("DELETE FROM deployments;")
        cursor.execute("DELETE FROM dora_metrics;")
        
        conn.commit()
        logger.info("✅ All tables cleared successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to clear tables: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    clear_all_tables()
