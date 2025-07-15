import logging
from db_utils import get_db_connection, initialize_db
from github_backfill import backfill
from metrics_processor import process_metrics

# Configure logging (no emojis for Windows console safety)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dora_metrics_reset.log')
    ]
)
logger = logging.getLogger(__name__)

def clear_all_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE TABLE deployment_prs CASCADE;")
        cursor.execute("TRUNCATE TABLE deployments CASCADE;")
        cursor.execute("TRUNCATE TABLE pull_requests CASCADE;")
        cursor.execute("TRUNCATE TABLE incidents CASCADE;")
        cursor.execute("TRUNCATE TABLE dora_metrics CASCADE;")
        conn.commit()
        logger.info("All tables cleared successfully.")
    except Exception as e:
        logger.error(f"Failed to clear data: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    logger.info("Starting full reset and backfill process...")
    clear_all_data()
    initialize_db()
    logger.info("Running GitHub backfill...")
    backfill()
    logger.info("Calculating historical DORA metrics...")
    process_metrics()
    logger.info("Reset, backfill and metrics complete!")

if __name__ == '__main__':
    main()
