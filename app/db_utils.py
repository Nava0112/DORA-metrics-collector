import os
import psycopg2
from dotenv import load_dotenv
import logging
import psycopg2.errors


load_dotenv()
logger = logging.getLogger(__name__)

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        sslmode='require'
    )

def drop_existing_tables(cursor):
    """Drop existing tables if they exist"""
    tables = [
        'deployment_prs',
        'incidents',
        'pull_requests',
        'deployments',
        'dora_metrics'
    ]
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            logger.info(f"Dropped table {table} if it existed")
        except Exception as e:
            logger.error(f"Error dropping table {table}: {str(e)}")
            raise

def initialize_db():

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        existing_tables = {row[0] for row in cursor.fetchall()}
        expected_tables = {
            'deployments', 'pull_requests', 'incidents', 'deployment_prs', 'dora_metrics'
        }

        if not expected_tables.issubset(existing_tables):
            logger.info("Some expected tables are missing. Creating missing tables...")
            _create_tables(cursor)
            conn.commit()
            return

        if existing_tables - expected_tables:
            print("❗ Unexpected tables found in DB:", existing_tables - expected_tables)
            choice = input("Drop ALL and recreate expected schema? (y/n): ")
            if choice.lower() == 'y':
                drop_existing_tables(cursor)
                _create_tables(cursor)
                conn.commit()
            else:
                print("Aborting initialization.")
                exit(1)
        else:
            logger.info("✅ Tables already exist. No changes made.")

    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def _create_tables(cursor):
    cursor.execute("""CREATE TABLE IF NOT EXISTS deployments (
        id SERIAL PRIMARY KEY,
        repo_id BIGINT NOT NULL,
        deployment_id BIGINT NOT NULL UNIQUE,
        environment TEXT,
        status TEXT,
        created_at TIMESTAMPTZ NOT NULL,
        commit_sha TEXT,
        payload JSONB
    );

    CREATE TABLE IF NOT EXISTS pull_requests (
        id SERIAL PRIMARY KEY,
        repo_id BIGINT NOT NULL,
        pr_id BIGINT NOT NULL UNIQUE,
        pr_name TEXT,
        merged_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        first_commit_at TIMESTAMPTZ,
        base_branch TEXT,
        commit_sha TEXT,
        payload JSONB
    );

    CREATE TABLE IF NOT EXISTS incidents (
        id SERIAL PRIMARY KEY,
        repo_id BIGINT NOT NULL,
        issue_id BIGINT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL,
        closed_at TIMESTAMPTZ,
        is_incident BOOLEAN DEFAULT FALSE,
        deployment_id BIGINT REFERENCES deployments(deployment_id) ON DELETE SET NULL,
        payload JSONB
    );

    CREATE TABLE IF NOT EXISTS deployment_prs (
        deployment_id BIGINT NOT NULL REFERENCES deployments(deployment_id),
        pr_id BIGINT NOT NULL REFERENCES pull_requests(pr_id),
        PRIMARY KEY (deployment_id, pr_id)
    );

    CREATE TABLE IF NOT EXISTS dora_metrics (
        id SERIAL PRIMARY KEY,
        repo_id BIGINT NOT NULL,
        metric_date DATE NOT NULL,
        deployment_frequency INT DEFAULT 0,
        lead_time_hours FLOAT DEFAULT 0,
        change_failure_rate FLOAT DEFAULT 0,
        mttr_hours FLOAT DEFAULT 0,
        last_updated TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(repo_id, metric_date)
    );""")

    cursor.execute("""-- Indexes
        CREATE INDEX IF NOT EXISTS idx_deployments_repo ON deployments(repo_id);
        CREATE INDEX IF NOT EXISTS idx_prs_repo ON pull_requests(repo_id);
        CREATE INDEX IF NOT EXISTS idx_incidents_repo ON incidents(repo_id);
        CREATE INDEX IF NOT EXISTS idx_metrics_repo_date ON dora_metrics(repo_id, metric_date);
        CREATE INDEX IF NOT EXISTS idx_deployment_prs ON deployment_prs(deployment_id, pr_id);
        CREATE INDEX IF NOT EXISTS idx_deployments_sha ON deployments(commit_sha);
        CREATE INDEX IF NOT EXISTS idx_prs_sha ON pull_requests(commit_sha);
        CREATE INDEX IF NOT EXISTS idx_metrics_updated ON dora_metrics(last_updated);
    """)
