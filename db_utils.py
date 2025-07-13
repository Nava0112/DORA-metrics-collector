import os
import psycopg2
from dotenv import load_dotenv
import logging

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
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Drop existing tables first
        drop_existing_tables(cursor)
        
        # Create fresh tables
        cursor.execute("""
        CREATE TABLE deployments (
            id SERIAL PRIMARY KEY,
            repo_id BIGINT NOT NULL,
            deployment_id BIGINT NOT NULL UNIQUE,
            environment TEXT,
            status TEXT,
            created_at TIMESTAMPTZ NOT NULL,
            commit_sha TEXT,
            payload JSONB
        );

        CREATE TABLE pull_requests (
            id SERIAL PRIMARY KEY,
            repo_id BIGINT NOT NULL,
            pr_id BIGINT NOT NULL UNIQUE,
            merged_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            first_commit_at TIMESTAMPTZ,
            base_branch TEXT,
            commit_sha TEXT,
            payload JSONB
        );

        CREATE TABLE incidents (
            id SERIAL PRIMARY KEY,
            repo_id BIGINT NOT NULL,
            issue_id BIGINT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL,
            closed_at TIMESTAMPTZ,
            is_incident BOOLEAN DEFAULT FALSE,
            deployment_id BIGINT REFERENCES deployments(deployment_id) ON DELETE SET NULL,
            payload JSONB
        );

        CREATE TABLE deployment_prs (
            deployment_id BIGINT NOT NULL REFERENCES deployments(deployment_id),
            pr_id BIGINT NOT NULL REFERENCES pull_requests(pr_id),
            PRIMARY KEY (deployment_id, pr_id)
        );

        CREATE TABLE dora_metrics (
            id SERIAL PRIMARY KEY,
            repo_id BIGINT NOT NULL,
            metric_date DATE NOT NULL,
            deployment_frequency INT DEFAULT 0,
            lead_time_hours FLOAT DEFAULT 0,
            change_failure_rate FLOAT DEFAULT 0,
            mttr_hours FLOAT DEFAULT 0,
            last_updated TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(repo_id, metric_date)
        );
        """)
        
        # Create indexes
        cursor.execute("""
        CREATE INDEX idx_deployments_repo ON deployments(repo_id);
        CREATE INDEX idx_prs_repo ON pull_requests(repo_id);
        CREATE INDEX idx_incidents_repo ON incidents(repo_id);
        CREATE INDEX idx_metrics_repo_date ON dora_metrics(repo_id, metric_date);
        CREATE INDEX idx_deployment_prs ON deployment_prs(deployment_id, pr_id);
        CREATE INDEX idx_deployments_sha ON deployments(commit_sha);
        CREATE INDEX idx_prs_sha ON pull_requests(commit_sha);
        CREATE INDEX idx_metrics_updated ON dora_metrics(last_updated);
        """)
        
        conn.commit()
        logger.info("Database initialized successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()