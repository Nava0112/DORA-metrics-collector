import os
import logging
import json
from datetime import datetime, timedelta
from db_utils import get_db_connection
from dora_calculations import (
    calculate_lead_time,
    calculate_failure_rate,
    calculate_mttr
)
import psycopg2

logger = logging.getLogger(__name__)

PRODUCTION_KEYWORDS = ['production', 'prod', 'release', 'deploy', 'main', 'live']

def is_production_deployment(environment, payload):
    try:
        if environment and any(k in environment.lower() for k in PRODUCTION_KEYWORDS):
            return True
        if payload:
            payload_data = payload if isinstance(payload, dict) else json.loads(payload)
            workflow_name = payload_data.get('workflow_run', {}).get('name', '').lower()
            return any(k in workflow_name for k in PRODUCTION_KEYWORDS)
    except Exception as e:
        logger.warning(f"Error checking production filter: {e}")
    return False

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(days=n)

def process_repo_metrics(cursor, repo_id, start_time, end_time, metric_date):
    cursor.execute("""
        SELECT 
            d.deployment_id, d.commit_sha, d.created_at,
            d.environment, d.payload
        FROM deployments d
        WHERE 
            d.repo_id = %s AND
            d.created_at BETWEEN %s AND %s AND
            d.status = 'success'
    """, (repo_id, start_time, end_time))
    all_deployments = cursor.fetchall()

    prod_deployments = [
        (dep_id, commit_sha, created_at)
        for dep_id, commit_sha, created_at, environment, payload in all_deployments
        if is_production_deployment(environment, payload)
    ]

    deployment_count = len(prod_deployments)

    lead_times = []
    for dep_id, commit_sha, deploy_time in prod_deployments:
        cursor.execute("""
            SELECT pr.first_commit_at
            FROM deployment_prs dp
            JOIN pull_requests pr ON dp.pr_id = pr.pr_id
            WHERE dp.deployment_id = %s
        """, (dep_id,))
        for (first_commit,) in cursor.fetchall():
            lead_time = calculate_lead_time(first_commit, None, deploy_time)
            lead_times.append(lead_time)

    prod_deployment_ids = tuple([d[0] for d in prod_deployments]) or (0,)
    cursor.execute(f"""
        SELECT d.created_at
        FROM deployments d
        LEFT JOIN deployment_prs dp ON d.deployment_id = dp.deployment_id
        WHERE 
            d.repo_id = %s AND
            d.created_at BETWEEN %s AND %s AND
            d.status = 'success' AND
            dp.pr_id IS NULL AND
            d.deployment_id IN %s
    """, (repo_id, start_time, end_time, prod_deployment_ids))
    for (deploy_time,) in cursor.fetchall():
        lead_times.append(calculate_lead_time(deploy_time, None, deploy_time))

    mean_lead_time = sum(lead_times) / len(lead_times) if lead_times else 0.0

    cursor.execute("""
        SELECT COUNT(DISTINCT i.deployment_id)
        FROM incidents i
        WHERE i.repo_id = %s
              AND i.deployment_id IN (
                  SELECT d.deployment_id
                  FROM deployments d
                  WHERE d.repo_id = %s
                    AND d.created_at BETWEEN %s AND %s
              ) 
    """, (repo_id, repo_id, start_time, end_time))
    failed_deployments = cursor.fetchone()[0] or 0
    failure_rate = calculate_failure_rate(deployment_count, failed_deployments)

    cursor.execute("""
        SELECT created_at, closed_at
        FROM incidents
        WHERE repo_id = %s
          AND closed_at IS NOT NULL
          AND DATE(closed_at) = %s
    """, (repo_id, metric_date))
    rows = cursor.fetchall()

    mttr_times = [calculate_mttr(created_at, closed_at) for created_at, closed_at in rows]
    mean_mttr = sum(mttr_times) / len(mttr_times) if mttr_times else 0.0

    mean_lead_time = round(mean_lead_time, 3)
    failure_rate = round(failure_rate, 3)
    mean_mttr = round(mean_mttr, 3)

    cursor.execute("""
        INSERT INTO dora_metrics (
            repo_id, metric_date, 
            deployment_frequency, lead_time_hours,
            change_failure_rate, mttr_hours
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (repo_id, metric_date) DO UPDATE SET
            deployment_frequency = EXCLUDED.deployment_frequency,
            lead_time_hours = EXCLUDED.lead_time_hours,
            change_failure_rate = EXCLUDED.change_failure_rate,
            mttr_hours = EXCLUDED.mttr_hours,
            last_updated = NOW()
    """, (
        repo_id,
        metric_date,
        deployment_count,
        mean_lead_time,
        failure_rate,
        mean_mttr
    ))

    return {
        'deployments': deployment_count,
        'lead_time': mean_lead_time,
        'failure_rate': failure_rate,
        'mttr': mean_mttr
    }

def process_metrics(start_date=None):
    logger.info("Starting historical daily metrics processing...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT MIN(min_date) FROM (
                SELECT MIN(created_at) AS min_date FROM deployments
                UNION
                SELECT MIN(created_at) AS min_date FROM pull_requests
                UNION
                SELECT MIN(created_at) AS min_date FROM incidents
            ) AS dates
        """)
        first_date_row = cursor.fetchone()
        default_start = first_date_row[0].date() if first_date_row and first_date_row[0] else datetime.utcnow().date()

        if not start_date:
            start_date = default_start
        else:
            start_date = max(start_date, default_start)

        today = datetime.utcnow().date()

        cursor.execute("""
            SELECT DISTINCT repo_id FROM (
                SELECT repo_id FROM deployments
                UNION SELECT repo_id FROM pull_requests
                UNION SELECT repo_id FROM incidents
            ) AS active_repos
        """)
        repos = [row[0] for row in cursor.fetchall()]

        results = {}
        for single_date in daterange(start_date, today):  # ✅ corrected this line
            start_time = datetime.combine(single_date, datetime.min.time())
            end_time = start_time + timedelta(days=1)
            metric_date = single_date

            for repo_id in repos:
                logger.info(f"Processing metrics for repo: {repo_id} on {metric_date}")
                results[(repo_id, metric_date)] = process_repo_metrics(
                    cursor, repo_id, start_time, end_time, metric_date
                )

        conn.commit()
        logger.info("Historical metrics processing completed successfully")
        return results

    except Exception as e:
        logger.error(f"Error processing historical metrics: {e}")
        return {}
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
