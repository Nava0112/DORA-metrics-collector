import os
import logging
from datetime import datetime, timedelta
from db_utils import get_db_connection
from dora_calculations import (
    calculate_lead_time,
    calculate_failure_rate,
    calculate_mttr,
    median
)
import psycopg2

logger = logging.getLogger(__name__)

def process_repo_metrics(cursor, repo_id, start_time, end_time, metric_date):
    # Deployment Frequency
    cursor.execute("""
        SELECT COUNT(*) 
        FROM deployments
        WHERE 
            repo_id = %s AND
            created_at BETWEEN %s AND %s AND
            status = 'success'
    """, (repo_id, start_time, end_time))
    deployment_count = cursor.fetchone()[0] or 0
    
    # Lead Time for Changes
    lead_times = []
    cursor.execute("""
        SELECT 
            d.created_at AS deploy_time,
            pr.first_commit_at
        FROM deployments d
        JOIN deployment_prs dp ON d.deployment_id = dp.deployment_id
        JOIN pull_requests pr ON dp.pr_id = pr.pr_id
        WHERE 
            d.repo_id = %s AND
            d.created_at BETWEEN %s AND %s AND
            d.status = 'success'
    """, (repo_id, start_time, end_time))
    
    for deploy_time, first_commit in cursor.fetchall():
        lead_time = calculate_lead_time(first_commit, None, deploy_time)
        lead_times.append(lead_time)
    
    # Handle direct commits
    cursor.execute("""
        SELECT d.created_at
        FROM deployments d
        LEFT JOIN deployment_prs dp ON d.deployment_id = dp.deployment_id
        WHERE 
            d.repo_id = %s AND
            d.created_at BETWEEN %s AND %s AND
            d.status = 'success' AND
            dp.pr_id IS NULL
    """, (repo_id, start_time, end_time))
    
    for (deploy_time,) in cursor.fetchall():
        lead_times.append(calculate_lead_time(deploy_time, None, deploy_time))
    
    median_lead_time = median(lead_times)
    
    # Change Failure Rate
    cursor.execute("""
        SELECT COUNT(*)
        FROM incidents i
        JOIN deployments d ON i.deployment_id = d.deployment_id
        WHERE 
            i.repo_id = %s AND
            i.is_incident = TRUE AND
            d.created_at BETWEEN %s AND %s
    """, (repo_id, start_time, end_time))
    failed_deployments = cursor.fetchone()[0] or 0
    
    failure_rate = calculate_failure_rate(deployment_count, failed_deployments)
    
    # Time to Restore Service
    mttr_times = []
    cursor.execute("""
        SELECT 
            created_at,
            closed_at
        FROM incidents
        WHERE 
            repo_id = %s AND
            is_incident = TRUE AND
            closed_at IS NOT NULL AND
            closed_at BETWEEN %s AND %s
    """, (repo_id, start_time, end_time))
    
    for created_at, closed_at in cursor.fetchall():
        mttr_times.append(calculate_mttr(created_at, closed_at))
    
    median_mttr = median(mttr_times)
    
    # Store results
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
        median_lead_time,
        failure_rate,
        median_mttr
    ))
    
    return {
        'deployments': deployment_count,
        'lead_time': median_lead_time,
        'failure_rate': failure_rate,
        'mttr': median_mttr
    }

def process_metrics():
    logger.info("Starting metrics processing...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Calculate time window (previous day)
        end_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_time = end_time - timedelta(days=1)
        metric_date = start_time.date()
        
        # Get all repositories with activity
        cursor.execute("""
            SELECT DISTINCT repo_id FROM (
                SELECT repo_id FROM deployments
                UNION SELECT repo_id FROM pull_requests
                UNION SELECT repo_id FROM incidents
            ) AS active_repos
        """)
        repos = [row[0] for row in cursor.fetchall()]
        
        results = {}
        for repo_id in repos:
            logger.info(f"Processing metrics for repo: {repo_id}")
            results[repo_id] = process_repo_metrics(cursor, repo_id, start_time, end_time, metric_date)
        
        conn.commit()
        logger.info("Metrics processing completed successfully")
        return results
        
    except Exception as e:
        logger.error(f"Error processing metrics: {str(e)}")
        return {}
    finally:
        cursor.close()
        conn.close()