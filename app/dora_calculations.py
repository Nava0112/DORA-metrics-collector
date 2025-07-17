from datetime import datetime, timedelta
import math
import logging

logger = logging.getLogger(__name__)

def calculate_lead_time(first_commit, merged_at, deploy_time):
    if not merged_at:
        return max((deploy_time - first_commit).total_seconds() / 3600, 0.1)
    if deploy_time < merged_at:
        return max((deploy_time - first_commit).total_seconds() / 3600, 0.1)
    return (deploy_time - first_commit).total_seconds() / 3600

def calculate_failure_rate(total_deployments, failed_deployments):
    if total_deployments == 0:
        return 0.0
    return (failed_deployments / total_deployments) * 100

def calculate_mttr(created_at, closed_at):
    if not closed_at or closed_at < created_at:
        return 0.0
    return (closed_at - created_at).total_seconds() / 3600

def median(values):
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]

def detect_production_deployment(environment, payload):
    try:
        env = environment.lower() if environment else ''
        if any(term in env for term in ['prod', 'production', 'live']):
            return True

        workflow_run = payload.get('workflow_run', {}) if payload else {}
        workflow_name = workflow_run.get('name', '').lower()
        if any(term in workflow_name for term in ['deploy', 'prod', 'release']):
            return True
    except Exception as e:
        logger.warning(f"Error detecting production deployment: {e}")
    return False

