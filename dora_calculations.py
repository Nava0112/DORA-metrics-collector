from datetime import datetime, timedelta
import math

def calculate_lead_time(first_commit, merged_at, deploy_time):
    """Calculate lead time with edge cases"""
    if not merged_at:
        # Direct commit, use deployment time as reference
        return max((deploy_time - first_commit).total_seconds() / 3600, 0.1)
    
    if deploy_time < merged_at:
        # Handle deployments before PR merge (hotfixes)
        return max((deploy_time - first_commit).total_seconds() / 3600, 0.1)
    
    return (deploy_time - first_commit).total_seconds() / 3600

def calculate_failure_rate(total_deployments, failed_deployments):
    """Safely calculate failure rate"""
    if total_deployments == 0:
        return 0.0
    return (failed_deployments / total_deployments) * 100

def calculate_mttr(created_at, closed_at):
    """Calculate time to restore with validation"""
    if not closed_at or closed_at < created_at:
        return 0.0
    return (closed_at - created_at).total_seconds() / 3600

def median(values):
    """Calculate median of a list"""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid-1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]

def detect_production_deployment(payload):
    """Determine if deployment is production"""
    environment = payload.get('environment', '').lower()
    if any(prod_term in environment for prod_term in ['prod', 'production', 'live']):
        return True
    
    workflow_run = payload.get('workflow_run', {})
    workflow_name = workflow_run.get('name', '').lower() if workflow_run else ''
    if any(prod_term in workflow_name for prod_term in ['deploy', 'prod', 'release']):
        return True
    
    return False