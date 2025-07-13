import hmac
import hashlib
import json
import os
from flask import Flask, request, jsonify
from datetime import datetime
from db_utils import get_db_connection
import psycopg2

app = Flask(__name__)

# Helper Functions
def verify_signature(payload_body, signature_header):
    secret = os.getenv('GITHUB_WEBHOOK_SECRET').encode()
    hash_object = hmac.new(secret, msg=payload_body, digestmod=hashlib.sha256)
    expected_signature = 'sha256=' + hash_object.hexdigest()
    return hmac.compare_digest(expected_signature, signature_header)

def is_production_deployment(payload):
    """Determine if deployment is production"""
    # Check environment name
    environment = payload.get('environment', '').lower()
    if any(prod_term in environment for prod_term in ['prod', 'production', 'live']):
        return True
    
    # Check workflow name
    workflow_run = payload.get('workflow_run', {})
    workflow_name = workflow_run.get('name', '').lower() if workflow_run else ''
    if any(prod_term in workflow_name for prod_term in ['deploy', 'prod', 'release']):
        return True
    
    return False

def link_pr_to_deployment(cursor, repo_id, pr_id, commit_sha):
    """Link PR to deployment via commit SHA"""
    try:
        cursor.execute("""
            INSERT INTO deployment_prs (deployment_id, pr_id)
            SELECT deployment_id, %s
            FROM deployments
            WHERE 
                repo_id = %s AND
                commit_sha = %s
            ON CONFLICT DO NOTHING
        """, (pr_id, repo_id, commit_sha))
    except Exception as e:
        print(f"Error linking PR to deployment: {str(e)}")

def handle_deployment_event(cursor, payload):
    deployment = payload['deployment']
    status = payload['deployment_status']['state']
    repo_id = payload['repository']['id']
    commit_sha = deployment.get('sha', '')
    created_at = datetime.strptime(payload['deployment_status']['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    
    # Only store successful production deployments
    if status == 'success' and is_production_deployment(payload):
        try:
            cursor.execute("""
                INSERT INTO deployments (
                    repo_id, deployment_id, environment, status, 
                    created_at, commit_sha, payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (deployment_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    payload = EXCLUDED.payload
            """, (
                repo_id,
                deployment['id'],
                deployment.get('environment', ''),
                status,
                created_at,
                commit_sha,
                json.dumps(payload)
            ))
        except Exception as e:
            print(f"Error storing deployment: {str(e)}")

def handle_pull_request_event(cursor, payload):
    if payload['action'] != 'closed' or not payload['pull_request']['merged']:
        return
    
    pr = payload['pull_request']
    repo_id = payload['repository']['id']
    pr_id = pr['id']
    commit_sha = pr['merge_commit_sha'] if 'merge_commit_sha' in pr else ''
    created_at = datetime.strptime(pr['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    merged_at = datetime.strptime(pr['merged_at'], '%Y-%m-%dT%H:%M:%SZ') if pr.get('merged_at') else None
    
    # Get first commit time (approximation)
    first_commit_at = created_at  # Default to PR creation time
    if 'commits' in pr and pr['commits'] > 0:
        try:
            # Use the oldest commit in the PR
            cursor.execute("""
                SELECT MIN(commit->'commit'->'author'->>'date')::TIMESTAMPTZ
                FROM jsonb_array_elements(%s) AS commit
            """, (json.dumps(pr.get('commits', [])),))
            result = cursor.fetchone()
            if result and result[0]:
                first_commit_at = result[0]
        except:
            pass
    
    try:
        # Insert/update PR
        cursor.execute("""
            INSERT INTO pull_requests (
                repo_id, pr_id, merged_at, created_at, 
                first_commit_at, base_branch, commit_sha, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pr_id) DO UPDATE SET
                merged_at = EXCLUDED.merged_at,
                commit_sha = EXCLUDED.commit_sha,
                payload = EXCLUDED.payload
        """, (
            repo_id,
            pr_id,
            merged_at,
            created_at,
            first_commit_at,
            pr['base']['ref'],
            commit_sha,
            json.dumps(payload)
        ))
        
        # Link PR to deployment
        if commit_sha:
            link_pr_to_deployment(cursor, repo_id, pr_id, commit_sha)
            
    except Exception as e:
        print(f"Error storing PR: {str(e)}")

def handle_issues_event(cursor, payload):
    if payload['action'] not in ['opened', 'closed', 'reopened', 'labeled', 'unlabeled']:
        return
    
    issue = payload['issue']
    repo_id = payload['repository']['id']
    issue_id = issue['id']
    created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ') if issue.get('closed_at') else None
    
    # Check if incident
    labels = [label['name'].lower() for label in issue.get('labels', [])]
    is_incident = any(incident_term in label for label in labels 
                     for incident_term in ['incident', 'outage', 'failure', 'sev'])
    
    try:
        # Insert/update incident
        cursor.execute("""
            INSERT INTO incidents (
                repo_id, issue_id, created_at, closed_at, 
                is_incident, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (issue_id) DO UPDATE SET
                closed_at = EXCLUDED.closed_at,
                is_incident = EXCLUDED.is_incident,
                payload = EXCLUDED.payload
        """, (
            repo_id,
            issue_id,
            created_at,
            closed_at,
            is_incident,
            json.dumps(payload)
        ))
        
        # Try to link to deployment
        cursor.execute("""
            UPDATE incidents
            SET deployment_id = (
                SELECT deployment_id
                FROM deployments
                WHERE 
                    repo_id = %s AND
                    created_at BETWEEN %s - INTERVAL '24 HOURS' AND %s + INTERVAL '24 HOURS'
                ORDER BY ABS(EXTRACT(EPOCH FROM (created_at - %s))) 
                LIMIT 1
            )
            WHERE issue_id = %s
        """, (repo_id, created_at, created_at, created_at, issue_id))
            
    except Exception as e:
        print(f"Error storing incident: {str(e)}")

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    signature = request.headers.get('X-Hub-Signature-256')
    if not signature or not verify_signature(request.data, signature):
        return jsonify({'error': 'Invalid signature'}), 401
    
    event_type = request.headers.get('X-GitHub-Event')
    payload = request.json
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if event_type == 'deployment_status':
            handle_deployment_event(cursor, payload)
        elif event_type == 'pull_request':
            handle_pull_request_event(cursor, payload)
        elif event_type == 'issues':
            handle_issues_event(cursor, payload)
        
        conn.commit()
        return jsonify({'status': 'success'}), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    from db_utils import initialize_db
    initialize_db()
    app.run(host='0.0.0.0', port=5000, debug=True)