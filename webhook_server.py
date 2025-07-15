import os
import json
import logging
import hmac
import hashlib
import requests
from flask import Flask, request, jsonify
from datetime import datetime
from db_utils import get_db_connection
from dora_calculations import detect_production_deployment, median
from webhook_processor import handle_deployment_event, handle_pull_request_event, handle_issues_event
from db_utils import initialize_db
from metrics_processor import process_metrics

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def verify_signature(payload_body, signature_header):
    secret = os.getenv('GITHUB_WEBHOOK_SECRET').encode()
    hash_object = hmac.new(secret, msg=payload_body, digestmod=hashlib.sha256)
    expected_signature = 'sha256=' + hash_object.hexdigest()
    return hmac.compare_digest(expected_signature, signature_header)

def link_pr_to_deployment(cursor, repo_id, pr_id, commit_sha):
    try:
        cursor.execute("""
            INSERT INTO deployment_prs (deployment_id, pr_id)
            SELECT deployment_id, %s
            FROM deployments
            WHERE repo_id = %s AND commit_sha = %s
            ON CONFLICT DO NOTHING
        """, (pr_id, repo_id, commit_sha))
    except Exception as e:
        logger.error(f"Error linking PR to deployment: {str(e)}")

def handle_deployment_event(cursor, payload):
    deployment = payload['deployment']
    status = payload['deployment_status']['state']
    repo_id = payload['repository']['id']
    commit_sha = deployment.get('sha', '')

    try:
        created_at = datetime.strptime(payload['deployment_status']['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    except:
        created_at = datetime.utcnow()

    if status == 'success' and detect_production_deployment(payload):
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
            logger.info(f"✅ Stored deployment {deployment['id']}")
        except Exception as e:
            logger.error(f"❌ Error storing deployment: {str(e)}")

def handle_pull_request_event(cursor, payload):
    if payload['action'] != 'closed' or not payload['pull_request']['merged']:
        return

    pr = payload['pull_request']
    repo_id = payload['repository']['id']
    pr_id = pr['id']
    commit_sha = pr.get('merge_commit_sha', '')
    created_at = datetime.strptime(pr['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    merged_at = datetime.strptime(pr['merged_at'], '%Y-%m-%dT%H:%M:%SZ')
    pr_name = pr.get('title', '')

    # 🔥 Fetch actual first commit date
    try:
        commits_url = pr['_links']['commits']['href']
        commits_resp = requests.get(commits_url, headers={
            "Authorization": f"token {os.getenv('GITHUB_TOKEN')}",
            "Accept": "application/vnd.github+json"
        })
        commits = commits_resp.json()
        if commits and isinstance(commits, list):
            first_commit_at = min(
                datetime.strptime(c['commit']['author']['date'], '%Y-%m-%dT%H:%M:%SZ')
                for c in commits
            )
        else:
            first_commit_at = created_at
    except Exception as e:
        logger.warning(f"Could not fetch commits for PR {pr_id}: {e}")
        first_commit_at = created_at

    try:
        cursor.execute("""
            INSERT INTO pull_requests (
                repo_id, pr_id, merged_at, created_at,
                first_commit_at, base_branch, commit_sha, pr_name, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pr_id) DO UPDATE SET
                merged_at = EXCLUDED.merged_at,
                commit_sha = EXCLUDED.commit_sha,
                pr_name = EXCLUDED.pr_name,
                first_commit_at = EXCLUDED.first_commit_at,
                payload = EXCLUDED.payload
        """, (
            repo_id, pr_id, merged_at, created_at,
            first_commit_at, pr['base']['ref'], commit_sha, pr_name, json.dumps(payload)
        ))

        if commit_sha:
            link_pr_to_deployment(cursor, repo_id, pr_id, commit_sha)

    except Exception as e:
        logger.error(f"Error storing PR: {str(e)}")

def handle_issues_event(cursor, payload):
    if payload['action'] not in ['opened', 'closed', 'reopened', 'labeled', 'unlabeled']:
        return

    issue = payload['issue']
    repo_id = payload['repository']['id']
    issue_id = issue['id']
    created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ') if issue.get('closed_at') else None

    # 🔥 Always mark as incident
    is_incident = True

    try:
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
            repo_id, issue_id, created_at, closed_at, is_incident, json.dumps(payload)
        ))

        # Link to nearest deployment
        cursor.execute("""
            UPDATE incidents
            SET deployment_id = (
                SELECT deployment_id
                FROM deployments
                WHERE repo_id = %s AND
                      created_at BETWEEN %s - INTERVAL '24 HOURS' AND %s + INTERVAL '24 HOURS'
                ORDER BY ABS(EXTRACT(EPOCH FROM (created_at - %s)))
                LIMIT 1
            )
            WHERE issue_id = %s
        """, (repo_id, created_at, created_at, created_at, issue_id))

        logger.info(f"✅ Inserted incident {issue_id} for repo {repo_id}")
    except Exception as e:
        logger.error(f"Error storing incident: {str(e)}")

@app.route('/webhook', methods=['POST'])
def webhook():
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
        logger.error(f"Webhook processing failed: {str(e)}")
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/calculate', methods=['POST'])
def calculate_now():
    try:
        process_metrics()
        return jsonify({"status": "Metrics calculated"}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logs', methods=['GET'])
def get_logs():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT deployment_id, repo_id, environment, status, created_at
            FROM deployments
            ORDER BY created_at DESC
            LIMIT 20
        """)
        deployments = cursor.fetchall()

        cursor.execute("""
            SELECT pr_id, repo_id, merged_at, base_branch, pr_name
            FROM pull_requests
            ORDER BY merged_at DESC NULLS LAST
            LIMIT 20
        """)
        prs = cursor.fetchall()

        cursor.execute("""
            SELECT issue_id, repo_id, created_at, closed_at, is_incident
            FROM incidents
            ORDER BY created_at DESC
            LIMIT 20
        """)
        incidents = cursor.fetchall()

        return jsonify({
            "deployments": [
                {
                    "deployment_id": d[0], "repo_id": d[1], "environment": d[2],
                    "status": d[3], "created_at": d[4].isoformat()
                } for d in deployments
            ],
            "pull_requests": [
                {
                    "pr_id": p[0], "repo_id": p[1],
                    "merged_at": p[2].isoformat() if p[2] else None,
                    "base_branch": p[3],
                    "pr_name": p[4]
                } for p in prs
            ],
            "incidents": [
                {
                    "issue_id": i[0], "repo_id": i[1],
                    "created_at": i[2].isoformat(),
                    "closed_at": i[3].isoformat() if i[3] else None,
                    "is_incident": i[4]
                } for i in incidents
            ]
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# webhook_server.py
@app.route('/daily_metrics', methods=['GET'])
def get_daily_metrics():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT repo_id, metric_date, 
                   deployment_frequency, 
                   lead_time_hours,
                   change_failure_rate,
                   mttr_hours
            FROM dora_metrics
            ORDER BY metric_date DESC
            LIMIT 30  -- Last 30 days
        """)
        results = []
        for row in cursor.fetchall():
            results.append({
                "repo_id": row[0],
                "date": row[1].isoformat(),
                "deployment_frequency": row[2],
                "lead_time_hours": row[3],
                "change_failure_rate": row[4],
                "mttr_hours": row[5]
            })
        return jsonify({"daily_metrics": results}), 200
    finally:
        cursor.close()
        conn.close()

@app.route('/metrics', methods=['GET'])
def get_overall_metrics():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get all distinct repos
        cursor.execute("""
            SELECT DISTINCT repo_id FROM (
                SELECT repo_id FROM deployments
                UNION
                SELECT repo_id FROM pull_requests
                UNION
                SELECT repo_id FROM incidents
            ) AS repos
        """)
        repos = [row[0] for row in cursor.fetchall()]

        metrics = []

        for repo_id in repos:
            # Total successful deployments
            cursor.execute("""
                SELECT COUNT(*) FROM deployments
                WHERE repo_id = %s AND status='success'
            """, (repo_id,))
            successful_deployments = cursor.fetchone()[0]

            # Total deployments (for failure rate)
            cursor.execute("""
                SELECT COUNT(*) FROM deployments
                WHERE repo_id = %s
            """, (repo_id,))
            total_deployments = cursor.fetchone()[0]

            # Change Failure Rate
            cursor.execute("""
                SELECT COUNT(DISTINCT i.deployment_id)
                FROM incidents i
                WHERE i.repo_id = %s AND i.is_incident = TRUE AND i.deployment_id IS NOT NULL
            """, (repo_id,))
            failed_deployments = cursor.fetchone()[0]
            failure_rate = (failed_deployments / total_deployments * 100) if total_deployments else 0.0

            # Median Lead Time (based on DISTINCT lead_time_hours from dora_metrics)
            cursor.execute("""
                SELECT
                    COALESCE(
                        percentile_cont(0.5) WITHIN GROUP (ORDER BY lead_time_hours),
                        0.0
                    ) AS median_lead_time
                FROM (
                    SELECT DISTINCT lead_time_hours
                    FROM dora_metrics
                    WHERE repo_id = %s
                ) AS distinct_lead_times
            """, (repo_id,))
            median_lead_time = cursor.fetchone()[0]

            # Median MTTR
            cursor.execute("""
                SELECT EXTRACT(EPOCH FROM (closed_at - created_at)) / 3600.0
                FROM incidents
                WHERE repo_id = %s AND is_incident = TRUE AND closed_at IS NOT NULL
            """, (repo_id,))
            mttrs = [row[0] for row in cursor.fetchall()]
            median_mttr = median(mttrs) if mttrs else 0.0

            metrics.append({
                "repo_id": repo_id,
                "total_successful_deployments": successful_deployments,
                "median_lead_time_hours": median_lead_time,
                "change_failure_rate": failure_rate,
                "median_mttr_hours": median_mttr
            })

        return jsonify({"overall_metrics": metrics}), 200

    except Exception as e:
        logger.error(f"Error computing overall metrics: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor: cursor.close()
        if conn: conn.close()


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"}), 200

@app.route('/', methods=['GET'])
def home():
    return "🚀 Dora Metrics Webhook Server is running!", 200

if __name__ == '__main__':
    initialize_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
