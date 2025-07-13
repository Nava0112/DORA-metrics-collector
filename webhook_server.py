# webhook_server.py
import os
import json
import logging
import hmac
import hashlib
from flask import Flask, request, jsonify
from datetime import datetime
from db_utils import get_db_connection
from dora_calculations import detect_production_deployment
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
    first_commit_at = created_at

    try:
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
            repo_id, pr_id, merged_at, created_at,
            first_commit_at, pr['base']['ref'], commit_sha, json.dumps(payload)
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
    labels = [label['name'].lower() for label in issue.get('labels', [])]
    is_incident = any(term in label for label in labels for term in ['incident', 'outage', 'failure', 'sev'])

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

        cursor.execute("SELECT deployment_id, repo_id, environment, status, created_at FROM deployments ORDER BY created_at DESC LIMIT 20")
        deployments = cursor.fetchall()

        cursor.execute("SELECT pr_id, repo_id, merged_at, base_branch FROM pull_requests ORDER BY merged_at DESC NULLS LAST LIMIT 20")
        prs = cursor.fetchall()

        cursor.execute("SELECT issue_id, repo_id, created_at, closed_at, is_incident FROM incidents ORDER BY created_at DESC LIMIT 20")
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
                    "base_branch": p[3]
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

@app.route('/metrics', methods=['GET'])
def get_latest_metrics():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT DISTINCT ON (repo_id)
                repo_id, metric_date, deployment_frequency,
                lead_time_hours, change_failure_rate, mttr_hours
            FROM dora_metrics
            ORDER BY repo_id, metric_date DESC
        """)

        rows = cursor.fetchall()
        metrics = []
        for row in rows:
            metrics.append({
                "repo_id": row[0],
                "date": row[1].isoformat(),
                "deployment_frequency": row[2],
                "lead_time_hours": row[3],
                "change_failure_rate": row[4],
                "mttr_hours": row[5]
            })

        return jsonify({"metrics": metrics}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"}), 200

@app.route('/', methods=['GET'])
def home():
    return "🚀 Dora Metrics Webhook Server is running!", 200

if __name__ == '__main__':
    from db_utils import initialize_db
    initialize_db()
    app.run(host='0.0.0.0', port=5000, debug=True)
# webhook_server.py