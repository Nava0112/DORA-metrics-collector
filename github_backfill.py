import os
import json
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from db_utils import get_db_connection

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

def github_get(url):
    return requests.get(url, headers=HEADERS)

def insert_pull_request(cursor, pr, repo_id):
    pr_id = pr['id']
    created_at = datetime.strptime(pr['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    merged_at = datetime.strptime(pr['merged_at'], '%Y-%m-%dT%H:%M:%SZ') if pr.get('merged_at') else None
    commit_sha = pr.get('merge_commit_sha', '')
    base_branch = pr['base']['ref']
    pr_name = pr.get('title', '')
    payload = json.dumps({"pull_request": pr})
    cursor.execute("""
        INSERT INTO pull_requests 
            (repo_id, pr_id, merged_at, created_at, first_commit_at, base_branch, commit_sha, pr_name, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pr_id) DO UPDATE SET
            merged_at = EXCLUDED.merged_at,
            commit_sha = EXCLUDED.commit_sha,
            pr_name = EXCLUDED.pr_name,
            payload = EXCLUDED.payload
    """, (repo_id, pr_id, merged_at, created_at, created_at, base_branch, commit_sha, pr_name, payload))

def insert_deployment(cursor, deployment, status, repo_id):
    deployment_id = deployment['id']
    environment = deployment.get('environment', '')
    state = status['state']
    created_at = datetime.strptime(status['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    commit_sha = deployment.get('sha', '')
    payload = json.dumps({"deployment": deployment, "deployment_status": status})
    cursor.execute("""
        INSERT INTO deployments 
            (repo_id, deployment_id, environment, status, created_at, commit_sha, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (deployment_id) DO UPDATE SET
            status = EXCLUDED.status,
            payload = EXCLUDED.payload
    """, (repo_id, deployment_id, environment, state, created_at, commit_sha, payload))

def insert_incident(cursor, issue, repo_id):
    issue_id = issue['id']
    created_at = datetime.strptime(issue['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    closed_at = datetime.strptime(issue['closed_at'], '%Y-%m-%dT%H:%M:%SZ') if issue.get('closed_at') else None
    labels = [l['name'].lower() for l in issue.get('labels', [])]
    is_incident = any(lbl in labels for lbl in ['incident', 'outage', 'failure', 'sev'])
    payload = json.dumps({"issue": issue})
    cursor.execute("""
        INSERT INTO incidents 
            (repo_id, issue_id, created_at, closed_at, is_incident, payload)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (issue_id) DO UPDATE SET
            closed_at = EXCLUDED.closed_at,
            is_incident = EXCLUDED.is_incident,
            payload = EXCLUDED.payload
    """, (repo_id, issue_id, created_at, closed_at, is_incident, payload))

def backfill():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Fetch repo once to get repo_id
    print("Fetching repository details...")
    repo_data = github_get(f"https://api.github.com/repos/{GITHUB_REPO}").json()
    repo_id = repo_data['id']
    print(f"✅ Working with repo_id={repo_id}")

    # Fetch PRs
    print("Fetching PRs...")
    url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls?state=closed&per_page=100"
    while url:
        r = github_get(url)
        r.raise_for_status()
        for pr in r.json():
            if pr.get('merged_at'):
                insert_pull_request(cursor, pr, repo_id)
        url = r.links.get('next', {}).get('url')

    # Fetch Deployments
    print("Fetching Deployments...")
    url = f"https://api.github.com/repos/{GITHUB_REPO}/deployments?per_page=100"
    while url:
        r = github_get(url)
        r.raise_for_status()
        for deployment in r.json():
            statuses = github_get(deployment['statuses_url']).json()
            for status in statuses:
                if status['state'] == 'success':
                    insert_deployment(cursor, deployment, status, repo_id)
        url = r.links.get('next', {}).get('url')

    # Fetch Issues (for incidents)
    print("Fetching Issues...")
    url = f"https://api.github.com/repos/{GITHUB_REPO}/issues?state=all&per_page=100"
    while url:
        r = github_get(url)
        r.raise_for_status()
        for issue in r.json():
            if 'pull_request' not in issue:
                insert_incident(cursor, issue, repo_id)
        url = r.links.get('next', {}).get('url')

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Backfill completed!")

if __name__ == "__main__":
    backfill()
