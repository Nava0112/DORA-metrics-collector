import os
import json
import requests
from datetime import datetime
from db_utils import get_db_connection
from github_auth import get_installation_token  # Import the GitHub App auth function
from datetime import timezone

# Remove GITHUB_TOKEN since we're using GitHub App auth now
GITHUB_REPO = os.getenv("GITHUB_REPO")

def github_get(url):
    # Get a fresh installation token for each request (tokens last 1 hour)
    token = get_installation_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }
    return requests.get(url, headers=headers, timeout=10)

def insert_pull_request(cursor, pr, repo_id):
    pr_id = pr['id']
    created_at = datetime.strptime(pr['created_at'], '%Y-%m-%dT%H:%M:%SZ')
    merged_at = datetime.strptime(pr['merged_at'], '%Y-%m-%dT%H:%M:%SZ') if pr.get('merged_at') else None
    commit_sha = pr.get('merge_commit_sha', '')
    base_branch = pr['base']['ref']
    pr_name = pr.get('title', '')

    # --- 🔥 Fetch earliest commit in this PR
    commits_url = pr['_links']['commits']['href']
    commits_resp = github_get(commits_url)
    commits = commits_resp.json()

    if commits and isinstance(commits, list):
        first_commit_at = min(
            datetime.strptime(c['commit']['author']['date'], '%Y-%m-%dT%H:%M:%SZ')
            for c in commits
        )
    else:
        first_commit_at = created_at

    payload = json.dumps({"pull_request": pr})

    cursor.execute("""
        INSERT INTO pull_requests 
            (repo_id, pr_id, merged_at, created_at, first_commit_at, base_branch, commit_sha, pr_name, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pr_id) DO UPDATE SET
            merged_at = EXCLUDED.merged_at,
            commit_sha = EXCLUDED.commit_sha,
            pr_name = EXCLUDED.pr_name,
            first_commit_at = EXCLUDED.first_commit_at,
            payload = EXCLUDED.payload
    """, (repo_id, pr_id, merged_at, created_at, first_commit_at, base_branch, commit_sha, pr_name, payload))

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
    is_incident = True
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
    """Backfill GitHub data including PRs, deployments, incidents, and their relationships"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get last webhook timestamp from sync_state
        cursor.execute("SELECT last_webhook_at FROM sync_state WHERE id = 1")
        row = cursor.fetchone()
        last_webhook_at = row[0] if row and row[0] else None
        print(f"🕒 Last webhook received at: {last_webhook_at}")

        # 1. Fetch repository details
        print(f"⏳ Fetching repository details for {GITHUB_REPO}...")
        repo_data = github_get(f"https://api.github.com/repos/{GITHUB_REPO}").json()
        if 'id' not in repo_data:
            raise ValueError(f"Could not fetch repo data: {repo_data.get('message', 'Unknown error')}")

        repo_id = repo_data['id']
        print(f"✅ Repository ID: {repo_id}")

        # 2. Fetch and process Pull Requests
        print("\n⏳ Fetching Pull Requests...")
        pr_count = 0
        url = f"https://api.github.com/repos/{GITHUB_REPO}/pulls?state=closed&sort=updated&direction=asc&per_page=100"
        while url:
            response = github_get(url)
            response.raise_for_status()
            prs = response.json()

            for pr in prs:
                updated_at = datetime.strptime(pr['updated_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                if last_webhook_at and updated_at <= last_webhook_at:
                    continue  # Skip old PRs

                if pr.get('merged_at'):  # Only process merged PRs
                    insert_pull_request(cursor, pr, repo_id)
                    pr_count += 1

            print(f"📦 Processed {pr_count} PRs so far...", end='\r')
            url = response.links.get('next', {}).get('url')
        print(f"\n✅ Processed {pr_count} total PRs")

        # 3. Fetch and process Deployments
        print("\n⏳ Fetching Deployments...")
        deployment_count = 0
        url = f"https://api.github.com/repos/{GITHUB_REPO}/deployments?per_page=100"
        while url:
            response = github_get(url)
            response.raise_for_status()
            deployments = response.json()

            for deployment in deployments:
                # Get the successful status for each deployment
                statuses = github_get(deployment['statuses_url']).json()
                success_status = next((s for s in statuses if s['state'] == 'success'), None)

                if success_status:
                    created_at = datetime.strptime(success_status['created_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                    if last_webhook_at and created_at <= last_webhook_at:
                        continue  # Skip old deployments

                    insert_deployment(cursor, deployment, success_status, repo_id)
                    deployment_count += 1

            print(f"🚀 Processed {deployment_count} deployments so far...", end='\r')
            url = response.links.get('next', {}).get('url')
        print(f"\n✅ Processed {deployment_count} total deployments")

        # 4. Fetch and process Issues (for incidents)
        print("\n⏳ Fetching Issues...")
        issue_count = 0
        incident_count = 0
        url = f"https://api.github.com/repos/{GITHUB_REPO}/issues?state=all&sort=updated&direction=asc&per_page=100"
        while url:
            response = github_get(url)
            response.raise_for_status()
            issues = response.json()

            for issue in issues:
                if 'pull_request' in issue:
                    continue  # Skip PRs

                updated_at = datetime.strptime(pr['updated_at'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                if last_webhook_at and updated_at <= last_webhook_at:
                    continue  # Skip old issues

                insert_incident(cursor, issue, repo_id)
                issue_count += 1

                if any(label['name'].lower() in ['incident', 'bug'] for label in issue.get('labels', [])):
                    incident_count += 1

            print(f"⚠️  Processed {issue_count} issues ({incident_count} incidents) so far...", end='\r')
            url = response.links.get('next', {}).get('url')
        print(f"\n✅ Processed {issue_count} total issues ({incident_count} incidents)")

        # 5. Link PRs to deployments via commit SHA
        print("\n⏳ Linking PRs to deployments...")
        cursor.execute("""
            INSERT INTO deployment_prs (deployment_id, pr_id)
            SELECT d.deployment_id, pr.pr_id
            FROM deployments d
            JOIN pull_requests pr ON d.commit_sha = pr.commit_sha
            WHERE d.repo_id = %s AND pr.repo_id = %s
            ON CONFLICT DO NOTHING
        """, (repo_id, repo_id))
        print(f"✅ Linked {cursor.rowcount} PRs to deployments")

        # 6. Link incidents to the most recent deployment before the incident
        print("\n⏳ Linking incidents to deployments...")
        cursor.execute("""
            UPDATE incidents i
            SET deployment_id = (
                SELECT d.deployment_id
                FROM deployments d
                WHERE d.repo_id = i.repo_id
                AND d.created_at <= i.created_at
                ORDER BY d.created_at DESC
                LIMIT 1
            )
            WHERE i.repo_id = %s AND i.deployment_id IS NULL
        """, (repo_id,))
        print(f"✅ Linked {cursor.rowcount} incidents to deployments")

        conn.commit()
        print("\n🎉 Backfill completed successfully!")

    except Exception as e:
        print(f"\n❌ Error during backfill: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()
    print("✅ Backfill completed!")

if __name__ == "__main__":
    backfill()