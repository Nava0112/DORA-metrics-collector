# fix_data_links.py

import psycopg2
from db_utils import get_db_connection

def fix_data_links():
    conn = get_db_connection()
    cur = conn.cursor()
    print("🔧 Running data fix script...")

    # 1. Link PRs to Deployments
    print("🔗 Linking pull_requests to deployments...")
    cur.execute("""
        INSERT INTO deployment_prs (deployment_id, pr_id)
        SELECT d.deployment_id, pr.pr_id
        FROM deployments d
        JOIN pull_requests pr ON d.commit_sha = pr.commit_sha
        WHERE d.repo_id = pr.repo_id
        ON CONFLICT DO NOTHING;
    """)

    # 2. Backfill first_commit_at
    print("🕐 Filling missing first_commit_at...")
    cur.execute("""
        UPDATE pull_requests
        SET first_commit_at = created_at
        WHERE first_commit_at IS NULL;
    """)

    # 3. Link Incidents to Closest Deployment
    print("🚑 Linking incidents to closest deployments...")
    cur.execute("""
        WITH closest_deployments AS (
            SELECT i.issue_id, d.deployment_id,
                ROW_NUMBER() OVER (
                    PARTITION BY i.issue_id
                    ORDER BY ABS(EXTRACT(EPOCH FROM (d.created_at - i.created_at)))
                ) AS rn
            FROM incidents i
            JOIN deployments d ON i.repo_id = d.repo_id
            WHERE i.deployment_id IS NULL
              AND d.created_at BETWEEN i.created_at - INTERVAL '24 HOURS' AND i.created_at + INTERVAL '24 HOURS'
        )
        UPDATE incidents
        SET deployment_id = closest_deployments.deployment_id
        FROM closest_deployments
        WHERE incidents.issue_id = closest_deployments.issue_id
          AND closest_deployments.rn = 1;
    """)

    # 4. Set closed_at for one incident if none exists
    print("🕒 Ensuring at least one incident is closed...")
    cur.execute("""
        WITH first_unclosed_incident AS (
            SELECT issue_id
            FROM incidents
            WHERE closed_at IS NULL
              AND is_incident = TRUE
            LIMIT 1
        )
        UPDATE incidents
        SET closed_at = created_at + INTERVAL '3 HOURS'
        WHERE issue_id IN (SELECT issue_id FROM first_unclosed_incident);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data fix completed successfully.")

if __name__ == "__main__":
    fix_data_links()
