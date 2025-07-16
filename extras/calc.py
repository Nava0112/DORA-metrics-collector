import os
import argparse
from datetime import datetime
from db_utils import get_db_connection
from metrics_processor import process_metrics

def parse_args():
    parser = argparse.ArgumentParser(
        description="Delete and recalculate DORA metrics"
    )
    parser.add_argument(
        "--repo-id", type=int, help="GitHub repo ID to filter"
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Start date (inclusive) for metrics deletion"
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="End date (inclusive) for metrics deletion"
    )
    return parser.parse_args()

def delete_metrics(cursor, repo_id=None, start_date=None, end_date=None):
    conditions = []
    params = []
    if repo_id:
        conditions.append("repo_id = %s")
        params.append(repo_id)
    if start_date:
        conditions.append("metric_date >= %s")
        params.append(start_date)
    if end_date:
        conditions.append("metric_date <= %s")
        params.append(end_date)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = f"DELETE FROM dora_metrics {where_clause};"
    cursor.execute(sql, tuple(params))
    deleted = cursor.rowcount
    print(f"🗑️  Deleted {deleted} rows from dora_metrics")
    return deleted

def main():
    args = parse_args()

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Step 1: Delete existing metrics
        delete_metrics(
            cursor,
            repo_id=args.repo_id,
            start_date=args.start_date,
            end_date=args.end_date
        )
        conn.commit()

        # Step 2: Recalculate metrics
        print("🔄 Running process_metrics() to rebuild metrics...")
        results = process_metrics()
        print(f"✅ Recalculation complete: {len(results)} metrics entries processed")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during recalculation: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()