"""Independently compare real MySQL rows with the CSV totals and Airflow states."""

import json
import sys
from pathlib import Path

import mysql.connector
from airflow.models.xcom import XCom


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
from aurika_hw07 import dag, is_fresh


def main():
    runs = json.loads((ROOT / "evidence" / "runs.json").read_text())
    source = json.loads((ROOT / "evidence" / "source.json").read_text())
    connection = mysql.connector.connect(host="127.0.0.1", port=13307, user="root")
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT VERSION() AS version, @@session.time_zone AS session_zone, @@system_time_zone AS system_zone")
        server = cursor.fetchone()
        cursor.execute("SELECT id, medal_type, `count`, created_at FROM aurika_hryhorova.aurika_hw07_medal_counts ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == len(runs), (len(rows), len(runs))
        by_id = {row["id"]: row for row in rows}
        for run in runs:
            row_id = XCom.get_one(dag_id=dag.dag_id, run_id=run["run_id"],
                                  task_id="count_" + run["selected"], key="return_value")
            row = by_id[row_id]
            assert row["medal_type"].lower() == run["selected"]
            assert row["count"] == source["medal_counts"][row["medal_type"]]
            assert run["state"] == ("failed" if run["delay_seconds"] == 35 else "success")
            run["row_id"] = row_id
        boundaries = {}
        for value in (-1, 0, 29_999_999, 30_000_000, 30_000_001, 35_000_000):
            cursor.execute("SELECT %s BETWEEN 0 AND 30000000 AS fresh", (value,))
            fresh = bool(cursor.fetchone()["fresh"])
            assert fresh == is_fresh(value)
            boundaries[str(value)] = fresh
        report = {"passed": True, "mysql": server, "runs": runs, "rows": rows,
                  "source_rows": source["rows"], "source_sha256": source["sha256"],
                  "boundaries_microseconds": boundaries,
                  "method": "Real Airflow dag.test runs against local MySQL; no mocked SQL or sleep"}
        path = ROOT / "evidence" / "real-verification.json"
        path.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2) + "\n")
        print(json.dumps(report, ensure_ascii=False, default=str, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
