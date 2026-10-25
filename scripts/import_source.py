"""Import the medal column of the pinned Olympics CSV into a private local MySQL."""

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

import mysql.connector


SOURCE_URL = "https://media.githubusercontent.com/media/sfc-gh-mconsoli/olympic_games_data_hub/21b851f8fbe775332b1bc43b9dac213b4087e80b/dataset/Olympic_Athlete_Event_Results.csv"
SOURCE_SHA256 = "4a76c66fc6f69a2f69b4a953298d475fd61fb46a40ac5995ea738bc94695905b"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--port", type=int, default=13307)
    args = parser.parse_args()
    digest = hashlib.sha256(args.csv_path.read_bytes()).hexdigest()
    if digest != SOURCE_SHA256:
        raise SystemExit("CSV checksum differs from the documented source")
    with args.csv_path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        medal_column = next(name for name in reader.fieldnames if name.lower() == "medal")
        medals = [row[medal_column] for row in reader]
    connection = mysql.connector.connect(host="127.0.0.1", port=args.port, user="root")
    try:
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS olympic_dataset")
        cursor.execute("CREATE DATABASE IF NOT EXISTS aurika_hryhorova")
        cursor.execute("""CREATE TABLE IF NOT EXISTS olympic_dataset.athlete_event_results (
            source_row_id BIGINT UNSIGNED PRIMARY KEY,
            medal VARCHAR(30) NOT NULL
        )""")
        cursor.execute("SELECT COUNT(*) FROM olympic_dataset.athlete_event_results")
        if cursor.fetchone()[0] != 0:
            raise SystemExit("Local source is not empty; refusing to replace or duplicate it")
        for start in range(0, len(medals), 10000):
            cursor.executemany(
                "INSERT INTO olympic_dataset.athlete_event_results VALUES (%s, %s)",
                [(index + 1, medals[index]) for index in range(start, min(start + 10000, len(medals)))],
            )
        connection.commit()
        cursor.execute("SELECT medal, COUNT(*) FROM olympic_dataset.athlete_event_results GROUP BY medal")
        actual = dict(cursor.fetchall())
        expected = dict(Counter(medals))
        if actual != expected:
            raise RuntimeError("Imported medal counts differ from the CSV")
        report = {"source_url": SOURCE_URL, "sha256": digest, "rows": len(medals),
                  "medal_counts": expected, "mysql_counts": actual,
                  "projection": ["source row number", "medal"],
                  "scope": "Own isolated local MySQL; not the remote GoIT database"}
        target = Path(__file__).resolve().parents[1] / "evidence" / "source.json"
        target.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(report, ensure_ascii=False, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
