"""Load CSV, wait for ClickHouse, compare independent results, export CSV and benchmark."""
import argparse
import base64
import csv
import io
import json
import os
import statistics
import time
import urllib.request
import uuid
from decimal import Decimal
from pathlib import Path

from produce import messages, produce


def query(sql, query_id=None):
    url = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
    if query_id:
        url += f"/?query_id={query_id}"
    request = urllib.request.Request(url, data=sql.encode())
    credentials = f'{os.getenv("CLICKHOUSE_USER", "hw2")}:{os.getenv("CLICKHOUSE_PASSWORD", "hw2_local_password")}'
    request.add_header("Authorization", "Basic " + base64.b64encode(credentials.encode()).decode())
    with urllib.request.urlopen(request, timeout=300) as response:
        return response.read().decode()


def reference(paths):
    winners = {}
    total = 0
    for path in paths:
        for row in messages(path):
            value = (Decimal(row["amount"]), row["cat_id"], row["transaction_id"])
            state = row["us_state"]
            if state not in winners or value > winners[state]:
                winners[state] = value
            total += 1
    return total, {state: (item[1], item[0]) for state, item in winners.items()}


def decoded(text):
    return {row["us_state"]: (row["cat_id"], Decimal(row["max_amount"]))
            for row in csv.DictReader(io.StringIO(text))}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", nargs="+")
    parser.add_argument("--output", default="/results")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    expected_count, expected = reference(args.csv)
    if not expected_count:
        raise RuntimeError("CSV contains no transactions")
    current = int(query("SELECT count() FROM hw2.transactions"))
    if not args.verify_only:
        if current:
            raise RuntimeError("Table already contains data. Use --verify-only with the same CSV files, or reset the HW2 volumes before a new load.")
        assert produce(args.csv) == expected_count
    deadline = time.monotonic() + args.timeout
    while True:
        count = int(query("SELECT count() FROM hw2.transactions"))
        if count >= expected_count:
            break
        if time.monotonic() > deadline:
            raise TimeoutError(f"ClickHouse received {count}/{expected_count} rows")
        print(f"ClickHouse: {count:,}/{expected_count:,}", flush=True)
        time.sleep(2)
    if count != expected_count:
        raise RuntimeError(f"Unexpected row count: {count} != {expected_count}")
    queries = {"baseline": Path("sql/02_max_transaction_by_state.sql").read_text(),
               "optimized": Path("sql/03_max_transaction_by_state_optimized.sql").read_text()}
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    # Both independent SQL implementations must equal the Python Decimal reference.
    for name, sql in queries.items():
        result = query(sql)
        if decoded(result) != expected:
            raise AssertionError(f"{name} SQL differs from CSV reference")
        (output / f"max_transaction_by_state_{name}.csv").write_text(result)
    # Measure baseline physical storage on exactly the same data.
    query(Path("sql/00_baseline.sql").read_text())
    query("TRUNCATE TABLE hw2.transactions_baseline")
    query("INSERT INTO hw2.transactions_baseline SELECT * FROM hw2.transactions")
    for table in ("transactions", "transactions_baseline", "state_max"):
        query(f"OPTIMIZE TABLE hw2.{table} FINAL")
    queries["unoptimized_storage"] = queries["baseline"].replace("hw2.transactions", "hw2.transactions_baseline")
    measurements = {}
    for name, sql in queries.items():
        query(sql)  # Warm-up; query cache disabled by default.
        durations, identifiers = [], []
        for _ in range(5):
            identifier = "hw2_" + uuid.uuid4().hex
            start = time.perf_counter()
            result = query(sql, identifier)
            durations.append((time.perf_counter() - start) * 1000)
            identifiers.append(identifier)
            if decoded(result) != expected:
                raise AssertionError(f"Benchmark {name} differs from reference")
        query("SYSTEM FLUSH LOGS")
        ids = ",".join(f"'{item}'" for item in identifiers)
        logs = query(f"SELECT query_duration_ms, read_rows, read_bytes, memory_usage FROM system.query_log WHERE type='QueryFinish' AND query_id IN ({ids}) FORMAT JSONEachRow")
        measurements[name] = {"client_median_ms": statistics.median(durations),
                              "client_runs_ms": durations,
                              "server_runs": [json.loads(line) for line in logs.splitlines()]}
    storage = query("SELECT table, sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, sum(data_compressed_bytes) AS compressed_bytes, sum(data_uncompressed_bytes) AS uncompressed_bytes FROM system.parts WHERE database='hw2' AND active GROUP BY table ORDER BY table FORMAT JSONEachRow")
    report = {"csv_files": [Path(p).name for p in args.csv], "transactions": count,
              "states": len(expected), "verification": "both SQL queries match independent CSV reference",
              "clickhouse_version": query("SELECT version()").strip(),
              "benchmark": measurements,
              "storage": [json.loads(line) for line in storage.splitlines()]}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    print("PASS: CSV -> Kafka -> ClickHouse; both SQL results match CSV reference")


if __name__ == "__main__":
    main()
