#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import math
import os
import pathlib
import time
import urllib.parse
import urllib.request


QUERIES = {
    "process_cpu_seconds_total": 'process_cpu_seconds_total{job=~"tidb|tikv"}',
    "process_resident_memory_bytes": 'process_resident_memory_bytes{job=~"tidb|tikv"}',
    "commit_txn_counter": "tidb_tikvclient_commit_txn_counter",
    "async_commit_txn_counter": "tidb_tikvclient_async_commit_txn_counter",
    "one_pc_txn_counter": "tidb_tikvclient_one_pc_txn_counter",
    "transaction_duration_count": 'tidb_session_transaction_duration_seconds_count{scope="general"}',
    "transaction_duration_sum": 'tidb_session_transaction_duration_seconds_sum{scope="general"}',
    "region_count": 'tikv_raftstore_region_count{type=~"leader|region"}',
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_root", type=pathlib.Path)
    parser.add_argument("output_root", type=pathlib.Path)
    parser.add_argument(
        "--prometheus-url",
        default="http://127.0.0.1:9393",
    )
    return parser.parse_args()


def parse_time(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def query_range(base_url, query, start, end):
    params = urllib.parse.urlencode(
        {
            "query": query,
            "start": f"{start:.3f}",
            "end": f"{end:.3f}",
            "step": "15",
        }
    )
    url = f"{base_url}/api/v1/query_range?{params}"
    last_error = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                payload = json.load(response)
            if payload.get("status") != "success":
                raise RuntimeError(payload)
            return payload["data"]["result"]
        except Exception as error:
            last_error = error
            if attempt < 2:
                time.sleep(attempt + 1)
    raise last_error


def samples_in_window(series, start, end):
    values = [(float(ts), float(value)) for ts, value in series.get("values", [])]
    selected = [(ts, value) for ts, value in values if start <= ts <= end]
    if len(selected) >= 2:
        return selected
    before = [sample for sample in values if sample[0] < start]
    after = [sample for sample in values if sample[0] > end]
    if before:
        selected.insert(0, before[-1])
    if after:
        selected.append(after[0])
    return selected


def counter_delta(samples):
    if len(samples) < 2:
        return 0.0
    delta = 0.0
    previous = samples[0][1]
    for _, value in samples[1:]:
        if value >= previous:
            delta += value - previous
        else:
            delta += value
        previous = value
    return delta


def summarize_counter(series_list, start, end, label_name=None):
    result = {}
    for series in series_list:
        samples = samples_in_window(series, start, end)
        delta = counter_delta(samples)
        label = series.get("metric", {}).get(label_name, "total") if label_name else "total"
        result[label] = result.get(label, 0.0) + delta
    return result


def summarize_cpu(series_list, start, end):
    by_job = {}
    for series in series_list:
        samples = samples_in_window(series, start, end)
        if len(samples) < 2:
            continue
        elapsed = samples[-1][0] - samples[0][0]
        if elapsed <= 0:
            continue
        job = series.get("metric", {}).get("job", "unknown")
        by_job[job] = by_job.get(job, 0.0) + counter_delta(samples) / elapsed
    return by_job


def summarize_gauge_average(series_list, start, end, label_name):
    result = {}
    for series in series_list:
        samples = samples_in_window(series, start, end)
        if not samples:
            continue
        label = series.get("metric", {}).get(label_name, "unknown")
        average = sum(value for _, value in samples) / len(samples)
        result[label] = result.get(label, 0.0) + average
    return result


def safe_number(value):
    if math.isfinite(value):
        return value
    return None


def collect_one(result_path, output_root, prometheus_url):
    with result_path.open(encoding="utf-8") as result_file:
        benchmark = json.load(result_file)
    start = parse_time(benchmark["started_at"])
    end = parse_time(benchmark["finished_at"])
    query_start = start - 20
    query_end = end + 20
    raw = {
        name: query_range(prometheus_url, query, query_start, query_end)
        for name, query in QUERIES.items()
    }

    cpu = summarize_cpu(raw["process_cpu_seconds_total"], start, end)
    memory = summarize_gauge_average(
        raw["process_resident_memory_bytes"], start, end, "job"
    )
    region = summarize_gauge_average(raw["region_count"], start, end, "type")
    summary = {
        "tidb_cpu_average_cores": safe_number(cpu.get("tidb", 0.0)),
        "tikv_cpu_average_cores": safe_number(cpu.get("tikv", 0.0)),
        "tidb_rss_average_bytes": safe_number(memory.get("tidb", 0.0)),
        "tikv_rss_average_bytes": safe_number(memory.get("tikv", 0.0)),
        "commit_txn": summarize_counter(
            raw["commit_txn_counter"], start, end, "type"
        ),
        "async_commit_txn": summarize_counter(
            raw["async_commit_txn_counter"], start, end, "type"
        ),
        "one_pc_txn": summarize_counter(
            raw["one_pc_txn_counter"], start, end, "type"
        ),
        "transaction_duration_count": summarize_counter(
            raw["transaction_duration_count"], start, end, "txn_mode"
        ),
        "transaction_duration_seconds": summarize_counter(
            raw["transaction_duration_sum"], start, end, "txn_mode"
        ),
        "region_count_average": region,
    }

    repetition = result_path.stem.replace("repetition-", "")
    case_name = benchmark["case_name"]
    output_dir = output_root / case_name
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"repetition-{repetition}.json"
    with output_path.open("w", encoding="utf-8") as output_file:
        json.dump(
            {
                "case_name": case_name,
                "repetition": int(repetition),
                "benchmark_result": str(result_path),
                "started_at": benchmark["started_at"],
                "finished_at": benchmark["finished_at"],
                "summary": summary,
                "queries": QUERIES,
                "raw": raw,
            },
            output_file,
            indent=2,
            sort_keys=True,
        )
        output_file.write("\n")
    print(f"COLLECTED {case_name} repetition={repetition}")


def main():
    args = parse_args()
    result_paths = sorted(args.results_root.glob("**/repetition-*.json"))
    if not result_paths:
        raise SystemExit(f"no result files under {args.results_root}")
    args.output_root.mkdir(parents=True, exist_ok=True)
    for result_path in result_paths:
        collect_one(result_path, args.output_root, args.prometheus_url.rstrip("/"))


if __name__ == "__main__":
    main()

