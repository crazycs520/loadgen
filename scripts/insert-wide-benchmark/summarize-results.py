#!/usr/bin/env python3

import argparse
import csv
import json
import pathlib
import statistics


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_root", type=pathlib.Path)
    parser.add_argument("prometheus_root", type=pathlib.Path)
    parser.add_argument("output_root", type=pathlib.Path)
    return parser.parse_args()


def median(values):
    return statistics.median(values) if values else None


def load_json(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def nested_number(payload, *keys):
    value = payload
    for key in keys:
        value = value.get(key, {})
    return value if isinstance(value, (int, float)) else 0.0


def summarize_case(case_name, results, metrics):
    durations = [result["duration_seconds"] for result in results]
    rows_per_second = [result["rows_per_second"] for result in results]
    p95 = [result["transaction_latency_ms"]["p95"] for result in results]
    p99 = [result["transaction_latency_ms"]["p99"] for result in results]
    summary = {
        "case_name": case_name,
        "repetitions": len(results),
        "duration_median_seconds": median(durations),
        "duration_min_seconds": min(durations),
        "duration_max_seconds": max(durations),
        "duration_spread_percent": (
            (max(durations) - min(durations)) / median(durations) * 100
        ),
        "rows_per_second_median": median(rows_per_second),
        "transaction_p95_median_ms": median(p95),
        "transaction_p99_median_ms": median(p99),
        "protocol": results[0]["protocol"],
        "txn_mode": results[0]["txn_mode"],
        "txn_statements": results[0]["txn_statements"],
        "batch_size": results[0]["batch_size"],
        "threads": results[0]["threads"],
        "key_mode": results[0]["key_mode"],
        "session_variables": results[0]["session_variables"],
    }
    if metrics:
        summary.update(
            {
                "tidb_cpu_average_cores_median": median(
                    [
                        nested_number(
                            metric, "summary", "tidb_cpu_average_cores"
                        )
                        for metric in metrics
                    ]
                ),
                "tikv_cpu_average_cores_median": median(
                    [
                        nested_number(
                            metric, "summary", "tikv_cpu_average_cores"
                        )
                        for metric in metrics
                    ]
                ),
                "commit_2pc_ok_median": median(
                    [
                        nested_number(metric, "summary", "commit_txn", "ok")
                        for metric in metrics
                    ]
                ),
                "commit_async_ok_median": median(
                    [
                        nested_number(
                            metric, "summary", "async_commit_txn", "ok"
                        )
                        for metric in metrics
                    ]
                ),
                "commit_one_pc_ok_median": median(
                    [
                        nested_number(metric, "summary", "one_pc_txn", "ok")
                        for metric in metrics
                    ]
                ),
                "commit_one_pc_fallback_median": median(
                    [
                        nested_number(
                            metric, "summary", "one_pc_txn", "fallback"
                        )
                        for metric in metrics
                    ]
                ),
            }
        )
    return summary


def main():
    args = parse_args()
    result_paths = sorted(args.results_root.glob("**/repetition-*.json"))
    if not result_paths:
        raise SystemExit(f"no result files under {args.results_root}")

    results_by_case = {}
    for path in result_paths:
        payload = load_json(path)
        results_by_case.setdefault(payload["case_name"], []).append(payload)

    metrics_by_case = {}
    for path in sorted(args.prometheus_root.glob("**/repetition-*.json")):
        payload = load_json(path)
        metrics_by_case.setdefault(payload["case_name"], []).append(payload)

    summaries = [
        summarize_case(
            case_name,
            results_by_case[case_name],
            metrics_by_case.get(case_name, []),
        )
        for case_name in sorted(results_by_case)
    ]
    args.output_root.mkdir(parents=True, exist_ok=True)
    with (args.output_root / "results.json").open("w", encoding="utf-8") as output:
        json.dump(summaries, output, indent=2, sort_keys=True)
        output.write("\n")

    fieldnames = []
    for summary in summaries:
        for key in summary:
            if key not in fieldnames:
                fieldnames.append(key)
    with (args.output_root / "results.csv").open(
        "w", encoding="utf-8", newline=""
    ) as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summaries)
    print(f"WROTE {len(summaries)} case summaries")


if __name__ == "__main__":
    main()
