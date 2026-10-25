#!/usr/bin/env python3
"""Turn benchmark/loadtest/run.sh's results.tsv into Markdown tables.

Rows of a case that ran several times (the interleaved A/B rounds) are
summarised by their median, so a noisy shared host moves one run rather than
the table. Run it as:

    python3 benchmark/loadtest/report.py /tmp/fib-loadtest/<time>

It prints the campaign table, the per-round rows, and the growth table.
"""

import os
import statistics
import sys

COLUMNS = [
    "case", "proto", "conc", "reqs", "rps", "ok", "failed", "errored",
    "timeout", "p50_ms", "p99_ms", "mean_ms", "max_ms", "rss_before_mib",
    "rss_after_mib", "rss_settle_mib", "gor_before", "gor_after", "gor_peak",
    "gor_settle", "fds_after", "fds_peak", "fds_settle", "openconns",
    "pending", "paused", "stall_bytes", "stall_errors", "stall_empty",
    "warnings", "load_exit",
]


def read_tsv(path):
    rows = []
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            values = line.rstrip("\n").split("\t")
            if len(values) != len(header):
                continue
            rows.append(dict(zip(header, values)))
    return rows


def num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def median(rows, key):
    values = [num(r.get(key)) for r in rows]
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def fmt(value, digits=0):
    if value is None:
        return "-"
    if digits == 0:
        return f"{value:,.0f}"
    return f"{value:.{digits}f}"


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "/tmp/fib-loadtest"
    path = os.path.join(root, "results.tsv")
    rows = read_tsv(path)
    order = []
    groups = {}
    for row in rows:
        name = row["case"]
        if name not in groups:
            groups[name] = []
            order.append(name)
        groups[name].append(row)

    print("| Case | Proto | Concurrency | req/s | mean | p99 | max | errors | RSS before -> settle |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for name in order:
        group = groups[name]
        errors = sum(int(num(r.get("failed")) or 0) + int(num(r.get("errored")) or 0)
                     + int(num(r.get("timeout")) or 0) for r in group)
        p99 = median(group, "p99_ms")
        rss_before = median(group, "rss_before_mib")
        rss_settle = median(group, "rss_settle_mib")
        print(
            f"| {name} | {group[0]['proto']} | {group[0]['conc']} | "
            f"{fmt(median(group, 'rps'))} | {fmt(median(group, 'mean_ms'), 2)}ms | "
            f"{fmt(p99, 2)}{'ms' if p99 is not None else ''} | "
            f"{fmt(median(group, 'max_ms'), 2)}ms | {errors} | "
            f"{fmt(rss_before, 1)} -> {fmt(rss_settle, 1)} MiB |"
        )

    print()
    print("Per-run rows:")
    print()
    print("| Case | run | req/s | mean | p99 | max | RSS after | RSS settle | goroutines peak -> settle | errors |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for name in order:
        for i, row in enumerate(groups[name], 1):
            errors = (int(num(row.get("failed")) or 0) + int(num(row.get("errored")) or 0)
                      + int(num(row.get("timeout")) or 0))
            p99 = num(row.get("p99_ms"))
            print(
                f"| {name} | {i} | {fmt(num(row.get('rps')))} | "
                f"{fmt(num(row.get('mean_ms')), 2)}ms | "
                f"{fmt(p99, 2)}{'ms' if p99 is not None else ''} | "
                f"{fmt(num(row.get('max_ms')), 2)}ms | {row.get('rss_after_mib')} | "
                f"{row.get('rss_settle_mib')} | {row.get('gor_peak')} -> {row.get('gor_settle')} | {errors} |"
            )

    growth = os.path.join(root, "growth.tsv")
    if os.path.exists(growth):
        print()
        print("Repeated runs against one server (5 s each):")
        print()
        rows = read_tsv(growth)
        print("| Run | RSS MiB | goroutines | FDs | OpenConns |")
        print("| --- | --- | --- | --- | --- |")
        for row in rows:
            print(f"| {row.get('run')} | {int(row.get('rss_mib', 0))} | {row.get('goroutines')} | "
                  f"{row.get('fds')} | {row.get('openconns')} |")


if __name__ == "__main__":
    main()
