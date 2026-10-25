#!/usr/bin/env python3
"""Summarise one load-test case from the files benchmark/loadtest/run.sh wrote.

It reads the load generator's output and the server's /stats snapshots and
prints one tab-separated row, so a campaign's results stay greppable and a
table can be cut from them. See benchmark/loadtest/README.md.
"""

import argparse
import glob
import json
import os
import re
import sys


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def duration_ms(token):
    """Convert an h2load duration token (us, ms, s, m) to milliseconds."""
    m = re.match(r"^([0-9.]+)(us|ms|s|m)?$", token)
    if not m:
        return 0.0
    value = float(m.group(1))
    unit = m.group(2) or "s"
    return {
        "us": value / 1000.0,
        "ms": value,
        "s": value * 1000.0,
        "m": value * 60000.0,
    }[unit]


def parse_h2load(path):
    out = {
        "requests": 0, "rps": 0.0, "succeeded": 0, "failed": 0,
        "errored": 0, "timeout": 0, "status2xx": 0,
        "p50": None, "p99": None, "mean": 0.0, "max": 0.0,
        "bytes_per_sec": 0.0,
    }
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return out
    m = re.search(r"finished in ([\d.]+)s, ([\d.]+) req/s, ([\d.]+)([KMG]?)B/s", text)
    if m:
        out["rps"] = float(m.group(2))
        scale = {"": 1, "K": 1e3, "M": 1e6, "G": 1e9}[m.group(4)]
        out["bytes_per_sec"] = float(m.group(3)) * scale
    m = re.search(
        r"requests:\s*(\d+) total,\s*(\d+) started,\s*(\d+) done,\s*(\d+) succeeded,"
        r"\s*(\d+) failed,\s*(\d+) errored,\s*(\d+) timeout",
        text,
    )
    if m:
        out["requests"] = int(m.group(1))
        out["succeeded"] = int(m.group(4))
        out["failed"] = int(m.group(5))
        out["errored"] = int(m.group(6))
        out["timeout"] = int(m.group(7))
    m = re.search(r"status codes:\s*(\d+) 2xx", text)
    if m:
        out["status2xx"] = int(m.group(1))
    m = re.search(
        r"time for request:\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)", text
    )
    if m:
        out["min"] = duration_ms(m.group(1))
        out["max"] = duration_ms(m.group(2))
        out["mean"] = duration_ms(m.group(3))
    return out


def parse_h3(path):
    out = {
        "requests": 0, "rps": 0.0, "succeeded": 0, "failed": 0,
        "errored": 0, "timeout": 0, "status2xx": 0,
        "p50": None, "p99": None, "mean": 0.0, "max": 0.0,
        "bytes_per_sec": 0.0,
    }
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return out
    for line in text.splitlines():
        if not line.startswith("RESULT "):
            continue
        try:
            r = json.loads(line[len("RESULT "):])
        except ValueError:
            continue
        out["requests"] = r.get("requests", 0)
        out["rps"] = r.get("rps", 0.0)
        out["succeeded"] = r.get("status_2xx", 0)
        out["status2xx"] = r.get("status_2xx", 0)
        out["failed"] = r.get("errors", 0)
        out["p50"] = r.get("p50_us", 0) / 1000.0
        out["p99"] = r.get("p99_us", 0) / 1000.0
        out["mean"] = r.get("mean_us", 0) / 1000.0
        out["max"] = r.get("max_us", 0) / 1000.0
        out["stuck"] = r.get("stuck", 0)
        break
    return out


def parse_stall(directory):
    out = {"bytes": 0, "errors": 0, "empty": 0, "stuck": 0}
    for path in glob.glob(os.path.join(directory, "stall*.out")):
        parse_stall_file(path, out)
    return out


def parse_stall_file(path, out):
    try:
        text = open(path, errors="replace").read()
    except OSError:
        return
    for line in text.splitlines():
        if not line.startswith("RESULT "):
            continue
        try:
            r = json.loads(line[len("RESULT "):])
        except ValueError:
            continue
        out["bytes"] += r.get("bytes", 0)
        out["errors"] += r.get("errors", 0)
        out["empty"] += r.get("no_response", 0)


def mb(n):
    return round(n / (1 << 20), 1)


def warnings(path):
    try:
        lines = open(path, errors="replace").read().splitlines()
    except OSError:
        return 0
    pattern = re.compile(r"panic|fatal|goaway|reset|warning|error", re.I)
    return sum(1 for line in lines if pattern.search(line) and not line.startswith("{"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--name", required=True)
    ap.add_argument("--proto", required=True)
    ap.add_argument("--conc", required=True)
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--kind", choices=["h2load", "h3load"], required=True)
    ap.add_argument("--stall", action="store_true")
    ap.add_argument("--load-exit", type=int, default=0)
    args = ap.parse_args()

    d = args.dir
    before = load_json(os.path.join(d, "before.json"))
    after = load_json(os.path.join(d, "after.json"))
    settle = load_json(os.path.join(d, "settle.json"))
    if args.kind == "h2load":
        res = parse_h2load(os.path.join(d, "load.out"))
    else:
        res = parse_h3(os.path.join(d, "load.out"))
    res["duration"] = args.duration
    stall = parse_stall(d) if args.stall else {}

    def engine(snap, key):
        return snap.get("engine", {}).get(key, 0)

    fields = [
        args.name,
        args.proto,
        str(args.conc),
        str(res["requests"]),
        f'{res["rps"]:.0f}',
        str(res["succeeded"]),
        str(res["failed"]),
        str(res["errored"]),
        str(res["timeout"]),
        "" if res["p50"] is None else f'{res["p50"]:.2f}',
        "" if res["p99"] is None else f'{res["p99"]:.2f}',
        f'{res["mean"]:.2f}',
        f'{res["max"]:.2f}',
        f'{mb(before.get("rss_bytes", 0)):.1f}',
        f'{mb(after.get("rss_bytes", 0)):.1f}',
        f'{mb(settle.get("rss_bytes", after.get("rss_bytes", 0))):.1f}',
        str(before.get("goroutines", 0)),
        str(after.get("goroutines", 0)),
        str(after.get("goroutines_peak", 0)),
        str(settle.get("goroutines", after.get("goroutines", 0))),
        str(after.get("fds", 0)),
        str(after.get("fds_peak", 0)),
        str(settle.get("fds", after.get("fds", 0))),
        str(engine(after, "OpenConns")),
        str(engine(after, "PendingBytes")),
        str(engine(after, "ReadsPausedByWatermark")),
        str(stall.get("bytes", 0)),
        str(stall.get("errors", 0)),
        str(stall.get("empty", 0)),
        str(warnings(os.path.join(d, "server.log"))),
        str(args.load_exit),
    ]
    print("\t".join(fields))

    extra = ""
    if args.stall:
        extra = f' stall_bytes={stall.get("bytes", 0)} stall_errors={stall.get("errors", 0)}'
    if args.load_exit:
        extra += f" load_exit={args.load_exit}"
    pct = ""
    if res["p50"] is not None:
        pct = f' p50={res["p50"]:.2f}ms p99={res["p99"]:.2f}ms'
    print(
        f'{args.name}: {res["rps"]:.0f} req/s, {res["requests"]} done, '
        f'{res["succeeded"]} ok, {res["failed"]} failed, {res["errored"]} errored, '
        f'{res["timeout"]} timeout, mean={res["mean"]:.2f}ms max={res["max"]:.2f}ms{pct}'
        f' rss {mb(before.get("rss_bytes", 0)):.1f}->{mb(after.get("rss_bytes", 0)):.1f}'
        f'->{mb(settle.get("rss_bytes", after.get("rss_bytes", 0))):.1f}MiB '
        f'goroutines {before.get("goroutines", 0)}->{after.get("goroutines", 0)}'
        f' (peak {after.get("goroutines_peak", 0)}){extra}',
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
