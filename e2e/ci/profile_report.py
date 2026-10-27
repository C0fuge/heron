"""Throughput profile reports for e2e/ci/profile.sh (STATUS Stage 2: plugin throughput). Diagnostics only.

usage:
  python profile_report.py stats <dir>     # <dir>/<query>.json = Trino /v1/query/<id> -> per-query time breakdown (TSV)
  python profile_report.py jfr <file.json> # `jfr print --json --events jdk.ExecutionSample` -> where CPU samples go
"""
import json
import os
import re
import sys
from collections import Counter

UNITS = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
SIZES = {"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}


def seconds(value):
    m = re.fullmatch(r"([0-9.]+)\s*([a-z]+)", str(value or "0ns"))
    return float(m.group(1)) * UNITS[m.group(2)] if m else 0.0


def megabytes(value):
    m = re.fullmatch(r"([0-9.]+)\s*([kMGT]?B)", str(value or "0B"))
    return float(m.group(1)) * SIZES[m.group(2)] / 1e6 if m else 0.0


def stats(directory):
    """One line per query: elapsed, CPU, the plugin's read time (readNextPage: Flight read + Arrow->Block), the scan
    operators' blocked time (waiting for a batch), rows/MB read and the share of the elapsed time spent in the plugin."""
    print("query\telapsed_s\tcpu_s\tplugin_read_s\tscan_blocked_s\tscan_output_wall_s\tall_ops_wall_s\trows\tmb\tread_share")
    totals = Counter()
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(directory, name)) as f:
            info = json.load(f)
        qs = info.get("queryStats", {})
        ops = qs.get("operatorSummaries", [])
        scans = [o for o in ops if o.get("operatorType") in ("TableScanOperator", "ScanFilterAndProjectOperator")]
        row = {
            "elapsed_s": seconds(qs.get("elapsedTime")),
            "cpu_s": seconds(qs.get("totalCpuTime")),
            "plugin_read_s": seconds(qs.get("physicalInputReadTime")),
            "scan_blocked_s": sum(seconds(o.get("blockedWall")) for o in scans),
            "scan_output_wall_s": sum(seconds(o.get("getOutputWall")) for o in scans),
            "all_ops_wall_s": sum(seconds(o.get(k)) for o in ops for k in ("addInputWall", "getOutputWall", "finishWall")),
            "rows": qs.get("physicalInputPositions", 0),
            "mb": megabytes(qs.get("physicalInputDataSize")),
        }
        totals.update(row)
        share = row["plugin_read_s"] / row["all_ops_wall_s"] if row["all_ops_wall_s"] else 0
        print(name[:-5] + "\t" + "\t".join(f"{row[k]:.2f}" if isinstance(row[k], float) else str(row[k]) for k in row)
              + f"\t{share:.2f}")
    share = totals["plugin_read_s"] / totals["all_ops_wall_s"] if totals["all_ops_wall_s"] else 0
    print("TOTAL\t" + "\t".join(f"{totals[k]:.2f}" if isinstance(totals[k], float) else str(totals[k])
                                for k in ("elapsed_s", "cpu_s", "plugin_read_s", "scan_blocked_s", "scan_output_wall_s",
                                          "all_ops_wall_s", "rows", "mb")) + f"\t{share:.2f}")


# First matching rule (by inclusive frame, innermost first) names the sample's category.
CATEGORIES = [
    ("plugin: Arrow->Block conversion", ("ArrowTypeConverter.",)),
    ("plugin: aggregation merge/finish", ("AggregationReader",)),
    ("plugin: expression evaluation", ("AbstractArrowFlightPageSource.evaluate",)),
    ("arrow: IPC decode / vector load", ("org.apache.arrow.vector.", "org.apache.arrow.flight.ArrowMessage",
                                         "org.apache.arrow.flight.FlightStream")),
    ("grpc/netty: network + protobuf", ("io.grpc.", "io.netty.", "org.apache.arrow.flight.grpc.")),
    ("plugin: other", ("pro.surpin.",)),
    ("trino: operators", ("io.trino.operator.",)),
    ("trino: other", ("io.trino.",)),
    ("jvm/other", ("",)),
]


def jfr(path):
    with open(path) as f:
        events = json.load(f)["recording"]["events"]
    by_category, self_top, plugin_incl, threads = Counter(), Counter(), Counter(), Counter()
    for event in events:
        values = event.get("values", {})
        frames = [(fr.get("method", {}).get("type", {}).get("name", "?") + "." + fr.get("method", {}).get("name", "?"))
                  for fr in (values.get("stackTrace") or {}).get("frames", [])]
        if not frames:
            continue
        thread = (values.get("sampledThread") or {}).get("javaName") or "?"
        threads[re.sub(r"[-#]?\d+", "#", thread)] += 1
        self_top[frames[0]] += 1
        for frame in set(frames):
            if frame.startswith("pro.surpin."):
                plugin_incl[frame] += 1
        for category, prefixes in CATEGORIES:
            if any(p in fr for fr in frames for p in prefixes):
                by_category[category] += 1
                break
    total = sum(by_category.values()) or 1
    print(f"samples\t{total}")
    print("--- category (inclusive, first match)")
    for category, _ in CATEGORIES:
        print(f"{by_category[category] * 100 / total:6.1f}%\t{category}")
    print("--- threads")
    for name, n in threads.most_common(12):
        print(f"{n * 100 / total:6.1f}%\t{name}")
    print("--- top self frames")
    for name, n in self_top.most_common(30):
        print(f"{n * 100 / total:6.1f}%\t{name}")
    print("--- plugin methods (inclusive)")
    for name, n in plugin_incl.most_common(25):
        print(f"{n * 100 / total:6.1f}%\t{name}")


if __name__ == "__main__":
    {"stats": stats, "jfr": jfr}[sys.argv[1]](sys.argv[2])
