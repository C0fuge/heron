#!/usr/bin/env python3
"""Markdown report of one connector-comparison job (e2e/compare/run-compare.sh).

    report.py <out-dir> <reference-dir>

Each pass directory holds meta.txt, times.tsv (query round seconds status), answers-rN.tsv (compare.py summaries),
sums-rN.tsv (rows + checksum), stats.tsv (Trino rows/bytes read, peak user memory), memory.txt and explain/qNN.txt.
Round 1 is cold (fresh server + Trino); "warm" = median of rounds 2..N. Sums over all 22 queries count a failed query
with its time; the "common" sum only counts queries answered correctly by every pass, so the passes are comparable.
"""
import os
import re
import statistics
import sys


def read_tsv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        lines = [l.rstrip("\n").split("\t") for l in f if l.strip()]
    return [dict(zip(lines[0], l)) for l in lines[1:]] if lines else []


def read_meta(d):
    meta = {}
    p = os.path.join(d, "meta.txt")
    if os.path.exists(p):
        for line in open(p, encoding="utf-8"):
            k, _, v = line.rstrip("\n").partition("=")
            meta[k] = v if _ else meta.get("note", "") + line.strip()
    return meta


def mb(v):
    try:
        return f"{int(float(v)) / 1048576:.0f}"
    except (TypeError, ValueError):
        return "?"


def size_mb(s):
    """Trino DataSize string ('1.23GB', '512B', '12.5MB') -> MB."""
    m = re.fullmatch(r"([\d.]+)\s*([kMGT]?B)", str(s))
    if not m:
        return "?"
    f = {"B": 1, "kB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}[m.group(2)]
    return f"{float(m.group(1)) * f / 1e6:.0f}"


def secs(s):
    """Trino Duration string ('1.23s', '850.00ms', '2.50m') -> seconds."""
    m = re.fullmatch(r"([\d.]+)\s*(ns|us|ms|s|m|h|d)", str(s))
    if not m:
        return None
    f = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]
    return float(m.group(1)) * f


def main():
    out, ref = sys.argv[1:3]
    passes = sorted(d for d in os.listdir(out) if os.path.isfile(os.path.join(out, d, "meta.txt")))
    order = {"ours": 0, "up": 1, "ours446": 2}
    passes.sort(key=lambda p: (order.get(p, 9), p))
    queries = sorted(f[:-4] for f in os.listdir(ref) if re.fullmatch(r"q\d\d\.tsv", f))
    refsum = {r[0]: r[1:] for r in (l.rstrip("\n").split("\t") for l in open(os.path.join(out, "reference-sums.tsv")))}
    data = {}
    for p in passes:
        d = os.path.join(out, p)
        times = {}
        for r in read_tsv(os.path.join(d, "times.tsv")):
            times.setdefault(r["query"], {})[int(r["round"])] = (float(r["seconds"]), r["status"])
        rounds = sorted({rr for t in times.values() for rr in t})
        answers = {rr: {a["query"]: a for a in read_tsv(os.path.join(d, f"answers-r{rr}.tsv"))} for rr in rounds}
        sums = {}
        for rr in rounds:
            p2 = os.path.join(d, f"sums-r{rr}.tsv")
            if os.path.exists(p2):
                sums[rr] = {l.split("\t")[0]: l.rstrip("\n").split("\t")[1:] for l in open(p2) if l.strip()}
        stats = {(s["query"], int(s["round"])): s for s in read_tsv(os.path.join(d, "stats.tsv"))}
        mem = {}
        if os.path.exists(os.path.join(d, "memory.txt")):
            for line in open(os.path.join(d, "memory.txt")):
                parts = line.split()
                if len(parts) >= 2:
                    mem[parts[0]] = parts[1]
        data[p] = dict(meta=read_meta(d), times=times, rounds=rounds, answers=answers, sums=sums, stats=stats, mem=mem,
                       dir=d)

    def ok(p, q):
        a = data[p]["answers"]
        return bool(a) and all(a[rr].get(q, {}).get("result") == "OK" for rr in a)

    def cold(p, q):
        t = data[p]["times"].get(q, {}).get(1)
        return t[0] if t else None

    def warm(p, q):
        t = data[p]["times"].get(q, {})
        w = [v[0] for rr, v in t.items() if rr > 1]
        return statistics.median(w) if w else None

    def fmt(v):
        return "-" if v is None else f"{v:.2f}"

    ran = {q for p in passes for q in data[p]["times"]}
    if ran:
        queries = [q for q in queries if q in ran]   # QUERIES subset
    any_meta = next((data[p]["meta"] for p in passes), {})
    print(f"## SF{any_meta.get('sf', '?')}, server profile {any_meta.get('profile', '?')}\n")
    print("| pass | Trino | JVM | plugin zip (sha256) | note |\n|---|---|---|---|---|")
    for p in passes:
        m = data[p]["meta"]
        print(f"| {p} | {m.get('trino', '')} | {m.get('trino_jvm', '')} | {m.get('zip', '')} | {m.get('note', '')} |")
    common = [q for q in queries if all(ok(p, q) for p in passes if data[p]["rounds"])]
    print(f"\nQueries answered correctly (all rounds) by every pass that ran: {len(common)}/{len(queries)}.\n")
    print("| pass | correct (all rounds) | checksum = reference (round 1) | sum cold s | sum warm medians s | "
          "sum warm, common queries s | Trino peak MB | server peak MB |\n|---|---|---|---|---|---|---|---|")
    for p in passes:
        dd = data[p]
        if not dd["rounds"]:
            print(f"| {p} | not run | | | | | | |")
            continue
        n_ok = sum(ok(p, q) for q in queries)
        ck = sum(dd["sums"].get(1, {}).get(q, ["-", "-"])[1] == refsum.get(q, ["", "x"])[1] for q in queries)
        sc = sum(cold(p, q) or 0 for q in queries)
        sw = sum(warm(p, q) or 0 for q in queries)
        swc = sum(warm(p, q) or 0 for q in common)
        print(f"| {p} | {n_ok}/{len(queries)} | {ck}/{len(queries)} | {sc:.1f} | {sw:.1f} | {swc:.1f} | "
              f"{mb(dd['mem'].get('trino'))} | {mb(dd['mem'].get('flight1'))} |")
    print("\nPer query: cold / warm median seconds, answer (OK = all rounds equal to the reference), rows and MB the "
          "connector read from the server (Trino physical input, last round), Trino peak user memory MB.\n")
    head = "| query | ref rows | ref checksum |" + "".join(f" {p} cold | {p} warm | {p} answer | {p} rows read | "
                                                          f"{p} MB read | {p} mem MB |" for p in passes)
    print(head)
    print("|" + "---|" * (3 + 6 * len(passes)))
    for q in queries:
        row = f"| {q} | {refsum.get(q, ['?', '?'])[0]} | {refsum.get(q, ['?', '?'])[1]} |"
        for p in passes:
            dd = data[p]
            last = max(dd["rounds"]) if dd["rounds"] else 0
            st = dd["stats"].get((q, last), {})
            if ok(p, q):
                ans = "OK"
            elif not dd["rounds"]:
                ans = "-"
            else:
                bad = [dd["answers"][rr].get(q, {}).get("detail", "?") for rr in dd["answers"]
                       if dd["answers"][rr].get(q, {}).get("result") != "OK"]
                ans = "FAIL: " + (bad[0][:120].replace("|", "/") if bad else "?")
            row += (f" {fmt(cold(p, q))} | {fmt(warm(p, q))} | {ans} | {st.get('rows_read', '?')} | "
                    f"{size_mb(st.get('bytes_read'))} | {size_mb(st.get('peak_user_mem'))} |")
        print(row)
    if any("planning" in s for p in passes for s in data[p]["stats"].values()):
        print("\nTrino planning / execution time per query (warm rounds, median s; planning includes the connector's "
              "metadata and statistics calls):\n")
        print("| query |" + "".join(f" {p} planning | {p} execution |" for p in passes))
        print("|" + "---|" * (1 + 2 * len(passes)))
        for q in queries:
            row = f"| {q} |"
            for p in passes:
                dd = data[p]
                vals = {}
                for k in ("planning", "execution"):
                    v = [secs(dd["stats"][(q, rr)].get(k)) for rr in dd["rounds"] if rr > 1 and (q, rr) in dd["stats"]]
                    v = [x for x in v if x is not None]
                    vals[k] = statistics.median(v) if v else None
                row += f" {fmt(vals['planning'])} | {fmt(vals['execution'])} |"
            print(row)
    print("\n### Pushdown (EXPLAIN: table scans, i.e. what reaches the server)\n")
    for p in passes:
        ed = os.path.join(data[p]["dir"], "explain")
        if not os.path.isdir(ed):
            continue
        for f in sorted(os.listdir(ed)):
            lines = [l.strip() for l in open(os.path.join(ed, f), encoding="utf-8", errors="replace")
                     if "table = " in l or "Query failed" in l]
            print(f"- **{p} {f[:-4]}**")
            for l in lines:
                print(f"  - `{l[:600]}`")
    print()


if __name__ == "__main__":
    main()
