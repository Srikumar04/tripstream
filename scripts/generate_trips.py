#!/usr/bin/env python3
"""Synthetic trip CSV generator (stdlib only, deterministic per --seed).

Row mix for --rows N:
  bad   = round(N * bad_pct/100)  rows that fail validation (various reasons)
  dup   = round(N * dup_pct/100)  exact repeats of an earlier good row (same trip_id)
  good  = N - bad - dup           distinct valid trips
A manifest (<out>.manifest.json) records the expected outcome so pipeline tests can assert exact counts:
  expected_good_rows == distinct valid trips stored, expected_bad_rows == rows in the error report.
Duplicates are neither "bad" nor additional "good": they collapse onto an existing trip via upsert.
"""
import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone

HEADER = "trip_id,vehicle_id,driver_id,start_time,end_time,distance_km,status"
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "IN_PROGRESS"]
BAD_KINDS = ["missing_vehicle", "bad_timestamp", "end_before_start", "negative_distance",
             "non_numeric_distance", "wrong_column_count", "bad_status"]
EPOCH = datetime(2025, 1, 1, tzinfo=timezone.utc)


def fmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def good_row(rng, n):
    start = EPOCH + timedelta(seconds=rng.randrange(0, 365 * 24 * 3600))
    end = start + timedelta(minutes=rng.randrange(5, 240))
    return [f"T{n:09d}", f"V{rng.randrange(1, 501):04d}", f"D{rng.randrange(1, 2001):05d}",
            fmt(start), fmt(end), f"{rng.uniform(0.5, 300):.2f}", rng.choice(STATUSES)]


def bad_row(rng, n):
    f = good_row(rng, n)
    kind = rng.choice(BAD_KINDS)
    if kind == "missing_vehicle":
        f[1] = ""
    elif kind == "bad_timestamp":
        f[3] = "not-a-timestamp"
    elif kind == "end_before_start":
        f[3], f[4] = f[4], f[3]
    elif kind == "negative_distance":
        f[5] = "-12.50"
    elif kind == "non_numeric_distance":
        f[5] = "abc"
    elif kind == "wrong_column_count":
        f = f[:5]
    elif kind == "bad_status":
        f[6] = "UNKNOWN"
    return f


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, required=True, help="total data rows (excluding header)")
    ap.add_argument("--bad-pct", type=float, default=5.0)
    ap.add_argument("--dup-pct", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", required=True, help="output CSV path ('-' for stdout, no manifest)")
    a = ap.parse_args()

    n_bad = round(a.rows * a.bad_pct / 100)
    n_dup = round(a.rows * a.dup_pct / 100)
    n_good = a.rows - n_bad - n_dup
    if n_good < 1 or min(n_bad, n_dup) < 0:
        sys.exit("bad-pct + dup-pct must leave at least one good row")

    rng = random.Random(a.seed)
    # Decide each row's kind without materialising the file: a shuffled kind list is 1 byte/row.
    kinds = bytearray([0] * n_good + [1] * n_bad + [2] * n_dup)  # 0 good, 1 bad, 2 dup
    rng.shuffle(kinds)
    if kinds[0] != 0:  # a duplicate needs an earlier good row to copy
        i = kinds.index(0)
        kinds[0], kinds[i] = kinds[i], kinds[0]

    out = sys.stdout if a.out == "-" else open(a.out, "w", newline="")
    sample, seen_good, counter = [], 0, 0  # bounded reservoir of earlier good lines for duplicates
    out.write(HEADER + "\n")
    for k in kinds:
        counter += 1
        if k == 0:
            line = ",".join(good_row(rng, counter))
            seen_good += 1
            if len(sample) < 20000:
                sample.append(line)
            else:
                j = rng.randrange(seen_good)
                if j < 20000:
                    sample[j] = line
        elif k == 1:
            line = ",".join(bad_row(rng, counter))
        else:
            line = rng.choice(sample)
        out.write(line + "\n")
    if out is not sys.stdout:
        out.close()
        with open(a.out + ".manifest.json", "w") as m:
            json.dump({"total_rows": a.rows, "expected_good_rows": n_good, "expected_bad_rows": n_bad,
                       "duplicate_rows": n_dup, "seed": a.seed}, m, indent=2)
            m.write("\n")


if __name__ == "__main__":
    main()
