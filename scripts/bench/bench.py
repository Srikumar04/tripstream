#!/usr/bin/env python3
"""TripStream load and chaos harness (stdlib only).

Runs real, separate JVMs (api, splitter, N workers) against the docker-compose stack, uploads generated files and
measures the result. Scenarios:
  size    one file size at a fixed worker count          bench.py size --rows 1000000 --workers 4
  scale   same file, varying worker count               bench.py scale --rows 1000000 --workers 1,2,4,8
  chaos   kill workers mid-run + duplicate injection    bench.py chaos --rows 1000000 --workers 4
  poison  all-bad / garbage files next to a legit job   bench.py poison
  report  print the results table from the JSON file    bench.py report
Prereqs: `docker compose up -d --wait` (set POSTGRES_PORT / LOCALSTACK_PORT if the defaults clash) and a built jar.
Every scenario ends in assertions against the generator's manifest: exact good/bad counts, no loss, no double counting.
"""
import argparse
import atexit
import json
import os
import pathlib
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
JAR = ROOT / "target" / "tripstream-0.1.0-SNAPSHOT.jar"
DATA = ROOT / "data" / "generated"
LOGS = DATA / "bench-logs"
RESULTS = ROOT / "docs" / "benchmark-results.json"
API_PORT = int(os.environ.get("BENCH_API_PORT", "18080"))
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
LS_PORT = os.environ.get("LOCALSTACK_PORT", "4566")
BASE = f"http://localhost:{API_PORT}"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yml")]
ENV = dict(os.environ, DB_URL=f"jdbc:postgresql://localhost:{PG_PORT}/tripstream",
           AWS_ENDPOINT=f"http://localhost:{LS_PORT}", POSTGRES_PORT=PG_PORT, LOCALSTACK_PORT=LS_PORT)
PROCS = []


# ------------------------------------------------------------------------------------------------ infrastructure
def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, env=ENV, **kw)


def psql(sql):
    r = sh(COMPOSE + ["exec", "-T", "postgres", "psql", "-U", "tripstream", "-d", "tripstream", "-At", "-F", "|",
                      "-c", sql])
    if r.returncode != 0:
        raise RuntimeError(f"psql failed: {r.stderr.strip()}")
    return r.stdout.strip()


def awslocal(script):
    r = sh(COMPOSE + ["exec", "-T", "localstack", "bash", "-c", script])
    if r.returncode != 0:
        raise RuntimeError(f"awslocal failed: {r.stderr.strip()}")
    return r.stdout.strip()


def reset_state(visibility=None):
    """Empty tables and queues so every run starts from the same place."""
    psql("TRUNCATE trips, job_chunks, jobs, daily_vehicle_stats, processed_events")
    q = "http://localhost:4566/000000000000"
    awslocal("for q in file-events chunk-jobs aggregator-queue notifier-queue file-events-dlq chunk-jobs-dlq; do "
             f"awslocal sqs purge-queue --queue-url {q}/$q >/dev/null 2>&1; done")
    vt = visibility or 120
    awslocal(f"for q in file-events chunk-jobs; do awslocal sqs set-queue-attributes --queue-url {q}/$q "
             f"--attributes VisibilityTimeout={vt}; done")


def start_jvm(name, profile, args=(), xmx="512m"):
    LOGS.mkdir(parents=True, exist_ok=True)
    log = open(LOGS / f"{name}.log", "w")
    cmd = ["java", f"-Xmx{xmx}", "-jar", str(JAR), f"--spring.profiles.active={profile}", *args]
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=ENV, start_new_session=True)
    p.bench_name, p.bench_log = name, LOGS / f"{name}.log"
    PROCS.append(p)
    return p


def wait_started(p, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        if p.poll() is not None:
            raise RuntimeError(f"{p.bench_name} exited early, see {p.bench_log}")
        if "Started TripStreamApplication" in p.bench_log.read_text(errors="ignore"):
            return
        time.sleep(0.5)
    raise RuntimeError(f"{p.bench_name} did not start, see {p.bench_log}")


def stop(p):
    if p.poll() is None:
        p.kill()
        p.wait()
    if p in PROCS:
        PROCS.remove(p)


def stop_all():
    for p in list(PROCS):
        stop(p)


atexit.register(stop_all)


class Stack:
    """api + splitter once, workers on demand (a worker process = one ECS task)."""

    def __init__(self, chunk_rows=5000, worker_args=()):
        self.chunk_rows = chunk_rows
        self.worker_args = list(worker_args)
        ENV["DB_POOL_SIZE"] = "6"   # small pools: N JVMs x pool size must stay under Postgres max_connections (100)
        self.api = start_jvm("api", "api", [f"--server.port={API_PORT}"])
        self.splitter = start_jvm("splitter", "splitter", ["--server.port=0",
                                  f"--tripstream.splitter.chunk-rows={chunk_rows}"] + self.worker_args)
        wait_started(self.api)
        wait_started(self.splitter)
        self.workers = []
        self.seq = 0

    def add_workers(self, n, conc=1):
        new = []
        for _ in range(n):
            self.seq += 1
            ENV["DB_POOL_SIZE"] = str(conc + 2)
            new.append(start_jvm(f"worker-{self.seq}", "worker", [
                "--server.port=0", f"--tripstream.worker.concurrency={conc}", f"--tripstream.worker.poll-size={conc}",
                *self.worker_args], xmx="384m"))
        for w in new:
            wait_started(w)
        self.workers += new
        return new

    def stop_workers(self):
        for w in self.workers:
            stop(w)
        if self.workers:
            # A SIGKILLed worker leaves its SQS long poll registered at the broker for up to the poll timeout and
            # can swallow the next message (it then sits invisible until the visibility timeout). Let it expire.
            time.sleep(15)
        self.workers = []

    def close(self):
        self.stop_workers()
        stop(self.api)
        stop(self.splitter)


# ------------------------------------------------------------------------------------------------ data + jobs
def generate(rows, bad=5, dup=2, name=None, seed=42):
    DATA.mkdir(parents=True, exist_ok=True)
    default = f"trips-{rows}.csv" if (bad, dup) == (5, 2) else f"trips-{rows}-bad{bad}-dup{dup}.csv"
    csv = DATA / (name or default)
    manifest = pathlib.Path(str(csv) + ".manifest.json")
    if not (csv.exists() and manifest.exists()):
        t = time.time()
        subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_trips.py"), "--rows", str(rows),
                        "--bad-pct", str(bad), "--dup-pct", str(dup), "--seed", str(seed), "--out", str(csv)],
                       check=True)
        print(f"  generated {csv.name} ({csv.stat().st_size / 1e6:.0f} MB) in {time.time() - t:.0f}s")
    return csv, json.loads(manifest.read_text())


def http(method, url, data=None, timeout=60):
    req = urllib.request.Request(url, data=data, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def upload(csv):
    """returns (jobId, monotonic time the upload finished)"""
    _, body = http("POST", f"{BASE}/uploads")
    j = json.loads(body)
    r = subprocess.run(["curl", "-sf", "-X", "PUT", "--upload-file", str(csv), j["uploadUrl"]])
    if r.returncode != 0:
        raise RuntimeError("upload failed")
    return j["jobId"], time.time()


def job(job_id):
    return json.loads(http("GET", f"{BASE}/jobs/{job_id}")[1])


def wait_job(job_id, timeout, terminal=("COMPLETED", "FAILED")):
    end = time.time() + timeout
    while time.time() < end:
        try:
            j = job(job_id)
        except urllib.error.URLError:
            j = None
        if j and j["status"] in terminal:
            return j, time.time()
        time.sleep(0.5)
    raise TimeoutError(f"job {job_id} not finished in {timeout}s")


def chunk_ids(job_id):
    return [int(x) for x in psql(f"SELECT chunk_id FROM job_chunks WHERE job_id='{job_id}' ORDER BY 1").split()]


def chunks_done(job_id):
    return int(psql(f"SELECT count(*) FROM job_chunks WHERE job_id='{job_id}' AND status='DONE'") or 0)


def chunk_stats(job_id):
    r = psql(f"""SELECT count(*),
        percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch from finished_at - started_at)),
        percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch from finished_at - started_at)),
        percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch from finished_at - started_at)),
        sum(extract(epoch from finished_at - started_at)),
        extract(epoch from (max(finished_at) - min(started_at))),
        sum(valid_rows), max(attempts), extract(epoch from min(started_at))
        FROM job_chunks WHERE job_id='{job_id}' AND status='DONE'""").split("|")
    n, p50, p95, p99, busy, window, valid, attempts, first = r
    return dict(chunks=int(n), chunk_p50_s=round(float(p50), 2), chunk_p95_s=round(float(p95), 2),
                chunk_p99_s=round(float(p99), 2), worker_busy_s=round(float(busy), 1),
                processing_window_s=round(float(window), 1), valid_rows=int(valid), max_attempts=int(attempts),
                first_chunk_epoch=float(first))


def check_exact(job_id, final, manifest, expect_chunks=None):
    """The invariant every scenario ends on."""
    trips = int(psql(f"SELECT count(*) FROM trips WHERE job_id='{job_id}'"))
    done = int(psql(f"SELECT count(*) FROM job_chunks WHERE job_id='{job_id}' AND status='DONE'"))
    problems = []
    if final["status"] != "COMPLETED":
        problems.append(f"status {final['status']}")
    if final["totalRows"] != manifest["total_rows"]:
        problems.append(f"total {final['totalRows']} != {manifest['total_rows']}")
    if final["goodRows"] != manifest["expected_good_rows"]:
        problems.append(f"good {final['goodRows']} != {manifest['expected_good_rows']}")
    if final["badRows"] != manifest["expected_bad_rows"]:
        problems.append(f"bad {final['badRows']} != {manifest['expected_bad_rows']}")
    if trips != manifest["expected_good_rows"]:
        problems.append(f"trips table {trips} != {manifest['expected_good_rows']}")
    if expect_chunks is not None and done != expect_chunks:
        problems.append(f"chunks done {done} != {expect_chunks}")
    return problems


# ------------------------------------------------------------------------------------------------ resource sampling
class Sampler(threading.Thread):
    """Samples container and JVM CPU (in cores) every few seconds while a job runs."""

    def __init__(self, stack):
        super().__init__(daemon=True)
        self.stack, self.stop_evt, self.rows = stack, threading.Event(), []
        self.waits = {}   # "type:event" -> samples (active Postgres backends)

    def run(self):
        while not self.stop_evt.is_set():
            row = {}
            r = sh(["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}",
                    "tripstream-postgres-1", "tripstream-localstack-1"])
            for line in r.stdout.strip().splitlines():
                name, cpu = line.split("|")
                row[name.replace("tripstream-", "").replace("-1", "")] = float(cpu.strip("%")) / 100
            wpids = [str(w.pid) for w in self.stack.workers if w.poll() is None]
            if wpids:
                ps = sh(["ps", "-o", "%cpu=", "-p", ",".join(wpids)])
                row["workers"] = sum(float(x) for x in ps.stdout.split()) / 100
            ps = sh(["ps", "-o", "%cpu=", "-p", f"{self.stack.splitter.pid},{self.stack.api.pid}"])
            vals = [float(x) / 100 for x in ps.stdout.split()]
            row["splitter"] = vals[0] if vals else 0
            self.rows.append(row)
            try:
                for line in psql("SELECT coalesce(wait_event_type,'CPU') || ':' || coalesce(wait_event,'running'), count(*) "
                                 "FROM pg_stat_activity WHERE state = 'active' AND backend_type = 'client backend' "
                                 "AND pid <> pg_backend_pid() GROUP BY 1").splitlines():
                    k, n = line.split("|")
                    self.waits[k] = self.waits.get(k, 0) + int(n)
            except RuntimeError:
                pass
            self.stop_evt.wait(1)

    def summary(self):
        out = {}
        keys = {k for r in self.rows for k in r}
        for k in sorted(keys):
            xs = [r[k] for r in self.rows if k in r]
            out[f"cpu_{k}_avg"] = round(sum(xs) / len(xs), 2)
            out[f"cpu_{k}_max"] = round(max(xs), 2)
        total = sum(self.waits.values()) or 1
        out["pg_active_backend_states"] = {k: round(v / total, 2) for k, v in
                                           sorted(self.waits.items(), key=lambda kv: -kv[1])[:6]}
        return out


def save(record):
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(RESULTS.read_text()) if RESULTS.exists() else []
    record["at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    data.append(record)
    RESULTS.write_text(json.dumps(data, indent=2) + "\n")


# ------------------------------------------------------------------------------------------------ scenarios
def measure_run(stack, csv, manifest, label, timeout, extra=None):
    reset_state()
    sampler = Sampler(stack)
    sampler.start()
    job_id, t_up = upload(csv)
    final, t_done = wait_job(job_id, timeout)
    sampler.stop_evt.set()
    sampler.join(timeout=10)
    e2e = t_done - t_up
    cs = chunk_stats(job_id)
    first_latency = round(cs.pop("first_chunk_epoch") - t_up, 1)
    problems = check_exact(job_id, final, manifest, expect_chunks=cs["chunks"])
    rec = dict(scenario=label, rows=manifest["total_rows"], workers=len([w for w in stack.workers]),
               chunk_rows=stack.chunk_rows, e2e_s=round(e2e, 1), rows_per_s=round(manifest["total_rows"] / e2e),
               first_chunk_latency_s=first_latency,
               db_rows_per_s=round(cs["valid_rows"] / max(cs["processing_window_s"], 0.1)),
               worker_args=extra or [], **cs, **sampler.summary(), exact=not problems, problems=problems)
    save(rec)
    flag = "OK" if not problems else "MISMATCH " + "; ".join(problems)
    print(f"  {label}: {rec['rows']:,} rows, {rec['workers']} workers -> {rec['e2e_s']}s, "
          f"{rec['rows_per_s']:,} rows/s, chunk p50/p95/p99 {cs['chunk_p50_s']}/{cs['chunk_p95_s']}/{cs['chunk_p99_s']}s "
          f"[{flag}]")
    return rec


def timeout_for(rows):
    return max(300, rows // 1000)


def scenario_size(a):
    csv, manifest = generate(a.rows, dup=a.dup_pct)
    stack = Stack(a.chunk_rows, a.worker_arg)
    try:
        stack.add_workers(a.workers[0], a.conc)
        measure_run(stack, csv, manifest, a.label or "size", timeout_for(a.rows), a.worker_arg)
    finally:
        stack.close()


def scenario_scale(a):
    csv, manifest = generate(a.rows, dup=a.dup_pct)
    stack = Stack(a.chunk_rows, a.worker_arg)
    try:
        for n in a.workers:
            stack.stop_workers()
            stack.add_workers(n, a.conc)
            measure_run(stack, csv, manifest, a.label or "scale", timeout_for(a.rows), a.worker_arg)
    finally:
        stack.close()


def sqs_send(queue, body):
    """Unsigned SendMessage straight to LocalStack (it does not verify signatures): used to inject duplicates."""
    form = urllib.parse.urlencode({"Action": "SendMessage", "Version": "2012-11-05", "MessageBody": body}).encode()
    req = urllib.request.Request(f"http://localhost:{LS_PORT}/000000000000/{queue}", data=form, method="POST",
                                 headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/"
                                          "aws4_request, SignedHeaders=host, Signature=x",
                                          "Content-Type": "application/x-www-form-urlencoded"})
    urllib.request.urlopen(req, timeout=10).read()


def scenario_chaos(a):
    csv, manifest = generate(a.rows)
    # Short lease/visibility so a killed worker's chunks come back in seconds, not minutes (all configurable).
    timing = ["--tripstream.worker.lease=PT15S", "--tripstream.reliability.heartbeat-interval=PT5S",
              "--tripstream.reliability.visibility-timeout=PT20S"]
    stack = Stack(a.chunk_rows, timing + a.worker_arg)
    events = []
    try:
        reset_state(visibility=20)
        stack.add_workers(a.workers[0], a.conc)
        job_id, t_up = upload(csv)
        t0 = time.time()
        # wait until the splitter has enqueued everything
        while True:
            try:
                if job(job_id)["status"] in ("PROCESSING", "COMPLETED"):
                    break
            except urllib.error.URLError:
                pass
            time.sleep(0.5)
        ids = chunk_ids(job_id)
        total = len(ids)
        print(f"  job {job_id}: {total} chunks, {a.workers[0]} workers; killing 2 workers at 25% and 55%, "
              f"duplicating ~35% of chunk messages")
        stats = dict(kills=0, duplicates=0, replacements=0)
        dup_ids = set(random.Random(7).sample(ids, k=int(total * 0.35)))
        kill_points = [0.25, 0.55]
        final = None
        while True:
            done = chunks_done(job_id)
            # duplicate injection: re-send chunk messages while the job is running
            for c in list(dup_ids)[:20]:
                dup_ids.discard(c)
                sqs_send("chunk-jobs", json.dumps({"jobId": job_id, "chunkId": c, "startRow": c * a.chunk_rows + 1,
                                                   "rowCount": a.chunk_rows,
                                                   "key": f"chunks/{job_id}/{c:06d}.csv"}))
                stats["duplicates"] += 1
            if kill_points and done >= kill_points[0] * total:
                kill_points.pop(0)
                victims = random.sample([w for w in stack.workers if w.poll() is None], k=min(2, len(stack.workers)))
                for v in victims:
                    v.kill()           # SIGKILL: no ack, no claim release, heartbeats stop
                    stack.workers.remove(v)
                    stats["kills"] += 1
                events.append(f"killed {len(victims)} workers at {done}/{total} chunks done (t+{time.time() - t0:.0f}s)")
                print("  " + events[-1])
                stack.add_workers(len(victims), a.conc)   # an orchestrator replaces them
                stats["replacements"] += len(victims)
            j = job(job_id)
            if j["status"] in ("COMPLETED", "FAILED"):
                final = j
                break
            if time.time() - t0 > timeout_for(a.rows) * 2:
                raise TimeoutError("chaos run did not finish")
            time.sleep(1)
        # let late duplicate messages drain, then re-check nothing changed
        time.sleep(30)
        final = job(job_id)
        cs = chunk_stats(job_id)
        problems = check_exact(job_id, final, manifest, expect_chunks=total)
        progress = awslocal(f"awslocal dynamodb get-item --table-name tripstream-job-progress --key "
                            f"'{{\"jobId\":{{\"S\":\"{job_id}\"}}}}' --query 'Item.chunksDone.N' --output text")
        if int(progress) != total:
            problems.append(f"DynamoDB chunksDone {progress} != {total}")
        dlq = awslocal("awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/chunk-jobs-dlq "
                       "--attribute-names ApproximateNumberOfMessages --query Attributes.ApproximateNumberOfMessages "
                       "--output text")
        rec = dict(scenario=a.label or "chaos", worker_args=a.worker_arg, rows=a.rows, workers=a.workers[0], chunk_rows=a.chunk_rows, **stats,
                   e2e_s=round(time.time() - t_up, 1), max_attempts=cs["max_attempts"], chunks=total,
                   dynamodb_chunks_done=int(progress), dlq_depth=int(dlq), events=events,
                   exact=not problems, problems=problems)
        save(rec)
        print(f"  chaos: kills={stats['kills']} duplicates={stats['duplicates']} max_attempts={cs['max_attempts']} "
              f"chunksDone(DynamoDB)={progress}/{total} DLQ={dlq} -> "
              f"{'EXACT: zero loss, zero double counting' if not problems else 'FAILED: ' + '; '.join(problems)}")
    finally:
        stack.close()


def scenario_poison(a):
    legit_csv, legit_m = generate(100_000, name="trips-100000.csv")
    poison_csv, poison_m = generate(300_000, bad=100, dup=0, name="poison-allbad-300000.csv", seed=9)
    # wrong header, then garbage; and a binary blob
    wrong = DATA / "poison-wrong-header.csv"
    wrong.write_text("this,is,not,the,header\n" + "x,y,z\n" * 50_000)
    blob = DATA / "poison-binary.csv"
    blob.write_bytes(os.urandom(5_000_000))
    stack = Stack(a.chunk_rows)
    try:
        reset_state()
        stack.add_workers(a.workers[0], a.conc)
        t = time.time()
        ids = {}
        for name, f in (("poison-allbad", poison_csv), ("poison-wrong-header", wrong), ("poison-binary", blob),
                        ("legit", legit_csv)):
            ids[name], _ = upload(f)
        print(f"  uploaded 3 poison files + 1 legit job (all within {time.time() - t:.0f}s), waiting...")
        results, problems = {}, []
        for name, jid in ids.items():
            final, t_done = wait_job(jid, 900)
            results[name] = dict(final, finished_after_s=round(t_done - t, 1))
        legit_problems = check_exact(ids["legit"], results["legit"], legit_m)
        problems += [f"legit: {p}" for p in legit_problems]
        p = results["poison-allbad"]
        if not (p["status"] == "COMPLETED" and p["goodRows"] == 0 and p["badRows"] == poison_m["expected_bad_rows"]
                and p["totalRows"] == 300_000):
            problems.append(f"all-bad job unexpected: {p}")
        for n in ("poison-wrong-header", "poison-binary"):
            if results[n]["status"] != "FAILED":
                problems.append(f"{n} should FAIL cleanly but is {results[n]['status']}")
        trips_total = int(psql("SELECT count(*) FROM trips"))
        if trips_total != legit_m["expected_good_rows"]:
            problems.append(f"trips table has {trips_total}, expected only the legit job's {legit_m['expected_good_rows']}")
        dlq = awslocal("for q in file-events-dlq chunk-jobs-dlq; do awslocal sqs get-queue-attributes --queue-url "
                       "http://localhost:4566/000000000000/$q --attribute-names ApproximateNumberOfMessages "
                       "--query Attributes.ApproximateNumberOfMessages --output text; done").split()
        if any(int(x) for x in dlq):
            problems.append(f"messages parked in DLQs: {dlq}")
        rec = dict(scenario="poison", workers=a.workers[0], jobs={k: dict(status=v["status"], total=v["totalRows"],
                   good=v["goodRows"], bad=v["badRows"], finished_after_s=v["finished_after_s"])
                   for k, v in results.items()}, trips_in_db=trips_total, dlq_depths=dlq,
                   exact=not problems, problems=problems)
        save(rec)
        for k, v in rec["jobs"].items():
            print(f"    {k:22s} {v['status']:9s} total={v['total']:>7} good={v['good']:>7} bad={v['bad']:>7} "
                  f"after {v['finished_after_s']}s")
        print(f"  poison: DLQs {dlq}, trips in DB {trips_total} -> "
              f"{'OK: bad input contained, legit job exact' if not problems else 'FAILED: ' + '; '.join(problems)}")
    finally:
        stack.close()


def report(_):
    data = json.loads(RESULTS.read_text())
    print("| scenario | rows | workers | chunk rows | e2e s | rows/s | DB rows/s | chunk p50/p95/p99 s | exact |")
    print("|---|---|---|---|---|---|---|---|---|")
    for r in data:
        if r["scenario"] not in ("chaos", "poison"):
            print(f"| {r['scenario']} | {r['rows']:,} | {r['workers']} | {r['chunk_rows']} | {r['e2e_s']} | "
                  f"{r['rows_per_s']:,} | {r['db_rows_per_s']:,} | {r['chunk_p50_s']}/{r['chunk_p95_s']}/"
                  f"{r['chunk_p99_s']} | {'yes' if r['exact'] else 'NO'} |")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", choices=["size", "scale", "chaos", "poison", "report"])
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--workers", default="4", help="comma list for scale, single value otherwise")
    ap.add_argument("--chunk-rows", type=int, default=5000)
    ap.add_argument("--dup-pct", type=float, default=2, help="duplicate rows in the generated file")
    ap.add_argument("--label", default=None, help="scenario label stored with the result")
    ap.add_argument("--worker-arg", action="append", default=[], help="extra JVM arg for splitter+workers (use =)")
    ap.add_argument("--conc", type=int, default=1, help="chunks in parallel per worker process")
    a = ap.parse_args()
    a.workers = [int(x) for x in a.workers.split(",")]
    if a.scenario != "report" and not JAR.exists():
        sys.exit(f"build the jar first: mvn -DskipTests -DskipITs package ({JAR})")
    {"size": scenario_size, "scale": scenario_scale, "chaos": scenario_chaos, "poison": scenario_poison,
     "report": report}[a.scenario](a)


if __name__ == "__main__":
    main()
