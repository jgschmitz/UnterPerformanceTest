#this is called TF_Test locally for some crazy reason 
import copy
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pprint import pprint

import pymongo
from pymongo import InsertOne


# ------------------------------------------------------------------------------
# Connections
# ------------------------------------------------------------------------------

ATLAS_URI = ""
LOCAL_URI = ""

DB_NAME = "unter"

atlas_client = pymongo.MongoClient(
    ATLAS_URI,
    serverSelectionTimeoutMS=10000,
    maxPoolSize=50,
)

local_client = pymongo.MongoClient(
    LOCAL_URI,
    serverSelectionTimeoutMS=10000,
    maxPoolSize=150,
    minPoolSize=25,
    maxConnecting=20,
)

atlas_db = atlas_client[DB_NAME]
local_db = local_client[DB_NAME]

atlas_contacts = atlas_db["contacts"]
atlas_driver_summary = atlas_db["driver_summary"]

contacts = local_db["contacts"]
driver_summary = local_db["driver_summary"]


# ------------------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------------------

SAMPLE_SIZE = 50_000          # Try 50_000 first; bump to 100_000 if desired.
COPY_BATCH_SIZE = 1_000

DROP_LOCAL_COLLECTIONS_FIRST = True
COPY_DRIVER_SUMMARY_LIMIT = None  # None = copy all driver_summary docs.

WORKERS = 50
N = 10_000

contacts_projection = {
    "_id": 0,
    "contact_id": 1,
    "customer_id": 1,
    "date": 1,
    "trip.driver_id": 1,
    "trip.distance": 1,
    "trip.duration": 1,
}

COMMENT_TEXT = "local benchmark comment"

RESULTS = []


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def banner(text):
    line = "═" * (len(text) + 8)
    print("")
    print(f"╔{line}╗")
    print(f"║    {text}    ║")
    print(f"╚{line}╝")


def month_range(date_str):
    year = int(date_str[0:4])
    month = int(date_str[5:7])

    start = f"{year:04d}-{month:02d}-01"

    if month == 12:
        end = f"{year + 1:04d}-01-01"
    else:
        end = f"{year:04d}-{month + 1:02d}-01"

    return start, end


def percentile(sorted_values, p):
    if not sorted_values:
        return 0.0

    index = int(len(sorted_values) * p)

    if index >= len(sorted_values):
        index = len(sorted_values) - 1

    return sorted_values[index]


def safe_mean(values):
    if not values:
        return 0.0

    return statistics.mean(values)


def find_plan_stage(plan, stage_name):
    if isinstance(plan, dict):
        if plan.get("stage") == stage_name:
            return plan

        for value in plan.values():
            found = find_plan_stage(value, stage_name)
            if found:
                return found

    elif isinstance(plan, list):
        for item in plan:
            found = find_plan_stage(item, stage_name)
            if found:
                return found

    return None


def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


# ------------------------------------------------------------------------------
# Local data setup
# ------------------------------------------------------------------------------

def reset_local_collections():
    if not DROP_LOCAL_COLLECTIONS_FIRST:
        return

    banner("RESETTING LOCAL COLLECTIONS")

    print("Dropping local unter.contacts...")
    contacts.drop()

    print("Dropping local unter.driver_summary...")
    driver_summary.drop()

    print("Local collections reset.")


def copy_sample_contacts_to_local():
    banner(f"COPYING {SAMPLE_SIZE:,} SAMPLE CONTACTS TO LOCAL")

    print("Sampling from Atlas contacts...")

    cursor = atlas_contacts.aggregate(
        [
            {"$sample": {"size": SAMPLE_SIZE}},
        ],
        allowDiskUse=True,
    )

    copied = 0
    batch = []
    started = time.perf_counter()

    for doc in cursor:
        # Keep original _id. Since this is a sample, _id values are already unique.
        # Deepcopy keeps nested structures safe if we mutate later.
        batch.append(copy.deepcopy(doc))

        if len(batch) >= COPY_BATCH_SIZE:
            result = contacts.bulk_write(
                [InsertOne(d) for d in batch],
                ordered=False,
            )
            copied += result.inserted_count
            batch.clear()

            pct = copied / SAMPLE_SIZE
            bar_len = 30
            filled = int(bar_len * min(pct, 1.0))
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"Progress : [{bar}] {pct * 100:5.1f}% ({copied:,}/{SAMPLE_SIZE:,})")

    if batch:
        result = contacts.bulk_write(
            [InsertOne(d) for d in batch],
            ordered=False,
        )
        copied += result.inserted_count

    elapsed = time.perf_counter() - started
    print(f"Copied {copied:,} contacts to local in {elapsed:,.2f}s.")


def copy_driver_summary_to_local():
    banner("COPYING DRIVER SUMMARY TO LOCAL")

    if COPY_DRIVER_SUMMARY_LIMIT is None:
        cursor = atlas_driver_summary.find({})
        expected = atlas_driver_summary.estimated_document_count()
        print(f"Copying all driver_summary docs; estimated count: {expected:,}")
    else:
        cursor = atlas_driver_summary.find({}).limit(COPY_DRIVER_SUMMARY_LIMIT)
        expected = COPY_DRIVER_SUMMARY_LIMIT
        print(f"Copying up to {expected:,} driver_summary docs.")

    copied = 0
    batch = []
    started = time.perf_counter()

    for doc in cursor:
        batch.append(copy.deepcopy(doc))

        if len(batch) >= COPY_BATCH_SIZE:
            result = driver_summary.bulk_write(
                [InsertOne(d) for d in batch],
                ordered=False,
            )
            copied += result.inserted_count
            batch.clear()

            if expected:
                pct = copied / expected
                bar_len = 30
                filled = int(bar_len * min(pct, 1.0))
                bar = "█" * filled + "░" * (bar_len - filled)
                print(f"Progress : [{bar}] {pct * 100:5.1f}% ({copied:,}/{expected:,})")

    if batch:
        result = driver_summary.bulk_write(
            [InsertOne(d) for d in batch],
            ordered=False,
        )
        copied += result.inserted_count

    elapsed = time.perf_counter() - started
    print(f"Copied {copied:,} driver_summary docs to local in {elapsed:,.2f}s.")


def ensure_local_indexes():
    banner("CREATING LOCAL INDEXES")

    print("Creating contacts.customer_date...")
    contacts.create_index(
        [("customer_id", 1), ("date", 1)],
        name="customer_date",
        background=True,
    )

    print("Creating contacts.driver_date...")
    contacts.create_index(
        [("trip.driver_id", 1), ("date", 1)],
        name="driver_date",
        background=True,
    )

    print("Creating contacts.contact_id_unique...")
    contacts.create_index(
        [("contact_id", 1)],
        name="contact_id_unique",
        unique=True,
        background=True,
    )

    print("Creating driver_summary.rides_desc...")
    driver_summary.create_index(
        [("rides", -1)],
        name="rides_desc",
        background=True,
    )

    print("Local indexes created.")


def verify_local_counts_and_indexes():
    banner("LOCAL DATA SUMMARY")

    print(f"contacts count       : {contacts.count_documents({}):,}")
    print(f"driver_summary count : {driver_summary.count_documents({}):,}")

    print("")
    print("contacts indexes:")
    pprint(contacts.index_information())

    print("")
    print("driver_summary indexes:")
    pprint(driver_summary.index_information())


# ------------------------------------------------------------------------------
# Benchmark setup
# ------------------------------------------------------------------------------

def warm_local_connection_pool(workers=WORKERS):
    banner("WARMING LOCAL CONNECTION POOL")

    def ping_once(_):
        local_client.admin.command("ping")
        return 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(ping_once, i) for i in range(workers)]

        completed = 0
        for future in as_completed(futures):
            future.result()
            completed += 1

    print(f"Local connection pool warmed with {completed:,} concurrent pings.")


def load_local_samples():
    banner("SAMPLING LOCAL TEST RECORDS")

    loaded_samples = list(
        contacts.aggregate(
            [
                {"$sample": {"size": min(5_000, SAMPLE_SIZE)}},
                {
                    "$project": {
                        "_id": 0,
                        "contact_id": 1,
                        "customer_id": 1,
                        "date": 1,
                        "driver_id": "$trip.driver_id",
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    if not loaded_samples:
        raise RuntimeError("No local contacts found in unter.contacts")

    print(f"Loaded {len(loaded_samples):,} local sample records")
    return loaded_samples


def explain_customer_contacts():
    banner("LOCAL CUSTOMER CONTACTS EXPLAIN")

    sample = samples[0]
    start_date, end_date = month_range(sample["date"])

    explain = (
        contacts.find(
            {
                "customer_id": sample["customer_id"],
                "date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            contacts_projection,
        )
        .hint("customer_date")
        .explain()
    )

    ixscan = find_plan_stage(explain, "IXSCAN")
    fetch = find_plan_stage(explain, "FETCH")

    if ixscan:
        print("IXSCAN found       : yes")
        print(f"indexName          : {ixscan.get('indexName')}")
        print(f"keyPattern         : {ixscan.get('keyPattern')}")
    else:
        print("IXSCAN found       : no")

    print(f"FETCH found        : {'yes' if fetch else 'no'}")

    execution_stats = explain.get("executionStats", {})
    if execution_stats:
        print(f"totalKeysExamined  : {execution_stats.get('totalKeysExamined')}")
        print(f"totalDocsExamined  : {execution_stats.get('totalDocsExamined')}")
        print(f"executionTimeMillis: {execution_stats.get('executionTimeMillis')}")


def explain_driver_contacts():
    banner("LOCAL DRIVER CONTACTS EXPLAIN")

    sample = samples[0]
    start_date, end_date = month_range(sample["date"])

    explain = (
        contacts.find(
            {
                "trip.driver_id": sample["driver_id"],
                "date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            contacts_projection,
        )
        .hint("driver_date")
        .explain()
    )

    ixscan = find_plan_stage(explain, "IXSCAN")
    fetch = find_plan_stage(explain, "FETCH")

    if ixscan:
        print("IXSCAN found       : yes")
        print(f"indexName          : {ixscan.get('indexName')}")
        print(f"keyPattern         : {ixscan.get('keyPattern')}")
    else:
        print("IXSCAN found       : no")

    print(f"FETCH found        : {'yes' if fetch else 'no'}")

    execution_stats = explain.get("executionStats", {})
    if execution_stats:
        print(f"totalKeysExamined  : {execution_stats.get('totalKeysExamined')}")
        print(f"totalDocsExamined  : {execution_stats.get('totalDocsExamined')}")
        print(f"executionTimeMillis: {execution_stats.get('executionTimeMillis')}")


# ------------------------------------------------------------------------------
# Workloads run against LOCAL MongoDB
# ------------------------------------------------------------------------------

def customer_contacts(i):
    sample = samples[i % len(samples)]
    start_date, end_date = month_range(sample["date"])

    docs = list(
        contacts.find(
            {
                "customer_id": sample["customer_id"],
                "date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            contacts_projection,
        )
        .hint("customer_date")
    )

    return len(docs)


def driver_contacts(i):
    sample = samples[i % len(samples)]
    start_date, end_date = month_range(sample["date"])

    docs = list(
        contacts.find(
            {
                "trip.driver_id": sample["driver_id"],
                "date": {
                    "$gte": start_date,
                    "$lt": end_date,
                },
            },
            contacts_projection,
        )
        .hint("driver_date")
    )

    return len(docs)


def add_comment(i):
    sample = samples[i % len(samples)]

    result = contacts.update_one(
        {
            "contact_id": sample["contact_id"]
        },
        {
            "$push": {
                "comments": {
                    "ts": datetime.now(timezone.utc),
                    "text": COMMENT_TEXT,
                }
            }
        },
        hint="contact_id_unique",
    )

    return result.modified_count


def driver_averages_read(i):
    docs = list(
        driver_summary
        .find({}, {"_id": 0})
        .sort("rides", -1)
        .limit(100)
        .hint("rides_desc")
    )

    return len(docs)


# ------------------------------------------------------------------------------
# Benchmark runner
# ------------------------------------------------------------------------------

def bench(step, workload_name, fn, n=N, workers=WORKERS):
    banner(f"▶ {step} – {workload_name}")

    print(f"Ops      : {n:,}")
    print(f"Workers  : {workers:,}")
    print("Target   : local MongoDB")
    print("Status   : running...")

    start = time.perf_counter()
    total_items = 0
    total_errors = 0
    latencies = []

    per_worker = n // workers
    remainder = n % workers

    def worker(worker_id, ops_for_worker, start_index):
        worker_items = 0
        worker_errors = 0
        worker_latencies = []

        for j in range(ops_for_worker):
            i = start_index + j
            op_start = time.perf_counter()

            try:
                result = fn(i)

                if isinstance(result, int):
                    worker_items += result

            except Exception as exc:
                worker_errors += 1

                if worker_errors <= 3:
                    print("ERROR    :", repr(exc))

            op_elapsed = time.perf_counter() - op_start
            worker_latencies.append(op_elapsed)

        return worker_items, worker_errors, worker_latencies

    futures = []
    next_start_index = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for worker_id in range(workers):
            ops_for_worker = per_worker + (1 if worker_id < remainder else 0)
            start_index = next_start_index
            next_start_index += ops_for_worker

            futures.append(
                pool.submit(worker, worker_id, ops_for_worker, start_index)
            )

        completed_workers = 0

        for future in as_completed(futures):
            completed_workers += 1

            worker_items, worker_errors, worker_latencies = future.result()

            total_items += worker_items
            total_errors += worker_errors
            latencies.extend(worker_latencies)

            pct = completed_workers / workers
            bar_len = 30
            filled = int(bar_len * pct)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(f"Progress : [{bar}] {pct * 100:5.1f}%")

    elapsed = time.perf_counter() - start
    req_per_sec = n / elapsed if elapsed else 0.0
    docs_per_sec = total_items / elapsed if elapsed else 0.0
    docs_per_req = total_items / n if n else 0.0

    latencies.sort()
    mean_ms = safe_mean(latencies) * 1000
    p50_ms = percentile(latencies, 0.50) * 1000
    p95_ms = percentile(latencies, 0.95) * 1000
    p99_ms = percentile(latencies, 0.99) * 1000

    result = {
        "step": step,
        "workload": workload_name,
        "elapsed": elapsed,
        "req_per_sec": req_per_sec,
        "docs_per_sec": docs_per_sec,
        "docs_per_req": docs_per_req,
        "mean_ms": mean_ms,
        "p50_ms": p50_ms,
        "p95_ms": p95_ms,
        "p99_ms": p99_ms,
        "errors": total_errors,
    }

    RESULTS.append(result)

    print("")
    print("Result")
    print("------")
    print(f"Elapsed      : {elapsed:,.2f}s")
    print(f"Throughput   : {req_per_sec:,.0f} req/s")
    print(f"Doc rate     : {docs_per_sec:,.0f} docs/s")
    print(f"Docs/req     : {docs_per_req:,.1f}")
    print(f"Mean latency : {mean_ms:,.2f}ms")
    print(f"P50 latency  : {p50_ms:,.2f}ms")
    print(f"P95 latency  : {p95_ms:,.2f}ms")
    print(f"P99 latency  : {p99_ms:,.2f}ms")
    print(f"Errors       : {total_errors}")

    return result


# ------------------------------------------------------------------------------
# Scoreboards
# ------------------------------------------------------------------------------

def print_story_scoreboard():
    banner("LOCAL STORY PERFORMANCE SCOREBOARD")

    rows = []

    for result in RESULTS:
        step_label = f"{result['step']} – {result['workload']}"

        rows.append({
            "step": step_label,
            "metric": "Mean latency",
            "current": f"{result['mean_ms']:,.2f}ms",
        })
        rows.append({
            "step": step_label,
            "metric": "Throughput",
            "current": f"{result['req_per_sec']:,.0f} req/s",
        })
        rows.append({
            "step": step_label,
            "metric": "p99 latency",
            "current": f"{result['p99_ms']:,.2f}ms",
        })

        if result["docs_per_req"] > 1:
            rows.append({
                "step": step_label,
                "metric": "Returned docs",
                "current": f"{result['docs_per_req']:,.1f} docs/req",
            })

    print(
        f"{'Step':<36} "
        f"{'Metric':<18} "
        f"{'Local run':>18}"
    )
    print("─" * 76)

    for row in rows:
        print(
            f"{row['step']:<36} "
            f"{row['metric']:<18} "
            f"{row['current']:>18}"
        )

    print("─" * 76)
    print("Notes:")
    print("  This is a local isolation test using a sampled subset from Atlas.")
    print("  It is meant to remove client-to-Atlas network latency from the benchmark.")
    print("  It is not a production Atlas throughput claim.")
    print("  Driver averages are read from a copied driver_summary collection.")
    print("  If driver averages are maintained at write time, include that write-side maintenance cost separately.")
    print("")


def print_raw_scoreboard():
    banner("LOCAL RAW BENCHMARK DETAILS")

    print(
        f"{'Workload':<28} "
        f"{'Req/s':>10} "
        f"{'Docs/s':>10} "
        f"{'Docs/req':>10} "
        f"{'Mean ms':>10} "
        f"{'P50 ms':>10} "
        f"{'P95 ms':>10} "
        f"{'P99 ms':>10} "
        f"{'Errors':>8}"
    )
    print("─" * 112)

    for result in RESULTS:
        print(
            f"{result['workload']:<28} "
            f"{result['req_per_sec']:>10,.0f} "
            f"{result['docs_per_sec']:>10,.0f} "
            f"{result['docs_per_req']:>10,.1f} "
            f"{result['mean_ms']:>10,.2f} "
            f"{result['p50_ms']:>10,.2f} "
            f"{result['p95_ms']:>10,.2f} "
            f"{result['p99_ms']:>10,.2f} "
            f"{result['errors']:>8}"
        )

    print("─" * 112)
    print("")


# ------------------------------------------------------------------------------
# Run setup and benchmark
# ------------------------------------------------------------------------------

print("Testing Atlas connection...")
print(atlas_client.admin.command("ping"))

print("Testing local MongoDB connection...")
print(local_client.admin.command("ping"))

reset_local_collections()
copy_sample_contacts_to_local()
copy_driver_summary_to_local()
ensure_local_indexes()
verify_local_counts_and_indexes()
warm_local_connection_pool()

samples = load_local_samples()

explain_customer_contacts()
explain_driver_contacts()


banner("LOCAL WARMUP")

for i in range(100):
    customer_contacts(i)
    driver_contacts(i)
    add_comment(i)
    driver_averages_read(i)

print("Local warmup complete.")


bench(
    step="Step 2",
    workload_name="Customer contacts",
    fn=customer_contacts,
    n=N,
    workers=WORKERS,
)

bench(
    step="Step 3",
    workload_name="Driver contacts",
    fn=driver_contacts,
    n=N,
    workers=WORKERS,
)

bench(
    step="Step 4",
    workload_name="Add comment",
    fn=add_comment,
    n=N,
    workers=WORKERS,
)

bench(
    step="Step 5",
    workload_name="Driver averages",
    fn=driver_averages_read,
    n=1_000,
    workers=50,
)

print_story_scoreboard()
print_raw_scoreboard()
