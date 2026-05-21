import random
import time
from datetime import datetime, timedelta, timezone

import pymongo
from pymongo import InsertOne


# -------- MongoDB connection --------

client = pymongo.MongoClient("mongodb+srv://:@perfworkshop.tnhx6.mongodb.net/?retryWrites=true&w=majority", serverSelectionTimeoutMS=10000)
db = client["unter"]
collection = db["contacts"]

# -------- Load settings --------

TOTAL_DOCS = 5000000 
BATCH_SIZE = 1_000


# -------- Random data helpers --------

def rand_id(prefix, n, width=8):
    return f"{prefix}{n:0{width}d}"


def random_date():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return start + timedelta(
        days=random.randint(0, 364),
        seconds=random.randint(0, 86_399),
        milliseconds=random.randint(0, 999),
    )


def acceleration_events(base_dt):
    count = random.randint(10, 60)
    events = []

    lat = random.uniform(33.0, 42.0)
    lon = random.uniform(-122.5, -70.0)

    for i in range(count):
        ts = base_dt + timedelta(seconds=i * random.randint(20, 90))
        events.append({
            "ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "x": round(random.uniform(-5, 5), 3),
            "y": round(random.uniform(-5, 5), 3),
            "z": round(random.uniform(8.5, 11.5), 3),
            "lat": round(lat + random.uniform(-0.1, 0.1), 5),
            "lon": round(lon + random.uniform(-0.1, 0.1), 5),
        })

    return events


def make_contact(i):
    dt = random_date()

    driver_num = random.randint(1, 25_000)
    customer_num = random.randint(1, 400_000)

    driver_id = rand_id("drv", driver_num)
    customer_id = rand_id("cst", customer_num)

    contact_id = (
        "cnt"
        + str(int(time.time() * 1000))
        + f"{i:08d}"
        + f"{random.randint(0, 9999):04d}"
    )

    trip_length = round(random.uniform(3, 90), 1)
    distance = round(random.uniform(0.5, 60), 2)
    surge = random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0])

    fare = round(
        (
            3.75
            + distance * random.uniform(1.5, 4.0)
            + trip_length * random.uniform(0.15, 0.6)
        )
        * surge,
        2,
    )

    city = random.choice(["Atlanta", "San Francisco", "New York", "Chicago", "Seattle"])
    airport = random.choice(["ATL", "SFO", "NYC", "CHI", "SEA"])

    return {
        "contact_id": contact_id,
        "timestamp": dt.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "date": dt.strftime("%Y-%m-%d"),
        "customer_id": customer_id,
        "trip": {
            "trip_id": rand_id("trp", random.randint(1, 9_999_999), width=10),
            "vehicle_id": rand_id("veh", random.randint(1, 25_000)),
            "vehicle_type": random.choice([
                "standard",
                "premium",
                "electric",
                "suv",
                "shared",
            ]),
            "driver_id": driver_id,
            "start_location_id": f"{airport}-{random.randint(1, 9999):06d}",
            "end_location_id": f"{airport}-{random.randint(1, 9999):06d}",
            "city": city,
            "trip_length_minutes": trip_length,
            "trip_distance_miles": distance,
            "fare_amount": fare,
            "surge_multiplier": surge,
            "acceleration_events": acceleration_events(dt),
        },
        "contact": {
            "channel": random.choice([
                "web_form",
                "mobile_app",
                "phone",
                "email",
                "chat",
            ]),
            "reason": random.choice([
                "driver_compliment",
                "refund_request",
                "billing_dispute",
                "lost_item",
                "safety_concern",
                "route_issue",
                "driver_late",
            ]),
            "status": random.choice([
                "open",
                "pending",
                "resolved",
                "closed",
            ]),
            "resolution": random.choice([
                "item_returned",
                "refund_issued",
                "credit_applied",
                "no_action",
                "escalated",
                "driver_warned",
            ]),
            "response_time_minutes": round(random.uniform(1, 240), 1),
            "sentiment": random.choice([
                "very_negative",
                "negative",
                "neutral",
                "positive",
                "very_positive",
            ]),
        },
        "star_rating": random.randint(1, 5),
        "driver_rating": {
            "driver_id": driver_id,
            "rating": random.randint(1, 5),
            "driver_lifetime_avg_rating": round(random.uniform(3.2, 5.0), 1),
            "driver_total_trips": random.randint(20, 20_000),
        },
    }


# -------- Main load --------

def main():
    print("Testing connection...")
    print(client.admin.command("ping"))

    print(f"Inserting {TOTAL_DOCS:,} docs into unter.contracts")

    started = time.perf_counter()
    inserted = 0
    batch = []

    for i in range(1, TOTAL_DOCS + 1):
        batch.append(InsertOne(make_contact(i)))

        if len(batch) == BATCH_SIZE:
            result = collection.bulk_write(batch, ordered=False)
            inserted += result.inserted_count
            print(f"Inserted {inserted:,}/{TOTAL_DOCS:,}")
            batch.clear()

    if batch:
        result = collection.bulk_write(batch, ordered=False)
        inserted += result.inserted_count

    elapsed = time.perf_counter() - started

    print("")
    print(f"Done. Inserted {inserted:,} docs in {elapsed:.2f}s")
    print(f"Rate: {inserted / elapsed:,.0f} inserts/sec")
    print(f"Collection count estimate: {collection.estimated_document_count():,}")


if __name__ == "__main__":
    main()
