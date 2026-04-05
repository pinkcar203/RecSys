"""Traffic simulator for the RecSys pipeline.

Generates realistic behavioral events using a Zipf distribution for item popularity
and weighted random event types (70% view, 25% click, 5% purchase).

Usage:
    python -m scripts.simulate_traffic --users 100 --items 50 --events 500 --concurrency 10
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
from datetime import datetime, timezone

import httpx
import numpy as np


EVENT_TYPES = ["view", "click", "purchase"]
EVENT_WEIGHTS = [0.70, 0.25, 0.05]

DEFAULT_INGESTION_URL = "http://localhost:8001/events"


def zipf_items(n_items: int, n_samples: int, alpha: float = 1.5) -> list[str]:
    """Generate item IDs following a Zipf distribution (popular items appear more)."""
    samples = np.random.zipf(alpha, size=n_samples)
    item_indices = np.clip(samples, 1, n_items)
    return [f"item_{int(i)}" for i in item_indices]


def generate_events(n_users: int, n_items: int, n_events: int) -> list[dict]:
    # Generate a batch of random events
    user_ids = [f"user_{i}" for i in range(1, n_users + 1)]
    item_ids = zipf_items(n_items, n_events)

    events = []
    for i in range(n_events):
        events.append({
            "user_id": random.choice(user_ids),
            "item_id": item_ids[i],
            "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    return events


async def send_event(client: httpx.AsyncClient, url: str, event: dict, stats: dict) -> None:
    # Send a single event to the ingestion service
    try:
        start = time.perf_counter()
        resp = await client.post(url, json=event)
        elapsed = time.perf_counter() - start
        stats["latencies"].append(elapsed)

        if resp.status_code == 202:
            stats["success"] += 1
        else:
            stats["errors"] += 1
    except httpx.HTTPError:
        stats["errors"] += 1


async def run(
    url: str,
    n_users: int,
    n_items: int,
    n_events: int,
    concurrency: int,
) -> None:
    # Run the traffic simulation
    print(f"Generating {n_events} events for {n_users} users and {n_items} items...")
    events = generate_events(n_users, n_items, n_events)

    stats: dict = {"success": 0, "errors": 0, "latencies": []}
    semaphore = asyncio.Semaphore(concurrency)

    async def limited_send(client: httpx.AsyncClient, event: dict) -> None:
        async with semaphore:
            await send_event(client, url, event, stats)

    print(f"Sending events to {url} (concurrency={concurrency})...")
    start = time.perf_counter()

    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [limited_send(client, event) for event in events]
        await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start

    latencies = stats["latencies"]
    if latencies:
        lat_arr = np.array(latencies) * 1000  # ms
        print(f"\n--- Results ---")
        print(f"Total events:  {n_events}")
        print(f"Successful:    {stats['success']}")
        print(f"Errors:        {stats['errors']}")
        print(f"Total time:    {elapsed:.2f}s")
        print(f"Throughput:    {n_events / elapsed:.1f} events/s")
        print(f"Latency p50:   {np.percentile(lat_arr, 50):.1f}ms")
        print(f"Latency p95:   {np.percentile(lat_arr, 95):.1f}ms")
        print(f"Latency p99:   {np.percentile(lat_arr, 99):.1f}ms")
    else:
        print("No successful requests.")


def main() -> None:
    parser = argparse.ArgumentParser(description="RecSys Traffic Simulator")
    parser.add_argument("--url", default=DEFAULT_INGESTION_URL, help="Ingestion service URL")
    parser.add_argument("--users", type=int, default=1000, help="Number of simulated users")
    parser.add_argument("--items", type=int, default=500, help="Number of simulated items")
    parser.add_argument("--events", type=int, default=500, help="Number of events to send")
    parser.add_argument("--concurrency", type=int, default=10, help="Max concurrent requests")
    args = parser.parse_args()

    asyncio.run(run(args.url, args.users, args.items, args.events, args.concurrency))


if __name__ == "__main__":
    main()
