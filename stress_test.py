#!/usr/bin/env python3
"""
Last Server Standing — Progressive Stress & Saturation Benchmark Tool

Ramps up request volume progressively until the server reaches its limit:
- Stage 1:  1,000 requests
- Stage 2:  5,000 requests
- Stage 3+: Multiples of 4,000 (8,000 -> 12,000 -> 16,000 -> 20,000 -> 24,000 -> ...)

Applies the official competition traffic mix:
- 60% GET /products/:id (read product)
- 15% GET /products?category=...&page=...&limit=... (category listing)
- 20% POST /orders (order placements)
-  5% GET /users/:userId/orders (order history)

Monitors throughput (RPS), p50/p95/p99 latency, and error rates.
Automatically stops when the server begins failing (5xx errors or connection timeouts).
Uses Python 3 standard library only (no pip dependencies required).
"""

import sys
import time
import json
import random
import argparse
import http.client
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

# ANSI Colors
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

CATEGORIES = ["smartphones", "laptops", "audio", "gaming", "accessories"]

def make_connection(host: str, port: int) -> http.client.HTTPConnection:
    return http.client.HTTPConnection(host, port, timeout=5)

def check_health(host: str, port: int) -> bool:
    try:
        conn = make_connection(host, port)
        conn.request("GET", "/health", headers={"Host": f"{host}:{port}"})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp.status == 200 and b'"status":"ok"' in data
    except Exception:
        return False

def seed_products(host: str, port: int, flash_count=5, regular_count=20):
    """Seed catalog with flash-sale and regular items"""
    conn = make_connection(host, port)
    flash_ids = []
    regular_ids = []
    
    timestamp = int(time.time())
    
    # Flash products (tight stock)
    for i in range(1, flash_count + 1):
        pid = f"stress_flash_{timestamp}_{i}"
        body = json.dumps({
            "id": pid,
            "name": f"Flash Drop Item #{i}",
            "category": CATEGORIES[i % len(CATEGORIES)],
            "price": 50000 + (i * 1000),
            "stock": 50000  # High enough to sustain stress tests
        })
        conn.request("POST", "/products", body=body, headers={
            "Host": f"{host}:{port}",
            "Content-Type": "application/json",
            "Content-Length": str(len(body))
        })
        resp = conn.getresponse()
        resp.read()
        if resp.status == 201:
            flash_ids.append(pid)

    # Regular products
    for i in range(1, regular_count + 1):
        pid = f"stress_reg_{timestamp}_{i}"
        body = json.dumps({
            "id": pid,
            "name": f"Catalog Item #{i}",
            "category": CATEGORIES[i % len(CATEGORIES)],
            "price": 1000 + (i * 200),
            "stock": 100000
        })
        conn.request("POST", "/products", body=body, headers={
            "Host": f"{host}:{port}",
            "Content-Type": "application/json",
            "Content-Length": str(len(body))
        })
        resp = conn.getresponse()
        resp.read()
        if resp.status == 201:
            regular_ids.append(pid)

    conn.close()
    return flash_ids, regular_ids

def run_worker_batch(host: str, port: int, req_count: int, flash_ids: list, reg_ids: list, worker_id: int):
    """Worker thread that executes a fixed slice of requests using persistent keep-alive"""
    conn = None
    latencies = []
    statuses = Counter()
    errors = 0
    all_products = flash_ids + reg_ids

    user_pool = [f"u_stress_{worker_id}_{i}" for i in range(50)]
    recent_users = []

    try:
        conn = make_connection(host, port)
    except Exception:
        errors += 1

    for _ in range(req_count):
        if conn is None:
            try:
                conn = make_connection(host, port)
            except Exception:
                errors += 1
                time.sleep(0.005)
                continue

        # 60% Read Product, 15% Category, 20% Order, 5% History
        roll = random.random()
        method = "GET"
        path = ""
        body = None

        if roll < 0.60:
            target = random.choice(flash_ids) if flash_ids and random.random() < 0.70 else random.choice(all_products)
            path = f"/products/{target}"
        elif roll < 0.75:
            cat = random.choice(CATEGORIES)
            path = f"/products?category={cat}&page={random.randint(1, 3)}&limit=20"
        elif roll < 0.95:
            method = "POST"
            path = "/orders"
            target = random.choice(flash_ids) if flash_ids and random.random() < 0.80 else random.choice(all_products)
            uid = random.choice(user_pool)
            recent_users.append(uid)
            if len(recent_users) > 30:
                recent_users.pop(0)
            body = json.dumps({"userId": uid, "productId": target})
        else:
            uid = random.choice(recent_users) if recent_users else random.choice(user_pool)
            path = f"/users/{uid}/orders"

        headers = {"Host": f"{host}:{port}"}
        if body:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))

        start = time.perf_counter()
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            resp.read()
            latencies.append((time.perf_counter() - start) * 1000.0)
            statuses[resp.status] += 1
        except Exception:
            errors += 1
            try:
                conn.close()
            except Exception:
                pass
            conn = None

    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    return latencies, statuses, errors

def run_stage(host: str, port: int, total_requests: int, concurrency: int, flash_ids: list, reg_ids: list):
    """Executes a single test stage with `total_requests` spread across `concurrency` workers"""
    per_worker = total_requests // concurrency
    remainder = total_requests % concurrency

    start_time = time.perf_counter()

    all_latencies = []
    total_statuses = Counter()
    total_errors = 0

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for w in range(concurrency):
            count = per_worker + (1 if w < remainder else 0)
            futures.append(executor.submit(run_worker_batch, host, port, count, flash_ids, reg_ids, w))

        for f in futures:
            lats, stats, errs = f.result()
            all_latencies.extend(lats)
            total_statuses.update(stats)
            total_errors += errs

    elapsed = time.perf_counter() - start_time
    total_executed = len(all_latencies)
    rps = total_executed / elapsed if elapsed > 0 else 0

    p50, p95, p99 = 0.0, 0.0, 0.0
    if all_latencies:
        all_latencies.sort()
        n = len(all_latencies)
        p50 = all_latencies[int(n * 0.50)]
        p95 = all_latencies[int(n * 0.95)]
        p99 = all_latencies[int(n * 0.99)]

    five_xx = sum(count for code, count in total_statuses.items() if code >= 500)
    error_rate = ((five_xx + total_errors) / total_requests) * 100 if total_requests > 0 else 0

    return {
        "total": total_requests,
        "executed": total_executed,
        "elapsed": elapsed,
        "rps": rps,
        "p50": p50,
        "p95": p95,
        "p99": p99,
        "statuses": total_statuses,
        "5xx": five_xx,
        "transport_errors": total_errors,
        "error_rate": error_rate
    }

def main():
    parser = argparse.ArgumentParser(description="Progressive Stress & Saturation Benchmark")
    parser.add_argument("--host", default=None, help="Target host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="Target port (default: 8080)")
    parser.add_argument("--concurrency", type=int, default=64, help="Concurrent workers (default: 64)")
    args = parser.parse_args()

    print(f"\n{BOLD}{CYAN}======================================================================{RESET}")
    print(f"{BOLD}{CYAN}      LAST SERVER STANDING — PROGRESSIVE SATURATION BENCHMARK         {RESET}")
    print(f"{BOLD}{CYAN}======================================================================{RESET}\n")

    host = args.host
    if not host:
        try:
            val = input("Enter Server Host [default: 127.0.0.1]: ").strip()
            host = val if val else "127.0.0.1"
        except (EOFError, KeyboardInterrupt):
            host = "127.0.0.1"

    port = args.port
    if not port:
        try:
            val = input("Enter Server Port [default: 8080]: ").strip()
            port = int(val) if val else 8080
        except (EOFError, KeyboardInterrupt, ValueError):
            port = 8080

    concurrency = args.concurrency

    print(f"\n==> Connecting to http://{host}:{port}/health...")
    if not check_health(host, port):
        print(f"{RED}Error: Server at http://{host}:{port}/health is not reachable!{RESET}")
        sys.exit(1)
    print(f"{GREEN}✓ Server is HEALTHY{RESET}\n")

    print("==> Seeding catalog dataset for stress testing...")
    flash_ids, reg_ids = seed_products(host, port)
    print(f"{GREEN}✓ Created {len(flash_ids)} flash products and {len(reg_ids)} catalog products.{RESET}\n")

    # Progressive test stages:
    # 1. 1,000 requests
    # 2. 5,000 requests
    # 3. 8,000 requests (then multiples of 4,000: 12k, 16k, 20k, 24k, 28k...)
    stages = [1000, 5000]
    curr = 8000
    while curr <= 80000:
        stages.append(curr)
        curr += 4000

    print(f"{BOLD}Starting progressive stress stages (Concurrency: {concurrency} workers):{RESET}")
    print(f"{'Stage':<10} | {'Requests':<10} | {'Throughput':<14} | {'p50 Lat':<10} | {'p95 Lat':<10} | {'p99 Lat':<10} | {'Error %':<8} | {'Status'}")
    print("-" * 92)

    peak_rps = 0.0
    highest_successful_reqs = 0

    for idx, req_count in enumerate(stages, 1):
        res = run_stage(host, port, req_count, concurrency, flash_ids, reg_ids)

        if res["rps"] > peak_rps:
            peak_rps = res["rps"]

        # Check failure condition: > 2% error rate or failed health check
        failed = res["error_rate"] > 2.0 or res["transport_errors"] > (req_count * 0.05)

        status_str = f"{RED}FAILED{RESET}" if failed else f"{GREEN}PASSED{RESET}"
        print(f"#{idx:<9} | {res['total']:<10,d} | {res['rps']:>8.1f} req/s | {res['p50']:>6.2f} ms | {res['p95']:>6.2f} ms | {res['p99']:>6.2f} ms | {res['error_rate']:>6.2f}% | {status_str}")

        if failed:
            print(f"\n{BOLD}{RED}==> SERVER SATURATION REACHED AT {req_count:,} REQUESTS!{RESET}")
            print(f"    Reason: Error rate was {res['error_rate']:.2f}% (5xx: {res['5xx']}, Transport Drops: {res['transport_errors']})")
            print(f"    Highest Sustained Successful Load: {BOLD}{highest_successful_reqs:,} requests{RESET}")
            print(f"    Peak Throughput Observed:          {BOLD}{GREEN}{peak_rps:,.2f} req/sec{RESET}\n")
            break
        else:
            highest_successful_reqs = req_count

        time.sleep(0.5)
    else:
        print(f"\n{BOLD}{GREEN}==> ALL STAGES PASSED! Server handled up to {highest_successful_reqs:,} requests seamlessly!{RESET}")
        print(f"    Peak Throughput Observed: {BOLD}{GREEN}{peak_rps:,.2f} req/sec{RESET}\n")

if __name__ == "__main__":
    main()
