import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def percentile(values, p):
    if not values:
        return None
    k = (len(values) - 1) * (p / 100)
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return round(values[int(k)], 2)
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return round(d0 + d1, 2)


def worker_loop(stop_at, url, headers, stats):
    while time.time() < stop_at:
        started = time.perf_counter()
        try:
            r = requests.get(url, headers=headers, timeout=10)
            elapsed = (time.perf_counter() - started) * 1000
            stats['lock'].acquire()
            stats['total'] += 1
            if r.status_code == 200:
                stats['successes'] += 1
                stats['latencies'].append(elapsed)
            else:
                stats['failures'] += 1
            stats['lock'].release()
        except Exception:
            elapsed = (time.perf_counter() - started) * 1000
            stats['lock'].acquire()
            stats['total'] += 1
            stats['failures'] += 1
            stats['lock'].release()


def run_load(url, token, concurrency, duration_s, extra_headers=None):
    headers = {'X-API-Version': '1'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    if extra_headers:
        headers.update(extra_headers)

    stats = {'total': 0, 'successes': 0, 'failures': 0, 'latencies': [], 'lock': threading.Lock()}
    stop_at = time.time() + duration_s

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(worker_loop, stop_at, url, headers, stats) for _ in range(concurrency)]
        # Wait for all to finish
        for f in as_completed(futures):
            pass

    lat_sorted = sorted(stats['latencies'])
    result = {
        'workers': concurrency,
        'duration_s': duration_s,
        'concurrency': concurrency,
        'total_requests': stats['total'],
        'successes': stats['successes'],
        'failures': stats['failures'],
        'p50_ms': percentile(lat_sorted, 50),
        'p95_ms': percentile(lat_sorted, 95),
        'p99_ms': percentile(lat_sorted, 99),
    }
    print(json.dumps(result))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', required=True)
    parser.add_argument('--token', default=None)
    parser.add_argument('--concurrency', type=int, default=50)
    parser.add_argument('--duration', type=int, default=30)
    args = parser.parse_args()

    run_load(args.url, args.token, args.concurrency, args.duration)
