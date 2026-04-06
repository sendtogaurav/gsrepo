# ============================================================
# Coding Test Answer: Sequential vs Multithreading vs Async I/O
# ============================================================

import time
import requests
from concurrent.futures import ThreadPoolExecutor

import asyncio
import aiohttp


# ------------------------------------------------------------
# 1. SEQUENTIAL IMPLEMENTATION (Baseline)
# ------------------------------------------------------------
def fetch_reference_data(url):
    """Blocking GET request using requests."""
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def fetch_all_sequential(urls):
    """Fetch each URL one-by-one."""
    results = []
    for url in urls:
        results.append(fetch_reference_data(url))
    return results


# ------------------------------------------------------------
# 2. MULTITHREADING IMPLEMENTATION
# ------------------------------------------------------------
def fetch_all_multithreaded(urls, max_workers=10):
    """
    Improve performance using ThreadPoolExecutor.
    Each thread performs a blocking requests.get call.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(fetch_reference_data, urls))


# ------------------------------------------------------------
# 3. ASYNC I/O IMPLEMENTATION
# ------------------------------------------------------------
async def fetch_async(session, url):
    """Async GET request using aiohttp."""
    async with session.get(url) as resp:
        return await resp.json()


async def fetch_all_async(urls):
    """Fetch all URLs concurrently using asyncio + aiohttp."""
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_async(session, url) for url in urls]
        return await asyncio.gather(*tasks)


# ------------------------------------------------------------
# DRIVER
# ------------------------------------------------------------
def main(urls):
    print("\n--- Sequential ---")
    start = time.time()
    seq_results = fetch_all_sequential(urls)
    print(f"Sequential took {time.time() - start:.2f}s")

    print("\n--- Multithreading ---")
    start = time.time()
    mt_results = fetch_all_multithreaded(urls)
    print(f"Multithreading took {time.time() - start:.2f}s")

    print("\n--- Async I/O ---")
    start = time.time()
    async_results = asyncio.run(fetch_all_async(urls))
    print(f"Async I/O took {time.time() - start:.2f}s")

    return seq_results, mt_results, async_results


# Example usage (not executed automatically):
# urls = [
#     "https://jsonplaceholder.typicode.com/todos/1",
#     "https://jsonplaceholder.typicode.com/todos/2",
#     "https://jsonplaceholder.typicode.com/todos/3",
# ]
# main(urls)