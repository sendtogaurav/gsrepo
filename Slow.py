import time
import requests
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp

# ------------------------------------------------------------
# Context
# Your team maintains a service that fetches reference data from an external API.
# The current implementation makes one request at a time, which becomes painfully slow when the number of URLs grows.
# You are asked to analyze the bottleneck and rewrite the program to improve performance.
# ------------------------------------------------------------
def fetch_reference_data(url):
    """Blocking GET request."""
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
# 1. MULTITHREADING IMPLEMENTATION (TODO)
# ------------------------------------------------------------
def fetch_all_multithreaded(urls, max_workers=10):
    """
    TODO: Rewrite this function using ThreadPoolExecutor
    to fetch URLs concurrently.
    """















    pass


# ------------------------------------------------------------
# 3. ASYNC I/O IMPLEMENTATION (TODO)
# ------------------------------------------------------------
async def fetch_async(session, url):
    """
    TODO: Implement an async GET request using aiohttp.
    """









    pass


async def fetch_all_async(urls):
    """
    TODO: Use asyncio.gather to fetch all URLs concurrently.
    """




    pass


# ------------------------------------------------------------
# Optional - Write unit tests for testing both the approaches)
# ------------------------------------------------------------
