"""Outbound rate limiting."""

import asyncio

import pytest

from nautilus_ctrader.common.rate_limit import RateLimiter, TokenBucket


async def test_a_fresh_bucket_allows_a_burst_up_to_capacity() -> None:
    bucket = TokenBucket(rate_per_sec=10.0, capacity=3.0)
    start = asyncio.get_running_loop().time()
    for _ in range(3):
        await bucket.acquire()
    assert asyncio.get_running_loop().time() - start < 0.05


async def test_acquiring_beyond_capacity_waits_for_a_refill() -> None:
    bucket = TokenBucket(rate_per_sec=20.0, capacity=1.0)
    await bucket.acquire()

    start = asyncio.get_running_loop().time()
    await bucket.acquire()
    elapsed = asyncio.get_running_loop().time() - start
    assert elapsed >= 0.04  # one token at 20/s is 50 ms, allowing for timer slack


async def test_pause_for_blocks_the_bucket() -> None:
    bucket = TokenBucket(rate_per_sec=1000.0, capacity=10.0)
    bucket.pause_for(0.1)

    start = asyncio.get_running_loop().time()
    await bucket.acquire()
    assert asyncio.get_running_loop().time() - start >= 0.09


async def test_pause_for_never_shortens_an_existing_pause() -> None:
    bucket = TokenBucket(rate_per_sec=1000.0, capacity=10.0)
    bucket.pause_for(0.2)
    bucket.pause_for(0.01)

    start = asyncio.get_running_loop().time()
    await bucket.acquire()
    assert asyncio.get_running_loop().time() - start >= 0.15


async def test_a_pause_resumes_with_one_token_not_a_burst() -> None:
    # A burst right after a venue-ordered pause could re-trigger the block.
    loop = asyncio.get_running_loop()
    bucket = TokenBucket(rate_per_sec=20.0, capacity=10.0)
    for _ in range(10):
        await bucket.acquire()

    start = loop.time()
    bucket.pause_for(0.1)
    await bucket.acquire()
    first = loop.time() - start
    await bucket.acquire()
    second = loop.time() - start

    # Measured from the pause, not from the first acquire, so a late wake-up cannot skew it.
    assert 0.1 <= first < 0.15
    assert 0.149 <= second < 0.5  # the next token at 20/s is 50 ms after the pause ends


def test_a_non_positive_rate_is_rejected() -> None:
    with pytest.raises(ValueError, match="positive"):
        TokenBucket(rate_per_sec=0.0)


async def test_buckets_are_independent() -> None:
    limiter = RateLimiter({"default": 1000.0, "historical": 1000.0})
    limiter.pause("historical", 0.1)

    start = asyncio.get_running_loop().time()
    await limiter.acquire("default")
    assert asyncio.get_running_loop().time() - start < 0.05


async def test_an_unknown_bucket_is_a_programming_error() -> None:
    limiter = RateLimiter({"default": 10.0})
    with pytest.raises(KeyError):
        await limiter.acquire("nope")
