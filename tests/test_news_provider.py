"""Tests for the Sprint 5j news providers.

Two layers:
  1. GoogleNewsRSSProvider — URL build, RSS XML parsing, HTTP failure
     surfaces (timeout / 4xx / 5xx / network error / malformed XML /
     empty body). Every failure must return [] so MacroAgent's prompt
     template degrades to "(no headlines)" gracefully.
  2. CachingNewsProvider — cache hit/miss accounting, TTL expiry,
     per-symbol isolation, asyncio.Lock against burst duplicates,
     stale-cache fallback when upstream returns empty.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from src.infrastructure.news_provider import (
    CachingNewsProvider,
    GoogleNewsRSSProvider,
    NewsProvider,
    build_news_provider,
    query_for_symbol,
)


# ── Sample RSS payloads ─────────────────────────────────────────────────


_GOOGLE_RSS_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Samsung Electronics - Google News</title>
    <link>https://news.google.com/</link>
    <description>Google News</description>
    <item>
      <title>Samsung beats Q4 earnings expectations on memory recovery</title>
      <link>https://example.com/1</link>
    </item>
    <item>
      <title>Memory chip prices firming as AI demand outstrips supply</title>
      <link>https://example.com/2</link>
    </item>
    <item>
      <title>Foundry rivalry with TSMC enters new phase</title>
      <link>https://example.com/3</link>
    </item>
  </channel>
</rss>
"""

_EMPTY_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>x</title></channel></rss>
"""


def _ok(text: str) -> httpx.Response:
    return httpx.Response(status_code=200, text=text)


# ════════════════════════════════════════════════════════════════════════
#                           Symbol → query map
# ════════════════════════════════════════════════════════════════════════


def test_query_for_known_symbol_returns_company_name():
    assert query_for_symbol("005930") == "Samsung Electronics"
    assert query_for_symbol("000660") == "SK Hynix"


def test_query_for_unknown_symbol_falls_back_to_bare_code():
    """Doesn't crash; gives a noisy but functional search."""
    assert query_for_symbol("999999") == "999999"


# ════════════════════════════════════════════════════════════════════════
#                       GoogleNewsRSSProvider — URL
# ════════════════════════════════════════════════════════════════════════


def test_rss_provider_builds_url_with_company_name_query():
    p = GoogleNewsRSSProvider()
    url = p.build_url("005930")
    assert url.startswith("https://news.google.com/rss/search?")
    # Spaces become +; "Samsung Electronics" should be in there
    assert "Samsung+Electronics" in url
    assert "hl=en-US" in url
    assert "ceid=US%3Aen" in url   # urlencoded `:`


def test_rss_provider_url_for_unknown_symbol_uses_bare_code():
    url = GoogleNewsRSSProvider().build_url("999999")
    assert "q=999999" in url


def test_rss_provider_locale_overrides_propagate():
    p = GoogleNewsRSSProvider(hl="ko-KR", gl="KR", ceid="KR:ko")
    url = p.build_url("005930")
    assert "hl=ko-KR" in url
    assert "gl=KR" in url
    assert "ceid=KR%3Ako" in url


# ════════════════════════════════════════════════════════════════════════
#                  GoogleNewsRSSProvider — happy parse
# ════════════════════════════════════════════════════════════════════════


async def test_rss_provider_parses_three_titles_from_sample():
    p = GoogleNewsRSSProvider()

    async def _fake_get(self, url):  # noqa: ANN001
        return _ok(_GOOGLE_RSS_SAMPLE)

    with patch.object(httpx.AsyncClient, "get", _fake_get):
        titles = await p.recent_headlines("005930")

    assert titles == [
        "Samsung beats Q4 earnings expectations on memory recovery",
        "Memory chip prices firming as AI demand outstrips supply",
        "Foundry rivalry with TSMC enters new phase",
    ]


async def test_rss_provider_caps_at_eight_headlines():
    items = "".join(
        f"<item><title>Headline number {i}</title></item>"
        for i in range(20)
    )
    big = f'<?xml version="1.0"?><rss><channel>{items}</channel></rss>'
    p = GoogleNewsRSSProvider()

    async def _fake_get(self, url):  # noqa: ANN001
        return _ok(big)

    with patch.object(httpx.AsyncClient, "get", _fake_get):
        titles = await p.recent_headlines("005930")

    assert len(titles) == 8
    assert titles[0] == "Headline number 0"
    assert titles[7] == "Headline number 7"


async def test_rss_provider_calls_url_built_for_symbol():
    """Sanity — the URL we GET must actually carry the symbol's keyword."""
    p = GoogleNewsRSSProvider()
    captured: dict[str, str] = {}

    async def _fake_get(self, url):  # noqa: ANN001
        captured["url"] = url
        return _ok(_EMPTY_RSS)

    with patch.object(httpx.AsyncClient, "get", _fake_get):
        await p.recent_headlines("000660")

    assert "SK+Hynix" in captured["url"]


# ════════════════════════════════════════════════════════════════════════
#                  GoogleNewsRSSProvider — failure surfaces
# ════════════════════════════════════════════════════════════════════════


async def test_rss_provider_timeout_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _raise(self, url):  # noqa: ANN001
        raise httpx.ConnectTimeout("simulated")
    with patch.object(httpx.AsyncClient, "get", _raise):
        assert await p.recent_headlines("005930") == []


async def test_rss_provider_network_error_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _raise(self, url):  # noqa: ANN001
        raise httpx.ConnectError("dns lookup failed")
    with patch.object(httpx.AsyncClient, "get", _raise):
        assert await p.recent_headlines("005930") == []


async def test_rss_provider_http_5xx_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _fake_get(self, url):  # noqa: ANN001
        return httpx.Response(status_code=503, text="service unavailable")
    with patch.object(httpx.AsyncClient, "get", _fake_get):
        assert await p.recent_headlines("005930") == []


async def test_rss_provider_http_4xx_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _fake_get(self, url):  # noqa: ANN001
        return httpx.Response(status_code=429, text="rate limited")
    with patch.object(httpx.AsyncClient, "get", _fake_get):
        assert await p.recent_headlines("005930") == []


async def test_rss_provider_malformed_xml_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _fake_get(self, url):  # noqa: ANN001
        return _ok("<<<not valid xml at all >>>")
    with patch.object(httpx.AsyncClient, "get", _fake_get):
        assert await p.recent_headlines("005930") == []


async def test_rss_provider_empty_body_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _fake_get(self, url):  # noqa: ANN001
        return _ok("")
    with patch.object(httpx.AsyncClient, "get", _fake_get):
        assert await p.recent_headlines("005930") == []


async def test_rss_provider_well_formed_but_no_items_returns_empty_list():
    p = GoogleNewsRSSProvider()
    async def _fake_get(self, url):  # noqa: ANN001
        return _ok(_EMPTY_RSS)
    with patch.object(httpx.AsyncClient, "get", _fake_get):
        assert await p.recent_headlines("005930") == []


def test_rss_provider_satisfies_news_provider_protocol():
    assert isinstance(GoogleNewsRSSProvider(), NewsProvider)


# ════════════════════════════════════════════════════════════════════════
#                          CachingNewsProvider
# ════════════════════════════════════════════════════════════════════════


class _CountingProvider:
    """Inner provider whose every call is recorded. Configurable per-symbol
    return values + a `should_raise` switch for failure-mode tests."""

    def __init__(
        self,
        *,
        per_symbol: dict[str, list[str]] | None = None,
        default:    list[str]                   | None = None,
        should_raise: bool                       = False,
    ) -> None:
        self._per_symbol = dict(per_symbol or {})
        self._default    = list(default) if default is not None else ["default headline"]
        self._raise      = should_raise
        self.call_count: int = 0
        self.calls:      list[str] = []

    async def recent_headlines(self, symbol: str) -> list[str]:
        self.call_count += 1
        self.calls.append(symbol)
        if self._raise:
            raise RuntimeError("simulated upstream failure")
        return list(self._per_symbol.get(symbol, self._default))


# ── Cache hit/miss accounting ──────────────────────────────────────────


async def test_cache_first_call_is_miss_and_populates():
    inner = _CountingProvider(per_symbol={"005930": ["A", "B"]})
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)
    out = await cache.recent_headlines("005930")
    assert out == ["A", "B"]
    assert inner.call_count == 1
    assert cache.miss_count == 1
    assert cache.hit_count == 0


async def test_cache_second_call_within_ttl_is_hit_no_inner_call():
    inner = _CountingProvider(per_symbol={"005930": ["A"]})
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)
    await cache.recent_headlines("005930")
    await cache.recent_headlines("005930")
    await cache.recent_headlines("005930")
    assert inner.call_count == 1
    assert cache.hit_count == 2


async def test_cache_returns_copy_not_live_list():
    """Mutating the returned list MUST NOT affect cached entry."""
    inner = _CountingProvider(per_symbol={"005930": ["A", "B"]})
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)
    first = await cache.recent_headlines("005930")
    first.append("MUTATED")
    second = await cache.recent_headlines("005930")
    assert second == ["A", "B"]


async def test_cache_per_symbol_isolation():
    inner = _CountingProvider(per_symbol={
        "005930": ["sam-1", "sam-2"],
        "000660": ["sk-1"],
    })
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)
    a = await cache.recent_headlines("005930")
    b = await cache.recent_headlines("000660")
    assert a == ["sam-1", "sam-2"]
    assert b == ["sk-1"]
    assert inner.call_count == 2
    assert cache.cached_count() == 2


# ── TTL expiry ─────────────────────────────────────────────────────────


async def test_cache_expires_after_ttl_and_refetches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Compress timeline by patching `time.monotonic` in the news_provider
    module so we don't actually have to sleep."""
    fake_now = [1000.0]
    monkeypatch.setattr(
        "src.infrastructure.news_provider.time.monotonic",
        lambda: fake_now[0],
    )

    inner = _CountingProvider(per_symbol={"005930": ["v1"]})
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)

    await cache.recent_headlines("005930")
    fake_now[0] = 1030.0  # 30s later — still fresh
    await cache.recent_headlines("005930")
    assert inner.call_count == 1, "still within TTL — should NOT refetch"

    fake_now[0] = 1100.0  # 100s after first fetch — expired
    inner._per_symbol["005930"] = ["v2"]
    out = await cache.recent_headlines("005930")
    assert inner.call_count == 2
    assert out == ["v2"]


# ── Concurrent-burst lock test ─────────────────────────────────────────


async def test_cache_concurrent_misses_for_same_symbol_fire_one_inner_call():
    """Burst of N coroutines on a cold cache must coalesce to ONE
    upstream fetch — proves the per-symbol asyncio.Lock works."""
    started = asyncio.Event()
    can_finish = asyncio.Event()
    call_count = [0]

    class _SlowInner:
        async def recent_headlines(self, symbol: str) -> list[str]:
            call_count[0] += 1
            started.set()
            await can_finish.wait()
            return ["coalesced"]

    cache = CachingNewsProvider(_SlowInner(), ttl_seconds=60.0)

    # Fire 10 coroutines simultaneously on the same symbol.
    tasks = [
        asyncio.create_task(cache.recent_headlines("005930"))
        for _ in range(10)
    ]
    await started.wait()           # first fetch is in flight, others lined up at lock
    can_finish.set()                # release the slow inner
    results = await asyncio.gather(*tasks)

    assert call_count[0] == 1, "concurrent burst must coalesce to ONE upstream fetch"
    assert all(r == ["coalesced"] for r in results)


async def test_cache_concurrent_misses_for_different_symbols_fan_out():
    """Different symbols don't share a lock — independent fetches in parallel."""
    call_log: list[str] = []

    class _RecordingInner:
        async def recent_headlines(self, symbol: str) -> list[str]:
            call_log.append(symbol)
            return [f"news-for-{symbol}"]

    cache = CachingNewsProvider(_RecordingInner(), ttl_seconds=60.0)
    symbols = ["005930", "000660", "035420"]
    results = await asyncio.gather(*[cache.recent_headlines(s) for s in symbols])
    assert sorted(call_log) == sorted(symbols)
    assert results == [["news-for-005930"], ["news-for-000660"], ["news-for-035420"]]


# ── Stale-cache fallback ───────────────────────────────────────────────


async def test_cache_returns_stale_data_when_inner_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First fetch succeeds → cache populated. TTL elapses → next call
    fires inner which now returns []. Cache should serve the stale data
    rather than empty."""
    fake_now = [1000.0]
    monkeypatch.setattr(
        "src.infrastructure.news_provider.time.monotonic",
        lambda: fake_now[0],
    )

    inner = _CountingProvider(per_symbol={"005930": ["good news"]})
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)

    first = await cache.recent_headlines("005930")
    assert first == ["good news"]

    # Make subsequent fetches return [] (simulating upstream outage).
    inner._per_symbol = {}
    inner._default = []

    fake_now[0] = 1100.0  # past TTL
    second = await cache.recent_headlines("005930")
    assert second == ["good news"], "stale cache must surface during outage"
    assert inner.call_count == 2  # we DID try the inner, it just came back empty


async def test_cache_returns_stale_data_when_inner_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive — even if a future provider impl raises (instead of
    returning [] like RSS does), cache must catch + serve stale."""
    fake_now = [1000.0]
    monkeypatch.setattr(
        "src.infrastructure.news_provider.time.monotonic",
        lambda: fake_now[0],
    )

    inner = _CountingProvider(per_symbol={"005930": ["good news"]})
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)
    await cache.recent_headlines("005930")  # populate

    inner._raise = True
    fake_now[0] = 1100.0
    out = await cache.recent_headlines("005930")
    assert out == ["good news"]


async def test_cache_no_stale_data_returns_empty_when_inner_fails():
    """First call ever, inner returns [] → cache returns []. No prior
    data to fall back to."""
    inner = _CountingProvider(default=[])
    cache = CachingNewsProvider(inner, ttl_seconds=60.0)
    out = await cache.recent_headlines("005930")
    assert out == []


# ── Construction validation ────────────────────────────────────────────


def test_cache_rejects_zero_or_negative_ttl():
    inner = _CountingProvider()
    with pytest.raises(ValueError):
        CachingNewsProvider(inner, ttl_seconds=0.0)
    with pytest.raises(ValueError):
        CachingNewsProvider(inner, ttl_seconds=-5.0)


def test_cache_satisfies_news_provider_protocol():
    inner = _CountingProvider()
    assert isinstance(CachingNewsProvider(inner, ttl_seconds=60.0), NewsProvider)


# ════════════════════════════════════════════════════════════════════════
#                              Factory
# ════════════════════════════════════════════════════════════════════════


def test_factory_none_returns_none():
    assert build_news_provider(provider="none", cache_ttl_seconds=60.0) is None


def test_factory_unknown_returns_none():
    assert build_news_provider(provider="bogus", cache_ttl_seconds=60.0) is None


def test_factory_google_rss_returns_caching_wrapper():
    p = build_news_provider(provider="google_rss", cache_ttl_seconds=300.0)
    assert isinstance(p, CachingNewsProvider)
    assert p.ttl_seconds == 300.0


def test_factory_provider_string_is_normalized():
    assert build_news_provider(provider="GOOGLE_RSS", cache_ttl_seconds=60.0) is not None
    assert build_news_provider(provider="  google_rss  ", cache_ttl_seconds=60.0) is not None


# ════════════════════════════════════════════════════════════════════════
#                      Sprint 5l — Multi-locale + Composite
# ════════════════════════════════════════════════════════════════════════

from src.infrastructure.news_provider import (   # noqa: E402
    CompositeNewsProvider,
    _parse_locale_list,
)


# ── Korean locale wiring on the underlying RSS provider ───────────────


async def test_rss_provider_korean_locale_url_uses_kr_params():
    """`news_locales=ko` configures GoogleNewsRSSProvider with KR params."""
    p = GoogleNewsRSSProvider(hl="ko-KR", gl="KR", ceid="KR:ko")
    url = p.build_url("005930")
    assert "hl=ko-KR" in url
    assert "gl=KR" in url
    assert "ceid=KR%3Ako" in url
    # Symbol→company map still used; English company name works as
    # search keyword in the Korean Google News index too.
    assert "Samsung+Electronics" in url


# ── Locale list parsing ──────────────────────────────────────────────


def test_locale_parser_single_token():
    assert _parse_locale_list("en") == ["en"]
    assert _parse_locale_list("ko") == ["ko"]


def test_locale_parser_multiple_tokens_preserve_order():
    assert _parse_locale_list("en,ko") == ["en", "ko"]
    assert _parse_locale_list("ko,en,ja") == ["ko", "en", "ja"]


def test_locale_parser_dedupes_keeping_first_occurrence():
    assert _parse_locale_list("en,ko,en,ko") == ["en", "ko"]


def test_locale_parser_normalizes_case_and_whitespace():
    assert _parse_locale_list(" KO , EN ") == ["ko", "en"]


def test_locale_parser_drops_unknown_tokens():
    assert _parse_locale_list("en,bogus,ko") == ["en", "ko"]


def test_locale_parser_empty_falls_back_to_english():
    assert _parse_locale_list("") == ["en"]
    assert _parse_locale_list("   ") == ["en"]
    assert _parse_locale_list("bogus,unknown") == ["en"]


# ── CompositeNewsProvider — happy paths ─────────────────────────────


async def test_composite_merges_results_from_all_inners():
    en = _CountingProvider(per_symbol={"005930": ["Samsung beats earnings"]})
    ko = _CountingProvider(per_symbol={"005930": ["삼성 실적 호조"]})
    composite = CompositeNewsProvider([en, ko])
    out = await composite.recent_headlines("005930")
    assert "Samsung beats earnings" in out
    assert "삼성 실적 호조" in out
    assert len(out) == 2


async def test_composite_dedupes_case_insensitive_first_seen_wins():
    """Same-case + capitalized variants of the same headline → one entry,
    keeping whichever comes first in inner-provider order."""
    a = _CountingProvider(per_symbol={"005930": ["Memory Pricing Firming"]})
    b = _CountingProvider(per_symbol={"005930": ["memory pricing firming"]})
    composite = CompositeNewsProvider([a, b])
    out = await composite.recent_headlines("005930")
    assert out == ["Memory Pricing Firming"]


async def test_composite_caps_at_max_total():
    """Even if 5 inners return 5 each, output is capped at max_total."""
    inners = [
        _CountingProvider(per_symbol={"005930": [f"src{i}-headline-{j}" for j in range(5)]})
        for i in range(5)
    ]
    composite = CompositeNewsProvider(inners, max_total=8)
    out = await composite.recent_headlines("005930")
    assert len(out) == 8


async def test_composite_calls_inners_in_parallel_not_serially():
    """Two slow inners should finish in ~one delay, not two."""
    started: list[asyncio.Event] = [asyncio.Event(), asyncio.Event()]
    can_finish = asyncio.Event()

    class _SlowInner:
        def __init__(self, idx: int) -> None:
            self._idx = idx
        async def recent_headlines(self, symbol: str) -> list[str]:
            started[self._idx].set()
            await can_finish.wait()
            return [f"slow-{self._idx}"]

    composite = CompositeNewsProvider([_SlowInner(0), _SlowInner(1)])
    task = asyncio.create_task(composite.recent_headlines("005930"))
    # Both inner calls should be in flight before either completes.
    await asyncio.wait_for(started[0].wait(), timeout=1.0)
    await asyncio.wait_for(started[1].wait(), timeout=1.0)
    can_finish.set()
    out = await task
    assert sorted(out) == ["slow-0", "slow-1"]


# ── CompositeNewsProvider — failure isolation ───────────────────────


async def test_composite_one_inner_failure_does_not_block_others():
    class _Boom:
        async def recent_headlines(self, symbol: str) -> list[str]:
            raise RuntimeError("upstream A is down")

    good = _CountingProvider(per_symbol={"005930": ["good news"]})
    composite = CompositeNewsProvider([_Boom(), good])
    out = await composite.recent_headlines("005930")
    assert out == ["good news"]
    assert composite.failure_count == 1


async def test_composite_all_inners_failing_returns_empty_not_raise():
    class _Boom:
        async def recent_headlines(self, symbol: str) -> list[str]:
            raise RuntimeError("everyone is down")
    composite = CompositeNewsProvider([_Boom(), _Boom()])
    out = await composite.recent_headlines("005930")
    assert out == []
    assert composite.failure_count == 2


async def test_composite_inners_returning_empty_dont_count_as_failures():
    a = _CountingProvider(default=[])
    b = _CountingProvider(per_symbol={"005930": ["one good"]})
    composite = CompositeNewsProvider([a, b])
    out = await composite.recent_headlines("005930")
    assert out == ["one good"]
    assert composite.failure_count == 0


# ── Construction validation ────────────────────────────────────────


def test_composite_rejects_empty_inner_list():
    with pytest.raises(ValueError):
        CompositeNewsProvider([])


def test_composite_rejects_zero_or_negative_max_total():
    inner = _CountingProvider()
    with pytest.raises(ValueError):
        CompositeNewsProvider([inner], max_total=0)


def test_composite_satisfies_news_provider_protocol():
    inner = _CountingProvider()
    assert isinstance(CompositeNewsProvider([inner]), NewsProvider)


# ── Factory + locale wiring ────────────────────────────────────────


def test_factory_single_locale_returns_caching_not_composite():
    """One locale = no need for the composite wrapper overhead."""
    p = build_news_provider(
        provider="google_rss", cache_ttl_seconds=60.0, locales="ko",
    )
    assert isinstance(p, CachingNewsProvider)


def test_factory_two_locales_returns_composite_of_two_caching():
    p = build_news_provider(
        provider="google_rss", cache_ttl_seconds=60.0, locales="en,ko",
    )
    assert isinstance(p, CompositeNewsProvider)
    assert p.inner_count == 2


def test_factory_unknown_locale_dropped_falls_back_to_english():
    """`locales='bogus'` → no valid locales → defaults to en → single Caching."""
    p = build_news_provider(
        provider="google_rss", cache_ttl_seconds=60.0, locales="bogus",
    )
    assert isinstance(p, CachingNewsProvider)


def test_factory_default_locale_is_english_backward_compat():
    """No locales arg → en, identical to pre-Sprint-5l behavior."""
    p = build_news_provider(provider="google_rss", cache_ttl_seconds=60.0)
    assert isinstance(p, CachingNewsProvider)
