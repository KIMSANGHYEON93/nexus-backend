"""SecuritiesRepository — asyncpg read surface for security_master + relations.

Three readers:
  • `list_securities` — filtered scan for `/v1/securities`. GIN-aware on
    `aliases` so `search=삼성` lights up Samsung Electronics on a single
    indexed lookup.
  • `get_security` — single-ticker fetch for `/v1/securities/{ticker}`.
  • `list_relations` — edge fetch for `/v1/securities/relations`.

Plus a fourth, `get_by_ticker_batch`, used by the existing snapshot/alarm
routes to enrich their payload with `display_name` / `sector` without
slapping a JOIN onto the existing repos (which would mix snapshot-domain
SQL with securities-domain SQL — bad fence).

Fault-tolerance policy (matches MarketRepository / ExecutionRepository):
  • DB / driver / OS errors collapse to empty results + warning log.
  • Domain mapping errors (bad row shape) raise ValueError so the router
    surfaces a 500 — that's a programmer error, not a transient outage.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg

from ..domain.securities.models import Market, RelationKind, Security, SecurityRelation

logger = logging.getLogger(__name__)


# ── Column projection — keep in lockstep with migration 003 ──────────────
# Listed once and reused so a column add/rename touches one constant.
_SECURITY_COLUMNS = """
    ticker, name_ko, name_en, aliases, market, sector, sector_label,
    currency, shares_outstanding, market_cap, is_subscribed, data_source,
    updated_at
"""

_RELATION_COLUMNS = """
    from_ticker, to_ticker, kind, weight, directed, evidence, updated_at
"""


class SecuritiesRepository:
    """Read-only repo for security_master + security_relation."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── /v1/securities — filtered list ─────────────────────────────────
    async def list_securities(
        self,
        markets:  list[str] | None = None,
        sectors:  list[str] | None = None,
        search:   str | None       = None,
        limit:    int               = 500,
    ) -> list[Security]:
        """Filtered scan. `search` matches ticker prefix OR name_ko prefix
        OR name_en prefix OR aliases GIN-overlap (case-insensitive).

        Empty list on DB error so the canvas renders an empty-state label
        rather than 500-ing.
        """
        if limit < 1:
            return []

        clauses: list[str] = []
        params:  list[Any] = []

        if markets:
            params.append(markets)
            clauses.append(f"market = ANY(${len(params)})")
        if sectors:
            params.append(sectors)
            clauses.append(f"sector = ANY(${len(params)})")
        if search:
            # Case-insensitive prefix on ticker / name_ko / name_en + GIN
            # overlap on aliases. The OR plan picks one of:
            #   idx_sec_aliases (GIN @>)         — strongest, alias match
            #   ticker primary index (ILIKE)
            #   seq scan                          — only on the residual.
            # We DON'T lowercase the alias column itself (it would defeat
            # the GIN index); aliases in the seed already include both
            # casings ("Samsung", "SAMSUNG"). For ticker prefix we ILIKE
            # since the column is text not citext.
            params.append(f"{search}%")
            pat_idx = len(params)
            params.append(f"%{search.lower()}%")
            sub_idx = len(params)
            params.append([search, search.lower(), search.upper()])
            ali_idx = len(params)
            clauses.append(
                f"(ticker ILIKE ${pat_idx} "
                f"OR LOWER(COALESCE(name_ko, '')) LIKE ${sub_idx} "
                f"OR LOWER(COALESCE(name_en, '')) LIKE ${sub_idx} "
                f"OR aliases && ${ali_idx}::TEXT[])"
            )

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        limit_idx = len(params)

        sql = f"""
            SELECT {_SECURITY_COLUMNS}
              FROM security_master
              {where}
             ORDER BY ticker ASC
             LIMIT ${limit_idx}
        """

        try:
            rows = await self._pool.fetch(sql, *params)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "securities_repo.list_failed",
                extra={
                    "event":      "securities_repo_list_failed",
                    "error_type": type(exc).__name__,
                    "error":      str(exc)[:200],
                },
            )
            return []

        return [self._row_to_security(r) for r in rows]

    async def count_securities(
        self,
        markets:  list[str] | None = None,
        sectors:  list[str] | None = None,
        search:   str | None       = None,
    ) -> int:
        """Total count matching the same filter set as `list_securities`.

        Powers the `SecurityListDTO.total` field — frontend uses it to
        render the "SHOWING 200 OF {total}" footer without paging through
        the whole universe.
        """
        clauses: list[str] = []
        params:  list[Any] = []

        if markets:
            params.append(markets)
            clauses.append(f"market = ANY(${len(params)})")
        if sectors:
            params.append(sectors)
            clauses.append(f"sector = ANY(${len(params)})")
        if search:
            params.append(f"{search}%")
            pat_idx = len(params)
            params.append(f"%{search.lower()}%")
            sub_idx = len(params)
            params.append([search, search.lower(), search.upper()])
            ali_idx = len(params)
            clauses.append(
                f"(ticker ILIKE ${pat_idx} "
                f"OR LOWER(COALESCE(name_ko, '')) LIKE ${sub_idx} "
                f"OR LOWER(COALESCE(name_en, '')) LIKE ${sub_idx} "
                f"OR aliases && ${ali_idx}::TEXT[])"
            )

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"SELECT COUNT(*)::BIGINT AS n FROM security_master {where}"

        try:
            row = await self._pool.fetchrow(sql, *params)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "securities_repo.count_failed",
                extra={
                    "event":      "securities_repo_count_failed",
                    "error_type": type(exc).__name__,
                },
            )
            return 0
        return int(row["n"]) if row else 0

    # ── /v1/securities/{ticker} — single lookup ────────────────────────
    async def get_security(self, ticker: str) -> Security | None:
        """Return one security or None when the ticker is unknown.

        The router maps None → 404 ProblemDetail (per spec §2 error table);
        a DB-side error also returns None and logs.
        """
        if not ticker:
            return None
        try:
            row = await self._pool.fetchrow(
                f"SELECT {_SECURITY_COLUMNS} FROM security_master WHERE ticker = $1",
                ticker,
            )
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "securities_repo.get_failed",
                extra={
                    "event":      "securities_repo_get_failed",
                    "ticker":     ticker,
                    "error_type": type(exc).__name__,
                },
            )
            return None
        if row is None:
            return None
        return self._row_to_security(row)

    # ── /v1/snapshot, /v1/alarms enrichment ────────────────────────────
    async def get_by_ticker_batch(
        self,
        tickers: list[str],
    ) -> dict[str, Security]:
        """Batch fetch by ticker — used by `/v1/snapshot` to enrich
        EntityDTO and `/v1/alarms` to populate `entity_display`.

        Returns a dict keyed by ticker so the caller does one lookup per
        entity/alarm row without re-scanning the list. Missing tickers
        are simply absent from the dict (the caller falls back to None).
        Empty result on DB error.
        """
        if not tickers:
            return {}
        try:
            rows = await self._pool.fetch(
                f"""
                SELECT {_SECURITY_COLUMNS}
                  FROM security_master
                 WHERE ticker = ANY($1::TEXT[])
                """,
                tickers,
            )
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "securities_repo.batch_failed",
                extra={
                    "event":      "securities_repo_batch_failed",
                    "n_tickers":  len(tickers),
                    "error_type": type(exc).__name__,
                },
            )
            return {}
        out: dict[str, Security] = {}
        for r in rows:
            sec = self._row_to_security(r)
            out[sec.ticker] = sec
        return out

    # ── /v1/securities/relations ───────────────────────────────────────
    async def list_relations(
        self,
        kinds:      list[str] | None = None,
        min_weight: float = 0.0,
        tickers:    list[str] | None = None,
    ) -> list[SecurityRelation]:
        """Filtered edge fetch. `tickers` filter matches BOTH endpoints —
        an edge is returned if either `from_ticker` OR `to_ticker` is in
        the set (operator wants "all edges around 005930").
        """
        clauses: list[str] = ["weight >= $1"]
        params:  list[Any] = [float(min_weight)]

        if kinds:
            params.append(kinds)
            clauses.append(f"kind = ANY(${len(params)})")
        if tickers:
            params.append(tickers)
            clauses.append(
                f"(from_ticker = ANY(${len(params)}) OR to_ticker = ANY(${len(params)}))"
            )

        where = "WHERE " + " AND ".join(clauses)
        sql = f"""
            SELECT {_RELATION_COLUMNS}
              FROM security_relation
              {where}
             ORDER BY weight DESC, from_ticker ASC, to_ticker ASC
        """

        try:
            rows = await self._pool.fetch(sql, *params)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ) as exc:
            logger.warning(
                "securities_repo.relations_failed",
                extra={
                    "event":      "securities_repo_relations_failed",
                    "error_type": type(exc).__name__,
                },
            )
            return []
        return [self._row_to_relation(r) for r in rows]

    async def count_relations(
        self,
        kinds:      list[str] | None = None,
        min_weight: float = 0.0,
        tickers:    list[str] | None = None,
    ) -> int:
        """Total count matching the relations filter set."""
        clauses: list[str] = ["weight >= $1"]
        params:  list[Any] = [float(min_weight)]

        if kinds:
            params.append(kinds)
            clauses.append(f"kind = ANY(${len(params)})")
        if tickers:
            params.append(tickers)
            clauses.append(
                f"(from_ticker = ANY(${len(params)}) OR to_ticker = ANY(${len(params)}))"
            )

        sql = (
            "SELECT COUNT(*)::BIGINT AS n FROM security_relation "
            "WHERE " + " AND ".join(clauses)
        )
        try:
            row = await self._pool.fetchrow(sql, *params)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError, TimeoutError,
        ):
            return 0
        return int(row["n"]) if row else 0

    # ── Row → domain mappers ────────────────────────────────────────────
    @staticmethod
    def _row_to_security(row: Any) -> Security:
        """asyncpg row → `Security` domain object.

        `aliases` lands as a Python list (asyncpg decodes TEXT[] natively).
        `updated_at` is the only datetime; tz-aware via PG TIMESTAMPTZ.
        Numeric fields are already DOUBLE PRECISION / BIGINT — no Decimal
        casting needed (unlike `market_tick.price`).
        """
        try:
            market_enum = Market(row["market"])
        except ValueError as e:  # pragma: no cover — CHECK constraint blocks
            raise ValueError(
                f"unknown market value {row['market']!r} for ticker {row['ticker']!r}"
            ) from e
        return Security(
            ticker=row["ticker"],
            name_ko=row["name_ko"],
            name_en=row["name_en"],
            aliases=list(row["aliases"] or []),
            market=market_enum,
            sector=row["sector"],
            sector_label=row["sector_label"],
            currency=row["currency"],
            shares_outstanding=(
                int(row["shares_outstanding"])
                if row["shares_outstanding"] is not None else None
            ),
            market_cap=(
                float(row["market_cap"]) if row["market_cap"] is not None else None
            ),
            # last_price / change_pct are NOT in security_master — they
            # come from the tick stream. The seed leaves them None; the
            # snapshot enrichment leaves them None too (frontend reads
            # ticks via the existing `/v1/ticks/snapshot` surface).
            last_price=None,
            change_pct=None,
            # anomaly / tx_vol live in the `entity` table; the repo
            # surfaces them from there via enrichment, not here. For the
            # standalone /v1/securities call they're omitted (default 0).
            anomaly=0.0,
            tx_vol=0.0,
            is_subscribed=bool(row["is_subscribed"]),
            data_source=row["data_source"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_relation(row: Any) -> SecurityRelation:
        """asyncpg row → `SecurityRelation` domain object."""
        try:
            kind_enum = RelationKind(row["kind"])
        except ValueError as e:  # pragma: no cover — CHECK constraint blocks
            raise ValueError(
                f"unknown relation kind {row['kind']!r} "
                f"for {row['from_ticker']}->{row['to_ticker']}"
            ) from e
        return SecurityRelation(
            from_ticker=row["from_ticker"],
            to_ticker=row["to_ticker"],
            kind=kind_enum,
            weight=float(row["weight"]),
            directed=bool(row["directed"]),
            evidence=row["evidence"],
        )


# ──────────────────────────────────────────────────────────────────────────
#  Seed helper — applied by `db/seed.py` extension or one-off scripts.
# ──────────────────────────────────────────────────────────────────────────

async def upsert_seed(
    pool: asyncpg.Pool,
    securities: list[dict[str, Any]],
    relations:  list[dict[str, Any]],
) -> tuple[int, int]:
    """Idempotent UPSERT of `securities_master.json` content.

    Returns `(n_securities, n_relations)` actually upserted. Errors propagate
    so the seed script can fail loud — unlike read-side fault tolerance, a
    failed seed must be visible to the operator. Called by the dev seed
    pipeline and (later) by external API refresh jobs.
    """
    n_sec = 0
    n_rel = 0
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        async with conn.transaction():
            for s in securities:
                await conn.execute(
                    """
                    INSERT INTO security_master (
                        ticker, name_ko, name_en, aliases, market,
                        sector, sector_label, currency,
                        shares_outstanding, market_cap,
                        is_subscribed, data_source, updated_at
                    )
                    VALUES ($1, $2, $3, $4::TEXT[], $5, $6, $7, $8,
                            $9, $10, $11, $12, $13)
                    ON CONFLICT (ticker) DO UPDATE SET
                        name_ko            = EXCLUDED.name_ko,
                        name_en            = EXCLUDED.name_en,
                        aliases            = EXCLUDED.aliases,
                        market             = EXCLUDED.market,
                        sector             = EXCLUDED.sector,
                        sector_label       = EXCLUDED.sector_label,
                        currency           = EXCLUDED.currency,
                        shares_outstanding = EXCLUDED.shares_outstanding,
                        market_cap         = EXCLUDED.market_cap,
                        is_subscribed      = EXCLUDED.is_subscribed,
                        data_source        = EXCLUDED.data_source,
                        updated_at         = EXCLUDED.updated_at
                    """,
                    s["ticker"], s.get("name_ko"), s.get("name_en"),
                    list(s.get("aliases") or []),
                    s["market"], s["sector"], s["sector_label"], s["currency"],
                    s.get("shares_outstanding"), s.get("market_cap"),
                    bool(s.get("is_subscribed", False)),
                    s.get("data_source", "static_master"),
                    now,
                )
                n_sec += 1
            for r in relations:
                await conn.execute(
                    """
                    INSERT INTO security_relation (
                        from_ticker, to_ticker, kind, weight,
                        directed, evidence, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (from_ticker, to_ticker, kind) DO UPDATE SET
                        weight     = EXCLUDED.weight,
                        directed   = EXCLUDED.directed,
                        evidence   = EXCLUDED.evidence,
                        updated_at = EXCLUDED.updated_at
                    """,
                    r["from_ticker"], r["to_ticker"], r["kind"],
                    float(r["weight"]), bool(r.get("directed", False)),
                    r.get("evidence"), now,
                )
                n_rel += 1
    return n_sec, n_rel
