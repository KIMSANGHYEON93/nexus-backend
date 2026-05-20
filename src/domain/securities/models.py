"""Securities domain model — pure Pydantic, no FastAPI / asyncpg imports.

These are the *domain* shapes that flow through the securities pipeline:
    seed JSON / external API → SecuritiesRepository → API → NEXUS canvas

The wire DTOs in `src/api/v1/dto.py` are constructed from these objects;
the only computed surface here is `Security.display_name` (spec §4.1: ko
first, en fallback, ticker last). Keeping the rule in the domain means
both `/v1/securities` and `/v1/snapshot` enrichment compute the same
label without copy-paste drift.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Market(str, Enum):
    """Trading venue — string enum so `.value` lands as plain JSON.

    `OTHER` is the documented escape hatch for instruments that don't fit
    KRX / KOSDAQ / NASDAQ / NYSE (e.g. crypto, OTC, foreign exotic). The
    seed never uses it today; spec §2 keeps it reserved.
    """

    KRX    = "KRX"
    KOSDAQ = "KOSDAQ"
    NASDAQ = "NASDAQ"
    NYSE   = "NYSE"
    OTHER  = "OTHER"


class RelationKind(str, Enum):
    """Edge type for `security_relation`.

    `sector` is the only kind populated by Sprint A seed; the rest are
    reserved for future enrichment (correlation rolls in once we have a
    rolling 30-day price series; same_chaebol / supply_chain / cross_listing
    are populated manually for now).
    """

    SECTOR        = "sector"
    CORRELATION   = "correlation"
    SAME_CHAEBOL  = "same_chaebol"
    SUPPLY_CHAIN  = "supply_chain"
    CROSS_LISTING = "cross_listing"


class Security(BaseModel):
    """One investable instrument — joined into entity/alarm enrichment.

    Invariants (Pydantic-validated):
      • `anomaly` ∈ [0.0, 1.0]
      • `market_cap`, `last_price`, `change_pct`, `shares_outstanding`
        are None-able (data may not be available for ETFs or pre-IPO).

    `display_name` is computed (not stored) per spec §4.1 option A:
    backend always returns Korean first, English fallback, ticker last.
    The frontend re-orders for English-mode users client-side.
    """

    ticker:             str
    name_ko:            Optional[str]    = None
    name_en:            Optional[str]    = None
    aliases:            list[str]        = Field(default_factory=list)
    market:             Market
    sector:             str
    sector_label:       str
    currency:           str
    shares_outstanding: Optional[int]    = None
    market_cap:         Optional[float]  = None
    last_price:         Optional[float]  = None
    change_pct:         Optional[float]  = None
    anomaly:            float            = Field(default=0.0, ge=0.0, le=1.0)
    tx_vol:             float            = 0.0
    is_subscribed:      bool             = False
    data_source:        str              = "static_master"
    updated_at:         datetime

    @property
    def display_name(self) -> str:
        """Operator-visible label — ko first, en next, ticker last.

        Stays a Python property (not a stored field) so a name update
        anywhere in the stack — DB row, external API, manual override —
        propagates without an extra migration. The router echoes this
        into the DTO; the frontend reorders to en-first only when its
        i18n mode is English.
        """
        return self.name_ko or self.name_en or self.ticker


class SecurityRelation(BaseModel):
    """One edge in the multi-kind securities graph.

    `to_ticker` carries the `SECTOR:` namespace prefix for synthetic
    sector-hub nodes (`SECTOR:SEMI` etc.) — spec §2.5 keeps these out of
    `security_master` so a sector rename doesn't need a master refresh.
    The frontend materialises them as virtual nodes on first sighting.

    `directed=False` means the edge is symmetric and the renderer
    draws it once; `True` means A → B is meaningful (e.g. NVDA →
    TSM supply_chain).
    """

    from_ticker: str
    to_ticker:   str
    kind:        RelationKind
    weight:      float           = Field(ge=0.0, le=1.0)
    directed:    bool            = False
    evidence:    Optional[str]   = None
