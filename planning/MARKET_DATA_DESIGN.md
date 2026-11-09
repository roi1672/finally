# Market Data Backend — Detailed Design (Rev. 2)

Implementation-ready design for the FinAlly market data subsystem: the unified API (data model, price cache, abstract source, factory), the GBM simulator, the Massive (Polygon.io) REST poller, the SSE stream, and how the rest of the backend plugs into it.

**Status and relationship to existing code.** A first version of this subsystem is already in `backend/app/market/`. It is summarized in `MARKET_DATA_SUMMARY.md`, and its original design is in `archive/MARKET_DATA_DESIGN.md`. This revision is the **target state**. It keeps the existing architecture and public names, and fixes the problems found while reviewing the code against `PLAN.md` and the real `massive` SDK (§12 lists every change). Each code listing below is a complete module. All of them were run against the real SDK: **106 tests pass, with 97% coverage of `app/market`**, and a live uvicorn smoke test streamed correct SSE frames.

> **Critical finding:** the current `MassiveDataSource` reads `snap.last_trade.timestamp`. The SDK's `LastTrade` model has **no `timestamp` attribute**. The field is `sip_timestamp`, in **nanoseconds**. Against the real API, every snapshot raises `AttributeError`, gets logged as "Skipping snapshot", and **no prices are ever written**. The tests miss this because they use `MagicMock`, which makes up any attribute you read. §6 fixes the bug and §10 adds tests built on real SDK objects.

---

## Table of Contents

1. [Requirements Traceability](#1-requirements-traceability)
2. [Architecture](#2-architecture)
3. [File Layout](#3-file-layout)
4. [Unified API](#4-unified-api)
5. [GBM Simulator](#5-gbm-simulator)
6. [Massive API Source](#6-massive-api-source)
7. [SSE Streaming](#7-sse-streaming)
8. [Integrating With the Rest of the Backend](#8-integrating-with-the-rest-of-the-backend)
9. [Edge Cases & Error Handling](#9-edge-cases--error-handling)
10. [Testing](#10-testing)
11. [Configuration](#11-configuration)
12. [Change List vs. Current Code](#12-change-list-vs-current-code)

---

## 1. Requirements Traceability

| PLAN.md requirement | Where it is met |
|---|---|
| Two implementations, one interface; downstream code is source-agnostic | `MarketDataSource` ABC (§4.3); all consumers read `PriceCache` |
| `MASSIVE_API_KEY` set and non-empty → Massive; otherwise → simulator | `create_market_data_source()` (§4.4) |
| GBM, ~500ms updates, correlated moves, 2–5% random events, realistic seeds | `GBMSimulator` (§5) |
| Massive REST polling for the union of watched tickers; 15s on the free tier | `MassiveDataSource` (§6), one snapshot call per poll |
| Shared cache: latest price, previous price, timestamp | `PriceCache` / `PriceUpdate` (§4.1–4.2) |
| `GET /api/stream/prices`, ~500ms cadence, ticker/price/prev/timestamp/direction | `stream.py` (§7) |
| Watchlist shows **daily change %** | `PriceUpdate.reference_price` / `day_change_percent` (new, §4.1) |
| Prices flash green/red on change | `direction` field, computed tick to tick |
| Trades fill at the current price | `PriceCache.get_price()` (§8.3) |
| SSE resilience / reconnect | `retry: 1000`, full-snapshot frames, heartbeat (§7) |

---

## 2. Architecture

```
                 ┌───────────────────────────────┐
  env var  ───▶  │ create_market_data_source()   │
MASSIVE_API_KEY  └──────────────┬────────────────┘
                                │ returns ONE of
              ┌─────────────────┴──────────────────┐
              ▼                                    ▼
   SimulatorDataSource                     MassiveDataSource
   asyncio task, every 0.5s:               asyncio task, every 15s:
     GBMSimulator.step()                     to_thread(get_snapshot_all)
              │                                    │ parse_snapshot()
              └──────────────┬─────────────────────┘
                             ▼  cache.update(ticker, price, ts, reference_price)
                    ┌──────────────────┐
                    │    PriceCache    │  dict[ticker → PriceUpdate] + version
                    └───────┬──────────┘
        ┌───────────────┬───┴──────────┬────────────────┬──────────────┐
        ▼               ▼              ▼                ▼              ▼
  SSE /api/stream   POST trade     GET portfolio   GET watchlist   LLM chat context
  (0.5s, if version  (fill price)   (valuation,      (latest          (prices in
   changed)                         30s snapshots)    prices)          prompt)
```

**Principles**

- **Strategy pattern.** Exactly one source is active. Nothing outside `app/market` knows which one.
- **Push into a cache, never pull from the source.** Sources write on their own schedule and consumers read whenever they need to. A slow Massive poll can never block a trade.
- **Immutable values.** `PriceUpdate` is a frozen dataclass, so a snapshot from `get_all()` can be shared across tasks without copying.
- **Concurrency model.** Everything runs on the single uvicorn event loop, except the blocking Massive HTTP call, which runs in a worker thread through `asyncio.to_thread`. Writes from that thread, and future no-GIL builds, are why `PriceCache` takes a `threading.Lock` around every read and write, including `version`. The lock is held for microseconds, so contention is negligible.

---

## 3. File Layout

```
backend/app/market/
├── __init__.py        # public API re-exports
├── models.py          # PriceUpdate, normalize_ticker()
├── cache.py           # PriceCache
├── interface.py       # MarketDataSource (ABC)
├── factory.py         # create_market_data_source()
├── seed_prices.py     # seed prices, per-ticker mu/sigma, correlation groups
├── simulator.py       # GBMSimulator + SimulatorDataSource
├── massive_client.py  # parse_snapshot() + MassiveDataSource
└── stream.py          # create_stream_router(), generate_price_events()

backend/tests/market/
├── test_models.py  test_cache.py  test_simulator.py  test_simulator_source.py
├── test_massive.py  test_factory.py
└── test_stream.py     # new
```

**Public API.** Other packages import only from `app.market`:

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate         - Immutable price snapshot dataclass
    PriceCache          - Thread-safe in-memory price store
    MarketDataSource    - Abstract interface for data providers
    create_market_data_source - Factory that selects simulator or Massive
    create_stream_router - FastAPI router factory for SSE endpoint
    normalize_ticker    - Canonical ticker validation/uppercasing
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate, normalize_ticker
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "normalize_ticker",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

---

## 4. Unified API

### 4.1 `models.py`: `PriceUpdate` and `normalize_ticker`

`PriceUpdate` is the only type that leaves the market layer. All derived fields are computed properties, so they can never disagree with `price` and `previous_price`.

There are two kinds of "change":

| Field | Baseline | Used for |
|---|---|---|
| `change`, `change_percent`, `direction` | `previous_price` (the last tick) | green/red flash, sparkline color |
| `day_change`, `day_change_percent` | `reference_price` (previous close or session open) | "daily change %" column in the watchlist |

`reference_price` is **new**. The current code has only tick-to-tick change, so the "daily change %" that PLAN.md asks for could not be shown. The simulator sets it to the seed price at the moment a ticker starts, which acts as the session open. Massive sets it to `prev_day.close`.

`normalize_ticker()` is **new**. It is the single gate for symbols coming from users, the LLM, or the database: it strips, uppercases and validates. Today the Massive source uppercases but the simulator does not, so `"aapl"` and `"AAPL"` could both be simulated.

```python
"""Data models for market data."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

# 1-10 chars, starts with a letter; allows class-share suffixes like BRK.B / BF-B
_TICKER_RE = re.compile(r"[A-Z][A-Z0-9.\-]{0,9}")


def normalize_ticker(raw: str) -> str:
    """Canonical form for every ticker that enters the market data layer.

    Raises ValueError for anything that is not a plausible US equity symbol.
    """
    ticker = raw.strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    # Baseline for the "daily change" column: previous close (Massive) or
    # session-start seed price (simulator). None only if a source never set one.
    reference_price: float | None = None

    @property
    def change(self) -> float:
        """Absolute change since the previous update (tick-to-tick)."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percent change since the previous update (tick-to-tick)."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' — drives the green/red flash in the UI."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    @property
    def day_change(self) -> float | None:
        """Absolute change vs. the reference (daily) price."""
        if self.reference_price is None:
            return None
        return round(self.price - self.reference_price, 4)

    @property
    def day_change_percent(self) -> float | None:
        """Percent change vs. the reference (daily) price."""
        if not self.reference_price:
            return None
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,
            "day_change": self.day_change,
            "day_change_percent": self.day_change_percent,
        }
```

### 4.2 `cache.py`: `PriceCache`

```python
"""Thread-safe in-memory price cache."""

from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Latest price per ticker. The single source of truth for prices.

    Writers: exactly one MarketDataSource (simulator or Massive poller).
    Readers: SSE stream, portfolio valuation, trade execution, watchlist API, LLM context.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0  # Monotonic; bumped on every write (update or remove)

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,
    ) -> PriceUpdate:
        """Record a new price and return the resulting PriceUpdate.

        - previous_price comes from the prior cached entry (first update: == price, 'flat').
        - reference_price, if omitted, is carried over from the prior entry; for the
          very first update it defaults to the price itself.
        """
        with self._lock:
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=time.time() if timestamp is None else timestamp,
                reference_price=round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow-copy snapshot (PriceUpdate is immutable, so this is safe to share)."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1  # So SSE clients see the ticker disappear

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

Design notes:

- **Version counter.** The SSE loop compares `cache.version` with the last version it sent and skips serialization when nothing changed. With Massive polling every 15s, that removes about 29 redundant frames out of every 30. `remove()` now bumps the version too, so a deleted ticker disappears from clients right away rather than on the next price tick.
- **`reference_price` carry-over.** Sources pass `reference_price` only when they know it: once at seed time for the simulator, and on every poll for Massive. Other updates inherit it, which keeps the simulator's hot loop unchanged: `cache.update(ticker, price)`.
- **`timestamp is None` rather than `timestamp or time.time()`.** The old form treated a valid `0.0` as missing. This is minor, but it is a correctness fix.
- **Rounding to cents** happens once, here. Sources may write raw floats.

**Example**

```python
>>> cache = PriceCache()
>>> cache.update("AAPL", 190.0, reference_price=192.0).to_dict()
{'ticker': 'AAPL', 'price': 190.0, 'previous_price': 190.0, 'timestamp': 1794197870.05,
 'change': 0.0, 'change_percent': 0.0, 'direction': 'flat',
 'reference_price': 192.0, 'day_change': -2.0, 'day_change_percent': -1.0417}
>>> u = cache.update("AAPL", 190.37)
>>> u.direction, u.change, u.reference_price
('up', 0.37, 192.0)
```

### 4.3 `interface.py`: `MarketDataSource`

```python
"""Abstract interface for market data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    A source owns ONE background task that pushes prices into a shared PriceCache.
    Nothing downstream asks the source for prices — it reads the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])   # once, at app startup
        await source.add_ticker("PYPL")              # watchlist add / new position
        await source.remove_ticker("GOOGL")          # watchlist remove (no position)
        await source.stop()                          # app shutdown

    All ticker arguments are normalized with normalize_ticker() by the source.
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing prices for `tickers`. Raises RuntimeError if already started."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Idempotent. No cache writes after it returns."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker and evict it from the cache. No-op if not tracked."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers (a copy)."""
```

Contract details that both implementations honor, with tests for each:

| Behavior | Simulator | Massive |
|---|---|---|
| `start()` called twice | `RuntimeError` | `RuntimeError` |
| Cache populated when `start()` returns | yes (seed prices) | yes (one synchronous poll; empty if the API fails) |
| `add_ticker()` → price available | immediately (seeded) | on the next poll (≤ `poll_interval`) |
| `remove_ticker()` | drops from the simulation and the cache | drops from the poll list and the cache; in-flight results ignored |
| `stop()` | idempotent, awaits cancellation | idempotent, awaits cancellation |

### 4.4 `factory.py`

```python
"""Factory for creating market data sources."""

from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)

DEFAULT_MASSIVE_POLL_SECONDS = 15.0  # free tier: 5 req/min
MIN_MASSIVE_POLL_SECONDS = 1.0


def _poll_interval_from_env() -> float:
    raw = os.environ.get("MASSIVE_POLL_INTERVAL", "").strip()
    if not raw:
        return DEFAULT_MASSIVE_POLL_SECONDS
    try:
        return max(float(raw), MIN_MASSIVE_POLL_SECONDS)
    except ValueError:
        logger.warning("Ignoring invalid MASSIVE_POLL_INTERVAL=%r", raw)
        return DEFAULT_MASSIVE_POLL_SECONDS


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty → Massive; otherwise → simulator.

    Returns an UNSTARTED source; the caller must `await source.start(tickers)`.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = _poll_interval_from_env()
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache, poll_interval=interval)

    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

`MASSIVE_POLL_INTERVAL` is an **optional addition** to PLAN.md §5. It lets paid-tier users poll every 2–5s without a code change. It has a 1s floor, and an invalid value falls back to 15s.

---

## 5. GBM Simulator

### 5.1 Math

For each 500ms tick, for every ticker *i*:

```
S_i(t+dt) = S_i(t) · exp( (μ_i − σ_i²/2)·dt + σ_i·√dt·Z_i )

dt = 0.5 s / (252 days × 6.5 h × 3600 s) = 0.5 / 5,896,800 ≈ 8.48e-8 years
Z  = L · z,   z ~ N(0, I),   L = cholesky(C)
```

- `exp(·)` keeps prices strictly positive. Log returns are normal, so prices are log-normal.
- `−σ²/2` is the Itô correction, so that `E[S(t)] = S(0)·e^{μt}`.
- `C` is the sector correlation matrix. Multiplying independent normals by its Cholesky factor `L` gives draws with `Corr(Z_i, Z_j) = C_ij`. A test checks the empirical correlation, within ±0.05 of 0.6 for tech-tech and 0.3 for tech-finance.

**Correlation structure**

| Pair | ρ |
|---|---|
| Both in tech (AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX) | 0.6 |
| Both in finance (JPM, V) | 0.5 |
| Anything with TSLA | 0.3 |
| Cross-sector, or any unknown ticker | 0.3 |

Any matrix with 1 on the diagonal and the same off-diagonal ρ ∈ [0,1) is positive definite. Block structures built from these values also are, for the ticker counts we expect (< 50). So `np.linalg.cholesky` will not fail when a user adds tickers.

**Random events.** On each tick, each ticker has probability `p = 0.001` of a multiplicative shock of ±U(2%, 5%).

### 5.2 Calibration: what the numbers mean on screen

| Quantity | AAPL (σ = 0.22) | TSLA (σ = 0.50) |
|---|---|---|
| Diffusion per tick, σ√dt | 0.0064% (≈ $0.012) | 0.0146% (≈ $0.036) |
| Diffusion per hour | ≈ 0.54% | ≈ 1.23% |
| Events per hour per ticker (p × 7200 ticks) | ≈ 7.2 | ≈ 7.2 |
| Event-driven std per hour, √(7.2 · E[s²]), E[s²] ≈ 1.3e-3 | **≈ 9.7%** | **≈ 9.7%** |

**Takeaway:** at the default `event_probability`, price movement is **driven by jumps, not by GBM noise**. That suits a lively demo, since flashes come from the GBM and drama from the events. In a long-running session, though, prices drift visibly (±10% per hour is typical). The knob is `SimulatorDataSource(event_probability=...)`: `0.0002` gives ≈ 4%/hr, `0.0` gives pure GBM. We keep `0.001` as the default because it matches PLAN.md's intent ("occasional… for drama"), and we record the trade-off here.

### 5.3 `seed_prices.py` (unchanged)

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist (as of project creation)
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# Per-ticker GBM parameters
# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default parameters for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the simulator's Cholesky decomposition
# Tickers in the same group have higher intra-group correlation
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6  # Tech stocks move together
INTRA_FINANCE_CORR = 0.5  # Finance stocks move together
CROSS_GROUP_CORR = 0.3  # Between sectors / unknown tickers
TSLA_CORR = 0.3  # TSLA does its own thing
```

Tickers that are not in `SEED_PRICES` start at a uniform random price between $50 and $300 and use `DEFAULT_PARAMS`. This is reproducible when a `seed` is given.

### 5.4 `simulator.py`

```python
"""GBM-based market simulator."""

from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker
from .seed_prices import (
    CORRELATION_GROUPS,
    CROSS_GROUP_CORR,
    DEFAULT_PARAMS,
    INTRA_FINANCE_CORR,
    INTRA_TECH_CORR,
    SEED_PRICES,
    TICKER_PARAMS,
    TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Correlated Geometric Brownian Motion for a set of tickers (pure, no I/O).

        S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)

    Z is a vector of correlated standard normals: Z = L @ z, where L is the
    Cholesky factor of the sector-based correlation matrix and z ~ N(0, I).
    """

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR  # one 500ms tick ≈ 8.48e-8 years

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._np_rng = np.random.default_rng(seed)  # seed → reproducible tests
        self._rng = random.Random(seed)

        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    # --- Public API ---

    def step(self) -> dict[str, float]:
        """Advance every ticker one tick. Returns {ticker: new_price}. Hot path."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z = self._np_rng.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        sqrt_dt = math.sqrt(self._dt)
        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]
            self._prices[ticker] *= math.exp(
                (mu - 0.5 * sigma**2) * self._dt + sigma * sqrt_dt * z[i]
            )

            # Drama: ~0.1% chance per ticker per tick of a sudden 2-5% jump.
            # 10 tickers x 2 ticks/s → an event somewhere roughly every 50s.
            if self._rng.random() < self._event_prob:
                shock = self._rng.uniform(0.02, 0.05) * self._rng.choice((-1, 1))
                self._prices[ticker] *= 1 + shock
                logger.debug("Random event on %s: %+.1f%%", ticker, shock * 100)

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, self._rng.uniform(50.0, 300.0))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """O(n^2) build + O(n^3) factorization; trivial for n < 50."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if t1 == "TSLA" or t2 == "TSLA":  # in "tech" by sector, but does its own thing
            return TSLA_CORR
        tech, finance = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR


class SimulatorDataSource(MarketDataSource):
    """MarketDataSource that steps a GBMSimulator every `update_interval` seconds."""

    def __init__(
        self,
        price_cache: PriceCache,
        update_interval: float = 0.5,
        event_probability: float = 0.001,
        seed: int | None = None,
    ) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._seed = seed
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("SimulatorDataSource already started")
        normalized = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        self._sim = GBMSimulator(normalized, event_probability=self._event_prob, seed=self._seed)
        for ticker in normalized:
            self._seed_cache(ticker)  # SSE has data on the very first frame
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(normalized))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim is None or ticker in self._sim.get_tickers():
            return
        self._sim.add_ticker(ticker)
        self._seed_cache(ticker)  # price is available to the very next request
        logger.info("Simulator: added %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    def _seed_cache(self, ticker: str) -> None:
        price = self._sim.get_price(ticker) if self._sim else None
        if price is not None:
            # The seed price doubles as the "day open" for day_change_percent
            self._cache.update(ticker, price, reference_price=price)

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker, price)
            except Exception:
                logger.exception("Simulator step failed")  # never let the loop die
            await asyncio.sleep(self._interval)
```

Changes from the current code:

- **`seed` parameter.** It drives a private `np.random.Generator` and a `random.Random`, so simulator tests are deterministic, and the simulator no longer touches global RNG state that other code or tests might reseed.
- **Normalization and dedup** of tickers in `start()` and `add_ticker()`.
- **`start()` guard** against a double start, which would otherwise leak a second loop task writing to the same cache.
- **Seed price as `reference_price`**, which powers "daily change %" in simulator mode.

**Example: the simulator on its own (no asyncio)**

```python
>>> from app.market.simulator import GBMSimulator
>>> sim = GBMSimulator(["AAPL", "TSLA", "PYPL"], seed=42)
>>> round(sim.get_price("PYPL"), 2)   # not in SEED_PRICES → seeded random in [50, 300)
118.76
>>> for _ in range(3):
...     print(sim.step())
{'AAPL': 190.0, 'TSLA': 249.97, 'PYPL': 118.76}
{'AAPL': 190.02, 'TSLA': 249.91, 'PYPL': 118.75}
{'AAPL': 190.02, 'TSLA': 249.9, 'PYPL': 118.75}
```

---

## 6. Massive API Source

### 6.1 Endpoint and rate limits

One request covers every tracked ticker:

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT
Authorization: Bearer $MASSIVE_API_KEY
```

SDK call: `RESTClient(api_key).get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=[...])`, which returns `list[TickerSnapshot]`.

| Tier | Limit | Poll interval |
|---|---|---|
| Free | 5 req/min | 15s (default) → 4 req/min, leaving headroom |
| Paid | effectively unlimited | 2–5s via `MASSIVE_POLL_INTERVAL` |

Because a single call covers N tickers, the rate limit does not depend on watchlist size. That is why `add_ticker()` does **not** fire an extra request: on the free tier, a burst of LLM-driven adds would trip the 429 limit.

### 6.2 Field mapping (verified against the installed SDK)

The archived `MASSIVE_API.md` lists `day.previous_close` and `last_trade.timestamp`. **Neither exists** on the SDK models. The correct mapping is:

| Cache field | SDK attribute | Wire JSON | Notes |
|---|---|---|---|
| `ticker` | `snap.ticker` | `ticker` | |
| `price` | `snap.last_trade.price` | `lastTrade.p` | primary |
| `price` (fallback) | `snap.min.close`, then `snap.day.close` | `min.c`, `day.c` | when there is no trade yet in the session |
| `timestamp` | `snap.last_trade.sip_timestamp / 1e9` | `lastTrade.t` | **nanoseconds** |
| `timestamp` (fallback) | `snap.updated / 1e9` | `updated` | nanoseconds; otherwise `time.time()` |
| `reference_price` | `snap.prev_day.close` | `prevDay.c` | daily-change baseline |
| (not used) | `snap.todays_change_percent` | `todaysChangePerc` | we compute it from `prev_day.close`, so both sources share one formula |

Example wire payload, trimmed:

```json
{
  "ticker": "AAPL",
  "todaysChange": -1.15,
  "todaysChangePerc": -0.598,
  "updated": 1707580800123456789,
  "day":     {"o": 190.2, "h": 192.0, "l": 189.1, "c": 191.0, "v": 51234567},
  "min":     {"o": 191.0, "h": 191.1, "l": 190.9, "c": 191.05, "v": 120000},
  "prevDay": {"o": 191.5, "h": 193.0, "l": 190.4, "c": 192.20, "v": 60123456},
  "lastTrade": {"p": 191.05, "s": 100, "x": 4, "t": 1707580800123456789}
}
```

### 6.3 `massive_client.py`

```python
"""Massive (formerly Polygon.io) REST poller for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, NamedTuple

from massive import RESTClient
from massive.exceptions import AuthError
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)

NS_PER_SECOND = 1_000_000_000
MAX_BACKOFF_SECONDS = 120.0


class ParsedSnapshot(NamedTuple):
    ticker: str
    price: float
    timestamp: float  # Unix seconds
    reference_price: float | None  # previous session close


def parse_snapshot(snap: Any) -> ParsedSnapshot | None:
    """Turn a massive TickerSnapshot into cache fields. Returns None if unusable.

    Field map (SDK attribute ← JSON key):
        snap.ticker                   ← ticker
        snap.last_trade.price         ← lastTrade.p        (primary price)
        snap.last_trade.sip_timestamp ← lastTrade.t        (NANOseconds)
        snap.min.close / day.close    ← min.c / day.c      (fallback price)
        snap.updated                  ← updated            (NANOseconds, fallback ts)
        snap.prev_day.close           ← prevDay.c          (daily-change baseline)

    Note: LastTrade has NO `.timestamp` attribute — reading it raises AttributeError.
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    trade = getattr(snap, "last_trade", None)
    price = getattr(trade, "price", None)
    ts_ns = getattr(trade, "sip_timestamp", None)

    if not price:  # no trade yet today → fall back to the latest bar close
        for bar in (getattr(snap, "min", None), getattr(snap, "day", None)):
            price = getattr(bar, "close", None)
            if price:
                break
    if not price:
        return None

    ts_ns = ts_ns or getattr(snap, "updated", None)
    timestamp = ts_ns / NS_PER_SECOND if ts_ns else time.time()

    prev_close = getattr(getattr(snap, "prev_day", None), "close", None) or None
    return ParsedSnapshot(ticker, float(price), timestamp, prev_close)


class MassiveDataSource(MarketDataSource):
    """Polls GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=A,B,C.

    One HTTP request per poll covers every tracked ticker, so the free tier
    (5 req/min) is safe at the default 15s interval.
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._consecutive_failures = 0

    async def start(self, tickers: list[str]) -> None:
        if self._task is not None:
            raise RuntimeError("MassiveDataSource already started")
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        await self._poll_once()  # populate the cache before the first SSE client arrives
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval", len(self._tickers), self._interval
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            # Appears on the next poll (≤ poll_interval). We don't fire an extra request
            # here: on the free tier that would eat into the 5 req/min budget.
            self._tickers.append(ticker)
            logger.info("Massive: added %s (next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    def _next_delay(self) -> float:
        """Normal interval, doubled per consecutive failure (429s, outages), capped."""
        if self._consecutive_failures == 0:
            return self._interval
        return min(self._interval * 2**self._consecutive_failures, MAX_BACKOFF_SECONDS)

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._next_delay())
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or self._client is None:
            return
        tickers = list(self._tickers)  # snapshot: add/remove may run during the await
        try:
            # RESTClient is synchronous (urllib3) — keep it off the event loop
            snapshots = await asyncio.to_thread(self._fetch_snapshots, tickers)
        except AuthError:
            self._consecutive_failures += 1
            logger.error("Massive rejected MASSIVE_API_KEY (401/403). Prices will not update.")
            return
        except Exception as e:  # 429, 5xx after SDK retries, network errors
            self._consecutive_failures += 1
            logger.warning("Massive poll failed (%d in a row): %s", self._consecutive_failures, e)
            return

        self._consecutive_failures = 0
        tracked = set(self._tickers)  # skip tickers removed while the request was in flight
        updated = 0
        for snap in snapshots:
            parsed = parse_snapshot(snap)
            if parsed is None or parsed.ticker not in tracked:
                continue
            self._cache.update(
                parsed.ticker,
                parsed.price,
                timestamp=parsed.timestamp,
                reference_price=parsed.reference_price,
            )
            updated += 1
        logger.debug("Massive poll: %d/%d tickers updated", updated, len(tickers))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking SDK call. Runs in a worker thread."""
        return self._client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=tickers)
```

Design notes:

- **`parse_snapshot()` is a pure module-level function**, so it can be unit-tested against real `TickerSnapshot.from_dict(...)` objects without any HTTP or mocks.
- **Snapshot the ticker list before the `await`**, then **filter by the live set after it**. This keeps a ticker removed during the in-flight request from reappearing in the cache.
- **Backoff.** Each consecutive failure doubles the delay (15 → 30 → 60 → 120s cap), and any success resets it. The SDK already retries 5xx errors three times inside a single call. `AuthError` (401/403) gets its own clear log line, since it is the most common setup mistake.
- **Never raises out of the loop.** A broken key or a network outage only leaves prices stale. The rest of the app, including trading at the last known price, keeps working.
- **Market closed.** The API returns the last trade (possibly after-hours). Prices stop changing, `version` stops moving, and the SSE stream sends heartbeats only. This is correct behavior and needs no special case.

---

## 7. SSE Streaming

### 7.1 Wire format

```
retry: 1000

id: 18
data: {"AAPL":{"ticker":"AAPL","price":190.0,"previous_price":190.0,"timestamp":1794197870.05,"change":0.0,"change_percent":0.0,"direction":"flat","reference_price":190.0,"day_change":0.0,"day_change_percent":0.0},"GOOGL":{...},"TSLA":{...}}

id: 19
data: {...}

: heartbeat
```

The `id: 18` frame above is the first frame from a real smoke run against uvicorn with the simulator. A few things to note about the format:

- **Each frame is a full snapshot** of every tracked ticker, not a delta. A client that reconnects is correct after its first frame, so `Last-Event-ID` replay is not needed. The `id` is informational: it is the cache version.
- **One frame per change, at most every 0.5s.** The simulator therefore produces about 2 frames/s, and Massive produces about 1 frame per 15s plus heartbeats.
- **Heartbeat** (`: heartbeat`, an SSE comment) every 15s while idle. It keeps proxies and load balancers (nginx, App Runner, about 60s idle timeouts) from cutting the connection while the market is closed. `EventSource` ignores comments.
- **Removed tickers disappear** from the next frame, because `remove()` bumps the version.

### 7.2 `stream.py`

```python
"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

PUSH_INTERVAL_SECONDS = 0.5
HEARTBEAT_SECONDS = 15.0  # keeps idle connections alive through proxies (Massive, market closed)


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build a fresh router per call (safe to call repeatedly in tests)."""
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            generate_price_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable proxy buffering (nginx, App Runner)
            },
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = PUSH_INTERVAL_SECONDS,
    heartbeat: float = HEARTBEAT_SECONDS,
) -> AsyncGenerator[str, None]:
    """Yield SSE frames: the full price map whenever the cache version changes.

    Wire format (one frame):
        id: 1234
        data: {"AAPL": {"ticker": "AAPL", "price": 190.5, ...}, "GOOGL": {...}}

    Full snapshots (not deltas) make reconnects trivial: any frame is complete.
    """
    yield "retry: 1000\n\n"  # EventSource reconnects 1s after a drop

    last_version = -1
    last_sent = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)

    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                payload = {t: u.to_dict() for t, u in price_cache.get_all().items()}
                yield f"id: {version}\ndata: {json.dumps(payload)}\n\n"
                last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= heartbeat:
                yield ": heartbeat\n\n"  # SSE comment — ignored by EventSource
                last_sent = time.monotonic()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

Change from the current code: the `APIRouter` is now created **inside** `create_stream_router()`. The current module-level `router` adds a duplicate `/prices` route every time the factory is called, which happens in tests that build several apps. `generate_price_events` is now public, so it can be tested directly (§10).

**Why poll the cache instead of using an event-driven push?** A fixed 0.5s cadence gives evenly spaced points for the frontend sparklines. It also handles any number of clients with no per-client queues, and caps the frame rate however fast a source writes.

### 7.3 Frontend consumption (reference for the Frontend agent)

```ts
type PriceUpdate = {
  ticker: string; price: number; previous_price: number; timestamp: number;
  change: number; change_percent: number; direction: "up" | "down" | "flat";
  reference_price: number | null; day_change: number | null; day_change_percent: number | null;
};

const es = new EventSource("/api/stream/prices");
es.onopen = () => setStatus("connected");                       // green dot
es.onerror = () => setStatus(es.readyState === EventSource.CLOSED ? "disconnected" : "reconnecting");
es.onmessage = (e) => {
  const prices: Record<string, PriceUpdate> = JSON.parse(e.data);
  for (const p of Object.values(prices)) {
    if (p.direction !== "flat") flash(p.ticker, p.direction);   // CSS class, ~500ms fade
    appendSparklinePoint(p.ticker, p.timestamp, p.price);       // accumulate since page load
  }
  replacePriceMap(prices);  // tickers missing from the map were removed
};
```

---

## 8. Integrating With the Rest of the Backend

### 8.1 App lifespan (`backend/app/main.py`)

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.market import MarketDataSource, PriceCache, create_market_data_source, create_stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()  # lazy schema + seed (backend/db), see PLAN.md §7

    cache = PriceCache()
    source = create_market_data_source(cache)
    await source.start(sorted(tracked_tickers()))  # watchlist ∪ open positions (§8.2)

    app.state.price_cache = cache
    app.state.market_source = source
    app.include_router(create_stream_router(cache))
    try:
        yield
    finally:
        await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# Dependencies: read from request.app.state so tests can build their own apps
def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

The SSE router must be included **before** the static-file mount at `/`. Otherwise the catch-all static route takes over `/api/stream/prices`.

### 8.2 Tracked set = watchlist ∪ open positions

PLAN.md says the stream covers "all tickers known to the system". To value a position correctly, its price has to keep flowing even after the user removes the ticker from the watchlist. The rule:

```
tracked = {watchlist tickers} ∪ {tickers with position quantity > 0}
```

Keep the coordination in one helper, used by the watchlist routes, the trade route, and the LLM action executor:

```python
# backend/app/services/market_sync.py
from app.market import MarketDataSource


async def sync_ticker(source: MarketDataSource, ticker: str) -> None:
    """Make the source's tracking of `ticker` match the database. Call after any
    watchlist or position change."""
    wanted = in_watchlist(ticker) or position_quantity(ticker) > 0
    tracked = ticker in source.get_tickers()
    if wanted and not tracked:
        await source.add_ticker(ticker)
    elif tracked and not wanted:
        await source.remove_ticker(ticker)
```

### 8.3 Route usage examples

```python
from fastapi import APIRouter, Depends, HTTPException

from app.market import MarketDataSource, PriceCache, normalize_ticker

router = APIRouter(prefix="/api")


@router.post("/watchlist", status_code=201)
async def add_watchlist(body: WatchlistAdd, source: MarketDataSource = Depends(get_market_source),
                        cache: PriceCache = Depends(get_price_cache)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(422, str(e))
    insert_watchlist(ticker)                 # UNIQUE(user_id, ticker) → 409 on duplicate
    await sync_ticker(source, ticker)
    update = cache.get(ticker)               # simulator: present now; Massive: None until next poll
    return {"ticker": ticker, "price": update.to_dict() if update else None}


@router.delete("/watchlist/{ticker}")
async def remove_watchlist(ticker: str, source: MarketDataSource = Depends(get_market_source)):
    ticker = normalize_ticker(ticker)
    delete_watchlist(ticker)
    await sync_ticker(source, ticker)        # keeps streaming if a position is still open
    return {"ticker": ticker, "removed": True}


@router.post("/portfolio/trade")
async def trade(body: TradeRequest, source: MarketDataSource = Depends(get_market_source),
                cache: PriceCache = Depends(get_price_cache)):
    ticker = normalize_ticker(body.ticker)
    price = cache.get_price(ticker)
    if price is None:
        # Not tracked yet (or Massive hasn't polled it) — refuse rather than guess
        raise HTTPException(409, f"No live price for {ticker} yet; add it to the watchlist and retry")
    result = execute_trade(ticker, body.side, body.quantity, price)   # validation + DB txn
    await sync_ticker(source, ticker)        # a full sell of a non-watched ticker stops tracking
    record_portfolio_snapshot(cache)         # PLAN.md: snapshot immediately after each trade
    return result
```

Portfolio valuation, the 30s snapshot task, and the LLM context all use the same pattern: `cache.get_all()` once, then value every position from that one consistent snapshot.

```python
def portfolio_value(cash: float, positions: list[Position], cache: PriceCache) -> float:
    prices = cache.get_all()
    return cash + sum(p.quantity * (prices[p.ticker].price if p.ticker in prices else p.avg_cost)
                      for p in positions)   # fall back to cost basis if a price is missing
```

---

## 9. Edge Cases & Error Handling

| Situation | Behavior |
|---|---|
| Empty watchlist at startup | `start([])` succeeds. The simulator steps nothing and Massive skips polls. Adds work normally. |
| Invalid symbol from user or LLM (`"AA PL"`, `"1ABC"`) | `normalize_ticker` raises `ValueError` → route returns 422; LLM flow reports it in `actions` |
| Unknown but valid symbol, simulator mode | Simulated from a random $50–300 seed with default params |
| Unknown symbol, Massive mode | The API returns no snapshot for it, so the cache has no entry. The watchlist shows "—" and trades get 409. |
| Bad `MASSIVE_API_KEY` | `AuthError` is logged on each poll with backoff to 120s. The app runs and prices stay empty. |
| 429 / network outage | Backoff; the last known prices stay in the cache and trades still fill at them |
| Ticker removed during an in-flight Massive request | Result is dropped by the `tracked` filter |
| Massive returns a ticker with no trade today | Falls back to the minute-bar close, then the day-bar close |
| Market closed | Prices are static and SSE sends heartbeats only |
| Simulator exception in `step()` | Logged; the loop continues on the next tick |
| SSE client disconnects | Detected within 0.5s through `request.is_disconnected()`; the generator exits |
| `start()` called twice | `RuntimeError` (prevents duplicate writer tasks) |

---

## 10. Testing

Run: `cd backend && uv run --extra dev pytest -v --cov=app.market`. Lint: `uv run --extra dev ruff check app tests`.

**Rule for the Massive tests: build fixtures with `TickerSnapshot.from_dict(<wire JSON>)`, never `MagicMock`.** A mock answers to any attribute name, which is exactly how the `timestamp`/`sip_timestamp` bug got through.

```python
from massive.rest.models import TickerSnapshot
from app.market.massive_client import parse_snapshot


def _make_snapshot(ticker, price, timestamp_ns, prev_close=None) -> TickerSnapshot:
    payload = {"ticker": ticker, "lastTrade": {"p": price, "s": 100, "t": timestamp_ns}}
    if prev_close is not None:
        payload["prevDay"] = {"c": prev_close}
    return TickerSnapshot.from_dict(payload)


def test_full_snapshot():
    snap = TickerSnapshot.from_dict({
        "ticker": "AAPL",
        "lastTrade": {"p": 191.05, "s": 100, "t": 1707580800123456789},
        "prevDay": {"c": 192.20},
    })
    parsed = parse_snapshot(snap)
    assert parsed.price == 191.05
    assert parsed.timestamp == pytest.approx(1707580800.123456789)   # ns → s
    assert parsed.reference_price == 192.20
```

**SSE generator, with no server needed:**

```python
class FakeRequest:
    def __init__(self, checks_before_disconnect): self._n = checks_before_disconnect; self.client = None
    async def is_disconnected(self): self._n -= 1; return self._n < 0


async def test_unchanged_version_sends_nothing():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    frames = [f async for f in generate_price_events(cache, FakeRequest(3), interval=0, heartbeat=3600)]
    assert len(frames) == 2          # "retry" + one data frame, despite 3 loop iterations
```

**Deterministic simulator:**

```python
def test_seed_is_reproducible():
    a, b = GBMSimulator(list(SEED_PRICES), seed=42), GBMSimulator(list(SEED_PRICES), seed=42)
    assert all(a.step() == b.step() for _ in range(100))
```

**Coverage map (106 tests)**

| Area | What is tested |
|---|---|
| models | change/direction/percent, divide-by-zero, `day_change*`, `to_dict` keys, `normalize_ticker` accept/reject |
| cache | first update is flat, direction, rounding, remove, version bumps (including remove), reference carry-over, `timestamp=0.0`, 8-thread concurrent writes (no lost updates) |
| simulator | positivity, determinism with a seed, add/remove rebuilds Cholesky, full 10-ticker factorization, **empirical correlation ≈ 0.6 / 0.3**, unknown-ticker seeding |
| simulator source | start seeds the cache, the loop updates, add/remove, stop is idempotent, exception resilience |
| massive | `parse_snapshot` on real SDK objects (full, bar fallback, no price, no ticker), ns→s, prev close → `day_change_percent`, malformed snapshot skipped, API error swallowed, backoff grows/resets, removal race, double start, normalization |
| factory | env-var selection, whitespace key, poll interval default/custom/floor/garbage |
| stream | retry preamble, `id`/`data` framing, version gating, heartbeat, removal → empty map, router factory re-entrant |

E2E (in `test/`, Playwright, PLAN.md §12) relies on simulator mode, so no `MASSIVE_API_KEY` should be set in `docker-compose.test.yml`.

---

## 11. Configuration

| Setting | Source | Default | Effect |
|---|---|---|---|
| `MASSIVE_API_KEY` | env / `.env` | empty | non-empty → Massive; empty → simulator |
| `MASSIVE_POLL_INTERVAL` | env (optional, **new**) | `15` | seconds between polls; floor 1s |
| `update_interval` | `SimulatorDataSource(...)` | `0.5` | simulator tick (seconds) |
| `event_probability` | `SimulatorDataSource(...)` | `0.001` | per-ticker, per-tick jump probability (§5.2) |
| `seed` | `SimulatorDataSource(...)` / `GBMSimulator(...)` | `None` | reproducible paths for tests |
| `PUSH_INTERVAL_SECONDS` | `stream.py` | `0.5` | SSE check/push cadence |
| `HEARTBEAT_SECONDS` | `stream.py` | `15` | idle keep-alive comment |

Dependencies (already in `backend/pyproject.toml`): `fastapi`, `uvicorn[standard]`, `numpy`, `massive`.

---

## 12. Change List vs. Current Code

These are the changes to apply to `backend/app/market/`, in priority order. All of them are already reflected in the listings above.

| # | Severity | File | Change |
|---|---|---|---|
| 1 | **Critical** | `massive_client.py` | Read `last_trade.sip_timestamp` (ns, ÷1e9), not the nonexistent `last_trade.timestamp` (÷1e3). Without this fix, Massive mode writes no prices. Extract `parse_snapshot()`. |
| 2 | High | `models.py`, `cache.py`, both sources | Add `reference_price` + `day_change` / `day_change_percent` for PLAN's "daily change %". |
| 3 | High | `test_massive.py` | Replace `MagicMock` snapshots with `TickerSnapshot.from_dict`; fix the ms→ns expectations. |
| 4 | Medium | `models.py`, both sources | `normalize_ticker()` everywhere (the simulator currently doesn't normalize). |
| 5 | Medium | `massive_client.py` | Snapshot and filter tickers around the `await` (removal race); exponential backoff; distinct `AuthError` log. |
| 6 | Medium | `stream.py` | Create the router inside the factory (no duplicate routes); heartbeat; `id:` field; public `generate_price_events`. |
| 7 | Medium | both sources | `start()` raises if already started. |
| 8 | Low | `cache.py` | `version` read under the lock; `remove()` bumps the version; `timestamp is None` check. |
| 9 | Low | `simulator.py` | `seed` parameter with private RNGs; per-ticker params copied (no shared mutable dict). |
| 10 | Low | `factory.py` | Optional `MASSIVE_POLL_INTERVAL`. |
| 11 | Docs | `archive/MASSIVE_API.md` | Its field names (`day.previous_close`, `last_trade.timestamp`) are wrong for the SDK; this document's §6.2 supersedes them. |
| 12 | Tests | `tests/market/` | Add `test_stream.py`, plus the cache/model/simulator/massive tests listed in §10. |

Public names are unchanged (`PriceUpdate`, `PriceCache`, `MarketDataSource`, `create_market_data_source`, `create_stream_router`), plus the new `normalize_ticker`. Downstream code written against `backend/CLAUDE.md` keeps working. The only addition to the SSE payload is three new keys, which existing consumers can ignore.
