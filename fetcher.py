import asyncio
import concurrent.futures
import functools
import io
import json
import tarfile
import time
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
import requests

import config
import db
import gank

logger = logging.getLogger(__name__)

# Module-level rate-limit timestamps (for synchronous callers only)
_last_esi_call: float = 0.0
_last_zkill_call: float = 0.0

# In-process market price cache (ESI /markets/prices/ is updated every hour)
_market_prices: dict = {}
_market_prices_fetched_at: float = 0.0
_MARKET_PRICES_TTL = 3600.0

_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": config.USER_AGENT, "Accept": "application/json"})
    return _session


def _wait_esi() -> None:
    global _last_esi_call
    elapsed = time.monotonic() - _last_esi_call
    if elapsed < config.ESI_RATE_LIMIT:
        time.sleep(config.ESI_RATE_LIMIT - elapsed)
    _last_esi_call = time.monotonic()


def _wait_zkill() -> None:
    global _last_zkill_call
    elapsed = time.monotonic() - _last_zkill_call
    if elapsed < config.ZKILL_RATE_LIMIT:
        time.sleep(config.ZKILL_RATE_LIMIT - elapsed)
    _last_zkill_call = time.monotonic()


def _esi_get(path: str, **kwargs) -> Optional[dict]:
    _wait_esi()
    url = f"{config.ESI_BASE}{path}"
    try:
        r = _get_session().get(url, timeout=15, **kwargs)
        remain = r.headers.get("X-ESI-Error-Limit-Remain")
        if remain and int(remain) < 20:
            logger.warning("ESI error limit running low (%s remaining), backing off 60s", remain)
            time.sleep(60)
        if r.status_code == 422:
            logger.warning("ESI 422 for %s — bad hash or invalid ID, skipping", url)
            return None
        if r.status_code == 404:
            logger.warning("ESI 404 for %s", url)
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        logger.error("ESI request failed for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# zKillboard
# ---------------------------------------------------------------------------

def fetch_zkill_recent(limit: int = 50) -> list:
    """
    Fetch recent kills from zKillboard (single page, up to 200).
    Returns list of {killmail_id, zkb: {hash, totalValue, droppedValue, ...}}.
    """
    _wait_zkill()
    url = f"{config.ZKILLBOARD_BASE}/kills/"
    try:
        r = _get_session().get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data[:limit]
    except requests.RequestException as e:
        logger.error("zKillboard fetch failed: %s", e)
        return []


def fetch_zkill_day_stubs(date_str: str, progress: Optional[dict] = None) -> dict:
    """
    Fetch all zKillboard stubs for a single day (YYYY-MM-DD).

    Uses startTime/endTime to bound to midnight–23:59 of that day, then
    paginates until empty. Returns {killmail_id: zkb} dict containing
    hash, totalValue, droppedValue etc. for fast lookup during ingest.
    """
    from datetime import datetime as _dt
    day = _dt.strptime(date_str, "%Y-%m-%d")
    start = day.strftime("%Y%m%d0000")
    end = day.strftime("%Y%m%d2359")

    stubs: dict = {}
    page = 1

    while page <= config.ZKILL_MAX_PAGES:
        _wait_zkill()
        url = f"{config.ZKILLBOARD_BASE}/kills/startTime/{start}/endTime/{end}/page/{page}/"
        try:
            r = _get_session().get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.error("zKillboard stubs page %d for %s failed: %s", page, date_str, e)
            break

        if not data:
            break

        for entry in data:
            stubs[entry["killmail_id"]] = entry.get("zkb", {})

        if progress is not None:
            progress.update({
                "message": f"{date_str}: fetching ISK values — page {page}, {len(stubs):,} kills so far…",
            })
        logger.info("zKillboard %s page %d: %d stubs (total %d)", date_str, page, len(data), len(stubs))
        page += 1

    logger.info("zKillboard %s: %d stubs total", date_str, len(stubs))
    return stubs


def fetch_zkill_history(days_back: int) -> list:
    """
    Fetch all killmail stubs from zKillboard for the past `days_back` days.

    Uses the startTime filter to bound the query, then paginates until the
    page returns empty (or ZKILL_MAX_PAGES is reached as a safety cap).

    Returns list of {killmail_id, zkb: {hash, totalValue, ...}} entries,
    newest-first as returned by the API.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    start_time_str = cutoff.strftime("%Y%m%d%H%M")

    all_entries: list = []
    page = 1

    while page <= config.ZKILL_MAX_PAGES:
        _wait_zkill()
        url = f"{config.ZKILLBOARD_BASE}/kills/startTime/{start_time_str}/page/{page}/"
        try:
            r = _get_session().get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as e:
            logger.error("zKillboard history page %d failed: %s", page, e)
            break

        if not data:
            logger.info("zKillboard history: empty page %d — done", page)
            break

        all_entries.extend(data)
        logger.info(
            "zKillboard history page %d: %d entries (total so far: %d)",
            page, len(data), len(all_entries),
        )
        page += 1
    else:
        logger.warning(
            "Reached ZKILL_MAX_PAGES=%d — some history may be missing. "
            "Increase ZKILL_MAX_PAGES in config.py if needed.",
            config.ZKILL_MAX_PAGES,
        )

    return all_entries


def fetch_redisq_once() -> Optional[dict]:
    """
    Poll RedisQ for one killmail package. Uses ?ttw=10 for a 10-second server
    wait before returning empty.
    """
    _wait_zkill()
    try:
        r = _get_session().get(config.ZKILLBOARD_REDISQ, params={"ttw": 10}, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data.get("package")
    except requests.RequestException as e:
        logger.error("RedisQ poll failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# ESI — synchronous helpers (used for system/type lookups)
# ---------------------------------------------------------------------------

def fetch_esi_killmail(killmail_id: int, killmail_hash: str) -> Optional[dict]:
    return _esi_get(f"/killmails/{killmail_id}/{killmail_hash}/")


def fetch_esi_names(ids: list) -> dict:
    """
    POST /universe/names/ — resolve up to 1000 IDs at once.
    Returns {id: name} dict.
    """
    if not ids:
        return {}
    result = {}
    for chunk_start in range(0, len(ids), 1000):
        chunk = ids[chunk_start:chunk_start + 1000]
        _wait_esi()
        url = f"{config.ESI_BASE}/universe/names/"
        try:
            r = _get_session().post(url, json=chunk, timeout=15)
            if r.status_code == 404:
                for id_ in chunk:
                    info = _esi_get(f"/universe/types/{id_}/")
                    if info:
                        result[info["type_id"]] = info.get("name", "")
                continue
            r.raise_for_status()
            for entry in r.json():
                result[entry["id"]] = entry["name"]
        except requests.RequestException as e:
            logger.error("ESI /universe/names/ failed: %s", e)
    return result


def fetch_esi_type(type_id: int) -> Optional[dict]:
    return _esi_get(f"/universe/types/{type_id}/")


def fetch_esi_system(system_id: int) -> Optional[dict]:
    return _esi_get(f"/universe/systems/{system_id}/")


# ---------------------------------------------------------------------------
# Type cache resolution
# ---------------------------------------------------------------------------

def resolve_and_cache_types(conn, type_ids: list) -> None:
    """
    Resolve uncached type_ids via ESI /universe/names/ (bulk POST, 1000 IDs at a time)
    and store names in type_cache.

    group_id and category_id are schema columns kept for future use but are not
    queried by any current code, so we skip the per-type /universe/types/{id}/ calls
    that would otherwise cost ~400 ms * N sequential requests.
    """
    uncached = db.get_uncached_type_ids(conn, type_ids)
    if not uncached:
        return

    names = fetch_esi_names(uncached)

    types_to_insert = [
        {"type_id": type_id, "name": name, "group_id": None, "category_id": None}
        for type_id, name in names.items()
    ]

    if types_to_insert:
        db.bulk_upsert_types(conn, types_to_insert)


def resolve_and_cache_characters(conn, character_ids: list) -> None:
    """Resolve uncached character IDs via ESI /universe/names/ and store in character_cache."""
    uncached = db.get_uncached_character_ids(conn, character_ids)
    if not uncached:
        return

    names = fetch_esi_names(uncached)
    characters_to_insert = [
        {"character_id": character_id, "name": name}
        for character_id, name in names.items()
    ]

    if characters_to_insert:
        db.bulk_upsert_characters(conn, characters_to_insert)


# ---------------------------------------------------------------------------
# ESI — async batch fetching
# ---------------------------------------------------------------------------

async def _fetch_esi_km_async(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    killmail_id: int,
    killmail_hash: str,
) -> Optional[dict]:
    """Fetch one ESI killmail asynchronously, bounded by the shared semaphore."""
    url = f"{config.ESI_BASE}/killmails/{killmail_id}/{killmail_hash}/"
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status in (404, 422):
                    logger.warning("ESI %d for killmail %d, skipping", r.status, killmail_id)
                    return None
                r.raise_for_status()
                return await r.json()
        except Exception as e:
            logger.error("Async ESI fetch failed for killmail %d: %s", killmail_id, e)
            return None


async def _batch_fetch_esi_async(entries: list) -> list:
    """
    Concurrently fetch ESI killmail data for all entries using aiohttp.

    Returns list of (zkill_entry, esi_data | None) pairs in the same order
    as the input entries.
    """
    sem = asyncio.Semaphore(config.ESI_CONCURRENCY)
    headers = {"User-Agent": config.USER_AGENT, "Accept": "application/json"}

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            _fetch_esi_km_async(session, sem, e["killmail_id"], e["zkb"]["hash"])
            for e in entries
        ]
        results = await asyncio.gather(*tasks)

    return list(zip(entries, results))


# ---------------------------------------------------------------------------
# Ingest pipeline — shared core
# ---------------------------------------------------------------------------

def _ingest_esi_data(conn, zkill_entry: dict, km_data: dict) -> tuple:
    """
    Ingest a pre-fetched ESI killmail into the database.

    Returns (newly_inserted: bool, type_ids: set, character_ids: set) so callers can defer
    type/character resolution to a single bulk call after a whole batch.
    """
    killmail_id = zkill_entry["killmail_id"]
    zkb = zkill_entry.get("zkb", {})

    solar_system_id = km_data["solar_system_id"]

    # System info — cache-first, synchronous ESI call only on cache miss
    system = db.get_cached_system(conn, solar_system_id)
    if system is None:
        sys_data = fetch_esi_system(solar_system_id)
        if sys_data:
            sys_data["system_id"] = sys_data.get("system_id", solar_system_id)
            db.upsert_system(conn, sys_data)
            security_status = sys_data["security_status"]
        else:
            security_status = 0.0
    else:
        security_status = system["security_status"]

    victim = km_data.get("victim", {})
    attackers = km_data.get("attackers", [])
    items = victim.get("items", [])

    is_candidate = gank.is_phase1_gank_candidate(security_status, attackers)

    newly_inserted = db.upsert_killmail(
        conn,
        killmail_id=killmail_id,
        killmail_hash=zkb.get("hash", ""),
        killmail_time=km_data["killmail_time"],
        solar_system_id=solar_system_id,
        total_value=zkb.get("totalValue", 0.0),
        dropped_value=zkb.get("droppedValue", 0.0),
        security_status=security_status,
        is_gank_candidate=is_candidate,
    )

    if not newly_inserted:
        return False, set(), set()

    db.insert_victim(conn, killmail_id, victim)
    db.insert_attackers(conn, killmail_id, attackers)
    db.insert_items(conn, killmail_id, items)

    # Collect IDs for deferred bulk resolution
    type_ids: set = set()
    character_ids: set = set()
    if victim.get("ship_type_id"):
        type_ids.add(victim["ship_type_id"])
    if victim.get("character_id"):
        character_ids.add(victim["character_id"])
    for a in attackers:
        if a.get("ship_type_id"):
            type_ids.add(a["ship_type_id"])
        if a.get("character_id"):
            character_ids.add(a["character_id"])
    for i in items:
        if i.get("item_type_id"):
            type_ids.add(i["item_type_id"])

    return True, type_ids, character_ids


# ---------------------------------------------------------------------------
# Single-kill ingest (kept for RedisQ streaming / recent-only sync)
# ---------------------------------------------------------------------------

def ingest_killmail(conn, zkill_entry: dict) -> bool:
    """
    Full ingest pipeline for one zKillboard entry (sequential ESI fetch).
    Returns True if newly ingested, False if skipped.
    """
    killmail_id = zkill_entry.get("killmail_id")
    zkb = zkill_entry.get("zkb", {})
    killmail_hash = zkb.get("hash")

    if not killmail_id or not killmail_hash:
        logger.warning("zKill entry missing id or hash: %s", zkill_entry)
        return False

    existing = conn.execute(
        "SELECT killmail_id FROM killmails WHERE killmail_id = ?", (killmail_id,)
    ).fetchone()
    if existing:
        return False

    km_data = fetch_esi_killmail(killmail_id, killmail_hash)
    if km_data is None:
        return False

    ingested, type_ids, character_ids = _ingest_esi_data(conn, zkill_entry, km_data)
    if ingested and type_ids:
        resolve_and_cache_types(conn, list(type_ids))
    if ingested and character_ids:
        resolve_and_cache_characters(conn, list(character_ids))
    return ingested


def sync_recent_kills(conn, count: int = 50) -> dict:
    """
    Fetch recent kills from zKillboard and ingest each one (sequential).
    Returns summary dict.
    """
    summary = {"fetched": 0, "ingested": 0, "skipped": 0, "errors": 0}

    zkill_entries = fetch_zkill_recent(limit=count)
    summary["fetched"] = len(zkill_entries)

    for entry in zkill_entries:
        try:
            ingested = ingest_killmail(conn, entry)
            if ingested:
                summary["ingested"] += 1
            else:
                summary["skipped"] += 1
        except Exception as e:
            logger.error("Error ingesting killmail %s: %s", entry.get("killmail_id"), e)
            summary["errors"] += 1

    conn.commit()
    return summary


# ---------------------------------------------------------------------------
# Batch ingest (new high-performance path)
# ---------------------------------------------------------------------------

def sync_kills_batch(conn, days_back: int = 1) -> dict:
    """
    Batch-download all kills from the past `days_back` days.

    Flow:
      1. Paginate zKillboard startTime API to collect all kill stubs.
      2. Filter out killmails already in the DB (cheap set lookup).
      3. Fetch ESI killmail data concurrently via aiohttp (ESI_CONCURRENCY
         simultaneous connections), committing every INGEST_BATCH_SIZE kills.
      4. Bulk-resolve all new type IDs at the end via /universe/names/.

    Returns summary dict with fetched/new/ingested/skipped/errors counts.
    """
    summary = {"fetched": 0, "new": 0, "ingested": 0, "skipped": 0, "errors": 0}

    # 1. Fetch stubs from zKillboard
    logger.info("Fetching zKillboard history for past %d day(s)...", days_back)
    all_entries = fetch_zkill_history(days_back)
    summary["fetched"] = len(all_entries)
    logger.info("zKillboard returned %d total entries", len(all_entries))

    # 2. Filter already-known killmails
    known_ids = {
        row[0]
        for row in conn.execute("SELECT killmail_id FROM killmails").fetchall()
    }
    new_entries = [e for e in all_entries if e["killmail_id"] not in known_ids]
    summary["new"] = len(new_entries)
    summary["skipped"] = len(all_entries) - len(new_entries)
    logger.info(
        "%d new killmails to fetch; %d already in DB",
        len(new_entries), summary["skipped"],
    )

    if not new_entries:
        return summary

    # 3. Concurrent ESI fetch + ingest in INGEST_BATCH_SIZE chunks
    all_type_ids: set = set()
    all_character_ids: set = set()
    batch_size = config.INGEST_BATCH_SIZE

    for batch_start in range(0, len(new_entries), batch_size):
        batch = new_entries[batch_start:batch_start + batch_size]
        batch_end = min(batch_start + batch_size, len(new_entries))
        logger.info(
            "ESI batch fetch: kills %d–%d / %d",
            batch_start + 1, batch_end, len(new_entries),
        )

        pairs = asyncio.run(_batch_fetch_esi_async(batch))

        for zkill_entry, esi_data in pairs:
            if esi_data is None:
                summary["errors"] += 1
                continue
            try:
                ingested, type_ids, character_ids = _ingest_esi_data(conn, zkill_entry, esi_data)
                if ingested:
                    summary["ingested"] += 1
                    all_type_ids |= type_ids
                    all_character_ids |= character_ids
                else:
                    summary["skipped"] += 1
            except Exception as e:
                logger.error(
                    "Ingest error for killmail %d: %s",
                    zkill_entry.get("killmail_id"), e,
                )
                summary["errors"] += 1

        conn.commit()
        logger.info(
            "Batch committed — %d ingested so far, %d errors",
            summary["ingested"], summary["errors"],
        )

    # 4. Bulk type resolution (all new types from the whole batch at once)
    if all_type_ids:
        logger.info("Resolving %d distinct type IDs via ESI /universe/names/...", len(all_type_ids))
        resolve_and_cache_types(conn, list(all_type_ids))
    if all_character_ids:
        logger.info("Resolving %d distinct character IDs via ESI /universe/names/...", len(all_character_ids))
        resolve_and_cache_characters(conn, list(all_character_ids))
        conn.commit()

    logger.info(
        "sync_kills_batch complete: fetched=%d new=%d ingested=%d skipped=%d errors=%d",
        summary["fetched"], summary["new"], summary["ingested"],
        summary["skipped"], summary["errors"],
    )
    return summary


# ---------------------------------------------------------------------------
# Market prices — Jita IV-4 minimum sell prices via ESI market orders
# ---------------------------------------------------------------------------

JITA_REGION_ID  = 10000002   # The Forge
JITA_STATION_ID = 60003760   # Jita IV - Moon 4 - Caldari Navy Assembly Plant


async def _fetch_jita_prices_async(progress: Optional[dict] = None) -> dict:
    """
    Fetch minimum Jita sell price for every item currently listed at
    Jita IV-4 (station 60003760).

    Paginates GET /markets/10000002/orders/?order_type=sell with up to
    MARKET_ORDER_CONCURRENCY concurrent requests, reads X-Pages from the
    first response to know total page count, then gathers all remaining
    pages in parallel. Returns {type_id: min_sell_price}.
    """
    url = f"{config.ESI_BASE}/markets/{JITA_REGION_ID}/orders/"
    headers = {"User-Agent": config.USER_AGENT, "Accept": "application/json"}
    sem = asyncio.Semaphore(config.MARKET_ORDER_CONCURRENCY)

    async with aiohttp.ClientSession(headers=headers) as session:
        # Page 1: fetch data + discover total page count from X-Pages header
        async with session.get(
            url,
            params={"order_type": "sell", "page": 1},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            n_pages = int(r.headers.get("X-Pages", 1))
            page1 = await r.json() if r.status == 200 else []

        logger.info("Jita market orders: %d page(s) to fetch", n_pages)
        if progress is not None:
            progress.update(message=f"Fetching Jita sell prices — {n_pages} pages…")

        pages_done = 1

        async def _get_page(page: int) -> list:
            nonlocal pages_done
            async with sem:
                try:
                    async with session.get(
                        url,
                        params={"order_type": "sell", "page": page},
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as r:
                        result = await r.json() if r.status == 200 else []
                except Exception as e:
                    logger.warning("Jita market page %d failed: %s", page, e)
                    result = []
            pages_done += 1
            if progress is not None:
                progress.update(
                    message=f"Fetching Jita sell prices — {pages_done}/{n_pages} pages…"
                )
            return result

        remaining = await asyncio.gather(*[_get_page(p) for p in range(2, n_pages + 1)])

    # Consolidate: minimum sell price per type at Jita station
    prices: dict = {}
    for order in page1:
        if order.get("location_id") == JITA_STATION_ID:
            t, p = order["type_id"], order["price"]
            if t not in prices or p < prices[t]:
                prices[t] = p
    for page_orders in remaining:
        for order in page_orders:
            if order.get("location_id") == JITA_STATION_ID:
                t, p = order["type_id"], order["price"]
                if t not in prices or p < prices[t]:
                    prices[t] = p

    logger.info("Jita sell prices consolidated: %d distinct type(s)", len(prices))
    return prices


def fetch_market_prices(force: bool = False, progress: Optional[dict] = None) -> dict:
    """
    Fetch minimum Jita IV-4 sell prices for all currently listed items.

    Uses ESI /markets/10000002/orders/?order_type=sell (paginated, parallel),
    filters for location_id 60003760, and returns {type_id: min_sell_price}.
    Results are cached in-process for _MARKET_PRICES_TTL seconds (1 hour).
    Pass force=True to bypass the cache and re-fetch immediately.
    """
    global _market_prices, _market_prices_fetched_at
    now = time.monotonic()
    if not force and _market_prices and (now - _market_prices_fetched_at) < _MARKET_PRICES_TTL:
        return _market_prices

    try:
        _market_prices = asyncio.run(_fetch_jita_prices_async(progress=progress))
        _market_prices_fetched_at = time.monotonic()
        logger.info("Jita price cache updated: %d items", len(_market_prices))
    except Exception as e:
        logger.error("Failed to fetch Jita market prices: %s", e)
        # Fall through returning stale cache (or empty dict on first failure)

    return _market_prices


def _calc_kill_value(km_data: dict, prices: dict) -> tuple:
    """
    Calculate (total_value, dropped_value) for a killmail using ESI adjusted prices.

    total_value  = ship hull + all destroyed items + all dropped items
    dropped_value = dropped items only

    Uses minimum Jita IV-4 sell prices. Items not listed in Jita will have
    a price of 0 ISK.
    """
    victim = km_data.get("victim", {})
    items = victim.get("items", [])

    ship_price = prices.get(victim.get("ship_type_id", 0), 0.0)

    dropped = 0.0
    destroyed = 0.0
    for item in items:
        p = prices.get(item.get("item_type_id", 0), 0.0)
        dropped += p * item.get("quantity_dropped", 0)
        destroyed += p * item.get("quantity_destroyed", 0)

    return ship_price + dropped + destroyed, dropped


# ---------------------------------------------------------------------------
# EVE Ref bulk ingest — one archive per day, zero per-kill ESI calls
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Solar system cache — bulk prefetch
# ---------------------------------------------------------------------------

def _prefetch_all_systems(conn, progress: Optional[dict] = None) -> dict:
    """
    Ensure every EVE solar system is in the local system_cache.

    1. GET /universe/systems/ — single request, returns all ~8 285 system IDs.
    2. Parallel-fetch /universe/systems/{id}/ for any IDs not yet cached
       (40 concurrent workers).
    3. Upsert results and return the complete {system_id: security_status} map.

    On the first ever sync this takes ~30–60 s.  All subsequent syncs skip
    the fetch entirely (or add only the handful of new systems CCP released).
    """
    def _p(**kw):
        if progress is not None:
            progress.update(kw)

    _p(phase="fetching_systems", message="Fetching EVE system list from ESI…")
    all_ids = _esi_get("/universe/systems/")
    if not all_ids:
        logger.warning("_prefetch_all_systems: /universe/systems/ returned nothing — using DB cache only")
        return dict(conn.execute("SELECT system_id, security_status FROM system_cache").fetchall())

    cached = {row[0] for row in conn.execute("SELECT system_id FROM system_cache").fetchall()}
    missing = [sid for sid in all_ids if sid not in cached]

    if missing:
        logger.info(
            "_prefetch_all_systems: fetching %d uncached systems (%d already cached, %d total in game)",
            len(missing), len(cached), len(all_ids),
        )
        _p(message=f"Fetching {len(missing):,} solar systems from ESI…")

        def _fetch_one(sid):
            return sid, fetch_esi_system(sid)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=40, thread_name_prefix="sys-prefetch"
        ) as pool:
            results = list(pool.map(_fetch_one, missing))

        for sid, sys_data in results:
            if sys_data:
                db.upsert_system(conn, sys_data)
        conn.commit()
        logger.info("_prefetch_all_systems: done — system cache now complete")
    else:
        logger.info("_prefetch_all_systems: system cache already complete (%d systems)", len(cached))

    return dict(conn.execute("SELECT system_id, security_status FROM system_cache").fetchall())


EVEREF_BASE = "https://data.everef.net/killmails"


def _parse_everef_archive(data: bytes, date_str: str) -> list:
    """Parse a tar.bz2 blob into a list of ESI killmail dicts."""
    killmails = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:bz2") as tar:
        for member in tar.getmembers():
            if not member.name.endswith(".json"):
                continue
            fobj = tar.extractfile(member)
            if fobj is None:
                continue
            try:
                killmails.append(json.loads(fobj.read()))
            except json.JSONDecodeError:
                pass
    logger.info("EVE Ref: %s → %d killmails parsed", date_str, len(killmails))
    return killmails


_DL_CHUNK = 1 << 18  # 256 KB read buffer


def _download_everef_day(date_str: str, file_progress: Optional[dict] = None) -> tuple:
    """
    Download and parse one EVE Ref daily archive using a single connection.

    Streams in _DL_CHUNK-sized pieces so file_progress[date_str] is updated
    continuously for live per-file progress bars.

    file_progress: shared dict updated in-place; key is date_str, value is
        {"bytes_done": int, "bytes_total": int, "done": bool}.
        Safe to call from threads — only this thread writes to its own key.

    Returns (killmails: list, bytes_downloaded: int).
    Returns ([], 0) on 404 (future/missing date) or error.
    """
    year = date_str[:4]
    url = f"{EVEREF_BASE}/{year}/killmails-{date_str}.tar.bz2"
    logger.info("EVE Ref: downloading %s", url)

    if file_progress is not None:
        file_progress[date_str] = {"bytes_done": 0, "bytes_total": 0, "done": False}

    req = urllib.request.Request(url, headers={"User-Agent": config.USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            total = int(resp.headers.get("Content-Length") or 0)
            if file_progress is not None:
                file_progress[date_str]["bytes_total"] = total

            chunks = []
            bytes_done = 0
            while True:
                chunk = resp.read(_DL_CHUNK)
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_done += len(chunk)
                if file_progress is not None:
                    file_progress[date_str]["bytes_done"] = bytes_done

            data = b"".join(chunks)

    except urllib.error.HTTPError as e:
        if file_progress is not None:
            file_progress[date_str]["done"] = True
        if e.code == 404:
            logger.info("EVE Ref: no archive for %s (404 — future or missing date)", date_str)
            return [], 0
        logger.error("EVE Ref: HTTP %s for %s", e.code, date_str)
        return [], 0
    except Exception as e:
        logger.error("EVE Ref: download failed for %s: %s", date_str, e)
        if file_progress is not None:
            file_progress[date_str]["done"] = True
        return [], 0

    n_bytes = len(data)
    logger.info("EVE Ref: %.1f MB downloaded for %s, extracting...", n_bytes / 1024 / 1024, date_str)
    if file_progress is not None:
        file_progress[date_str].update({"bytes_done": n_bytes, "bytes_total": n_bytes, "done": True})
    return _parse_everef_archive(data, date_str), n_bytes


def _prepare_kill_rows(km: dict, prices: dict, system_sec_map: dict) -> dict:
    """
    Pure-Python data transformation for one EVE Ref killmail — no DB or network I/O.

    Reads from pre-loaded in-memory dicts only, making it safe to run concurrently
    in a ThreadPoolExecutor. Returns a dict with row tuples ready for bulk executemany.
    """
    killmail_id     = km["killmail_id"]
    solar_system_id = km["solar_system_id"]
    security_status = system_sec_map.get(solar_system_id, 0.0)
    victim          = km.get("victim", {})
    attackers       = km.get("attackers", [])
    items           = victim.get("items", [])

    total_value, dropped_value = _calc_kill_value(km, prices)
    is_candidate = gank.is_phase1_gank_candidate(security_status, attackers)
    fetched_at   = datetime.now(timezone.utc).isoformat()

    km_row = (
        killmail_id, "",                    # hash unavailable from EVE Ref
        km["killmail_time"], solar_system_id,
        total_value, dropped_value, security_status,
        int(is_candidate), fetched_at,
    )
    victim_row = (
        killmail_id,
        victim.get("character_id"),
        victim.get("corporation_id"),
        victim.get("alliance_id"),
        victim.get("ship_type_id", 0),
        victim.get("damage_taken", 0),
    )
    attacker_rows = [
        (
            killmail_id,
            a.get("character_id"),
            a.get("corporation_id"),
            a.get("alliance_id"),
            a.get("ship_type_id"),
            a.get("damage_done", 0),
            int(bool(a.get("final_blow"))),
            a.get("security_status"),
            int(a.get("corporation_id") == config.CONCORD_CORP_ID),
        )
        for a in attackers
    ]
    item_rows = [
        (
            killmail_id,
            i["item_type_id"],
            i.get("quantity_dropped", 0),
            i.get("quantity_destroyed", 0),
            i.get("flag", 0),
            i.get("singleton", 0),
        )
        for i in items
    ]
    type_ids = (
        ({victim["ship_type_id"]} if victim.get("ship_type_id") else set())
        | {a["ship_type_id"] for a in attackers if a.get("ship_type_id")}
        | {i["item_type_id"]  for i in items    if i.get("item_type_id")}
    )
    character_ids = (
        ({victim["character_id"]} if victim.get("character_id") else set())
        | {a["character_id"] for a in attackers if a.get("character_id")}
    )
    return {
        "killmail_id":    killmail_id,
        "km_row":         km_row,
        "victim_row":     victim_row,
        "attacker_rows":  attacker_rows,
        "item_rows":      item_rows,
        "type_ids":       type_ids,
        "character_ids":  character_ids,
    }


def sync_kills_everef(
    conn,
    days_back: int = 1,
    progress: Optional[dict] = None,
    cancel_event=None,
) -> dict:
    """
    Bulk-download killmails from EVE Ref (data.everef.net).

    One tar.bz2 archive per day (~3-15 MB, ~10-20k kills) replaces tens of
    thousands of individual ESI calls. Full victim/attacker/item data is included.

    ISK values are calculated locally using ESI /markets/prices/ (one bulk call).
    Killmail hashes are unavailable from EVE Ref and stored as empty strings.

    Performance:
    - Downloads: all daily archives fetched in parallel (EVEREF_DL_CONNECTIONS
      workers, one full connection per file).
    - Ingestion: up to EVEREF_INGEST_WORKERS days prepared concurrently — each
      worker waits for its download future then filters + builds row tuples from
      in-memory dicts. DB writes remain sequential (SQLite single-writer).
    - 4 bulk executemany calls per day instead of ~90k individual INSERTs.

    cancel_event: optional threading.Event — set it to abort the sync cleanly.
    progress: optional dict updated in-place for live status reporting.
    Returns summary dict with fetched/new/ingested/skipped/errors counts.
    """
    summary = {"fetched": 0, "new": 0, "ingested": 0, "skipped": 0, "errors": 0}

    def _cancelled() -> bool:
        return cancel_event is not None and cancel_event.is_set()

    def _prog(**kwargs):
        if progress is not None:
            progress.update(kwargs)

    today = datetime.now(timezone.utc).date()
    dates = [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(1, days_back + 1)  # start from yesterday; today's archive isn't published yet
    ]

    # Fetch Jita sell prices — paginated async fetch, cached for 1 hour
    _prog(phase="fetching_prices", message="Fetching Jita sell prices from ESI…")
    prices = fetch_market_prices(progress=progress)
    logger.info("Market prices ready (%d items)", len(prices))

    # Bulk-prefetch every EVE solar system before touching killmails.
    # On first run this fetches ~8 285 systems via ESI; subsequent runs are instant.
    system_sec_map: dict = _prefetch_all_systems(conn, progress)
    logger.info("System cache ready: %d systems", len(system_sec_map))

    # Load all known killmail IDs up front for fast dedup
    known_ids: set = {
        row[0] for row in conn.execute("SELECT killmail_id FROM killmails").fetchall()
    }

    all_type_ids: set = set()
    all_character_ids: set = set()

    # -----------------------------------------------------------------------
    # Stage 1: download all archives in parallel (EVEREF_DL_CONNECTIONS workers).
    # Stage 2: prepare rows in parallel (EVEREF_INGEST_WORKERS workers) — each
    #          worker waits for its download future then filters + builds row
    #          tuples from in-memory dicts. Safe to run concurrently because
    #          all shared data (prices, system_sec_map, known_ids) is read-only
    #          at this point.
    # Stage 3: DB writes (main thread, sequential — SQLite single-writer).
    # -----------------------------------------------------------------------
    n_dl_workers     = min(len(dates), config.EVEREF_DL_CONNECTIONS)
    n_ingest_workers = min(len(dates), config.EVEREF_INGEST_WORKERS)
    logger.info(
        "EVE Ref: %d date(s), %d download workers, %d ingest workers",
        len(dates), n_dl_workers, n_ingest_workers,
    )

    # Shared dicts updated in real-time by worker threads for live progress bars
    dl_file_progress: dict = {}     # keyed by date_str; bytes_done/bytes_total/done
    ingest_day_progress: dict = {}  # keyed by date_str; kills_done/kills_total/done
    _prog(
        phase="downloading",
        message=f"Downloading {len(dates)} day archive(s) in parallel…",
        dl_files_total=len(dates),
        dl_files_done=0,
        dl_bytes_done=0.0,
        dl_rate_mbps=0.0,
        dl_file_progress=dl_file_progress,
        ingest_total=len(dates),
        ingest_done=0,
        ingest_day_progress=ingest_day_progress,
    )

    dl_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=n_dl_workers, thread_name_prefix="everef-dl"
    )
    dl_fn = functools.partial(_download_everef_day, file_progress=dl_file_progress)
    futures_in_order = [dl_pool.submit(dl_fn, d) for d in dates]
    dl_pool.shutdown(wait=False)

    def _ingest_day(date_str: str, dl_future) -> tuple:
        """
        Ingest worker: block until the download future resolves, filter out
        already-known kills, and build row tuples from in-memory dicts.
        Read-only access to shared objects — safe to run concurrently.
        Updates ingest_day_progress[date_str] in real-time.
        Returns (date_str, n_bytes, total_kills, already_known, prepared_rows).
        """
        kills, n_bytes = dl_future.result()
        new_kills = [km for km in kills if km["killmail_id"] not in known_ids]
        already_known = len(kills) - len(new_kills)

        ingest_day_progress[date_str] = {
            "kills_done": 0,
            "kills_total": len(new_kills),
            "done": False,
        }

        if not new_kills:
            ingest_day_progress[date_str]["done"] = True
            return date_str, n_bytes, len(kills), already_known, []

        fn = functools.partial(_prepare_kill_rows, prices=prices, system_sec_map=system_sec_map)
        step = max(1, len(new_kills) // 100)  # up to 100 progress updates per day
        prepared = []
        for i, km in enumerate(new_kills):
            prepared.append(fn(km))
            if (i + 1) % step == 0:
                ingest_day_progress[date_str]["kills_done"] = i + 1
        ingest_day_progress[date_str]["kills_done"] = len(prepared)
        # done=True is set by the main thread after the DB commit
        return date_str, n_bytes, len(kills), already_known, prepared

    ingest_pool = concurrent.futures.ThreadPoolExecutor(
        max_workers=n_ingest_workers, thread_name_prefix="everef-ingest"
    )

    # Keep ingestion workers bounded: never queue all days at once.
    # This prevents large prepared row payloads from piling up in memory while
    # the single DB writer thread drains results.
    pending_days = list(zip(dates, futures_in_order))
    in_flight: dict = {}

    def _fill_ingest_queue() -> None:
        while pending_days and len(in_flight) < n_ingest_workers:
            d, f = pending_days.pop(0)
            fut = ingest_pool.submit(_ingest_day, d, f)
            in_flight[fut] = d

    _fill_ingest_queue()

    # Rate tracking — ingestion
    ingest_records_done: int = 0  # total records ingested (for rate badge)
    ingest_days_done: int = 0     # days processed (for progress bar)
    ingest_cp_time: float = time.monotonic()
    ingest_cp_done: int = 0

    ingested_any = False

    while in_flight:
        done, _ = concurrent.futures.wait(
            in_flight.keys(),
            return_when=concurrent.futures.FIRST_COMPLETED,
        )
        ingest_future = next(iter(done))
        date_str = in_flight.pop(ingest_future)

        if _cancelled():
            ingest_future.cancel()
            _fill_ingest_queue()
            continue

        date_str, n_bytes, total_kills, already_known, prepared = ingest_future.result()

        summary["fetched"] += total_kills
        summary["skipped"] += already_known
        logger.info(
            "EVE Ref %s: %d total, %d new, %d already in DB",
            date_str, total_kills, total_kills - already_known, already_known,
        )

        ingest_days_done += 1

        if not prepared:
            _prog(ingest_done=ingest_days_done, message=f"{date_str}: no new kills")
            _fill_ingest_queue()
            continue

        if _cancelled():
            _fill_ingest_queue()
            continue

        # Bulk write: 4 executemany calls instead of ~90k individual INSERT calls
        _prog(
            phase="ingesting",
            message=f"{date_str}: writing {len(prepared):,} kills to DB…",
        )
        db.bulk_insert_killmails(conn, [p["km_row"]     for p in prepared])
        db.bulk_insert_victims  (conn, [p["victim_row"] for p in prepared])
        db.bulk_insert_attackers(conn, [r for p in prepared for r in p["attacker_rows"]])
        db.bulk_insert_items    (conn, [r for p in prepared for r in p["item_rows"]])
        conn.commit()
        ingest_day_progress[date_str]["done"] = True

        for p in prepared:
            all_type_ids |= p["type_ids"]
            all_character_ids |= p["character_ids"]
            known_ids.add(p["killmail_id"])

        summary["ingested"] += len(prepared)
        summary["new"]      += len(prepared)
        ingested_any = True

        ingest_records_done += len(prepared)
        now = time.monotonic()
        dt = now - ingest_cp_time
        ingest_rate = (ingest_records_done - ingest_cp_done) / dt if dt > 0.05 else 0.0
        ingest_cp_time = now
        ingest_cp_done = ingest_records_done
        _prog(
            ingest_done=ingest_days_done,
            ingest_records_done=ingest_records_done,
            ingest_rate_rps=round(max(0.0, ingest_rate)),
            message=f"{date_str}: done ({len(prepared):,} kills ingested)",
        )
        logger.info("EVE Ref %s: %d kills committed", date_str, len(prepared))

        _fill_ingest_queue()

    ingest_pool.shutdown(wait=False)

    # Handle cancellation — mark done here so app.py _run() can check progress["cancelled"]
    if _cancelled():
        _prog(
            done=True, cancelled=True, phase="cancelled",
            message="Cancelled.",
        )
        logger.info(
            "sync_kills_everef cancelled: ingested=%d skipped=%d so far",
            summary["ingested"], summary["skipped"],
        )
        return summary

    if ingested_any and all_type_ids:
        _prog(
            phase="resolving_types",
            message=f"Resolving {len(all_type_ids):,} ship/item type names…",
        )
        logger.info("Resolving %d type IDs via ESI /universe/names/...", len(all_type_ids))
        resolve_and_cache_types(conn, list(all_type_ids))
        conn.commit()

    if ingested_any and all_character_ids:
        _prog(
            phase="resolving_characters",
            message=f"Resolving {len(all_character_ids):,} character names…",
        )
        logger.info("Resolving %d character IDs via ESI /universe/names/...", len(all_character_ids))
        resolve_and_cache_characters(conn, list(all_character_ids))
        conn.commit()

    logger.info(
        "sync_kills_everef complete: fetched=%d new=%d ingested=%d skipped=%d errors=%d",
        summary["fetched"], summary["new"], summary["ingested"],
        summary["skipped"], summary["errors"],
    )
    return summary
