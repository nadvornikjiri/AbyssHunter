import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional
import config

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS killmails (
    killmail_id         INTEGER PRIMARY KEY,
    killmail_hash       TEXT    NOT NULL,
    killmail_time       TEXT    NOT NULL,
    solar_system_id     INTEGER NOT NULL,
    total_value         REAL    DEFAULT 0,
    dropped_value       REAL    DEFAULT 0,
    security_status     REAL,
    is_gank_candidate   INTEGER DEFAULT 0,
    is_confirmed_gank   INTEGER DEFAULT 0,
    concord_killmail_id INTEGER DEFAULT NULL,
    fetched_at          TEXT    NOT NULL,
    FOREIGN KEY (concord_killmail_id) REFERENCES killmails(killmail_id)
);

CREATE TABLE IF NOT EXISTS victims (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    killmail_id     INTEGER NOT NULL UNIQUE,
    character_id    INTEGER,
    corporation_id  INTEGER,
    alliance_id     INTEGER,
    ship_type_id    INTEGER NOT NULL,
    damage_taken    INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (killmail_id) REFERENCES killmails(killmail_id)
);

CREATE TABLE IF NOT EXISTS attackers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    killmail_id     INTEGER NOT NULL,
    character_id    INTEGER,
    corporation_id  INTEGER,
    alliance_id     INTEGER,
    ship_type_id    INTEGER,
    damage_done     INTEGER NOT NULL DEFAULT 0,
    final_blow      INTEGER NOT NULL DEFAULT 0,
    security_status REAL,
    is_concord      INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (killmail_id) REFERENCES killmails(killmail_id)
);

CREATE TABLE IF NOT EXISTS items (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    killmail_id        INTEGER NOT NULL,
    item_type_id       INTEGER NOT NULL,
    quantity_dropped   INTEGER NOT NULL DEFAULT 0,
    quantity_destroyed INTEGER NOT NULL DEFAULT 0,
    flag               INTEGER NOT NULL DEFAULT 0,
    singleton          INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (killmail_id) REFERENCES killmails(killmail_id)
);

CREATE TABLE IF NOT EXISTS type_cache (
    type_id     INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    group_id    INTEGER,
    category_id INTEGER,
    cached_at   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS system_cache (
    system_id        INTEGER PRIMARY KEY,
    name             TEXT    NOT NULL,
    security_status  REAL    NOT NULL,
    security_class   TEXT    NOT NULL,
    constellation_id INTEGER,
    region_id        INTEGER,
    cached_at        TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS character_cache (
    character_id INTEGER PRIMARY KEY,
    name         TEXT    NOT NULL,
    cached_at    TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_killmails_time     ON killmails(killmail_time DESC);
CREATE INDEX IF NOT EXISTS idx_killmails_sec      ON killmails(security_status);
CREATE INDEX IF NOT EXISTS idx_killmails_gank     ON killmails(is_confirmed_gank, is_gank_candidate);
CREATE INDEX IF NOT EXISTS idx_victims_ship       ON victims(ship_type_id);
CREATE INDEX IF NOT EXISTS idx_victims_kill       ON victims(killmail_id);
CREATE INDEX IF NOT EXISTS idx_attackers_ship     ON attackers(ship_type_id);
CREATE INDEX IF NOT EXISTS idx_attackers_kill     ON attackers(killmail_id);
CREATE INDEX IF NOT EXISTS idx_attackers_concord  ON attackers(is_concord);
CREATE INDEX IF NOT EXISTS idx_items_type         ON items(item_type_id);
CREATE INDEX IF NOT EXISTS idx_items_kill         ON items(killmail_id);
CREATE INDEX IF NOT EXISTS idx_character_name     ON character_cache(name);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db_ctx():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_db() -> sqlite3.Connection:
    """For Flask g usage — caller manages commit/close."""
    return _connect()


def init_db():
    with get_db_ctx() as conn:
        conn.executescript(SCHEMA)


# ---------------------------------------------------------------------------
# Killmail persistence
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_killmail(
    conn: sqlite3.Connection,
    killmail_id: int,
    killmail_hash: str,
    killmail_time: str,
    solar_system_id: int,
    total_value: float,
    dropped_value: float,
    security_status: float,
    is_gank_candidate: bool,
) -> bool:
    """Insert killmail row. Returns True if newly inserted, False if already existed."""
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO killmails
            (killmail_id, killmail_hash, killmail_time, solar_system_id,
             total_value, dropped_value, security_status, is_gank_candidate, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (killmail_id, killmail_hash, killmail_time, solar_system_id,
         total_value, dropped_value, security_status, int(is_gank_candidate), _now_utc()),
    )
    return cursor.rowcount == 1


def insert_victim(conn: sqlite3.Connection, killmail_id: int, victim: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO victims
            (killmail_id, character_id, corporation_id, alliance_id, ship_type_id, damage_taken)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (killmail_id,
         victim.get("character_id"),
         victim.get("corporation_id"),
         victim.get("alliance_id"),
         victim["ship_type_id"],
         victim.get("damage_taken", 0)),
    )


def insert_attackers(conn: sqlite3.Connection, killmail_id: int, attackers: list) -> None:
    rows = []
    for a in attackers:
        is_concord = int(a.get("corporation_id") == config.CONCORD_CORP_ID)
        rows.append((
            killmail_id,
            a.get("character_id"),
            a.get("corporation_id"),
            a.get("alliance_id"),
            a.get("ship_type_id"),
            a.get("damage_done", 0),
            int(bool(a.get("final_blow"))),
            a.get("security_status"),
            is_concord,
        ))
    conn.executemany(
        """
        INSERT INTO attackers
            (killmail_id, character_id, corporation_id, alliance_id, ship_type_id,
             damage_done, final_blow, security_status, is_concord)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def bulk_insert_killmails(conn: sqlite3.Connection, rows: list) -> int:
    """
    INSERT OR IGNORE a batch of killmail rows in one executemany call.
    Row tuple order: (killmail_id, killmail_hash, killmail_time, solar_system_id,
                      total_value, dropped_value, security_status, is_gank_candidate, fetched_at)
    Returns the number of rows actually inserted (ignoring duplicates).
    """
    cur = conn.executemany(
        """
        INSERT OR IGNORE INTO killmails
            (killmail_id, killmail_hash, killmail_time, solar_system_id,
             total_value, dropped_value, security_status, is_gank_candidate, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return cur.rowcount


def bulk_insert_victims(conn: sqlite3.Connection, rows: list) -> None:
    """
    INSERT OR IGNORE a batch of victim rows.
    Row tuple order: (killmail_id, character_id, corporation_id, alliance_id,
                      ship_type_id, damage_taken)
    """
    conn.executemany(
        """
        INSERT OR IGNORE INTO victims
            (killmail_id, character_id, corporation_id, alliance_id, ship_type_id, damage_taken)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def bulk_insert_attackers(conn: sqlite3.Connection, rows: list) -> None:
    """
    INSERT a batch of attacker rows.
    Row tuple order: (killmail_id, character_id, corporation_id, alliance_id, ship_type_id,
                      damage_done, final_blow, security_status, is_concord)
    """
    conn.executemany(
        """
        INSERT INTO attackers
            (killmail_id, character_id, corporation_id, alliance_id, ship_type_id,
             damage_done, final_blow, security_status, is_concord)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def bulk_insert_items(conn: sqlite3.Connection, rows: list) -> None:
    """
    INSERT a batch of item rows.
    Row tuple order: (killmail_id, item_type_id, quantity_dropped, quantity_destroyed,
                      flag, singleton)
    """
    conn.executemany(
        """
        INSERT INTO items
            (killmail_id, item_type_id, quantity_dropped, quantity_destroyed, flag, singleton)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def insert_items(conn: sqlite3.Connection, killmail_id: int, items: list) -> None:
    rows = [
        (killmail_id,
         i["item_type_id"],
         i.get("quantity_dropped", 0),
         i.get("quantity_destroyed", 0),
         i.get("flag", 0),
         i.get("singleton", 0))
        for i in items
    ]
    conn.executemany(
        """
        INSERT INTO items
            (killmail_id, item_type_id, quantity_dropped, quantity_destroyed, flag, singleton)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def get_cached_type(conn: sqlite3.Connection, type_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM type_cache WHERE type_id = ?", (type_id,)
    ).fetchone()


def get_uncached_type_ids(conn: sqlite3.Connection, type_ids: list) -> list:
    if not type_ids:
        return []
    placeholders = ",".join("?" * len(type_ids))
    cached = conn.execute(
        f"SELECT type_id FROM type_cache WHERE type_id IN ({placeholders})", type_ids
    ).fetchall()
    cached_set = {row["type_id"] for row in cached}
    return [t for t in type_ids if t not in cached_set]


def bulk_upsert_types(conn: sqlite3.Connection, types: list) -> None:
    now = _now_utc()
    conn.executemany(
        """
        INSERT OR REPLACE INTO type_cache (type_id, name, group_id, category_id, cached_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(t["type_id"], t["name"], t.get("group_id"), t.get("category_id"), now) for t in types],
    )


def get_cached_system(conn: sqlite3.Connection, system_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM system_cache WHERE system_id = ?", (system_id,)
    ).fetchone()


def upsert_system(conn: sqlite3.Connection, system_data: dict) -> None:
    sec = system_data["security_status"]
    if sec >= config.HIGHSEC_MIN:
        sec_class = "highsec"
    elif sec >= config.LOWSEC_MIN:
        sec_class = "lowsec"
    elif sec >= 0.0:
        sec_class = "nullsec"
    else:
        sec_class = "wormhole"
    conn.execute(
        """
        INSERT OR REPLACE INTO system_cache
            (system_id, name, security_status, security_class, constellation_id, region_id, cached_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (system_data["system_id"], system_data["name"], sec,
         sec_class, system_data.get("constellation_id"), system_data.get("region_id"), _now_utc()),
    )


def get_cached_character(conn: sqlite3.Connection, character_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM character_cache WHERE character_id = ?", (character_id,)
    ).fetchone()


def get_uncached_character_ids(conn: sqlite3.Connection, character_ids: list) -> list:
    if not character_ids:
        return []
    placeholders = ",".join("?" * len(character_ids))
    cached = conn.execute(
        f"SELECT character_id FROM character_cache WHERE character_id IN ({placeholders})", character_ids
    ).fetchall()
    cached_set = {row["character_id"] for row in cached}
    return [c for c in character_ids if c not in cached_set]


def bulk_upsert_characters(conn: sqlite3.Connection, characters: list) -> None:
    now = _now_utc()
    conn.executemany(
        """
        INSERT OR REPLACE INTO character_cache (character_id, name, cached_at)
        VALUES (?, ?, ?)
        """,
        [(c["character_id"], c["name"], now) for c in characters],
    )


# ---------------------------------------------------------------------------
# Kill list query (with filters)
# ---------------------------------------------------------------------------

_SORT_MAP = {
    "time":          "k.killmail_time",
    "system":        "COALESCE(sc.name, '')",
    "sec":           "k.security_status",
    "total_value":   "k.total_value",
    "dropped_value": "k.dropped_value",
    "attackers":     "(SELECT COUNT(*) FROM attackers _a WHERE _a.killmail_id = k.killmail_id)",
}


def get_killmails_page(
    conn: sqlite3.Connection,
    page: int,
    page_size: int,
    item_ids: Optional[list[int]] = None,
    item_match: str = "any",
    ship_type_id: Optional[int] = None,
    character_id: Optional[int] = None,
    system_id: Optional[int] = None,
    min_sec: Optional[float] = None,
    max_sec: Optional[float] = None,
    gank_filter: str = "all",
    sort_by: str = "time",
    sort_dir: str = "desc",
) -> tuple:
    """Return (rows, total_count) for the kill list with optional filters."""
    params = []

    where_clauses = []

    if item_ids:
        placeholders = ",".join("?" for _ in item_ids)
        if item_match == "all":
            where_clauses.extend(
                "EXISTS (SELECT 1 FROM items i WHERE i.killmail_id = k.killmail_id AND i.item_type_id = ?)"
                for _ in item_ids
            )
            params.extend(item_ids)
        else:
            where_clauses.append(
                f"EXISTS (SELECT 1 FROM items i WHERE i.killmail_id = k.killmail_id AND i.item_type_id IN ({placeholders}))"
            )
            params.extend(item_ids)

    if ship_type_id is not None:
        where_clauses.append(
            """(
                EXISTS (SELECT 1 FROM victims v WHERE v.killmail_id = k.killmail_id AND v.ship_type_id = ?)
                OR
                EXISTS (SELECT 1 FROM attackers a WHERE a.killmail_id = k.killmail_id AND a.ship_type_id = ?)
            )"""
        )
        params.extend([ship_type_id, ship_type_id])

    if character_id is not None:
        where_clauses.append(
            """(
                EXISTS (SELECT 1 FROM victims v WHERE v.killmail_id = k.killmail_id AND v.character_id = ?)
                OR
                EXISTS (SELECT 1 FROM attackers a WHERE a.killmail_id = k.killmail_id AND a.character_id = ?)
            )"""
        )
        params.extend([character_id, character_id])

    if system_id is not None:
        where_clauses.append("k.solar_system_id = ?")
        params.append(system_id)

    if min_sec is not None:
        where_clauses.append("k.security_status >= ?")
        params.append(min_sec)

    if max_sec is not None:
        where_clauses.append("k.security_status <= ?")
        params.append(max_sec)

    if gank_filter == "confirmed":
        where_clauses.append("k.is_confirmed_gank = 1")
    elif gank_filter == "candidate":
        where_clauses.append("k.is_gank_candidate = 1")

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    count_sql = f"SELECT COUNT(*) FROM killmails k {where_sql}"
    total_count = conn.execute(count_sql, params).fetchone()[0]

    # Safe sort — key must be in whitelist, direction must be asc/desc
    order_col = _SORT_MAP.get(sort_by, _SORT_MAP["time"])
    order_dir = "ASC" if sort_dir == "asc" else "DESC"
    # Always add a stable secondary sort so pagination is deterministic
    if sort_by != "time":
        order_clause = f"{order_col} {order_dir}, k.killmail_time DESC"
    else:
        order_clause = f"{order_col} {order_dir}"

    offset = (page - 1) * page_size
    data_sql = f"""
        SELECT
            k.killmail_id,
            k.killmail_time,
            k.solar_system_id,
            k.total_value,
            k.dropped_value,
            k.security_status,
            k.is_gank_candidate,
            k.is_confirmed_gank,
            v.ship_type_id   AS victim_ship_type_id,
            v.character_id   AS victim_character_id,
            (SELECT COUNT(*) FROM attackers a WHERE a.killmail_id = k.killmail_id) AS attacker_count,
            sc.name          AS system_name,
            tc_ship.name     AS victim_ship_name
        FROM killmails k
        LEFT JOIN victims v   ON v.killmail_id = k.killmail_id
        LEFT JOIN system_cache sc ON sc.system_id = k.solar_system_id
        LEFT JOIN type_cache tc_ship ON tc_ship.type_id = v.ship_type_id
        {where_sql}
        ORDER BY {order_clause}
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(data_sql, params + [page_size, offset]).fetchall()
    return rows, total_count


# ---------------------------------------------------------------------------
# Kill detail
# ---------------------------------------------------------------------------

def get_killmail_detail(conn: sqlite3.Connection, killmail_id: int) -> Optional[dict]:
    kill = conn.execute(
        """
        SELECT k.*, sc.name AS system_name, sc.security_class
        FROM killmails k
        LEFT JOIN system_cache sc ON sc.system_id = k.solar_system_id
        WHERE k.killmail_id = ?
        """,
        (killmail_id,),
    ).fetchone()
    if kill is None:
        return None

    victim = conn.execute(
        """
        SELECT v.*, tc.name AS ship_name
        FROM victims v
        LEFT JOIN type_cache tc ON tc.type_id = v.ship_type_id
        WHERE v.killmail_id = ?
        """,
        (killmail_id,),
    ).fetchone()

    attackers = conn.execute(
        """
        SELECT a.*, tc.name AS ship_name
        FROM attackers a
        LEFT JOIN type_cache tc ON tc.type_id = a.ship_type_id
        WHERE a.killmail_id = ?
        ORDER BY a.damage_done DESC
        """,
        (killmail_id,),
    ).fetchall()

    items = conn.execute(
        """
        SELECT i.*, tc.name AS item_name
        FROM items i
        LEFT JOIN type_cache tc ON tc.type_id = i.item_type_id
        WHERE i.killmail_id = ?
        ORDER BY i.quantity_dropped DESC, i.quantity_destroyed DESC
        """,
        (killmail_id,),
    ).fetchall()

    return {
        "kill": kill,
        "victim": victim,
        "attackers": [dict(a) for a in attackers],
        "items": [dict(i) for i in items],
    }


# ---------------------------------------------------------------------------
# Gank helpers
# ---------------------------------------------------------------------------

def get_killmails_needing_gank_phase2(conn: sqlite3.Connection) -> list:
    """Return gank candidates that haven't been confirmed yet."""
    return conn.execute(
        """
        SELECT k.killmail_id, k.killmail_time
        FROM killmails k
        WHERE k.is_gank_candidate = 1 AND k.is_confirmed_gank = 0
        """
    ).fetchall()


def get_attackers_for_kill(conn: sqlite3.Connection, killmail_id: int) -> list:
    return conn.execute(
        "SELECT character_id FROM attackers WHERE killmail_id = ? AND character_id IS NOT NULL",
        (killmail_id,),
    ).fetchall()


def find_concord_kill_for_character(
    conn: sqlite3.Connection,
    character_id: int,
    killmail_time: str,
    window_seconds: int,
) -> Optional[sqlite3.Row]:
    """Find a kill where character_id is the victim and CONCORD is an attacker, within window.
    Kept for backwards compatibility; Phase 2 now uses the CONCORD-first approach below."""
    return conn.execute(
        """
        SELECT k.killmail_id
        FROM killmails k
        JOIN victims v  ON v.killmail_id = k.killmail_id
        JOIN attackers a ON a.killmail_id = k.killmail_id
        WHERE v.character_id = ?
          AND a.is_concord = 1
          AND ABS(CAST((julianday(k.killmail_time) - julianday(?)) * 86400 AS INTEGER)) <= ?
        LIMIT 1
        """,
        (character_id, killmail_time, window_seconds),
    ).fetchone()


def get_concord_kills(conn: sqlite3.Connection) -> list:
    """
    Return all kills where CONCORD is an attacker (the CONCORD response kills).

    The victim of each such kill is the ganker — the character who committed a
    crime in highsec and was subsequently killed by CONCORD.

    Returns list of rows: (killmail_id, solar_system_id, ganker_character_id).
    """
    return conn.execute(
        """
        SELECT k.killmail_id, k.solar_system_id, v.character_id AS ganker_character_id
        FROM killmails k
        JOIN attackers a ON a.killmail_id = k.killmail_id AND a.is_concord = 1
        JOIN victims   v ON v.killmail_id = k.killmail_id
        WHERE v.character_id IS NOT NULL
        """
    ).fetchall()


def find_ganked_kill_for_character(
    conn: sqlite3.Connection,
    ganker_character_id: int,
    solar_system_id: int,
    concord_kill_id: int,
    window: int,
    min_value: float,
    pod_type_ids: frozenset,
) -> Optional[sqlite3.Row]:
    """
    Find the original gank victim kill for a ganker character.

    Searches for a kill where:
    - The ganker is listed as an attacker
    - Same solar system as the CONCORD response kill
    - killmail_id is lower than (and within `window` of) the CONCORD kill
    - Not already confirmed as a gank
    - Total value >= min_value ISK
    - Victim was not in a pod (ship_type_id not in pod_type_ids)
    - CONCORD is NOT an attacker on that kill (would mean it's another CONCORD kill)

    Returns the best match (highest killmail_id below the CONCORD kill), or None.
    """
    pod_placeholders = ",".join("?" * len(pod_type_ids))
    params = [
        ganker_character_id,
        solar_system_id,
        concord_kill_id,
        concord_kill_id,
        window,
        min_value,
        *sorted(pod_type_ids),
    ]
    return conn.execute(
        f"""
        SELECT k.killmail_id
        FROM killmails k
        JOIN attackers a ON a.killmail_id = k.killmail_id
        JOIN victims   v ON v.killmail_id = k.killmail_id
        WHERE a.character_id = ?
          AND k.solar_system_id = ?
          AND k.killmail_id < ?
          AND (? - k.killmail_id) <= ?
          AND k.is_confirmed_gank = 0
          AND k.total_value >= ?
          AND v.ship_type_id NOT IN ({pod_placeholders})
          AND NOT EXISTS (
              SELECT 1 FROM attackers ca
              WHERE ca.killmail_id = k.killmail_id AND ca.is_concord = 1
          )
        ORDER BY k.killmail_id DESC
        LIMIT 1
        """,
        params,
    ).fetchone()


def mark_confirmed_gank(
    conn: sqlite3.Connection,
    killmail_id: int,
    concord_killmail_id: int,
) -> None:
    conn.execute(
        "UPDATE killmails SET is_confirmed_gank = 1, concord_killmail_id = ? WHERE killmail_id = ?",
        (concord_killmail_id, killmail_id),
    )


def bulk_find_ganked_kills(
    conn: sqlite3.Connection,
    window: int,
    min_value: float,
    pod_type_ids: frozenset,
) -> list:
    """
    Single-query bulk gank detection — replaces the N-queries-per-CONCORD-kill loop.

    Algorithm (mirrors zKillboard cron/9.ganked.php):
      1. Find every kill where CONCORD is an attacker (the ganker's punishment kill).
         The *victim* of that kill is the ganker character.
      2. In the same query, look for a kill where that ganker is an attacker, in the
         same solar system, with a killmail_id just below the CONCORD kill's id
         (within `window`), meeting value/pod/CONCORD filters.
      3. For each CONCORD kill return the best match (highest victim killmail_id).

    Returns list of sqlite3.Row with columns (concord_kill_id, victim_kill_id).
    No API calls needed — all data is already in the local DB.
    """
    pod1, pod2 = sorted(pod_type_ids)[:2]  # exactly two pod type IDs
    return conn.execute(
        """
        WITH concord_kills AS (
            -- Kills where CONCORD is the attacker; victim = the ganker
            SELECT k.killmail_id   AS concord_kill_id,
                   k.solar_system_id,
                   v.character_id  AS ganker_character_id
            FROM   killmails k
            JOIN   attackers a ON a.killmail_id = k.killmail_id AND a.is_concord = 1
            JOIN   victims   v ON v.killmail_id = k.killmail_id
            WHERE  v.character_id IS NOT NULL
        ),
        candidates AS (
            -- For every CONCORD kill find all matching victim kills
            SELECT ck.concord_kill_id,
                   k.killmail_id AS victim_kill_id
            FROM   concord_kills ck
            JOIN   attackers a ON a.character_id = ck.ganker_character_id
            JOIN   killmails k ON k.killmail_id  = a.killmail_id
            JOIN   victims   v ON v.killmail_id  = k.killmail_id
            WHERE  k.solar_system_id              = ck.solar_system_id
              AND  k.killmail_id                 <  ck.concord_kill_id
              AND  (ck.concord_kill_id - k.killmail_id) <= ?
              AND  k.is_confirmed_gank            = 0
              AND  k.total_value                 >= ?
              AND  v.ship_type_id NOT IN (?, ?)
              AND  NOT EXISTS (
                       SELECT 1 FROM attackers ca
                       WHERE  ca.killmail_id = k.killmail_id
                         AND  ca.is_concord  = 1
                   )
        )
        -- Best (highest killmail_id) match per CONCORD kill
        SELECT concord_kill_id,
               MAX(victim_kill_id) AS victim_kill_id
        FROM   candidates
        GROUP  BY concord_kill_id
        """,
        (window, min_value, pod1, pod2),
    ).fetchall()


# ---------------------------------------------------------------------------
# Admin helpers
# ---------------------------------------------------------------------------

def reinit_db() -> int:
    """
    Completely recreate the database by deleting the .db / .db-wal / .db-shm
    files and calling init_db() to lay down a fresh schema.

    This is O(1) regardless of dataset size — no WAL checkpoint, no per-row
    work. Preserves nothing (type_cache and system_cache are wiped too; they
    will be repopulated on the next sync).

    Returns the number of killmails that existed before clearing.
    Retries file deletion for up to 3 s to handle briefly-open Flask g.db
    connections on Windows.
    """
    t0 = time.monotonic()

    # Count before we delete anything
    conn = _connect()
    try:
        count = conn.execute("SELECT COUNT(*) FROM killmails").fetchone()[0]
        logger.info("reinit_db: %d killmails — deleting database files", count)
    finally:
        conn.close()

    # Delete the three SQLite files; retry on Windows "file in use" errors
    deadline = time.monotonic() + 3.0
    while True:
        try:
            for suffix in ("", "-wal", "-shm"):
                path = config.DB_PATH + suffix
                try:
                    os.unlink(path)
                    logger.info("reinit_db: deleted %s", path)
                except FileNotFoundError:
                    pass
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise
            logger.info("reinit_db: file in use, retrying in 50 ms…")
            time.sleep(0.05)

    # Recreate schema
    init_db()
    logger.info("reinit_db: done in %.3fs", time.monotonic() - t0)
    return count


# ---------------------------------------------------------------------------
# Autocomplete search
# ---------------------------------------------------------------------------

def search_type_cache(conn: sqlite3.Connection, query: str, limit: int = 20) -> list:
    return conn.execute(
        "SELECT type_id, name FROM type_cache WHERE name LIKE ? ORDER BY name LIMIT ?",
        (f"%{query}%", limit),
    ).fetchall()


def search_system_cache(conn: sqlite3.Connection, query: str, limit: int = 20) -> list:
    return conn.execute(
        "SELECT system_id, name FROM system_cache WHERE name LIKE ? ORDER BY name LIMIT ?",
        (f"%{query}%", limit),
    ).fetchall()


def search_character_cache(conn: sqlite3.Connection, query: str, limit: int = 20) -> list:
    return conn.execute(
        "SELECT character_id, name FROM character_cache WHERE name LIKE ? ORDER BY name LIMIT ?",
        (f"%{query}%", limit),
    ).fetchall()
