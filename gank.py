import logging
from typing import Optional

import config
import db

logger = logging.getLogger(__name__)


def classify_security(security_status: float) -> str:
    """Return human-readable security class from raw ESI float."""
    if security_status >= config.HIGHSEC_MIN:
        return "highsec"
    elif security_status >= config.LOWSEC_MIN:
        return "lowsec"
    elif security_status >= 0.0:
        return "nullsec"
    else:
        return "wormhole"


def security_display(security_status: float) -> str:
    """Return EVE-style display value (rounds towards zero for low values)."""
    import math
    if security_status > 0:
        return f"{math.floor(security_status * 10) / 10:.1f}"
    return f"{security_status:.1f}"


def is_phase1_gank_candidate(security_status: float, attackers: list) -> bool:
    """
    Phase 1: mark highsec kills as gank candidates for the '?' badge.

    A kill is a candidate if it occurred in highsec AND CONCORD is NOT in the
    attacker list (CONCORD kills are the *response* to a gank, not the victim).

    The old criminal-security-status filter (-5.0 threshold) is removed because
    first-time and occasional gankers may still have positive or only slightly
    negative sec status, causing many real ganks to be missed.
    """
    if security_status < config.HIGHSEC_MIN:
        return False

    for a in attackers:
        if a.get("corporation_id") == config.CONCORD_CORP_ID:
            return False  # this IS a CONCORD response kill — not a gank victim

    return True


def run_phase2_gank_detection(conn, progress: Optional[dict] = None) -> dict:
    """
    CONCORD-first gank confirmation engine — bulk SQL implementation.

    Algorithm (mirrors zKillboard's cron/9.ganked.php):
      1. A single CTE query finds every (CONCORD kill → gank victim kill) pair
         in one pass over the DB — no Python loop, no per-kill SELECT calls.
      2. Deduplicate on victim_kill_id (a victim may be matched by multiple
         CONCORD kills if several gankers were killed).
      3. One executemany UPDATE marks all newly confirmed ganks at once.

    No API calls are needed — all data is already in the local DB from the
    EVE Ref bulk download (attackers.is_concord, victims.ship_type_id, etc.).

    Returns summary dict.
    """
    def _p(**kw):
        if progress is not None:
            progress.update(kw)

    _p(gank_phase="running", gank_total=0, gank_done=0,
       message="Running gank detection — querying CONCORD matches…")
    matches = db.bulk_find_ganked_kills(
        conn,
        window=config.GANK_KILL_ID_WINDOW,
        min_value=config.GANK_MIN_VALUE,
        pod_type_ids=config.POD_TYPE_IDS,
    )
    logger.info("Phase 2 gank detection: bulk query returned %d candidate pairs", len(matches))

    n_matches = len(matches)
    _p(gank_total=n_matches, gank_done=0,
       message=f"Processing {n_matches:,} candidate pairs…")

    # Deduplicate: a victim kill may appear under multiple CONCORD kills
    # (fleet gank — several gankers were killed by CONCORD). Keep first occurrence.
    step = max(1, n_matches // 200)  # ~200 progress updates max
    seen_victims: set = set()
    rows_to_update = []
    for i, row in enumerate(matches):
        victim_kill_id  = row["victim_kill_id"]
        concord_kill_id = row["concord_kill_id"]
        if victim_kill_id not in seen_victims:
            seen_victims.add(victim_kill_id)
            rows_to_update.append((concord_kill_id, victim_kill_id))
        if (i + 1) % step == 0:
            _p(gank_done=i + 1)
    _p(gank_done=n_matches)

    # Bulk update: mark confirmed + ensure candidate flag + link CONCORD kill.
    # AND is_confirmed_gank = 0 guard ensures already-confirmed kills are untouched.
    newly_confirmed = 0
    if rows_to_update:
        cur = conn.executemany(
            """
            UPDATE killmails
               SET is_confirmed_gank   = 1,
                   concord_killmail_id = ?,
                   is_gank_candidate   = 1
             WHERE killmail_id         = ?
               AND is_confirmed_gank   = 0
            """,
            rows_to_update,
        )
        newly_confirmed = cur.rowcount

    already_confirmed = len(rows_to_update) - newly_confirmed

    logger.info(
        "Phase 2 complete: %d newly confirmed, %d already confirmed",
        newly_confirmed, already_confirmed,
    )
    _p(gank_phase="done", gank_confirmed=newly_confirmed, gank_checked=len(matches))
    return {
        "concord_kills_checked": len(matches),
        "newly_confirmed": newly_confirmed,
        "already_confirmed": already_confirmed,
    }
