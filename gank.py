import logging
import sqlite3
import time
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
    attacker list (CONCORD kills are the response to a gank, not the victim).

    The old criminal-security-status filter (-5.0 threshold) is removed because
    first-time and occasional gankers may still have positive or only slightly
    negative sec status, causing many real ganks to be missed.
    """
    if security_status < config.HIGHSEC_MIN:
        return False

    for a in attackers:
        if a.get("corporation_id") == config.CONCORD_CORP_ID:
            return False

    return True


def run_phase2_gank_detection(conn, progress: Optional[dict] = None, cancel_event=None) -> dict:
    """
    CONCORD-first gank confirmation engine.

    For operator visibility this reports progress as:
      checked gank candidates / total unconfirmed gank candidates.
    """

    def _p(**kw):
        if progress is not None:
            progress.update(kw)

    def _cancelled() -> bool:
        return cancel_event is not None and cancel_event.is_set()

    def _check_cancel(stage: str) -> None:
        if _cancelled():
            logger.info("Phase 2 gank detection cancelled during %s", stage)
            _p(message="Gank detection cancelled.")
            raise RuntimeError("gank detection cancelled")

    overall_start = time.monotonic()

    _p(
        gank_phase="running",
        gank_total=0,
        gank_done=0,
        message="Running gank detection - loading gank candidates...",
    )
    logger.info("Phase 2 gank detection: loading unconfirmed phase-1 candidates")
    t0 = time.monotonic()
    candidates = db.get_killmails_needing_gank_phase2(conn)
    total_candidates = len(candidates)
    logger.info(
        "Phase 2 gank detection: loaded %d candidates in %.2fs",
        total_candidates,
        time.monotonic() - t0,
    )

    _p(gank_total=total_candidates, gank_done=0)
    _check_cancel("candidate load")

    if total_candidates == 0:
        _p(
            gank_phase="done",
            gank_total=0,
            gank_done=0,
            gank_confirmed=0,
            gank_checked=0,
            message="No gank candidates to check.",
        )
        return {
            "concord_kills_checked": 0,
            "checked_candidates": 0,
            "total_candidates": 0,
            "newly_confirmed": 0,
            "already_confirmed": 0,
        }

    _p(message="Running gank detection - querying CONCORD correlations...")
    logger.info(
        "Phase 2 gank detection: running bulk CONCORD correlation query "
        "(window=%d, min_value=%.0f, pod_types=%d)",
        config.GANK_KILL_ID_WINDOW,
        config.GANK_MIN_VALUE,
        len(config.POD_TYPE_IDS),
    )

    t1 = time.monotonic()
    try:
        matches = db.bulk_find_ganked_kills(
            conn,
            window=config.GANK_KILL_ID_WINDOW,
            min_value=config.GANK_MIN_VALUE,
            pod_type_ids=config.POD_TYPE_IDS,
        )
    except sqlite3.OperationalError as e:
        if _cancelled() and "interrupted" in str(e).lower():
            logger.info("Phase 2 gank detection: SQL interrupted by cancellation")
            _p(message="Gank detection cancelled.")
            raise RuntimeError("gank detection cancelled")
        raise

    logger.info(
        "Phase 2 gank detection: bulk query returned %d candidate pairs in %.2fs",
        len(matches),
        time.monotonic() - t1,
    )
    _check_cancel("bulk correlation query")

    _p(message="Running gank detection - consolidating matches...")
    t2 = time.monotonic()
    seen_victims: set = set()
    rows_to_update = []
    for row in matches:
        victim_kill_id = row["victim_kill_id"]
        concord_kill_id = row["concord_kill_id"]
        if victim_kill_id not in seen_victims:
            seen_victims.add(victim_kill_id)
            rows_to_update.append((concord_kill_id, victim_kill_id))

    logger.info(
        "Phase 2 gank detection: consolidated to %d unique victim updates in %.2fs",
        len(rows_to_update),
        time.monotonic() - t2,
    )
    _check_cancel("match consolidation")

    _p(message="Running gank detection - checking gank candidates...")
    step = max(1, total_candidates // 200)
    for i, _ in enumerate(candidates, start=1):
        if _cancelled():
            logger.info("Phase 2 gank detection: cancelled at %d/%d candidates", i, total_candidates)
            _p(message="Gank detection cancelled.", gank_done=i)
            raise RuntimeError("gank detection cancelled")
        if i % step == 0:
            _p(gank_done=i)
    _p(gank_done=total_candidates)

    _check_cancel("candidate progress")

    _p(message="Running gank detection - applying confirmed matches...")
    newly_confirmed = 0
    if rows_to_update:
        t3 = time.monotonic()
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
        logger.info(
            "Phase 2 gank detection: applied updates in %.2fs",
            time.monotonic() - t3,
        )

    already_confirmed = len(rows_to_update) - newly_confirmed

    logger.info(
        "Phase 2 complete: candidates=%d, matched=%d, newly confirmed=%d, already confirmed=%d, total %.2fs",
        total_candidates,
        len(rows_to_update),
        newly_confirmed,
        already_confirmed,
        time.monotonic() - overall_start,
    )

    _p(
        gank_phase="done",
        gank_done=total_candidates,
        gank_total=total_candidates,
        gank_confirmed=newly_confirmed,
        gank_checked=total_candidates,
        message=(
            f"Gank detection done: {newly_confirmed} newly confirmed, "
            f"{already_confirmed} already confirmed."
        ),
    )

    return {
        "concord_kills_checked": len(matches),
        "checked_candidates": total_candidates,
        "total_candidates": total_candidates,
        "newly_confirmed": newly_confirmed,
        "already_confirmed": already_confirmed,
    }


def run_phase2_gank_detection_for_day(conn, date_str: str) -> dict:
    """
    Run phase 2 confirmation only for CONCORD response kills on one UTC day.

    Intended for incremental background execution during ingestion.
    """
    matches = db.bulk_find_ganked_kills_for_day(
        conn,
        date_str=date_str,
        window=config.GANK_KILL_ID_WINDOW,
        min_value=config.GANK_MIN_VALUE,
        pod_type_ids=config.POD_TYPE_IDS,
    )

    seen_victims: set = set()
    rows_to_update = []
    for row in matches:
        victim_kill_id = row["victim_kill_id"]
        concord_kill_id = row["concord_kill_id"]
        if victim_kill_id not in seen_victims:
            seen_victims.add(victim_kill_id)
            rows_to_update.append((concord_kill_id, victim_kill_id))

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
    return {
        "date": date_str,
        "concord_kills_checked": len(matches),
        "newly_confirmed": newly_confirmed,
        "already_confirmed": already_confirmed,
    }
