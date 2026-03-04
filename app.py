import math
import logging
import threading
import uuid
from typing import Optional
from urllib.parse import urlencode

from flask import Flask, g, render_template, request, redirect, url_for, flash, jsonify, abort

import config
import db
import fetcher
import gank

# In-memory job store: job_id -> progress dict (lives only for this process run)
_jobs: dict = {}
# Separate dict for cancel events — threading.Event is not JSON-serialisable
_job_cancel_events: dict = {}
_clear_job: dict = {}   # keys: running, done, count, error

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "eve-killmail-browser-dev-secret"  # change for production


EFT_FLAG_GROUPS = {
    "low": set(range(11, 19)),
    "mid": set(range(19, 27)),
    "high": set(range(27, 35)),
    "rig": set(range(92, 100)),
    "subsystem": set(range(125, 133)),
    "service": set(range(164, 172)),
    "cargo": {5},
    "drone": {87},
    "fighter": {158},
    "implant": {89},
    "booster": {88},
}


def _eft_line(item_name: str, qty: int) -> str:
    return f"{item_name} x{qty}" if qty > 1 else item_name


def build_eft_export(victim: dict, items: list[dict]) -> str:
    """Build an EFT-like export string compatible with pyfa paste import."""
    ship_name = (victim.get("ship_name") or f"Type ID {victim['ship_type_id']}").strip()
    lines = [f"[{ship_name}, Loss #{victim['killmail_id']}]"]

    grouped: dict[str, list[str]] = {
        "low": [], "mid": [], "high": [], "rig": [], "subsystem": [], "service": [],
        "cargo": [], "drone": [], "fighter": [], "implant": [], "booster": [], "other": [],
    }

    for item in items:
        qty = int(item.get("quantity_dropped", 0) or 0) + int(item.get("quantity_destroyed", 0) or 0)
        if qty <= 0:
            continue
        name = (item.get("item_name") or f"Type ID {item['item_type_id']}").strip()
        line = _eft_line(name, qty)
        flag = int(item.get("flag", 0) or 0)
        group = next((k for k, flags in EFT_FLAG_GROUPS.items() if flag in flags), "other")
        grouped[group].append(line)

    for section in ("high", "mid", "low", "rig", "subsystem", "service"):
        lines.extend(grouped[section])
        lines.append("")

    named_sections = (
        ("cargo", "Cargo"),
        ("drone", "Drone Bay"),
        ("fighter", "Fighter Bay"),
        ("implant", "Implants"),
        ("booster", "Boosters"),
        ("other", "Other"),
    )
    for section, label in named_sections:
        if not grouped[section]:
            continue
        lines.append(f"{label}:")
        lines.extend(grouped[section])
        lines.append("")

    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DB lifecycle
# ---------------------------------------------------------------------------

@app.before_request
def open_db():
    g.db = db.get_db()


@app.teardown_appcontext
def close_db(error):
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


# ---------------------------------------------------------------------------
# Jinja2 filters
# ---------------------------------------------------------------------------

@app.template_filter("isk")
def format_isk(value: float) -> str:
    if value is None:
        return "—"
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:.0f}"


@app.template_filter("security_color")
def security_color(sec) -> str:
    """Return a CSS class name based on EVE security status colour scale."""
    if sec is None:
        return "sec-unknown"
    # EVE uses floor for display
    bucket = min(10, max(0, math.floor(sec * 10)))
    return f"sec-{bucket}"


@app.template_filter("security_display")
def security_display_filter(sec) -> str:
    if sec is None:
        return "?"
    return gank.security_display(sec)


# ---------------------------------------------------------------------------
# Filter context helper
# ---------------------------------------------------------------------------

def _parse_filters() -> dict:
    """Parse and validate filter GET params. Returns dict with typed values."""
    def _int(key) -> Optional[int]:
        v = request.args.get(key, "").strip()
        try:
            return int(v) if v else None
        except ValueError:
            return None

    def _float(key) -> Optional[float]:
        v = request.args.get(key, "").strip()
        try:
            return float(v) if v else None
        except ValueError:
            return None

    _SORT_COLS = {"time", "system", "sec", "total_value", "dropped_value", "attackers"}
    sort = request.args.get("sort", "time")
    if sort not in _SORT_COLS:
        sort = "time"
    order = request.args.get("order", "desc")
    if order not in ("asc", "desc"):
        order = "desc"

    page = max(1, _int("page") or 1)
    filters = {
        "page": page,
        "item_id": _int("item_id"),
        "ship_type_id": _int("ship_type_id"),
        "system_id": _int("system_id"),
        "min_isk_lost": _int("min_isk_lost"),
        "max_isk_lost": _int("max_isk_lost"),
        "min_sec": _float("min_sec"),
        "max_sec": _float("max_sec"),
        "ganks_only": bool(request.args.get("ganks_only")),
        "sort": sort,
        "order": order,
    }
    # Build a querystring without 'page', 'sort', 'order' for link generation
    qs_params = {k: v for k, v in request.args.items() if k not in ("page", "sort", "order") and v}
    filters["filter_qs"] = urlencode(qs_params)
    return filters


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    filters = _parse_filters()

    rows, total_count = db.get_killmails_page(
        g.db,
        page=filters["page"],
        page_size=config.PAGE_SIZE,
        item_id=filters["item_id"],
        ship_type_id=filters["ship_type_id"],
        system_id=filters["system_id"],
        min_isk_lost=filters["min_isk_lost"],
        max_isk_lost=filters["max_isk_lost"],
        min_sec=filters["min_sec"],
        max_sec=filters["max_sec"],
        ganks_only=filters["ganks_only"],
        sort_by=filters["sort"],
        sort_dir=filters["order"],
    )

    isk_lost_bounds = db.get_isk_lost_bounds(g.db)
    total_pages = max(1, math.ceil(total_count / config.PAGE_SIZE))

    # Resolve filter label names for display in the form
    item_name = None
    ship_name = None
    system_name = None
    if filters["item_id"]:
        row = db.get_cached_type(g.db, filters["item_id"])
        item_name = row["name"] if row else str(filters["item_id"])
    if filters["ship_type_id"]:
        row = db.get_cached_type(g.db, filters["ship_type_id"])
        ship_name = row["name"] if row else str(filters["ship_type_id"])
    if filters["system_id"]:
        row = db.get_cached_system(g.db, filters["system_id"])
        system_name = row["name"] if row else str(filters["system_id"])

    return render_template(
        "index.html",
        kills=[dict(r) for r in rows],
        total_count=total_count,
        total_pages=total_pages,
        filters=filters,
        isk_lost_bounds=isk_lost_bounds,
        item_name=item_name,
        ship_name=ship_name,
        system_name=system_name,
    )


@app.route("/kill/<int:killmail_id>")
def kill_detail(killmail_id: int):
    detail = db.get_killmail_detail(g.db, killmail_id)
    if detail is None:
        abort(404)
    # Annotate each item with ISK values from the in-process market price cache
    prices = fetcher.fetch_market_prices()
    for item in detail["items"]:
        p = prices.get(item["item_type_id"], 0.0)
        item["isk_dropped"]   = p * item["quantity_dropped"]
        item["isk_destroyed"] = p * item["quantity_destroyed"]
        item["isk_total"]     = item["isk_dropped"] + item["isk_destroyed"]
    detail["victim"] = dict(detail["victim"])
    detail["victim"]["killmail_id"] = killmail_id
    detail["eft_export"] = build_eft_export(detail["victim"], detail["items"])
    return render_template("kill_detail.html", **detail)


@app.route("/sync", methods=["GET", "POST"])
def sync():
    if request.method == "POST":
        days_back = min(30, max(1, int(request.form.get("days_back", 1))))
        job_id = uuid.uuid4().hex[:12]
        cancel_event = threading.Event()
        progress = {
            "done": False,
            "cancelled": False,
            "error": None,
            "phase": "starting",
            "message": "Starting…",
            # Download progress
            "dl_files_done": 0,
            "dl_files_total": days_back,
            "dl_bytes_done": 0.0,
            "dl_rate_mbps": 0.0,
            "dl_file_progress": {},
            # Ingestion progress
            "ingest_done": 0,
            "ingest_total": 0,
            "ingest_records_done": 0,
            "ingest_rate_rps": 0.0,
            "ingest_day_progress": {},
            # Gank detection progress
            "gank_phase": "idle",
            "gank_total": 0,
            "gank_done": 0,
            "gank_confirmed": 0,
            "gank_checked": 0,
            "summary": None,
            "gank_summary": None,
        }
        _jobs[job_id] = progress
        _job_cancel_events[job_id] = cancel_event

        def _run():
            conn = db.get_db()
            try:
                summary = fetcher.sync_kills_everef(
                    conn, days_back=days_back, progress=progress, cancel_event=cancel_event
                )
                # sync_kills_everef sets done=True itself on cancellation
                if progress.get("cancelled"):
                    return
                progress["phase"] = "gank_detection"
                progress["message"] = "Running gank detection…"
                gank_summary = gank.run_phase2_gank_detection(conn, progress=progress)
                conn.commit()
                progress.update({
                    "done": True,
                    "phase": "done",
                    "message": "Complete.",
                    "summary": summary,
                    "gank_summary": gank_summary,
                })
            except Exception as e:
                logger.exception("Background sync failed")
                progress.update({"done": True, "error": str(e), "phase": "error"})
            finally:
                conn.close()

        threading.Thread(target=_run, daemon=True).start()
        return redirect(url_for("sync_progress", job_id=job_id))

    # GET: show sync form with stats
    total_kills = g.db.execute("SELECT COUNT(*) FROM killmails").fetchone()[0]
    gank_candidates = g.db.execute(
        "SELECT COUNT(*) FROM killmails WHERE is_gank_candidate = 1"
    ).fetchone()[0]
    confirmed_ganks = g.db.execute(
        "SELECT COUNT(*) FROM killmails WHERE is_confirmed_gank = 1"
    ).fetchone()[0]
    last_kill = g.db.execute(
        "SELECT killmail_time FROM killmails ORDER BY killmail_time DESC LIMIT 1"
    ).fetchone()

    return render_template(
        "sync.html",
        total_kills=total_kills,
        gank_candidates=gank_candidates,
        confirmed_ganks=confirmed_ganks,
        last_kill_time=last_kill["killmail_time"] if last_kill else None,
    )


@app.route("/sync/progress/<job_id>")
def sync_progress(job_id: str):
    if job_id not in _jobs:
        flash("Job not found.", "error")
        return redirect(url_for("sync"))
    return render_template("sync_progress.html", job_id=job_id)


@app.route("/api/sync/status/<job_id>")
def sync_status(job_id: str):
    job = _jobs.get(job_id)
    if job is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)


@app.route("/api/sync/cancel/<job_id>", methods=["POST"])
def sync_cancel(job_id: str):
    event = _job_cancel_events.get(job_id)
    if event is None:
        return jsonify({"ok": False, "error": "not found"}), 404
    event.set()
    return jsonify({"ok": True})


@app.route("/admin/clear-db", methods=["POST"])
def admin_clear_db():
    logger.info("admin_clear_db: request received")
    if any(not j.get("done") for j in _jobs.values()):
        logger.warning("admin_clear_db: blocked — sync in progress")
        flash("Cannot clear the database while a sync is running.", "error")
        return redirect(url_for("sync"))
    if _clear_job.get("running"):
        logger.info("admin_clear_db: clear already in progress, redirecting to progress page")
        return redirect(url_for("clear_progress"))
    _clear_job.update({"running": True, "done": False, "count": 0, "error": None})
    logger.info("admin_clear_db: starting background clear thread")

    def _do_clear():
        try:
            count = db.reinit_db()
            logger.info("admin_clear_db: background clear done, %d killmails removed", count)
            _clear_job.update({"running": False, "done": True, "count": count})
        except Exception as e:
            logger.exception("admin_clear_db: background clear failed")
            _clear_job.update({"running": False, "done": True, "error": str(e)})

    threading.Thread(target=_do_clear, daemon=True).start()
    return redirect(url_for("clear_progress"))


@app.route("/admin/clear-progress")
def clear_progress():
    return render_template("clear_progress.html")


@app.route("/api/clear-status")
def api_clear_status():
    return jsonify(_clear_job)


@app.route("/admin/clear-done")
def clear_done():
    """Relay route: flashes the success message then bounces to /sync."""
    job = _clear_job
    if job.get("done") and not job.get("error"):
        flash(f"Database cleared — {job.get('count', 0):,} killmail(s) removed.", "success")
    elif job.get("error"):
        flash(f"Clear failed: {job['error']}", "error")
    return redirect(url_for("sync"))


@app.route("/admin/refresh-prices", methods=["POST"])
def admin_refresh_prices():
    prices = fetcher.fetch_market_prices(force=True)
    flash(f"Market prices refreshed — {len(prices):,} items loaded from ESI.", "success")
    return redirect(url_for("sync"))


@app.route("/api/type-search")
def type_search():
    query = request.args.get("q", "").strip()
    if len(query) < 2:
        return jsonify([])
    results = db.search_type_cache(g.db, query, limit=20)
    return jsonify([{"type_id": r["type_id"], "name": r["name"]} for r in results])


@app.route("/api/system-search")
def system_search():
    query = request.args.get("q", "").strip()
    if len(query) < 2:
        return jsonify([])
    results = db.search_system_cache(g.db, query, limit=20)
    return jsonify([{"system_id": r["system_id"], "name": r["name"]} for r in results])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    db.init_db()
    app.run(debug=True, host="127.0.0.1", port=5000)
