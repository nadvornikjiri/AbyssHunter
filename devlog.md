# Devlog

## 2026-03-04 — Remove cap on imported days

### Changes
- **app.py**: Removed the `min(30, …)` server-side clamp on `days_back`; now only enforces a minimum of 1.
- **sync.html**: Removed `max="30"` from the days-back input and updated the label to drop the "(1–30)" range hint.

---

## 2026-03-04 — Kill detail EFT export for pyfa

### Changes
- **app.py**: Added `build_eft_export()` to convert a killmail's victim ship + item flags into EFT-format sections (high/mid/low/rig/subsystem/service plus cargo/drone/fighter/implants/boosters) and pass the string to the kill detail template.
- **kill_detail.html**: Added a new `Copy EFT` action with a readonly export textarea and clipboard copy fallback behavior.
- **style.css**: Styled the EFT export block and monospaced textarea for copy/paste readability.

---

## 2026-03-04 — Jita IV-4 minimum sell prices replace ESI adjusted prices

### Changes
- **config.py**: Added `MARKET_ORDER_CONCURRENCY = 20`.
- **fetcher.py**: Added `JITA_REGION_ID = 10000002` and `JITA_STATION_ID = 60003760`. Added `_fetch_jita_prices_async()` — paginates `GET /markets/10000002/orders/?order_type=sell`, discovers total pages from `X-Pages` header on page 1, then fetches remaining pages with up to `MARKET_ORDER_CONCURRENCY` concurrent requests via aiohttp. Filters for `location_id == 60003760`, takes minimum sell price per `type_id`. `fetch_market_prices()` now calls this via `asyncio.run()` and accepts an optional `progress` dict for live page-count messages. Called from `sync_kills_everef` with `progress=progress`. Items not currently listed in Jita will have a price of 0 ISK.

---

## 2026-03-04 — Per-day ingestion progress bars

### Changes
- **fetcher.py**: Added `ingest_day_progress` shared dict (keyed by `date_str`, values `{kills_done, kills_total, done}`). `_ingest_day` creates an entry when the download completes and updates `kills_done` every `N/100` kills during preparation. Main thread sets `done=True` after `conn.commit()`. Empty days are marked `done=True` immediately by the worker.
- **app.py**: Added `"ingest_day_progress": {}` to the initial progress dict.
- **sync_progress.html**: `<div id="ingest-day-bars">` below the ingestion overall bar. `updateIngestDayBars()` mirrors `updateFileBars`: creates a row per day when first seen, shows `kills_done / kills_total` as a determinate bar (or "no new kills" for zero-kill days), removes 1 second after `done`.

---

## 2026-03-04 — Parallel ingestion (download + prepare overlap)

### Changes
- **config.py**: Added `EVEREF_INGEST_WORKERS = 8`. Renamed `EVEREF_DL_CONNECTIONS` comment for clarity.
- **fetcher.py**: Replaced the sequential download→prepare→write loop with a two-pool pipeline. An `ingest_pool` (up to `EVEREF_INGEST_WORKERS` workers) submits one `_ingest_day` closure per date; each worker blocks on its download future, then filters new kills and prepares row tuples from in-memory dicts. All shared data accessed by workers is read-only (`prices`, `system_sec_map`, `known_ids`). The main thread collects ingest futures in date order and performs sequential DB writes. Removed the inner per-day `ThreadPoolExecutor` — each ingest worker now does preparation sequentially in its own thread, giving equivalent CPU utilisation with simpler structure.

---

## 2026-03-04 — Gank detection candidate progress tracking

### Changes
- **gank.py**: `run_phase2_gank_detection` now emits `gank_total` / `gank_done` progress through the deduplication loop. After the bulk SQL query returns `N` candidate pairs, progress updates are emitted every `N/200` rows (max ~200 updates). While the SQL is still running the bar stays indeterminate (`gank_total=0`); once the query returns it becomes determinate.
- **app.py**: Added `gank_total: 0` and `gank_done: 0` to the initial progress dict.
- **sync_progress.html**: Gank bar switches from indeterminate to determinate as soon as `gank_total > 0`. Label shows `"X / Y candidate pairs"` while running and the confirmed count when done.

---

## 2026-03-04 — Day-based ingestion progress + gank detection progress bar

### Changes
- **fetcher.py**: `ingest_total` is now set to `len(dates)` (days) before the download loop instead of growing per-day. `ingest_done` tracks days processed (incremented by 1 after each day's DB commit, including empty days). Added `ingest_records_done` field for the actual kill count (used for the rate badge). Rate calculation updated to use `ingest_records_done` for records/s.
- **gank.py**: `run_phase2_gank_detection` accepts an optional `progress` dict. Emits `gank_phase="running"` at entry and `gank_phase="done"`, `gank_confirmed`, `gank_checked` on completion.
- **app.py**: Added `ingest_records_done`, `gank_phase`, `gank_confirmed`, `gank_checked` to the initial progress dict. Passes `progress=progress` to `run_phase2_gank_detection`.
- **sync_progress.html**: Ingestion bar label changed to `N / M days · X records`. Added third "Gank detection" progress bar — indeterminate while pending/running, full when done, label shows confirmed count and CONCORD kills checked.

---

## 2026-03-04 — Bulk system prefetch from ESI

### Changes
- **fetcher.py**: Added `_prefetch_all_systems(conn, progress)`. Calls `GET /universe/systems/` (returns all ~8 285 system IDs in one request), then parallel-fetches `/universe/systems/{id}/` for any not yet in `system_cache` using 40 concurrent workers. One-time cost ~30–60 s on first run; instant thereafter.
- **fetcher.py**: `sync_kills_everef` now calls `_prefetch_all_systems` before any killmail downloads instead of loading from the DB and lazily fetching missing systems per-day. The entire per-day `missing_sys` / `_fetch_sys` block is removed.

---

## 2026-03-04 — Per-file download progress bars

### Changes
- **fetcher.py**: `_download_everef_day` now streams in 256 KB chunks and updates `file_progress[date_str]` (`bytes_done`, `bytes_total`, `done`) in real-time from each download thread. Accepts optional `file_progress` dict.
- **fetcher.py**: `sync_kills_everef` creates a `dl_file_progress` shared dict, adds it to the progress object, and passes it to all download threads via `functools.partial`.
- **app.py**: `dl_file_progress: {}` added to initial progress dict.
- **sync_progress.html**: Per-file bars rendered below the overall download bar. Each bar appears when a file starts downloading and is removed 1 second after its `done` flag is set. Shows date, a small progress bar (indeterminate if Content-Length unknown), and MB downloaded.
- **style.css**: `.file-progress-row` grid layout, `.file-bar` (6 px tall), `.file-date`, `.file-size`.

---

## 2026-03-04 — Parallel downloads + dual progress bars

### Changes
- **fetcher.py**: Removed byte-range chunked download (`_download_everef_day` with Range headers). Downloads are now one full connection per file, multiple files in parallel via `ThreadPoolExecutor` (up to `EVEREF_DL_CONNECTIONS` workers). `_download_everef_day` now returns `(killmails, bytes_downloaded)`.
- **fetcher.py**: `sync_kills_everef` progress dict restructured — dropped `day_current/total`, `day_date`, `kills_this_day/total`, `download_phase/date`; added `dl_files_done`, `dl_files_total`, `dl_bytes_done`, `dl_rate_mbps`, `ingest_done`, `ingest_total`, `ingest_rate_rps`. Rates computed from per-update deltas.
- **app.py**: Initial progress dict updated to match new fields.
- **sync_progress.html**: Two progress bars — "Download progress (overall)" (files + MB/s) and "Ingestion progress (overall)" (records; total grows dynamically, records/s). Rate displayed as gold badge.
- **style.css**: Added `.progress-stats` and `.rate-badge` styles.
