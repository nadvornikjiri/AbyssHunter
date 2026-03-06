# API endpoints
ZKILLBOARD_BASE   = "https://zkillboard.com/api"
ZKILLBOARD_REDISQ = "https://redisq.zkillboard.com/listen.php"
ESI_BASE          = "https://esi.evetech.net/latest"

# Rate limiting (seconds between requests)
ESI_RATE_LIMIT   = 0.0   # no per-request delay; concurrency is managed by ESI_CONCURRENCY
ZKILL_RATE_LIMIT = 1.0   # zKillboard requires polite pacing between page requests

# Batch download settings
ESI_CONCURRENCY      = 20   # concurrent ESI killmail fetches via aiohttp
MARKET_ORDER_CONCURRENCY = 20  # parallel pages when fetching Jita sell orders
ZKILL_MAX_PAGES      = 200  # safety cap on zKillboard history pagination (200 kills/page)
INGEST_BATCH_SIZE    = 10000  # commit to DB after every N newly fetched killmails
EVEREF_DL_CONNECTIONS  = 8   # parallel download workers (one full connection per file)
EVEREF_INGEST_WORKERS  = 8   # parallel prepare workers (filter + build row tuples per day)

# Gank detection
CONCORD_CORP_ID     = 1000125
GANK_TIME_WINDOW_S  = 120    # kept for legacy; Phase 2 now uses killmail_id proximity
CRIMINAL_SEC_STATUS = -5.0   # kept for reference; Phase 1 no longer filters on this
GANK_MIN_VALUE      = 1_000_000  # victim kill must be worth at least 1M ISK (per zKillboard)
GANK_KILL_ID_WINDOW = 500    # max killmail_id gap between victim kill and CONCORD response
GANK_PARALLEL_WORKERS = 4  # incremental phase-2 workers during ingestion
POD_TYPE_IDS        = frozenset({670, 33328})  # Capsule, Capsule - Genolution 'Auroral'

# Pagination
PAGE_SIZE = 100
ISK_LOST_FILTER_MAX = 20_000_000_000  # 20B ISK hard cap for the max ISK-lost filter

# Security class thresholds (raw ESI float values)
HIGHSEC_MIN = 0.5
LOWSEC_MIN  = 0.1

# HTTP headers — zKillboard requires a descriptive User-Agent
USER_AGENT = "EveKillmailBrowser/1.0 (github.com/user/eve-killmail-browser)"

DB_PATH = "killmails.db"

