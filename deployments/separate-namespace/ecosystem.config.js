// PM2 process file for the algo-dev separate-namespace stack.
//
// Infra (postgres/kafka/redis) runs in Docker (docker-compose.yml); THESE are
// the Go application services, run under PM2 so `pm2 logs` works. They connect
// to the Docker infra over host-published ports:
//   postgres localhost:5442 · kafka localhost:9192 · redis localhost:6389
//
//   cd ~/Algo-Treading/deployments/separate-namespace
//   pm2 start ecosystem.config.js
//   pm2 logs          # all services   |   pm2 logs orderstatus   # one service
//   pm2 status        |   pm2 stop ecosystem.config.js
//
// ENCRYPTION_KEY is read from ./.env (0600) so the real key isn't committed.
const fs = require('fs');
const path = require('path');

const envFile = path.join(__dirname, '.env');
const ENV = Object.fromEntries(
  fs.readFileSync(envFile, 'utf8').split('\n')
    .filter(l => l.includes('=') && !l.trim().startsWith('#'))
    .map(l => { const i = l.indexOf('='); return [l.slice(0, i).trim(), l.slice(i + 1).trim()]; })
);

// Shared infra wiring (host ports → Docker infra).
const PG = { POSTGRES_HOST: 'localhost', POSTGRES_PORT: '5442', POSTGRES_USER: 'postgres',
             POSTGRES_PASSWORD: 'postgres', POSTGRES_SSL_MODE: 'disable', POSTGRES_SSLMODE: 'disable' };
const KAFKA = { KAFKA_BROKERS: 'localhost:9192' };
// Local cache / ws-pubsub redis = the algo-dev docker redis (6389).
const REDIS = { REDIS_ADDRS: 'localhost:6389', REDIS_HOST: 'localhost', REDIS_PORT: '6389' };
// External LTP (live-price) feed = the real market-data redis. Address +
// password come from .env (uncommitted); falls back to local (no live prices).
const LTP = {
  EXT_REDIS_ADDR:     ENV.EXT_REDIS_ADDR     || 'localhost:6389',
  EXT_REDIS_PASSWORD: ENV.EXT_REDIS_PASSWORD || '',
  // api-gateway's livealgos LTP store reads its OWN addr var — point external too.
  LIVEALGOS_LTP_REDIS_ADDR:     ENV.EXT_REDIS_ADDR     || 'localhost:6389',
  LIVEALGOS_LTP_REDIS_PASSWORD: ENV.EXT_REDIS_PASSWORD || '',
};
// External feed split into host/port for services (trade-execution's price
// client) that take REDIS_HOST + REDIS_PORT separately — so every LTP /
// market:nse:{token} / NSE-token read hits the real feed, never the empty local.
const EXT_HOST = (ENV.EXT_REDIS_ADDR || 'localhost:6389').split(':')[0];
const EXT_PORT = (ENV.EXT_REDIS_ADDR || 'localhost:6389').split(':')[1] || '6379';
const EXT_PW   = ENV.EXT_REDIS_PASSWORD || '';
const KEY = { ENCRYPTION_KEY: ENV.ENCRYPTION_KEY, ALLOW_INSECURE_ENCRYPTION_KEY: 'false' };
// Indira b2b api-key — order-services (place/modify/cancel) 403 "unknown_client"
// without it. From .env (uncommitted).
const BROKER = { INDIRA_API_KEY: ENV.INDIRA_API_KEY || '' };
const BASE = { ...PG, ...KAFKA, ...REDIS, ...LTP, ...BROKER };

const common = {
  cwd: __dirname,
  max_restarts: 6,          // crash-loopers stop instead of spinning forever
  restart_delay: 4000,
  time: true,               // PM2 default per-app logs: pm2 logs <name>
};

module.exports = {
  apps: [
    { name: 'user-config', script: './bin/user-config', ...common, env: {
        ...BASE, ...KEY, GRPC_PORT: '9003',
        DB_HOST: 'localhost', DB_PORT: '5442', DB_NAME: 'trading_db', DB_USER: 'postgres',
        DB_PASSWORD: 'postgres', DB_SSLMODE: 'disable',
        EXECUTION_DB_HOST: 'localhost', EXECUTION_DB_PORT: '5442', EXECUTION_DB_NAME: 'execution_db',
        EXECUTION_DB_USER: 'postgres', EXECUTION_DB_PASSWORD: 'postgres', EXECUTION_DB_SSLMODE: 'disable',
        KAFKA_ENABLED: 'true', KAFKA_TOPIC: 'user-config-events', LOG_LEVEL: 'info',
    }},
    { name: 'rules-engine', script: './bin/rules-engine', ...common, env: {
        ...BASE, GRPC_PORT: '9005', METRICS_PORT: '9091',
        POSTGRES_DB: 'trading_db', MANTHAN_SIGNALS_DB: 'signals_db',
        USER_CONFIG_GRPC_ADDR: 'localhost:9003', USER_CONFIG_SERVICE_ADDR: 'localhost:9003',
        ENVIRONMENT: 'production',
    }},
    { name: 'trade-execution', script: './bin/trade-execution', ...common, env: {
        ...BASE, ...KEY, SERVICE_PORT: '9004', METRICS_PORT: '9090', PAPER_WS_PORT: '8091',
        POSTGRES_DB: 'execution_db', USER_CONFIG_GRPC_ADDR: 'localhost:9003',
        // Protective replayer: 09:14 morning arm (re-checks DEFERRED SLs vs
        // today's DPR band) + 16:35 EOD Phase A (AMO for open positions) +
        // Phase C conversion check + ArmRetryWorker (wakes on fresh login via
        // USER_CREDENTIALS_UPDATED, 5-min poll fallback). Without this flag
        // deferred SLs are never re-armed (2026-08-05: 5/8 unprotected).
        MANTHAN_PROTECTIVE_REPLAY_ENABLED: 'true',
        // Manual-exit ledger projector (2026-09-03 ghost formation fix):
        // ON by default; set MANTHAN_MANUAL_EXIT_LEDGER=off in .env to
        // disable (rollback path — must be forwarded here or it never
        // reaches the process; env blocks are whitelists).
        MANTHAN_MANUAL_EXIT_LEDGER: ENV.MANTHAN_MANUAL_EXIT_LEDGER || '',
        // Price client (paper monitor, OCO trailing SL, price-monitor fallback)
        // reads market:nse:{token} LTP — point REDIS_HOST/PORT at the external
        // market feed, NOT the empty local redis. EXT_REDIS_ADDR (broker adapter)
        // is already external via BASE.
        REDIS_HOST: EXT_HOST, REDIS_PORT: EXT_PORT, REDIS_PASSWORD: EXT_PW,
    }},
    { name: 'orderstatus', script: './bin/orderstatus', ...common, env: {
        ...BASE, HTTP_PORT: '9006', ORDER_STATUS_DB: 'order_status_db',
        USER_CONFIG_GRPC_ADDR: 'localhost:9003',
    }},
    { name: 'positions', script: './bin/positions', ...common, env: {
        ...BASE, HTTP_PORT: '9007', POSITIONS_DB: 'positions_db',
        TRADE_EXEC_GRPC_ADDR: 'localhost:9004', RECONCILER_ENABLED: 'false', RECONCILER_INTERVAL_SEC: '300',
    }},
    { name: 'portfolio', script: './bin/portfolio', ...common, env: {
        ...BASE, GRPC_PORT: '9008', HTTP_PORT: '9009', POSITIONS_DB: 'positions_db',
        POSITIONS_DB_USER: 'postgres', POSITIONS_DB_PASSWORD: 'postgres',
    }},
    { name: 'risk-management', script: './bin/risk-management', ...common, env: {
        ...BASE, GRPC_PORT: '9010',
    }},
    // ── best-effort: need external creds this box may lack; kept so their
    //    logs are visible. Expect errors until creds are supplied. ──
    { name: 'api-gateway', script: './bin/api-gateway', ...common, env: {
        ...BASE, HTTP_PORT: '8099', CODIFI_JWT_SECRET: 'dev-secret-change-me',
        USER_CONFIG_GRPC_ADDR: 'localhost:9003',          // was defaulting to :50051
        TRADE_EXECUTION_PAPER_URL: 'http://localhost:8091',
        CORS_ALLOWED_ORIGINS: 'https://manthan-dev.stockk.trade',
        REDIS_LOCAL_ADDR: 'localhost:6389',               // ws pub/sub redis (algo-dev)
        PORTFOLIO_GRPC_ADDR: 'localhost:9008',            // portfolio svc gRPC (NOT 9005 = rules-engine)
        // Ghost reaper (2026-09-03): unset/'' = OFF, 'dry_run' = audit-only
        // daily 15:50 IST sweep, 'enabled' = auto-heal through the same
        // GhostPreview gate as the manual flow. Value comes from .env so
        // flipping modes is a .env edit + restart, no code change.
        MANTHAN_GHOST_REAPER_MODE: ENV.MANTHAN_GHOST_REAPER_MODE || '',
    }},
    // manthan-live now runs as a WATCH DAEMON (2026-08-18): after the initial
    // run it fingerprints the BuySignal tab every MANTHAN_WATCH_INTERVAL secs
    // (Mon–Fri 08:45–15:25 IST) and re-runs the pipeline automatically when
    // the operator edits the sheet — no manual re-runs. Publisher idempotency
    // means only NEW symbols publish; rules-engine fans them to every active
    // strategy. cron_restart (BOX TIME = UTC; '30 3' = 09:00 IST) restarts the
    // daemon daily pre-open — the restart's initial run IS the morning load,
    // and doubles as a watchdog if the loop ever wedges.
    { name: 'data-ingestion', script: './bin/data-ingestion', ...common,
      autorestart: true, cron_restart: '30 3 * * 1-5', env: {
        ...BASE,
        MANTHAN_WATCH: '1', MANTHAN_WATCH_INTERVAL: '120',
        MANTHAN_SHEET_ID: '1E_MzQNQFNvnmR8wMZMCyzPKc-SjOei4wwQp4QAey5sc',
        MANTHAN_CREDS: '/home/ubuntu/Algo-Treading/services/data-ingestion/credentials/manthan-sheet.json',
        // manthan-live writes signals via MARKET_DATA_DB_* (default port 5432 =
        // the OTHER stack's postgres). Point it at algo-dev's signals_db.
        MARKET_DATA_DB_HOST: 'localhost', MARKET_DATA_DB_PORT: '5442',
        MARKET_DATA_DB_USER: 'postgres', MARKET_DATA_DB_PASSWORD: 'postgres',
        MARKET_DATA_DB_NAME: 'signals_db', MARKET_DATA_DB_SSLMODE: 'disable',
        // EMA allocations (manthan:ema:allocations) must land on the SAME
        // redis rules-engine's allocator reads (its REDIS_ADDRS = the local
        // algo-dev redis). Unset REDIS_URI meant manthan-live pinged a dead
        // localhost:6379, logged "EMA allocations not cached", and the
        // allocator fell back to the hardcoded 0.30 split every day.
        REDIS_URI: 'localhost:6389', REDIS_PASSWORD: '',
    }},
  ],
};
