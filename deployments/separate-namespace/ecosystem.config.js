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
const REDIS = { REDIS_ADDRS: 'localhost:6389', EXT_REDIS_ADDR: 'localhost:6389',
                REDIS_HOST: 'localhost', REDIS_PORT: '6389' };
const KEY = { ENCRYPTION_KEY: ENV.ENCRYPTION_KEY, ALLOW_INSECURE_ENCRYPTION_KEY: 'false' };
const BASE = { ...PG, ...KAFKA, ...REDIS };

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
    }},
    { name: 'data-ingestion', script: './bin/data-ingestion', ...common, env: {
        ...BASE, MANTHAN_SHEET_ID: '', MANTHAN_CREDS: '',
    }},
  ],
};
