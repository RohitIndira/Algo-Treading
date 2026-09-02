#!/usr/bin/env bash
# Phase 0/1 — provision a fresh Ubuntu 24.04 box for the Manthan stack.
# Run ON THE NEW SERVER as ubuntu. Idempotent.
set -euo pipefail

GO_VERSION=1.25.12
NODE_MAJOR=20

echo "== apt base =="
sudo apt-get update -q
sudo apt-get install -yq ca-certificates curl gnupg git jq nginx postgresql-client-16 prometheus-node-exporter protobuf-compiler build-essential

echo "== docker =="
if ! command -v docker >/dev/null; then
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" |
    sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -q
  sudo apt-get install -yq docker-ce docker-ce-cli containerd.io docker-compose-plugin
  sudo usermod -aG docker "$USER"
fi

echo "== node + pm2 =="
if ! command -v node >/dev/null; then
  curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | sudo -E bash -
  sudo apt-get install -yq nodejs
fi
sudo npm install -g pm2

echo "== go ${GO_VERSION} =="
if ! /usr/local/go/bin/go version 2>/dev/null | grep -q "$GO_VERSION"; then
  curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o /tmp/go.tgz
  sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz && rm /tmp/go.tgz
  grep -q '/usr/local/go/bin' ~/.profile || echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile
fi

echo "== repo =="
if [ ! -d ~/Algo-Treading ]; then
  git clone https://github.com/RohitIndira/Algo-Treading.git ~/Algo-Treading
fi
cd ~/Algo-Treading && git checkout dev && git pull --ff-only

echo "== generate protobuf .pb.go (gitignored; a fresh clone has only .proto) =="
export PATH=$PATH:/usr/local/go/bin:$(go env GOPATH)/bin
( cd api/proto && make install-tools && make generate-all )

echo "== build all service binaries =="
mkdir -p deployments/separate-namespace/bin
# main package path per service (data-ingestion's is cmd/manthan-live, not cmd)
declare -A MAIN=(
  [api-gateway]=./services/api-gateway/cmd
  [data-ingestion]=./services/data-ingestion/cmd/manthan-live
  [orderstatus]=./services/orderstatus/cmd
  [portfolio]=./services/portfolio/cmd
  [positions]=./services/positions/cmd
  [risk-management]=./services/risk-management/cmd
  [rules-engine]=./services/rules-engine/cmd
  [trade-execution]=./services/trade-execution/cmd
  [user-config]=./services/user-config/cmd
)
for svc in "${!MAIN[@]}"; do
  echo "building $svc"
  CGO_ENABLED=0 go build -o "deployments/separate-namespace/bin/$svc" "${MAIN[$svc]}"
done

echo "== data infra =="
docker compose -f deployments/manthan-infra/docker-compose.yml up -d
echo "  waiting for postgres..."; sleep 8

echo "== gateway_admin role (admin console's grant-hardened DB user) =="
# Migrations 019/020 GRANT to this role; it must exist before trading_db is
# restored/migrated or the grants error out. Password from ~/admin-db.env.
if [ -f ~/admin-db.env ]; then
  ADMPW=$(grep -E '^ADMIN_DB_PASSWORD=' ~/admin-db.env | cut -d= -f2)
  docker exec algo-dev-postgres psql -U postgres -c \
    "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='gateway_admin') \
     THEN CREATE ROLE gateway_admin LOGIN PASSWORD '$ADMPW'; END IF; END \$\$;" || true
  echo "  gateway_admin ensured"
else
  echo "  ⚠ ~/admin-db.env not found — create gateway_admin manually before restoring trading_db"
fi

echo
echo "DONE. Manual steps remaining (deliberately not scripted — they carry secrets):"
echo "  1. Copy ecosystem.config.js + env from the old box (scp, never chat/git):"
echo "       ENCRYPTION_KEY, INDIRA_API_KEY, EXT_REDIS_ADDR(+PASSWORD),"
echo "       LIVEALGOS_LTP_REDIS_ADDR(+PASSWORD), KAFKA_BROKERS, POSTGRES_*"
echo "  2. Copy the nginx vhost + TLS certs; nginx -t && systemctl reload nginx"
echo "  3. Install the two crons (sync_a844 15:00+18:00, sync_nifty 10:35 Mon-Fri)"
echo "  4. pm2 start ecosystem.config.js && pm2 save && pm2 startup"
echo "  5. pm2 install pm2-logrotate; pm2 set pm2-logrotate:max_size 200M;"
echo "     pm2 set pm2-logrotate:retain 7; pm2 set pm2-logrotate:compress true"
