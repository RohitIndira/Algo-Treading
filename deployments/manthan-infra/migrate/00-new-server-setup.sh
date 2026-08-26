#!/usr/bin/env bash
# Phase 0/1 — provision a fresh Ubuntu 24.04 box for the Manthan stack.
# Run ON THE NEW SERVER as ubuntu. Idempotent.
set -euo pipefail

GO_VERSION=1.25.12
NODE_MAJOR=20

echo "== apt base =="
sudo apt-get update -q
sudo apt-get install -yq ca-certificates curl gnupg git jq nginx postgresql-client-16 prometheus-node-exporter

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

echo "== build all service binaries =="
export PATH=$PATH:/usr/local/go/bin
mkdir -p deployments/separate-namespace/bin
for svc in api-gateway data-ingestion orderstatus portfolio positions risk-management rules-engine trade-execution user-config; do
  echo "building $svc"
  CGO_ENABLED=0 go build -o "deployments/separate-namespace/bin/$svc" "./services/$svc/cmd"
done

echo "== data infra =="
docker compose -f deployments/manthan-infra/docker-compose.yml up -d

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
