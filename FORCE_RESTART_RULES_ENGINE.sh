#!/bin/bash

set -e

echo "======================================================"
echo "🔄 FORCE RESTART rules-engine with latest build"
echo "======================================================"
echo ""

# Step 1: Stop ALL running instances
echo "Step 1: Stopping ALL rules-engine processes..."
pkill -9 -f "rules-engine" || echo "No running process found"
sleep 2

# Step 2: Verify binary exists and show timestamp
echo ""
echo "Step 2: Checking binary..."
if [ -f "/home/ubuntu/Algo-Treading/services/rules-engine/bin/rules-engine" ]; then
    ls -lh /home/ubuntu/Algo-Treading/services/rules-engine/bin/rules-engine
    echo "✅ Binary exists"
else
    echo "❌ Binary NOT found! Running build..."
    cd /home/ubuntu/Algo-Treading/services/rules-engine
    ./build.sh
fi

# Step 3: Start fresh service
echo ""
echo "Step 3: Starting rules-engine with NEW binary..."
cd /home/ubuntu/Algo-Treading/services/rules-engine

# Start in background and save PID
nohup ./bin/rules-engine > /tmp/rules-engine-latest.log 2>&1 &
SERVICE_PID=$!

echo "✅ Service started with PID: $SERVICE_PID"
echo ""

# Step 4: Wait and check if service is running
sleep 3
if ps -p $SERVICE_PID > /dev/null; then
    echo "✅ Service is RUNNING"
    echo ""
    echo "======================================================"
    echo "📊 Monitoring logs for NEW diagnostic messages..."
    echo "======================================================"
    echo ""
    echo "Watching for:"
    echo "  📋 ConfigStore snapshot retrieved <-- MUST see this!"
    echo "  ✅ Breakout dispatched to user workers <-- MUST see this!"
    echo ""
    echo "Press Ctrl+C to stop monitoring"
    echo "======================================================"
    echo ""
    
    # Monitor logs
    tail -f /tmp/rules-engine-latest.log | grep --line-buffered -E "Processing 52W breakout|ConfigStore snapshot|Breakout dispatched|No users configured"
else
    echo "❌ Service FAILED to start!"
    echo ""
    echo "Last 30 lines of log:"
    tail -30 /tmp/rules-engine-latest.log
    exit 1
fi
