# Test Order Flow - Verify orders are being processed end-to-end
Write-Host "=== Testing Order Flow ===" -ForegroundColor Cyan

Write-Host "`n1. Checking RabbitMQ Queue Status..." -ForegroundColor Yellow
try {
    $queueInfo = docker exec -i trading-rabbitmq rabbitmqadmin -u admin -p admin123 list queues name messages messages_ready messages_unacknowledged 2>&1
    Write-Host $queueInfo
} catch {
    Write-Host "Error checking queue: $_" -ForegroundColor Red
}

Write-Host "`n2. Checking PostgreSQL trade_signals table..." -ForegroundColor Yellow
try {
    $signalCount = docker exec -i trading-postgres psql -U postgres -d trading_db -t -c "SELECT COUNT(*) FROM trade_signals;" 2>&1
    Write-Host "Total trade signals: $signalCount"
    
    $recentSignals = docker exec -i trading-postgres psql -U postgres -d trading_db -c "SELECT order_id, user_id, stock_code, price, status, created_at FROM trade_signals ORDER BY created_at DESC LIMIT 5;" 2>&1
    Write-Host "`nRecent trade signals:"
    Write-Host $recentSignals
} catch {
    Write-Host "Error checking database: $_" -ForegroundColor Red
}

Write-Host "`n3. Checking Kafka trade-signals topic..." -ForegroundColor Yellow
try {
    Write-Host "Last 3 messages from trade-signals topic:"
    docker exec -i trading-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trade-signals --from-beginning --max-messages 3 --timeout-ms 3000 2>&1 | Select-Object -Last 10
} catch {
    Write-Host "Error checking Kafka: $_" -ForegroundColor Red
}

Write-Host "`n4. Checking orders table in PostgreSQL..." -ForegroundColor Yellow
try {
    $orderCount = docker exec -i trading-postgres psql -U postgres -d trading_db -t -c "SELECT COUNT(*) FROM orders;" 2>&1
    Write-Host "Total orders: $orderCount"
    
    $recentOrders = docker exec -i trading-postgres psql -U postgres -d trading_db -c "SELECT order_id, user_id, stock_code, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5;" 2>&1
    Write-Host "`nRecent orders:"
    Write-Host $recentOrders
} catch {
    Write-Host "Error checking orders: $_" -ForegroundColor Red
}

Write-Host "`n=== Test Complete ===" -ForegroundColor Cyan
