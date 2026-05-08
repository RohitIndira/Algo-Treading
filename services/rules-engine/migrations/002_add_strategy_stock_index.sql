-- Speeds up HasSignalToday: per-strategy per-stock daily uniqueness check
CREATE INDEX IF NOT EXISTS idx_trade_signals_strategy_stock_date
    ON trade_signals(strategy_id, stock_code, created_at);
