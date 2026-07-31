-- FIX B (recovery worker): make manthan_signal_decisions a true transactional
-- outbox by storing the EXACT Kafka value that was published to trade-signals.
--
-- Why: the recovery worker re-publishes stuck PROPOSED rows verbatim from this
-- column. Reconstructing the message from row columns + live strategy config
-- would risk a PAPER/LIVE (trading_mode) mismatch on the re-fired order, because
-- trading_mode / stop_loss_pct / trailing_sl_pct are NOT stored per-decision —
-- they come from the strategy. Storing the exact bytes eliminates that class of
-- bug entirely: the worker can only ever re-send what was already committed.
--
-- Existing rows predate this column and are already DISPATCHED or terminal, so
-- the recovery scan (status='PROPOSED' AND kafka_payload IS NOT NULL) ignores
-- them. No backfill required.

ALTER TABLE manthan_signal_decisions
    ADD COLUMN IF NOT EXISTS kafka_payload JSONB;
