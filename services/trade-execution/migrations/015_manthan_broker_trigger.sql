-- Migration 015: broker-real SL trigger tracking (single-source-of-truth fix)
--
-- Problem: trade-execution stored only the *requested* SL trigger in
-- manthan_orders.trigger_price, and worse, it DPR-clamped that value before
-- storing — so the stored trigger drifted from both the intended 20% level and
-- the broker's actual resting trigger. The trail ratchet then compared against
-- the clamped value and refused to relax the SL back to 20% (SANDHAR freeze).
--
-- Fix: split the two concepts.
--   trigger_price        = INTENDED stop (high * 0.80, un-clamped) — drives the
--                          trail + ratchet so it keeps modifying on 2% moves and
--                          can relax back to the true 20%.
--   broker_trigger_price = BROKER-REAL resting trigger (post DPR/tick clamp), the
--                          value actually accepted by the exchange. Written from
--                          the broker adapter's return value on place/modify and
--                          re-synced every cycle by the reconciler.
--
-- Both new columns are additive + nullable → zero-downtime ADD COLUMN, no rewrite.

ALTER TABLE manthan_orders
    ADD COLUMN IF NOT EXISTS broker_trigger_price NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS broker_limit_price   NUMERIC(12,2);
