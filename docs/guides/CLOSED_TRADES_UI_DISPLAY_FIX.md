# Closed Trades UI Display Fix — Multi-Level (ML) Orders

**Audience:** Frontend team  
**Context:** Paper trade closed positions view was showing wrong qty, wrong timestamps, and incomplete PnL for ML orders. This document explains which fields to read from the API to render correct data.

---

## The 4 UI Issues and Their Fixes

### Issue 1 — Wrong Quantity (shows partial qty instead of full entry qty)

**What the UI was doing wrong:**  
Reading `filled_quantity` from the entry order to render the BUY qty. After the first ML partial exit fires, `filled_quantity` on the entry order is **decremented** to reflect the remaining open qty — it no longer equals the original entered qty.

**Correct field to use:**  
Always read `quantity` (not `filled_quantity`) from the entry order for display.

```
Entry order field:  quantity        ← use this  (always the original entered qty, e.g. 100)
                    filled_quantity  ← DO NOT use for display (decremented after partial exits)
```

---

### Issue 2 — Wrong Entry Time (shows order-created time instead of fill time)

**What the UI was doing wrong:**  
Displaying `created_at` from the entry order as the entry time.

For `STOP_LOSS` order type, the order is **created** when the signal fires, but the position is only **opened** when the stop-loss trigger price is hit — which can be minutes later.

**Correct field to use:**  
Use `executed_at` (the fill timestamp) as the entry time. Fall back to `created_at` only if `executed_at` is null.

```
Entry order field:  executed_at  ← use this as "Entry Time" (when position actually opened)
                    created_at   ← DO NOT use (signal-received time, before SL trigger fires)
```

**Example for this trade:**  
`created_at = 12:28 pm` (signal), `executed_at = 12:43 pm` (SL triggered) → show **12:43 pm**

---

### Issue 3 — Wrong Exit Time (shows strategy-cancellation time instead of actual exit time)

**What the UI was doing wrong:**  
Using `updated_at` from the exit order as the exit time. When a strategy is deactivated, **all orders get `updated_at` bumped** as part of the bulk cancel — this overwrites the real exit timestamp.

**Correct field to use:**  
For ML exits, read `triggered_at` from the `multi_level_exit_levels` row (joined via `entry_order_id`). This is set at the exact moment the level fires and is never modified by strategy deactivation.

```
ML level field:     triggered_at    ← use this as "Exit Time" for each ML level
Exit order field:   updated_at      ← DO NOT use (gets overwritten on strategy deactivation)
                    executed_at     ← safe to use for non-ML / manual exits
```

**API:** The closed positions endpoint already returns ML levels per order. Read `triggered_at` from each level row.

---

### Issue 4 — Incomplete PnL (misses force-exit portion)

**What the UI was doing wrong:**  
Summing PnL only from exit orders (`paper_pnl` on child SELL orders). When a strategy is force-exited (deactivated/deleted), the remaining open qty is closed by updating the **entry order's** `paper_pnl` directly — no separate filled exit order exists for that portion.

**Correct approach:**  
Total PnL = sum of `paper_pnl` from all child exit/square-off orders **+** `paper_pnl` from the entry order itself (if not null).

```
Entry order:   paper_pnl     ← includes force-exit PnL for any qty closed this way
Exit orders:   paper_pnl     ← includes PnL from ML-triggered partial exits

Total PnL = entry_order.paper_pnl + SUM(exit_orders.paper_pnl)
```

**For the example trade:**  
- SL L1 exit order `paper_pnl = -1001` (70 qty at ₹2,848.70)  
- Entry order `paper_pnl = +120` (remaining 30 qty force-exited at ₹2,867)  
- **Correct total = -₹881** (was showing -₹1,001)

---

## Summary Table

| UI Field | Wrong source | Correct source |
|---|---|---|
| Entry qty | `orders.filled_quantity` | `orders.quantity` |
| Entry time | `orders.created_at` | `orders.executed_at` (fallback: `created_at`) |
| Exit time per ML level | `exit_order.updated_at` | `multi_level_exit_levels.triggered_at` |
| Total PnL | SUM of exit orders' `paper_pnl` | `entry_order.paper_pnl` + SUM of exit orders' `paper_pnl` |

---

## Backend Fix (already deployed)

**Issue 5 (backend):** When a strategy was deactivated, `multi_level_exit_levels` rows with status `ACTIVE` or `PENDING` were never updated to `CANCELLED`. This caused orphaned rows and confused the UI into thinking the position was still partially open.

**Fix:** `CancelAllOrdersByStrategy` in `order_repository.go` now also cancels all `ACTIVE`/`PENDING` ML levels for that strategy's orders in the same call. No UI change needed for this — it is already fixed server-side.

---

## Relevant DB Columns Quick Reference

**`orders` table (entry order)**

| Column | Description |
|---|---|
| `order_id` | UUID, primary key |
| `quantity` | Original entered quantity — use for display |
| `filled_quantity` | Remaining open qty (decremented on partial exits) — not for display |
| `filled_price` | Entry fill price |
| `executed_at` | Timestamp when position opened (SL trigger fired) |
| `created_at` | Timestamp when order record was created (signal received) |
| `paper_exit_price` | Exit price for force-closed portion |
| `paper_pnl` | PnL for force-closed portion — include in total PnL |

**`multi_level_exit_levels` table**

| Column | Description |
|---|---|
| `entry_order_id` | FK to `orders.order_id` |
| `exit_type` | `SL` or `TP` |
| `level_num` | 1–5 |
| `trigger_price` | Price at which this level fires |
| `exit_qty` | Qty exited at this level (may be rebalanced) |
| `original_exit_qty` | Qty before rebalancing — use for display if rebalanced |
| `exit_price` | Actual exit price when triggered |
| `triggered_at` | Exact timestamp this level fired — use as exit time |
| `status` | `PENDING` / `ACTIVE` / `TRIGGERED` / `CANCELLED` |
