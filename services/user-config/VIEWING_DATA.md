# Viewing and Understanding Your Database Tables

## Table Relationships

Your database has 4 related tables that work together:

```
┌─────────────────┐
│   strategies    │ ← Main table (8 rows)
│  (Parent)       │
└────────┬────────┘
         │
         │ strategy_id (Foreign Key links)
         │
    ┌────┴────────────────────────────┐
    │                                 │
    ▼                                 ▼
┌──────────────────┐         ┌──────────────────┐
│ strategy_        │         │  trade_configs   │
│ conditions       │         │                  │
│ (7 rows)         │         │  (7 rows)        │
└──────────────────┘         └──────────────────┘
                                      │
                                      │
                                      ▼
                             ┌──────────────────┐
                             │  risk_limits     │
                             │                  │
                             │  (7 rows)        │
                             └──────────────────┘
```

## What Each Table Contains

### 1. **strategies** (8 rows)
**Main table** - stores basic strategy information:
- `strategy_id` (Primary Key)
- `user_id` (e.g., "IS14415")
- `strategy_name` (e.g., "High Impact News Trader")
- `description`
- `active` (true/false)
- `created_at`, `updated_at`

### 2. **strategy_conditions** (7 rows)
**Child table** - stores conditions for when to trigger a strategy:
- `condition_id` (Primary Key)
- `strategy_id` (Foreign Key → links to strategies table)
- `impact_score_threshold`
- `sentiments` (array: ['positive', 'neutral'])
- `categories` (array)
- `stock_codes` (array of numbers)
- `price_range_min`, `price_range_max`

### 3. **trade_configs** (7 rows)
**Child table** - stores how to execute trades:
- `trade_config_id` (Primary Key)
- `strategy_id` (Foreign Key → links to strategies table)
- `order_type` (e.g., 'LIMIT', 'MARKET')
