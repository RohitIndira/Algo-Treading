# 🚀 Complete Production Implementation Plan

## 📋 **Remaining Tasks**

### **1. Position Tracker Integration + Kafka Publishing** ⚡ HIGH PRIORITY
**What:**
- Add Kafka publisher to PositionTracker
- Publish position state events to `position-states` topic
- Wire into Engine (call on entry/exit)
- Wire into ExitManager (call on partial exits)

**Events Published:**
- Position opened
- Position partial exit
- Position closed
- Unrealized P&L updates (every 5s)

**Files to Modify:**
- `internal/cash52w/position_tracker.go` - Add Kafka publisher
- `internal/cash52w/engine.go` - Add tracker, call TrackNewPosition
- `internal/cash52w/exit_manager.go` - Call RecordExit on exits
- `cmd/main.go` - Initialize position tracker with Kafka

---

### **2. Advanced Rebalancing Engine** ⚡ HIGH PRIORITY
**What:** Production-grade capital management system

**Features:**
- **Capital Tracking:**
  - Total capital per user (from Phase 1 config)
  - Deployed capital (sum of open positions)
  - Free capital (Total - Deployed)
  - Capital utilization % (Deployed/Total * 100)

- **Dynamic Position Sizing:**
  - Adjust `capital_per_stock` based on free capital
  - If free capital < capital_per_stock, use free capital
  - If free capital = 0, skip new entries

- **Auto-Rebalancing:**
  - When position exits → capital freed
  - Check if should enter new breakouts
  - Priority: oldest breakout from today first
  - Respect max_positions limit

- **Position Limits:**
  - Hard limit: max_stocks from Phase 1 config
  - Soft limit: pause_new_entries flag
  - Emergency: force_exit_all flag

**Kafka Events:**
```
Topic: rebalance-events

Events:
- CAPITAL_FREED (position exit)
- REBALANCE_TRIGGERED (auto-entry after exit)
- LIMIT_REACHED (max positions hit)
- CAPITAL_EXHAUSTED (no free capital)
- REBALANCE_SKIPPED (pause_new_entries)
```

**Files to Create:**
- `internal/cash52w/rebalancer.go` - Main rebalancing engine
- `internal/models/rebalance_event.go` - Kafka event model

**Files to Modify:**
- `internal/cash52w/engine.go` - Add rebalancer reference
- `internal/cash52w/exit_manager.go` - Trigger rebalance on exit
- `cmd/main.go` - Initialize rebalancer

---

### **3. File Cleanup** 🧹 MEDIUM PRIORITY
**Remove:**
- `internal/matcher/*` (news matching - not used)
- `internal/jobbing/*` (separate strategy)
- `internal/index/*` (elasticsearch - not used)
- `internal/sync/*` (strategy sync - handled by config consumer)
- `cmd/jobbing_config_loader.go`
- Unused imports in remaining files

---

## 🎯 **Implementation Order**

### **Phase A: Position Tracker + Kafka** (30 min)
1. Add Kafka publisher to PositionTracker
2. Add PublishPositionState() method
3. Wire into Engine.handleForUser()
4. Wire into ExitManager.executeExitSignal()
5. Build & test

### **Phase B: Advanced Rebalancing** (45 min)
1. Create rebalancer.go with capital tracking
2. Create rebalance_event.go model
3. Implement capital freed detection
4. Implement auto-entry logic
5. Add Kafka event publishing
6. Wire into exit_manager
7. Build & test

### **Phase C: File Cleanup** (15 min)
1. Remove unused directories
2. Clean imports
3. Final build

---

## 📊 **Kafka Topics Summary**

After implementation:

1. **trade-signals** - All BUY/SELL orders
2. **position-states** - Position lifecycle tracking
3. **rebalance-events** - Capital management events
4. **portfolio.allocations** - Portfolio snapshots
5. **portfolio.realtime** - Live P&L
6. **market:52w-breakouts** (consumed)
7. **user-configs.cash52w** (consumed)

---

## ⏱️ **Total Estimated Time: 90 minutes**

**Let's start with Phase A now!**
