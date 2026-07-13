// Package logmask provides a zap core wrapper that hides real strategy identity
// in the rules-engine logs. When enabled it:
//
//   - removes the "strategy_id" field from every log entry, and
//   - shows "strategy_name" as a single fixed placeholder (e.g. High_Impact_Score),
//     injecting it on lines that logged an id but no name.
//
// The result: every strategy logs under one masked name and no real IDs appear.
// It is used for mock/demo sessions and is driven by STRATEGY_NAME_LOG_OVERRIDE.
//
// Masking happens once, at the core level, so all call sites are covered without
// touching individual log statements.
package logmask

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// StrategyIDKey is the log field that is removed entirely.
	StrategyIDKey = "strategy_id"
	// StrategyNameKey is the log field forced to the placeholder value.
	StrategyNameKey = "strategy_name"
)

// Wrap returns logger unchanged when name is empty; otherwise it returns a
// logger that drops strategy_id and shows strategy_name as name in every entry.
func Wrap(logger *zap.Logger, name string) *zap.Logger {
	if logger == nil || name == "" {
		return logger
	}
	return logger.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return &maskCore{Core: c, name: name}
	}))
}

type maskCore struct {
	zapcore.Core
	name string
}

// With masks fields accumulated on child loggers. It does not inject a name here
// (only the terminal Write does) so a name is never duplicated across slices.
func (m *maskCore) With(fields []zapcore.Field) zapcore.Core {
	return &maskCore{Core: m.Core.With(transform(fields, m.name, false)), name: m.name}
}

// Check must register this wrapper (not the embedded core) so Write runs.
func (m *maskCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if m.Enabled(ent.Level) {
		return ce.AddCore(ent, m)
	}
	return ce
}

func (m *maskCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	return m.Core.Write(ent, transform(fields, m.name, true))
}

// transform drops strategy_id fields and rewrites strategy_name to name (keeping
// a single strategy_name). When inject is true and an id was dropped but no name
// was present, it appends strategy_name=name so the line still shows the strategy.
// The input slice is never mutated; a new slice is built only when a strategy
// field is present.
func transform(fields []zapcore.Field, name string, inject bool) []zapcore.Field {
	present := false
	for i := range fields {
		if fields[i].Key == StrategyIDKey || fields[i].Key == StrategyNameKey {
			present = true
			break
		}
	}
	if !present {
		return fields
	}

	out := make([]zapcore.Field, 0, len(fields)+1)
	droppedID := false
	haveName := false
	for _, f := range fields {
		switch {
		case f.Key == StrategyIDKey:
			droppedID = true // drop the id entirely
		case f.Key == StrategyNameKey && f.Type == zapcore.StringType:
			if haveName {
				continue // collapse any duplicates to one
			}
			f.String = name
			haveName = true
			out = append(out, f)
		default:
			out = append(out, f)
		}
	}
	if inject && droppedID && !haveName {
		out = append(out, zap.String(StrategyNameKey, name))
	}
	return out
}
