package wal

import (
	"encoding/json"
	"log"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
	"go.uber.org/zap"
)

type Change struct {
	Schema  string `json:"schema"`
	Table   string `json:"table"`
	Kind    string `json:"kind"`
	OldKeys Keys   `json:"oldkeys"`
	NewKeys Keys   `json:"newkeys"`
}
type Keys struct {
	KeyNames  []string      `json:"keynames"`
	KeyValues []interface{} `json:"keyvalues"`
}
type Envelope struct {
	Change []Change `json:"change"`
}

type Consumer struct {
	Reg  *reactive.Registry
	Deps reactive.Deps
}

func (c *Consumer) OnMessage(line []byte) {
	var env Envelope
	if err := json.Unmarshal(line, &env); err != nil {
		log.Printf("❌ WAL decode error: %v", err)
		return
	}

	if len(env.Change) == 0 {
		log.Println("⚠️  No 'change' entries in WAL message")
		return
	}

	for idx, ch := range env.Change {
		chlog := zap.L().With(
			zap.Int("idx", idx),
			zap.String("schema", ch.Schema),
			zap.String("table", ch.Table),
			zap.String("kind", ch.Kind),
		)

		keys := ch.OldKeys
		if ch.Kind == "insert" {
			keys = ch.NewKeys
		}

		kv := make(map[string]any, len(keys.KeyNames))
		for i, name := range keys.KeyNames {
			var val any
			if i < len(keys.KeyValues) {
				val = keys.KeyValues[i]
			}
			kv[name] = val
		}

		fq := ch.Schema + "." + ch.Table
		affected := map[string]map[string]any{fq: kv}

		// Single correlated record for the change
		chlog.Debug("wal_change",
			zap.String("fq", fq),
			zap.Strings("pk_names", keys.KeyNames),
			zap.Any("pk_values", keys.KeyValues),
			zap.Any("affected", affected),
		)

		matched := 0
		c.Reg.ForEach(func(q *reactive.LiveQuery) bool {
			if !contains(q.Tables, fq) {
				return true // skip noiselessly
			}
			matched++
			qlog := chlog.With(
				zap.String("live_query_id", q.ID),
				zap.String("rewritten", q.Rewritten),
			)
			// Trace PK columns for sanity, still correlated
			qlog.Debug("dispatch_partial_refresh", zap.Any("pk_cols", q.PKCols))
			go func(qp *reactive.LiveQuery) {
				reactive.PartialRefresh(c.Deps, qp, affected)
			}(q)
			return true
		})

		c.Reg.ForEach(func(q *reactive.LiveQuery) bool {
			chlog.Debug("registered_live_query",
				zap.String("id", q.ID),
				zap.Strings("tables", q.Tables),
			)
			return true
		})

		if matched == 0 {
			chlog.Warn("No matched queries in fanout; fanout complete", zap.Int("matched_queries", matched))
		} else {
			chlog.Debug("fanout_complete", zap.Int("matched_queries", matched))
		}

	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
