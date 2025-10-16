package wal

import (
	"encoding/json"
	"log"

	"github.com/zoravur/postgres-spreadsheet-view/server/internal/reactive"
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
	log.Printf("🛰️  OnMessage(raw): %s", string(line))

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
		log.Printf("🔸 Change[%d]: schema=%s table=%s kind=%s", idx, ch.Schema, ch.Table, ch.Kind)

		keys := ch.OldKeys
		if ch.Kind == "insert" {
			keys = ch.NewKeys
		}

		kv := make(map[string]any)
		for i, name := range keys.KeyNames {
			val := any(nil)
			if i < len(keys.KeyValues) {
				val = keys.KeyValues[i]
			}
			kv[name] = val
		}
		log.Printf("   ↳ KeyNames=%v  KeyValues=%v", keys.KeyNames, keys.KeyValues)

		fq := ch.Schema + "." + ch.Table
		affected := map[string]map[string]any{fq: kv}

		// Log the derived affected map before fan-out
		log.Printf("   🧩 Affected: %v", affected)

		// Fan out to matching live queries
		c.Reg.ForEach(func(q *reactive.LiveQuery) bool {
			if contains(q.Tables, fq) {
				log.Printf("   📡 Matched LiveQuery %s (%s)", q.ID, q.Rewritten)
				// Trace PKCols for sanity
				for alias, cols := range q.PKCols {
					log.Printf("      alias=%s pkCols=%v", alias, cols)
				}
				go func(qid string) {
					log.Printf("   🚀 Dispatching PartialRefresh for %s", qid)
					reactive.PartialRefresh(c.Deps, q, affected)
				}(q.ID)
			} else {
				log.Printf("   🚫 Skipped LiveQuery %s (tables=%v)", q.ID, q.Tables)
			}
			return true
		})
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
