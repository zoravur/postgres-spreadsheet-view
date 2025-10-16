package common

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// EncodeHandle returns a canonical base64 string of the form:
//
//	"public.actor|actor_id=5,seq=3"
func EncodeHandle(schema, table string, pkCols []string, pkVals []any) string {
	var kvPairs []string
	for i := range pkCols {
		kvPairs = append(kvPairs, fmt.Sprintf("%s=%v", pkCols[i], pkVals[i]))
	}
	raw := fmt.Sprintf("%s.%s|%s", schema, table, strings.Join(kvPairs, ","))
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeHandle parses a base64 handle in the same format.
func DecodeHandle(h string) (schema, table string, pk map[string]any, err error) {
	b, err := base64.RawURLEncoding.DecodeString(h)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid base64: %w", err)
	}

	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return "", "", nil, fmt.Errorf("malformed handle")
	}

	st := parts[0] // e.g. "public.actor"
	keyPart := parts[1]

	split := strings.SplitN(st, ".", 2)
	if len(split) != 2 {
		return "", "", nil, fmt.Errorf("malformed table path")
	}
	schema, table = split[0], split[1]

	pk = make(map[string]any)
	for _, kv := range strings.Split(keyPart, ",") {
		if kv == "" {
			continue
		}
		pair := strings.SplitN(kv, "=", 2)
		if len(pair) != 2 {
			continue
		}
		pk[strings.TrimSpace(pair[0])] = strings.TrimSpace(pair[1])
	}
	return schema, table, pk, nil
}
