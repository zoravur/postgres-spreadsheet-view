package pg_lineage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

type ProvenanceCase struct {
	ID            string              `json:"id"`
	Query         string              `json:"query"`
	Expected      map[string][]string `json:"expected"`
	ExpectedError string              `json:"expected_error"`
}

// Test-only demo catalog data (ordered)
var demoCols = map[string][]string{
	"actor":        {"id", "name", "first_name", "last_name"},
	"public.actor": {"id", "name", "first_name", "last_name"},
	"film":         {"id", "title", "revenue", "actor_id"},
	"public.film":  {"id", "title", "revenue", "actor_id"},
}

type DemoCatalog struct{ cols map[string][]string }

func (d *DemoCatalog) Columns(q string) ([]string, bool) { v, ok := d.cols[q]; return v, ok }

var testCatalog = &DemoCatalog{cols: demoCols}

func loadTestCases(t *testing.T) []ProvenanceCase {
	t.Helper()

	path := filepath.Join("testdata", "test_cases.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read testdata: %v", err)
	}

	var cases []ProvenanceCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("failed to unmarshal testdata: %v", err)
	}

	return cases
}

// --- helpers to compare full maps (including unexpected keys) ---

func sortMapValues(m map[string][]string) map[string][]string {
	out := make(map[string][]string, len(m))
	for k, v := range m {
		cp := append([]string(nil), v...)
		sort.Strings(cp)
		out[k] = cp
	}
	return out
}

func equalProv(a, b map[string][]string) bool {
	a = sortMapValues(a)
	b = sortMapValues(b)
	return reflect.DeepEqual(a, b)
}

func TestProvenanceCases(t *testing.T) {
	cases := loadTestCases(t)

	passed := 0
	total := len(cases)

	for _, c := range cases {
		if t.Run(c.ID, func(t *testing.T) {
			got, err := ResolveProvenance(c.Query, testCatalog)

			if c.ExpectedError != "" {
				if err == nil || err.Error() != c.ExpectedError {
					t.Fatalf("expected error %q, got %v", c.ExpectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !equalProv(got, c.Expected) {
				t.Fatalf("provenance mismatch\nexpected: %#v\ngot:      %#v",
					sortMapValues(c.Expected), sortMapValues(got))
			}
		}) {
			passed++
		}
	}

	t.Logf("%d/%d test cases passed", passed, total)
}
