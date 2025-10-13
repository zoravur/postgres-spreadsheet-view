package pg_lineage

import (
	"encoding/json"
	"os"
	"path/filepath"
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

func TestProvenanceCases(t *testing.T) {
	cases := loadTestCases(t)
	for _, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			got, err := ResolveProvenance(c.Query, testCatalog)
			if c.ExpectedError != "" {
				if err == nil || err.Error() != c.ExpectedError {
					t.Errorf("expected error %q, got %v", c.ExpectedError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for k, want := range c.Expected {
				gotVals := got[k]
				if !sameSet(want, gotVals) {
					t.Errorf("for %q: expected %v, got %v", k, want, gotVals)
				}
			}
		})
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, y := range b {
		m[y]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}
