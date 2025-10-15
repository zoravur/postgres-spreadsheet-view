package pg_lineage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Test Data Structs ---

type RewriteCase struct {
	ID            string              `json:"id"`
	Query         string              `json:"query"`
	PrimaryKeys   map[string][]string `json:"primary_keys"` // table → pk columns
	ExpectedSQL   string              `json:"expected_sql"`
	ExpectedAdds  map[string][]string `json:"expected_adds"` // alias → injected PK aliases
	ExpectedError string              `json:"expected_error"`
}

// --- Demo Catalog Stub ---

type DemoPKCatalog struct {
	cols map[string][]string
	pks  map[string][]string
}

func (d *DemoPKCatalog) Columns(q string) ([]string, bool) { v, ok := d.cols[q]; return v, ok }
func (d *DemoPKCatalog) PrimaryKeys(q string) ([]string, bool) {
	v, ok := d.pks[q]
	return v, ok
}

// --- Loader ---

func loadRewriteCases(t *testing.T) []RewriteCase {
	t.Helper()
	path := filepath.Join("testdata", "rewrite_pks_test_cases.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read testdata: %v", err)
	}
	var cases []RewriteCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatalf("failed to unmarshal testdata: %v", err)
	}
	return cases
}

// --- Tests ---

func TestRewriteInjectPKs(t *testing.T) {
	cases := loadRewriteCases(t)
	for _, c := range cases {
		t.Run(c.ID, func(t *testing.T) {
			cat := &DemoPKCatalog{
				cols: demoCols,
				pks:  c.PrimaryKeys,
			}

			gotSQL, gotAdds, err := RewriteSelectInjectPKs(c.Query, cat)

			if c.ExpectedError != "" {
				if err == nil {
					t.Fatalf("expected error %q, got nil", c.ExpectedError)
				}
				if err.Error() != c.ExpectedError {
					t.Fatalf("expected error %q, got %q", c.ExpectedError, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if normalizeSQL(gotSQL) != normalizeSQL(c.ExpectedSQL) {
				t.Fatalf("SQL mismatch\nexpected:\n%s\n\ngot:\n%s", c.ExpectedSQL, gotSQL)
			}

			if !equalProv(gotAdds, c.ExpectedAdds) {
				t.Fatalf("injected alias map mismatch\nexpected: %#v\ngot: %#v",
					c.ExpectedAdds, gotAdds)
			}
		})
	}
}

// Normalize spacing etc. for deparser variance.
func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
