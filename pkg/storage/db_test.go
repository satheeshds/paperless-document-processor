package storage

import (
	"encoding/json"
	"testing"
)

// TestMarshalOrderedRows_BasicOrdering verifies that marshalOrderedRows emits
// JSON objects whose keys appear in the order given by the headers slice rather
// than in Go's map iteration order.
func TestMarshalOrderedRows_BasicOrdering(t *testing.T) {
	headers := []string{"C", "A", "B"}
	rows := []map[string]interface{}{
		{"A": 1, "B": 2, "C": 3},
	}

	data, err := marshalOrderedRows(rows, headers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Decode back via standard JSON (which preserves key insertion order in
	// encoding/json when reading into []map[string]interface{} is NOT order-
	// preserving, so we check the raw bytes instead).
	raw := string(data)
	posC := indexOf(raw, `"C"`)
	posA := indexOf(raw, `"A"`)
	posB := indexOf(raw, `"B"`)

	if posC < 0 || posA < 0 || posB < 0 {
		t.Fatalf("expected all keys in output, got: %s", raw)
	}
	if !(posC < posA && posA < posB) {
		t.Errorf("expected key order C<A<B in JSON, positions: C=%d A=%d B=%d\nraw: %s", posC, posA, posB, raw)
	}
}

// TestMarshalOrderedRows_MissingKeyEmitsNull verifies that a key listed in
// headers but absent from a row is emitted as JSON null.
func TestMarshalOrderedRows_MissingKeyEmitsNull(t *testing.T) {
	headers := []string{"name", "amount", "optional"}
	rows := []map[string]interface{}{
		{"name": "Outlet A", "amount": 1000.0},
		// "optional" is absent
	}

	data, err := marshalOrderedRows(rows, headers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Decode back to verify null.
	var decoded []map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to decode output JSON: %v", err)
	}
	if len(decoded) != 1 {
		t.Fatalf("expected 1 row, got %d", len(decoded))
	}
	if v, ok := decoded[0]["optional"]; !ok || v != nil {
		t.Errorf("expected optional=null, got %v (ok=%v)", v, ok)
	}
}

// TestMarshalOrderedRows_MultipleRows verifies correct output for multiple rows.
func TestMarshalOrderedRows_MultipleRows(t *testing.T) {
	headers := []string{"id", "value"}
	rows := []map[string]interface{}{
		{"id": 1, "value": "a"},
		{"id": 2, "value": "b"},
		{"id": 3, "value": "c"},
	}

	data, err := marshalOrderedRows(rows, headers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded []map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to decode output JSON: %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(decoded))
	}
}

// TestMarshalOrderedRows_EmptyRows verifies that an empty input produces a
// valid empty JSON array.
func TestMarshalOrderedRows_EmptyRows(t *testing.T) {
	data, err := marshalOrderedRows([]map[string]interface{}{}, []string{"a", "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "[]" {
		t.Errorf("expected empty JSON array, got %s", string(data))
	}
}

// TestMarshalOrderedRows_ExtraKeyInRow verifies that keys present in a row but
// absent from headers are silently omitted from the output.
func TestMarshalOrderedRows_ExtraKeyInRow(t *testing.T) {
	headers := []string{"name"}
	rows := []map[string]interface{}{
		{"name": "Outlet", "hidden": "should not appear"},
	}

	data, err := marshalOrderedRows(rows, headers)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if containsBytes(data, "hidden") {
		t.Errorf("expected 'hidden' key to be omitted, but found it in: %s", data)
	}
}

// indexOf returns the byte position of substr in s, or -1 if not found.
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// containsBytes reports whether target appears in data.
func containsBytes(data []byte, target string) bool {
	return indexOf(string(data), target) >= 0
}
