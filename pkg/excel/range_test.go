package excel

import (
	"testing"
)

// TestNewCell_Valid verifies parsing of standard "A1" style references.
func TestNewCell_Valid(t *testing.T) {
	tests := []struct {
		input      string
		wantCol    string
		wantRow    int
	}{
		{"A1", "A", 1},
		{"Z100", "Z", 100},
		{"BH7", "BH", 7},
		{"AA200", "AA", 200},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			cell, err := NewCell(tt.input)
			if err != nil {
				t.Fatalf("NewCell(%q) unexpected error: %v", tt.input, err)
			}
			if cell.Column != tt.wantCol {
				t.Errorf("Column = %q, want %q", cell.Column, tt.wantCol)
			}
			if cell.Row != tt.wantRow {
				t.Errorf("Row = %d, want %d", cell.Row, tt.wantRow)
			}
		})
	}
}

// TestNewCell_ColumnOnly verifies that a column-only reference (no row number) is
// accepted and returns Row = 0.
func TestNewCell_ColumnOnly(t *testing.T) {
	cell, err := NewCell("BH")
	if err != nil {
		t.Fatalf("NewCell(\"BH\") unexpected error: %v", err)
	}
	if cell.Column != "BH" {
		t.Errorf("Column = %q, want \"BH\"", cell.Column)
	}
	if cell.Row != 0 {
		t.Errorf("Row = %d, want 0", cell.Row)
	}
}

// TestNewRange_Valid verifies parsing of a standard "A1:Z100" range expression.
func TestNewRange_Valid(t *testing.T) {
	tests := []struct {
		input     string
		wantStart string
		wantEnd   string
	}{
		{"A1:Z100", "A1", "Z100"},
		{"B2:D10", "B2", "D10"},
		{"A:BH", "A", "BH"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			r, err := NewRange(tt.input)
			if err != nil {
				t.Fatalf("NewRange(%q) unexpected error: %v", tt.input, err)
			}
			if r.Start.String() != tt.wantStart {
				t.Errorf("Start = %q, want %q", r.Start.String(), tt.wantStart)
			}
			if r.End.String() != tt.wantEnd {
				t.Errorf("End = %q, want %q", r.End.String(), tt.wantEnd)
			}
		})
	}
}

// TestNewRange_InvalidFormat verifies that a malformed range expression returns
// an error.
func TestNewRange_InvalidFormat(t *testing.T) {
	_, err := NewRange("A1")
	if err == nil {
		t.Fatal("expected error for range without colon, got nil")
	}
}

// TestNewRange_TooManyParts verifies that more than two colon-separated parts
// returns an error.
func TestNewRange_TooManyParts(t *testing.T) {
	_, err := NewRange("A1:B2:C3")
	if err == nil {
		t.Fatal("expected error for range with too many colons, got nil")
	}
}

// TestCell_String verifies the String() method for cells with and without row.
func TestCell_String(t *testing.T) {
	tests := []struct {
		cell Cell
		want string
	}{
		{Cell{Column: "A", Row: 1}, "A1"},
		{Cell{Column: "BH", Row: 0}, "BH"},
		{Cell{Column: "Z", Row: 100}, "Z100"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.cell.String()
			if got != tt.want {
				t.Errorf("Cell.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestRange_String verifies the String() method for ranges.
func TestRange_String(t *testing.T) {
	tests := []struct {
		r    Range
		want string
	}{
		{Range{Start: Cell{Column: "A", Row: 1}, End: Cell{Column: "Z", Row: 100}}, "A1:Z100"},
		{Range{Start: Cell{Column: "B", Row: 2}, End: Cell{Column: "D", Row: 10}}, "B2:D10"},
		{Range{Start: Cell{Column: "A", Row: 0}, End: Cell{Column: "BH", Row: 0}}, "A:BH"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.r.String()
			if got != tt.want {
				t.Errorf("Range.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestNewRange_RoundTrip verifies that parsing and re-stringifying a range
// returns the original expression.
func TestNewRange_RoundTrip(t *testing.T) {
	inputs := []string{"A1:Z100", "B2:D10", "A7:BH"}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			r, err := NewRange(input)
			if err != nil {
				t.Fatalf("NewRange(%q) unexpected error: %v", input, err)
			}
			if r.String() != input {
				t.Errorf("round-trip: got %q, want %q", r.String(), input)
			}
		})
	}
}
