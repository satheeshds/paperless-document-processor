package excel

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type Cell struct {
	Column string `json:"column"`
	Row    int    `json:"row"`
}

func NewCell(cell string) (Cell, error) {
	for i, r := range cell {
		if unicode.IsDigit(r) {
			row, err := strconv.Atoi(cell[i:])
			if err != nil && len(cell[i:]) > 0 {
				return Cell{}, fmt.Errorf("invalid cell format: %s", cell)
			}
			return Cell{Column: cell[:i], Row: row}, nil
		}
	}
	// Range might have letter only
	return Cell{Column: cell, Row: 0}, nil
}

type Range struct {
	Start Cell `json:"start"`
	End   Cell `json:"end"`
}

func NewRange(rangeExpr string) (Range, error) {
	parts := strings.Split(rangeExpr, ":")
	if len(parts) != 2 {
		return Range{}, fmt.Errorf("invalid range format: %s", rangeExpr)
	}
	startCell, err := NewCell(parts[0])
	if err != nil {
		return Range{}, err
	}
	endCell, err := NewCell(parts[1])
	if err != nil {
		return Range{}, err
	}
	return Range{Start: startCell, End: endCell}, nil
}

func (c Cell) String() string {
	if c.Row == 0 {
		return c.Column
	}
	return fmt.Sprintf("%s%d", c.Column, c.Row)
}

func (r Range) String() string {
	return fmt.Sprintf("%s:%s", r.Start.String(), r.End.String())
}
