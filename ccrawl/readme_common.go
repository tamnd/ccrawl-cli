package ccrawl

import (
	"fmt"
	"strings"
)

// rankBar renders a fixed-width filled/empty bar for a fraction in [0,1], the
// same style the arctic card uses for its by-year breakdown.
func rankBar(frac float64, width int) string {
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	filled := min(int(frac*float64(width)+0.5), width)
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

// barRow renders one labelled bar line: "  LABEL  ████░░░░  VALUE".
func barRow(label string, frac float64, value string) string {
	return fmt.Sprintf("  %-26s  %s  %s", label, rankBar(frac, 20), value)
}

// tocEntry renders one Markdown table-of-contents line linking to a heading.
func tocEntry(indent int, title, anchor string) string {
	return fmt.Sprintf("%s- [%s](#%s)", strings.Repeat("  ", indent), title, anchor)
}
