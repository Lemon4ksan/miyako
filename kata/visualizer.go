// Copyright (c) 2026 Lemon4ksan All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package kata

import (
	"fmt"
	"strings"
)

// ToDOT exports the state machine's transition graph as a Graphviz DOT
// representation. The output can be rendered via `dot -Tsvg` or any
// online Graphviz renderer to produce a visual state diagram.
func (f *FSM[State, Event]) ToDOT() string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var sb strings.Builder

	sb.WriteString("digraph FSM {\n")
	sb.WriteString("    rankdir=LR;\n")
	sb.WriteString("    node [shape=circle, style=filled, fillcolor=lightblue];\n\n")

	fmt.Fprintf(&sb, "    \"%v\" [fillcolor=lightgreen, style=filled, penwidth=2];\n\n", f.current)

	for from, events := range f.rules {
		for event, to := range events {
			fmt.Fprintf(&sb, "    \"%v\" -> \"%v\" [label=\"%v\"];\n", from, to, event)
		}
	}

	sb.WriteString("}\n")

	return sb.String()
}
