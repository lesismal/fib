// Command summarize turns the output of BenchmarkPools into a Markdown table:
// for each scenario and pool the median ns/op and cpu-ns/op over the runs,
// the fastest of each scenario in bold, and a note on the cells whose runs
// were spread wide.
//
//	go run ./summarize bench.txt [more.txt ...]
//
// With no file it reads standard input.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var line = regexp.MustCompile(`^BenchmarkPools/(\w+)/([\w-]+?)(?:-(\d+))?\s+\d+\s+([\d.]+) ns/op(?:\s+([\d.]+) cpu-ns/op)?`)

type cell struct{ ns, cpu []float64 }

// spreadNote is the max/min ratio over a cell's runs above which the table
// flags it.
const spreadNote = 1.3

func main() {
	var inputs []io.Reader
	for _, name := range os.Args[1:] {
		f, err := os.Open(name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		inputs = append(inputs, f)
	}
	if len(inputs) == 0 {
		inputs = append(inputs, os.Stdin)
	}
	// A run with several -cpu values reports each GOMAXPROCS as a suffix;
	// those are kept apart as scenarios of their own.
	var records [][]string
	procs := map[string]bool{}
	for _, in := range inputs {
		scanner := bufio.NewScanner(in)
		for scanner.Scan() {
			if m := line.FindStringSubmatch(scanner.Text()); m != nil {
				records = append(records, m)
				procs[m[3]] = true
			}
		}
	}
	var scenarios, pools []string
	cells := map[[2]string]*cell{}
	for _, m := range records {
		scenario, pool := m[1], m[2]
		if len(procs) > 1 {
			scenario += " (P=" + m[3] + ")"
		}
		key := [2]string{scenario, pool}
		if !slices.Contains(scenarios, scenario) {
			scenarios = append(scenarios, scenario)
		}
		if !slices.Contains(pools, pool) {
			pools = append(pools, pool)
		}
		c := cells[key]
		if c == nil {
			c = &cell{}
			cells[key] = c
		}
		ns, _ := strconv.ParseFloat(m[4], 64)
		cpu, _ := strconv.ParseFloat(m[5], 64)
		c.ns = append(c.ns, ns)
		c.cpu = append(c.cpu, cpu)
	}
	if len(scenarios) == 0 {
		fmt.Fprintln(os.Stderr, "summarize: no BenchmarkPools results found")
		os.Exit(1)
	}

	fmt.Println("median ns/op / cpu-ns/op; fastest in bold; * runs spread more than", spreadNote, "x")
	fmt.Println()
	printTable(scenarios, pools, cells)
}

// printTable prints the table with every column padded to one width, so that
// it reads as well in a terminal as it renders as Markdown. Within a column
// the ns/op and cpu-ns/op figures are padded apart, so that their slashes
// line up. The padding stays outside the bold markers, which Markdown only
// honours around text that starts and ends with a non-space, and a column
// that has a bold or flagged cell pads the others where the markers go.
func printTable(scenarios, pools []string, cells map[[2]string]*cell) {
	type entry struct {
		ns, cpu     string
		best, noisy bool
	}
	entries := make([][]*entry, len(scenarios))
	nsWidth := make([]int, len(pools))
	cpuWidth := make([]int, len(pools))
	hasBest := make([]bool, len(pools))
	hasNoisy := make([]bool, len(pools))
	for i, sc := range scenarios {
		best := -1.0
		for _, p := range pools {
			if c := cells[[2]string{sc, p}]; c != nil && (best < 0 || median(c.ns) < best) {
				best = median(c.ns)
			}
		}
		entries[i] = make([]*entry, len(pools))
		for j, p := range pools {
			c := cells[[2]string{sc, p}]
			if c == nil {
				continue
			}
			e := &entry{
				ns: format(median(c.ns)), cpu: format(median(c.cpu)),
				best: median(c.ns) == best, noisy: slices.Max(c.ns) > spreadNote*slices.Min(c.ns),
			}
			entries[i][j] = e
			nsWidth[j] = max(nsWidth[j], len(e.ns))
			cpuWidth[j] = max(cpuWidth[j], len(e.cpu))
			hasBest[j] = hasBest[j] || e.best
			hasNoisy[j] = hasNoisy[j] || e.noisy
		}
	}

	// A cell is "  **ns / cpu** *": the padding of ns, the bold markers
	// each side, and the flag.
	markWidth := func(j int) int {
		if hasBest[j] {
			return len("**")
		}
		return 0
	}
	flagWidth := func(j int) int {
		if hasNoisy[j] {
			return len(" *")
		}
		return 0
	}
	widths := []int{len("scenario")}
	for _, sc := range scenarios {
		widths[0] = max(widths[0], len(sc))
	}
	for j, p := range pools {
		cell := 2*markWidth(j) + nsWidth[j] + len(" / ") + cpuWidth[j] + flagWidth(j)
		widths = append(widths, max(len(p), cell))
	}

	row := func(cols []string) {
		for j, col := range cols {
			if j == 0 {
				cols[j] = col + strings.Repeat(" ", widths[j]-len(col))
			} else {
				cols[j] = strings.Repeat(" ", widths[j]-len(col)) + col
			}
		}
		fmt.Println("| " + strings.Join(cols, " | ") + " |")
	}
	row(append([]string{"scenario"}, pools...))
	var rule []string
	for _, w := range widths {
		rule = append(rule, strings.Repeat("-", w))
	}
	fmt.Println("| " + strings.Join(rule, " | ") + " |")
	for i, sc := range scenarios {
		cols := []string{sc}
		for j := range pools {
			e := entries[i][j]
			if e == nil {
				cols = append(cols, "-")
				continue
			}
			mark := strings.Repeat(" ", markWidth(j))
			if e.best {
				mark = "**"
			}
			flag := strings.Repeat(" ", flagWidth(j))
			if e.noisy {
				flag = " *"
			}
			pad := strings.Repeat(" ", nsWidth[j]-len(e.ns))
			cpu := strings.Repeat(" ", cpuWidth[j]-len(e.cpu)) + e.cpu
			closing := mark
			if !e.best {
				// The spaces standing in for the markers go before the
				// closing ones' place, keeping the cell's right edge.
				pad += mark
				mark, closing = "", strings.Repeat(" ", markWidth(j))
			}
			cols = append(cols, pad+mark+e.ns+" / "+cpu+closing+flag)
		}
		row(cols)
	}
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func format(v float64) string {
	if v >= 100 {
		s := strconv.FormatFloat(v, 'f', 0, 64)
		for i := len(s) - 3; i > 0; i -= 3 {
			s = s[:i] + "," + s[i:]
		}
		return s
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}
