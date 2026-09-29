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
	fmt.Println("| scenario | " + strings.Join(pools, " | ") + " |")
	fmt.Println("|---" + strings.Repeat("|---", len(pools)) + "|")
	for _, sc := range scenarios {
		best := -1.0
		for _, p := range pools {
			if c := cells[[2]string{sc, p}]; c != nil && (best < 0 || median(c.ns) < best) {
				best = median(c.ns)
			}
		}
		row := []string{sc}
		for _, p := range pools {
			c := cells[[2]string{sc, p}]
			if c == nil {
				row = append(row, "-")
				continue
			}
			text := format(median(c.ns)) + " / " + format(median(c.cpu))
			if median(c.ns) == best {
				text = "**" + text + "**"
			}
			if slices.Max(c.ns) > spreadNote*slices.Min(c.ns) {
				text += " *"
			}
			row = append(row, text)
		}
		fmt.Println("| " + strings.Join(row, " | ") + " |")
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
