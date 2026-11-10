// Package tutorial holds fib's tutorial; its tests check that the pages hold
// together: every relative link resolves to a file and heading, and every
// complete Go program in a code block builds.
package tutorial

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"unicode"
)

var (
	fence      = regexp.MustCompile("(?s)```.*?```")
	inlineCode = regexp.MustCompile("`[^`\n]*`")
	link       = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	heading    = regexp.MustCompile(`(?m)^#{1,6} (.*)$`)
	anchorTag  = regexp.MustCompile(`<a id="([^"]+)"`)
	textLink   = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
	scheme     = regexp.MustCompile(`^[a-z]+:`)
)

// chapters returns the tutorial's Markdown pages. It is the pages of this
// directory, not a recursive walk: the tutorial is flat.
func chapters(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.md")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no Markdown pages in docs/tutorial")
	}
	return files
}

// TestLinks checks every relative link in the tutorial: the file it names
// exists, and so does the heading or anchor after its #, as GitHub makes
// them. Targets outside the tutorial (fib's reference pages and README) are
// checked the same way.
func TestLinks(t *testing.T) {
	files := chapters(t)
	anchors := map[string]map[string]bool{}
	anchorsOf := func(path string) map[string]bool {
		if a, ok := anchors[path]; ok {
			return a
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		a := map[string]bool{}
		seen := map[string]int{}
		for _, m := range heading.FindAllStringSubmatch(fence.ReplaceAllString(string(data), ""), -1) {
			s := slug(textLink.ReplaceAllString(strings.ReplaceAll(m[1], "`", ""), "$1"))
			if n, ok := seen[s]; ok {
				seen[s] = n + 1
				s = fmt.Sprintf("%s-%d", s, n+1)
			} else {
				seen[s] = 0
			}
			a[s] = true
		}
		for _, m := range anchorTag.FindAllStringSubmatch(string(data), -1) {
			a[m[1]] = true
		}
		anchors[path] = a
		return a
	}

	checked := 0
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// Links in code blocks and inline code are examples, not links.
		text := inlineCode.ReplaceAllString(fence.ReplaceAllString(string(data), ""), "")
		for _, m := range link.FindAllStringSubmatch(text, -1) {
			target := m[1]
			if scheme.MatchString(target) || strings.HasPrefix(target, "mailto:") {
				continue
			}
			checked++
			name, frag, _ := strings.Cut(target, "#")
			dest := path
			if name != "" {
				dest = filepath.Join(filepath.Dir(path), name)
			}
			if _, err := os.Stat(dest); err != nil {
				t.Errorf("%s: %s: no such file", path, target)
				continue
			}
			if frag != "" && strings.HasSuffix(dest, ".md") && !anchorsOf(dest)[frag] {
				t.Errorf("%s: %s: no such heading or anchor", path, target)
			}
		}
	}
	if checked == 0 {
		t.Fatalf("%s: no links found", files[0])
	}
}

// slug is the anchor GitHub gives a heading: lower case, letters, digits, '-'
// and '_' kept, spaces made '-', everything else dropped.
func slug(h string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(h)) {
		switch {
		case r == ' ':
			b.WriteRune('-')
		case r == '-', r == '_', unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(r)
		}
	}
	return b.String()
}
