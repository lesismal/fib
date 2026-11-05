package fib

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lesismal/fib/core"
	"github.com/lesismal/fib/taskpool"
)

// scanDecls maps every exported declaration in dir that applies to this build
// to its kind: "type", "const", "var" or "func". With check set, it also
// requires each declaration to be a re-export of the same name in package core:
// a type alias, a constant or variable taken from core, or a function whose
// body calls core.
func scanDecls(t *testing.T, dir string, check bool) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	found := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		if ok, err := build.Default.MatchFile(dir, name); err != nil {
			t.Fatal(err)
		} else if !ok {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				if decl.Recv != nil || !decl.Name.IsExported() {
					continue
				}
				found[decl.Name.Name] = "func"
				if check && !callsCore(decl, decl.Name.Name) {
					t.Errorf("fib.%s does not call core.%s", decl.Name.Name, decl.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if !spec.Name.IsExported() {
							continue
						}
						found[spec.Name.Name] = "type"
						if check {
							if !spec.Assign.IsValid() {
								t.Errorf("fib.%s is a type definition, not an alias", spec.Name.Name)
							} else if !selectsCore(spec.Type, spec.Name.Name) {
								t.Errorf("fib.%s is not aliased to core.%s", spec.Name.Name, spec.Name.Name)
							}
						}
					case *ast.ValueSpec:
						kind := "var"
						if decl.Tok == token.CONST {
							kind = "const"
						}
						for _, ident := range spec.Names {
							if !ident.IsExported() {
								continue
							}
							found[ident.Name] = kind
							if check && (len(spec.Values) != 1 || !selectsCore(spec.Values[0], ident.Name)) {
								t.Errorf("fib.%s is not taken from core.%s", ident.Name, ident.Name)
							}
						}
					}
				}
			}
		}
	}
	return found
}

// selectsCore reports whether expr is the selector core.name.
func selectsCore(expr ast.Expr, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "core"
}

// callsCore reports whether fn's body calls core.name.
func callsCore(fn *ast.FuncDecl, name string) bool {
	found := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok && selectsCore(call.Fun, name) {
			found = true
		}
		return !found
	})
	return found
}

// TestReExportsCoverCore fails when package core exports a declaration package
// fib does not re-export, or re-exports one that is not core's: every public
// declaration of the implementation is re-exported here.
func TestReExportsCoverCore(t *testing.T) {
	coreDecls := scanDecls(t, "core", false)
	rootDecls := scanDecls(t, ".", true)
	for name, kind := range coreDecls {
		rootKind, ok := rootDecls[name]
		if !ok {
			t.Errorf("core.%s (%s) is not re-exported in package fib", name, kind)
			continue
		}
		if rootKind != kind {
			t.Errorf("fib.%s is a %s; core.%s is a %s", name, rootKind, name, kind)
		}
	}
	for name, kind := range rootDecls {
		if _, ok := coreDecls[name]; !ok {
			t.Errorf("fib.%s (%s) has no exported counterpart in package core", name, kind)
		}
	}
}

// TestReExportsAreCore checks at run time what TestReExportsCoverCore checks in
// the source: every re-export is core's value, and the wrappers reach core.
func TestReExportsAreCore(t *testing.T) {
	if DefaultName != core.DefaultName {
		t.Errorf("DefaultName = %q, core's is %q", DefaultName, core.DefaultName)
	}
	if DefaultUDPIdleTimeout != core.DefaultUDPIdleTimeout {
		t.Errorf("DefaultUDPIdleTimeout = %v, core's is %v", DefaultUDPIdleTimeout, core.DefaultUDPIdleTimeout)
	}
	if AcceptFDModulo != core.AcceptFDModulo || AcceptKernelHash != core.AcceptKernelHash {
		t.Error("AcceptPlacement constants are not core's")
	}
	if ProtocolTCP != core.ProtocolTCP || ProtocolUDP != core.ProtocolUDP || ProtocolUnix != core.ProtocolUnix {
		t.Error("Protocol constants are not core's")
	}
	for _, v := range []struct {
		name      string
		got, want error
	}{
		{"ErrNoLocalPort", ErrNoLocalPort, core.ErrNoLocalPort},
		{"ErrNotUDP", ErrNotUDP, core.ErrNotUDP},
		{"ErrSendFileDatagram", ErrSendFileDatagram, core.ErrSendFileDatagram},
		{"ErrSendQueueFull", ErrSendQueueFull, core.ErrSendQueueFull},
		{"ErrUDPIdleTimeout", ErrUDPIdleTimeout, core.ErrUDPIdleTimeout},
		{"ErrWouldBlock", ErrWouldBlock, core.ErrWouldBlock},
	} {
		if v.got != v.want {
			t.Errorf("%s is not core's", v.name)
		}
	}
	if cfg := DefaultConfig(); cfg.Name != core.DefaultName {
		t.Errorf("DefaultConfig().Name = %q, want %q", cfg.Name, core.DefaultName)
	}
	if cfg := DefaultConfigFor(4096); cfg.ReadBufferSize != 4096 {
		t.Errorf("DefaultConfigFor(4096).ReadBufferSize = %d, want 4096", cfg.ReadBufferSize)
	}
	if got, want := DefaultPoolSizing(taskpool.ModeAdaptive), core.DefaultPoolSizing(taskpool.ModeAdaptive); got != want {
		t.Errorf("DefaultPoolSizing = %+v, core's is %+v", got, want)
	}
	if got, want := DefaultStreamPoolSizing(taskpool.ModeAdaptive), core.DefaultStreamPoolSizing(taskpool.ModeAdaptive); got != want {
		t.Errorf("DefaultStreamPoolSizing = %+v, core's is %+v", got, want)
	}
	if got, want := DefaultSizing(4096, 1<<20, 100), core.DefaultSizing(4096, 1<<20, 100); got != want {
		t.Errorf("DefaultSizing = %+v, core's is %+v", got, want)
	}
	if got, want := DefaultWriteWatermark(4096, 1<<20, 100), core.DefaultWriteWatermark(4096, 1<<20, 100); got != want {
		t.Errorf("DefaultWriteWatermark = %d, core's is %d", got, want)
	}
	if got, want := DetectMachine(), core.DetectMachine(); got != want {
		t.Errorf("DetectMachine = %+v, core's is %+v", got, want)
	}
	engine, err := New(DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	engine.Stop()
}
