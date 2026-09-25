package fib

import (
	"fmt"
	"log/slog"
	"strings"
)

// Name reports the engine's name: Config.Name, or DefaultName when that was
// empty.
func (e *Engine) Name() string { return e.name }

// Engine reports the engine the connection belongs to: the one that
// accepted or dialed it, even when one of its pollers serves it.
func (c *Connection) Engine() *Engine { return c.engine.root() }

// logRun logs that the engine has started serving, under its name, with the
// addresses it listens on and the task pool its connections run on.
func (e *Engine) logRun() {
	listening := "none"
	if addrs, err := e.ListenAddrs(); err != nil {
		listening = err.Error()
	} else if len(addrs) > 0 {
		parts := make([]string, len(addrs))
		for i, addr := range addrs {
			parts[i] = addr.Network() + "://" + addr.String()
		}
		listening = strings.Join(parts, ",")
	}
	pool := fmt.Sprintf("%T", e.taskPool)
	if named, ok := e.taskPool.(interface{ Name() string }); ok {
		pool = named.Name()
	}
	slog.Info("fib: engine started", "engine", e.name, "listen", listening, "taskPool", pool,
		"pollers", len(e.pollers))
}
