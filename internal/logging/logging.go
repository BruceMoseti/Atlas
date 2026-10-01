// Package logging configures Atlas's structured logs.
//
// Every log line is an event with identifiers attached, never a sentence. Debugging a
// distributed failure means following one job across several processes, and that only
// works if `job_id=J42` is greppable in every line that touched it.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Options configures New.
type Options struct {
	// Level is one of debug, info, warn, error.
	Level string
	// JSON emits one JSON object per line instead of logfmt.
	JSON bool
	// Out defaults to stderr.
	Out io.Writer
}

// New builds a logger. In logfmt mode the message is rendered as `event=<name>` so
// that log lines read as the event stream they are:
//
//	event=job_assigned job_id=J42 attempt_id=A2 worker_id=W3 lease_id=L991
func New(opts Options) *slog.Logger {
	out := opts.Out
	if out == nil {
		out = os.Stderr
	}

	var lvl slog.Level
	switch strings.ToLower(opts.Level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	hopts := &slog.HandlerOptions{Level: lvl}
	if opts.JSON {
		return slog.New(slog.NewJSONHandler(out, hopts))
	}

	hopts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 && a.Key == slog.MessageKey {
			return slog.Attr{Key: "event", Value: a.Value}
		}
		return a
	}
	return slog.New(slog.NewTextHandler(out, hopts))
}

// Discard returns a logger that drops everything, for tests.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}
