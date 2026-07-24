package main

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/go-logr/logr"
	"github.com/projecteru2/core/log"
)

// crSink forwards controller-runtime's logr output to core/log; reconcile errors surface nowhere else.
type crSink struct {
	ctx  context.Context
	name string
	kv   []any
}

// The root name stays empty; controller-runtime adds its own via WithName.
func newCRLogger(ctx context.Context) logr.Logger {
	return logr.New(&crSink{ctx: ctx})
}

func (s *crSink) Init(logr.RuntimeInfo) {}

// Errors bypass this gate entirely (logr contract).
func (s *crSink) Enabled(level int) bool { return level == 0 }

func (s *crSink) Info(_ int, msg string, kvs ...any) {
	log.WithFunc(s.funcName()).Info(s.ctx, s.line(msg, kvs))
}

func (s *crSink) Error(err error, msg string, kvs ...any) {
	if err == nil {
		// logr allows Error(nil, ...) for anomaly reports, but core/log drops
		// nil-err Error lines entirely; keep them visible as warnings.
		log.WithFunc(s.funcName()).Warn(s.ctx, s.line(msg, kvs))
		return
	}
	log.WithFunc(s.funcName()).Error(s.ctx, err, s.line(msg, kvs))
}

func (s *crSink) WithValues(kvs ...any) logr.LogSink {
	next := *s
	next.kv = slices.Concat(s.kv, kvs)
	return &next
}

func (s *crSink) WithName(name string) logr.LogSink {
	next := *s
	if s.name == "" {
		next.name = name
	} else {
		next.name = s.name + "." + name
	}
	return &next
}

// funcName labels unnamed root output so its origin stays identifiable.
func (s *crSink) funcName() string {
	return cmp.Or(s.name, "controller-runtime")
}

func (s *crSink) line(msg string, kvs []any) string {
	pairs := slices.Concat(s.kv, kvs)
	if len(pairs) == 0 {
		return msg
	}
	var b strings.Builder
	b.WriteString(msg)
	for i := 0; i+1 < len(pairs); i += 2 {
		fmt.Fprintf(&b, " %v=%v", pairs[i], pairs[i+1])
	}
	return b.String()
}
