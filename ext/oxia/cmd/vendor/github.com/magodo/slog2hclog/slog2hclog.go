package slog2hclog

import (
	"context"
	"io"
	"log"
	"log/slog"
	"sort"
	"strings"

	"github.com/hashicorp/go-hclog"
)

type stdslogWrapper struct {
	slog    *slog.Logger
	oriSlog *slog.Logger
	lvar    *slog.LevelVar
	names   []string
	args    []interface{}
}

func (s *stdslogWrapper) clone() *stdslogWrapper {
	newSlog := *s.slog
	var oriSlog *slog.Logger = nil
	if s.oriSlog != nil {
		newOriSlog := *s.oriSlog
		oriSlog = &newOriSlog
	}
	return &stdslogWrapper{
		slog:    &newSlog,
		oriSlog: oriSlog,
		names:   append([]string{}, s.names...),
		args:    append([]interface{}{}, s.args...),
	}
}

var _ hclog.Logger = &stdslogWrapper{}

const (
	SlogLevelTrace = slog.LevelDebug - 4
	SlogLevelOff   = slog.LevelError + 4
)

var levelMapToSlog = map[hclog.Level]slog.Level{
	hclog.Off:   SlogLevelOff,
	hclog.Error: slog.LevelError,
	hclog.Warn:  slog.LevelWarn,
	hclog.Info:  slog.LevelInfo,
	hclog.Debug: slog.LevelDebug,
	hclog.Trace: SlogLevelTrace,
}

var levelMapFromSlog = map[slog.Level]hclog.Level{
	SlogLevelOff:    hclog.Off,
	slog.LevelError: hclog.Error,
	slog.LevelWarn:  hclog.Warn,
	slog.LevelInfo:  hclog.Info,
	slog.LevelDebug: hclog.Debug,
	SlogLevelTrace:  hclog.Trace,
}

// New wraps a slog.Logger to a hclog.Logger.
// The `SetLevel` method only works when the `lvar` is specified and set to the slog.Logger.
func New(l *slog.Logger, lvar *slog.LevelVar) hclog.Logger {
	return &stdslogWrapper{
		slog:    l,
		oriSlog: nil,
		lvar:    lvar,
		names:   []string{},
		args:    []interface{}{},
	}
}

func (s *stdslogWrapper) Trace(msg string, args ...interface{}) {
	s.slog.Log(context.Background(), SlogLevelTrace, msg, args...)
}

func (s *stdslogWrapper) Debug(msg string, args ...interface{}) {
	s.slog.Debug(msg, args...)
}

func (s *stdslogWrapper) Info(msg string, args ...interface{}) {
	s.slog.Info(msg, args...)
}

func (s *stdslogWrapper) Warn(msg string, args ...interface{}) {
	s.slog.Warn(msg, args...)
}

func (s *stdslogWrapper) Error(msg string, args ...interface{}) {
	s.slog.Error(msg, args...)
}

func (s *stdslogWrapper) GetLevel() hclog.Level {
	if s.lvar == nil {
		// lvar not set indicates the source slog.Logger has a fixed log level (or a default level, which equals to Info).
		// In this case, we enumerate the log levels from lowest (Trace) to get the effective log level.
		return s.getLowestLevel()
	}
	return levelMapFromSlog[s.lvar.Level()]
}

// SetLevel only applies when the source slog.Logger has a slog.LevelVar level.
func (s *stdslogWrapper) SetLevel(level hclog.Level) {
	if s.lvar != nil {
		s.lvar.Set(levelMapToSlog[level])
	}
}

func (s *stdslogWrapper) IsTrace() bool {
	return s.slog.Enabled(context.Background(), SlogLevelTrace)
}

func (s *stdslogWrapper) IsDebug() bool {
	return s.slog.Enabled(context.Background(), slog.LevelDebug)
}

func (s *stdslogWrapper) IsInfo() bool {
	return s.slog.Enabled(context.Background(), slog.LevelInfo)
}

func (s *stdslogWrapper) IsWarn() bool {
	return s.slog.Enabled(context.Background(), slog.LevelWarn)
}

func (s *stdslogWrapper) IsError() bool {
	return s.slog.Enabled(context.Background(), slog.LevelError)
}

func (s *stdslogWrapper) Log(level hclog.Level, msg string, args ...interface{}) {
	s.slog.Log(context.Background(), levelMapToSlog[level], msg, args...)
}

func (s *stdslogWrapper) Name() string {
	return strings.Join(s.names, ".")
}

func (s *stdslogWrapper) Named(name string) hclog.Logger {
	sl := s.clone()
	if len(s.names) == 0 {
		newSlog := *sl.slog
		sl.oriSlog = &newSlog
	}
	sl.names = append(sl.names, name)
	sl.slog = s.slog.WithGroup(name)
	return sl
}

func (s *stdslogWrapper) ResetNamed(name string) hclog.Logger {
	sl := s.clone()

	// Empty name indicates to clear the name
	if name == "" {
		if len(sl.names) == 0 {
			return sl
		}
		sl.names = []string{}
		sl.slog = sl.oriSlog
		sl.oriSlog = nil
		return sl
	}

	// Non-empty name indicates to set the name
	if len(sl.names) == 0 {
		return sl.Named(name)
	}
	sl.names = []string{}
	sl.slog = sl.oriSlog
	sl.oriSlog = nil
	return sl.Named(name)
}

func (s *stdslogWrapper) With(args ...interface{}) hclog.Logger {
	sl := s.clone()
	sl.slog = s.slog.With(args...)
	sl.args = append(sl.args, args...)
	return sl
}

func (s *stdslogWrapper) ImpliedArgs() []interface{} {
	return s.args
}

func (s *stdslogWrapper) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	if opts == nil {
		opts = &hclog.StandardLoggerOptions{}
	}

	return log.New(s.StandardWriter(opts), "", 0)
}

func (s *stdslogWrapper) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	newLog := s.clone()
	return &stdlogAdapter{
		log:                      newLog,
		inferLevels:              opts.InferLevels,
		inferLevelsWithTimestamp: opts.InferLevelsWithTimestamp,
		forceLevel:               opts.ForceLevel,
	}
}

func (s *stdslogWrapper) getLowestLevel() hclog.Level {
	ctx := context.Background()

	var slogLvls []slog.Level
	for lvlSlog := range levelMapFromSlog {
		slogLvls = append(slogLvls, lvlSlog)
	}
	// Sort the slog levels from Trace up to Error
	sort.Slice(slogLvls, func(i, j int) bool {
		return int(slogLvls[i]) < int(slogLvls[j])
	})

	for _, lvlSlog := range slogLvls {
		lvl := levelMapFromSlog[lvlSlog]
		if s.slog.Enabled(ctx, lvlSlog) {
			return lvl
		}
	}
	return hclog.Off
}
