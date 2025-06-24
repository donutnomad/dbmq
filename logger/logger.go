package logger

import (
	"log/slog"
	"os"
)

var logger *slog.Logger

func GetLogger() *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level:     slog.LevelDebug,
			AddSource: false,
		}))
	}
	return logger
}

func SetLogger(l *slog.Logger) {
	logger = l
}
