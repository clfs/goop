package main

import (
	"log/slog"
	"os"

	"github.com/deletescape/goop/cmd"
)

func main() {
	opts := slog.HandlerOptions{
		AddSource: true,
		Level:     slog.LevelInfo,
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &opts)))

	cmd.Execute()
}
