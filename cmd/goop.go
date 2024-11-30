// Package cmd defines a command type for the goop tool.
package cmd

import (
	"log/slog"
	"os"

	"github.com/deletescape/goop/pkg/goop"
	"github.com/spf13/cobra"
)

var force bool
var keep bool
var list bool
var rootCmd = &cobra.Command{
	Use:   "goop",
	Short: "goop is a very fast tool to grab sources from exposed .git folders",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var dir string
		if len(args) >= 2 {
			dir = args[1]
		}
		if list {
			if err := goop.CloneList(args[0], dir, force, keep); err != nil {
				slog.Error("exiting", "error", err)
				os.Exit(1)
			}
		} else {
			if err := goop.Clone(args[0], dir, force, keep); err != nil {
				slog.Error("exiting", "error", err)
				os.Exit(1)
			}
		}
	},
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&force, "force", "f", false, "overrides DIR if it already exists")
	rootCmd.PersistentFlags().BoolVarP(&keep, "keep", "k", false, "keeps already downloaded files in DIR, useful if you keep being ratelimited by server")
	rootCmd.PersistentFlags().BoolVarP(&list, "list", "l", false, "allows you to supply the name of a file containing a list of domain names instead of just one domain")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("exiting", "error", err)
		os.Exit(1)
	}
}
