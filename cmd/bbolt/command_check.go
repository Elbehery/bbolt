package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	bolt "go.etcd.io/bbolt"
	"go.etcd.io/bbolt/internal/guts_cli"
)

func newCheckCommand() *cobra.Command {
	checkCmd := &cobra.Command{
		Use:   "check <subcommand>",
		Short: "check related commands",
	}

	checkCmd.AddCommand(newCheckDBCommand())
	checkCmd.AddCommand(newCheckPageCommand())

	return checkCmd
}

func newCheckDBCommand() *cobra.Command {
	checkDBCmd := &cobra.Command{
		Use:   "db <bbolt-file>",
		Short: "Verifies integrity of whole bbolt database data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return checkFunc(cmd, args[0], nil)
		},
	}

	return checkDBCmd
}

type checkPageOption struct {
	pageId uint64
}

func (o *checkPageOption) AddFlags(fs *pflag.FlagSet) {
	fs.Uint64VarP(&o.pageId, "pageId", "", o.pageId, "page Id")
	_ = cobra.MarkFlagRequired(fs, "pageId")
}

func (o *checkPageOption) Validate() error {
	if o.pageId < 2 {
		return fmt.Errorf("the pageId must be at least 2, but got %d", o.pageId)
	}
	return nil
}

func newCheckPageCommand() *cobra.Command {
	var o checkPageOption
	checkPageCmd := &cobra.Command{
		Use:   "page <bbolt-file>",
		Short: "Verifies integrity of bbolt database data starting from give page-id",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Validate(); err != nil {
				return err
			}
			return checkFunc(cmd, args[0], &o)
		},
	}

	o.AddFlags(checkPageCmd.Flags())
	return checkPageCmd
}

func checkFunc(cmd *cobra.Command, dbPath string, cfg *checkPageOption) error {
	if _, err := checkSourceDBPath(dbPath); err != nil {
		return err
	}

	// Open database.
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	opts := []bolt.CheckOption{bolt.WithKVStringer(CmdKvStringer())}
	if cfg != nil {
		opts = append(opts, bolt.WithPageId(cfg.pageId))
	}

	// Perform consistency check.
	return db.View(func(tx *bolt.Tx) error {
		var count int
		for err := range tx.Check(opts...) {
			fmt.Fprintln(cmd.OutOrStdout(), err)
			count++
		}

		// Print summary of errors.
		if count > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), "%d errors found\n", count)
			return guts_cli.ErrCorrupt
		}

		// Notify user that database is valid.
		fmt.Fprintln(cmd.OutOrStdout(), "OK")
		return nil
	})
}
