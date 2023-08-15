package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type ExecSQL struct {
	*App
	query string
}

func (b *ExecSQL) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "exec",
		Short:        "exec the sql",
		Long:         `execute sql statement`,
		RunE:         b.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().StringVarP(&b.query, "sql", "", "", "bench sql statement")
	return cmd
}

func (b *ExecSQL) validateParas(cmd *cobra.Command) error {
	msg := "need specify `%s` parameter"
	var err error
	if b.query == "" {
		err = fmt.Errorf(msg, "sql")
	}
	return err
}

func (b *ExecSQL) RunE(cmd *cobra.Command, args []string) error {
	if err := b.validateParas(cmd); err != nil {
		fmt.Println(err.Error())
		fmt.Printf("-----------[ help ]-----------\n")
		return cmd.Help()
	}
	db := b.GetSQLCli()
	var err error
	start := time.Now()
	if strings.HasPrefix(strings.ToLower(b.query), "select") {
		cnt := 0
		err = util.QueryRows(db, b.query, func(row, cols []string) error {
			cnt++
			if cnt == 1 {
				println("----------- [columns] -----------")
				println(strings.Join(cols, "\t"))
				println("----------- [result] -----------")
			}
			println(strings.Join(row, "\t"))
			return nil
		})
		println("----------- end -----------")
	} else {
		_, err = db.Exec(b.query)
	}
	if err != nil {
		fmt.Printf("exec sql: %v, err: %v\n", b.query, err)
	} else {
		fmt.Printf("exec sql: %v successfully, cost: %v\n", b.query, time.Since(start))
	}
	return nil
}
