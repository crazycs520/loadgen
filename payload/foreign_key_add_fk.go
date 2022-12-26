package payload

import (
	"fmt"
	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
	"time"
)

type FKAddFKSuite struct {
	cfg *config.Config
}

func NewFKAddFKSuite(cfg *config.Config) cmd.CMDGenerater {
	return &FKAddFKSuite{
		cfg: cfg,
	}
}

func (c *FKAddFKSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "fk-add-fk",
		Short:        "add foreign key constraint in parent/child table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	return cmd
}

func (c *FKAddFKSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *FKAddFKSuite) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	start := time.Now()
	_, err := db.Exec("alter table fk_child add foreign key (pid) references fk_parent(id)")
	if err != nil {
		fmt.Printf("insert data error: %v\n", err)
	}
	fmt.Printf("add foreign key cost %v\n", time.Since(start).String())
	return nil
}
