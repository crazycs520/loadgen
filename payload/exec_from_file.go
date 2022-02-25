package payload

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type ExecFromFileSuite struct {
	cfg *config.Config

	file   string
	ignore bool
}

func NewExecFromFileSuite(cfg *config.Config) cmd.CMDGenerater {
	return &ExecFromFileSuite{
		cfg: cfg,
	}
}

func (c *ExecFromFileSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "exec-file",
		Short:        "payload of big-insert in 1 statements",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().StringVarP(&c.file, flagFile, "", "", "sql file path")
	cmd.Flags().BoolVarP(&c.ignore, flagIgnore, "", false, "ignore exec sql error if the value is true")
	return cmd
}

func (c *ExecFromFileSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *ExecFromFileSuite) Run() error {
	sqls, err := c.getSQLFromFile()
	if err != nil {
		return err
	}
	cnt := int64(0)
	for i := 0; i < c.cfg.Thread; i++ {
		go func() {
			db := util.GetSQLCli(c.cfg)
			defer func() {
				db.Close()
			}()
			for {
				for _, query := range sqls {
					var err error
					if strings.HasPrefix(strings.ToLower(query), "select") {
						var rows *sql.Rows
						rows, err = db.Query(query)
						if rows != nil {
							for rows.Next() {
							}
							err = rows.Close()
						}
					} else {
						_, err = db.Exec(query)
					}
					if err != nil && !c.ignore {
						fmt.Printf("exec %v failed, err: %v", query, err)
					}
				}
				atomic.AddInt64(&cnt, 1)
			}
		}()
	}
	fmt.Printf("start to run\ncfg: %v\n", c.cfg.String())
	lastCnt := int64(0)
	start := time.Now()
	for {
		secs := int64(10)
		time.Sleep(time.Second * time.Duration(secs))
		total := atomic.LoadInt64(&cnt)
		fmt.Printf("[ %vs ] exec_count: %v, tps: %v\n",
			int(time.Since(start).Seconds()), total, (total-lastCnt)/secs)
		lastCnt = total
	}
	return nil
}

func (c *ExecFromFileSuite) getSQLFromFile() ([]string, error) {
	if c.file == "" {
		return nil, fmt.Errorf("need specify the sql file path")
	}
	f, err := os.Open(c.file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	sqls := []string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		sqls = append(sqls, scanner.Text())
	}

	return sqls, scanner.Err()
}
