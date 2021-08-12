package payload

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
)

type genStmtSuite struct {
	cfg     *config.Config
	tblInfo *data.TableInfo

	queryLen int
	stmtCnt  int
}

func NewGenStmtSuite(cfg *config.Config) cmd.CMDGenerater {
	return &genStmtSuite{
		cfg: cfg,
	}
}

func (c *genStmtSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "gen-stmt",
		Short:        "payload of generate many kind of statements",
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVarP(&c.stmtCnt, "stmt-cnt", "", 10, "the statement count")
	cmd.Flags().IntVarP(&c.queryLen, "query-len", "", 32, "the query SQL length")
	return cmd
}

func (c *genStmtSuite) ParseCmd(combinedCmd string) bool {
	return false
}

func (c *genStmtSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *genStmtSuite) prepare() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()

	for i := 0; i < c.stmtCnt; i++ {
		createSQL := fmt.Sprintf("create table if not exists t_gen_stmt_%v (a bigint, b bigint, c bigint)", i)
		_, err := db.Exec(createSQL)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *genStmtSuite) genQuery(idx int) string {
	buf := bytes.NewBuffer(make([]byte, 0, c.queryLen+64))
	idx = idx % c.stmtCnt
	for buf.Len() < c.queryLen {
		if buf.Len() > 0 {
			buf.WriteString(" union all ")
		}
		query := fmt.Sprintf("select * from t_gen_stmt_%[1]v where a = %[1]v or b = %[1]v or c = %[1]v", idx)
		buf.WriteString(query)
		idx = (idx + 1) % c.stmtCnt
	}
	return buf.String()
}

func (c *genStmtSuite) genQueryPrepareStmt(idx int) string {
	var builder strings.Builder
	for builder.Len() < c.queryLen {
		if builder.Len() > 0 {
			builder.WriteString(" union all ")
		}
		query := "select * from t_gen_stmt_" + strconv.Itoa(idx) + " where a = ? or b = ? or c = ?;"
		builder.WriteString(query)
	}
	return builder.String()
}

func (c *genStmtSuite) genQueryArgs(idx int) []interface{} {
	return []interface{}{idx, idx, idx}
}

func (c *genStmtSuite) Run() error {
	ctx := context.Background()
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		return err
	}
	fmt.Printf("start to query: %v\n", c.genQuery(0))
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := rand.Intn(c.stmtCnt)
			// TODO: change to prepare statement
			err := execSQLLoop(ctx, c.cfg, func() string {
				idx++
				return c.genQuery(idx)
			})

			if err != nil {
				fmt.Println(err.Error())
			}
		}()
	}
	wg.Wait()
	return nil
}

func (c *genStmtSuite) exec(genSQL func() string) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		sql := genSQL()
		_, err := db.Exec(sql)
		if err != nil {
			return err
		}
	}
}
