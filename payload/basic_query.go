package payload

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
)

const (
	defLogTime    = time.Duration(10 * time.Second) // default duration of printing QPS
	defTimeLayout = "2006-01-02T15:04:05.00+0800"

	defBasicQueryRows   = 100000
	defBasicQueryTime   = 600
	defBasicQueryIsAgg  = true
	defBasicQueryIsBack = false
)

type QuerySuite interface {
	Name() string
	GenQueryPrepareStmt() string // prepare statements for query sql
	GenQueryArgs() []interface{} // arguments for prepare stmt
	CurrentQPS() float64         // QPS in the past 1 second
	AverageQPS() float64         // Average QPS in test
}

type basicQuerySuite struct {
	cfg        *config.Config
	tblInfo    *data.TableInfo
	querySuite QuerySuite

	start time.Time // time start query suite

	currentCount int64 // queries performed in current second
	totalCount   int64 // total queries performed

	rows   int  // rows of test data
	time   int  // seconds to run query
	isAgg  bool // is aggregation query
	isBack bool // is back table query
}

func NewBasicQuerySuite(cfg *config.Config, querySuite QuerySuite) *basicQuerySuite {
	return &basicQuerySuite{
		cfg:        cfg,
		querySuite: querySuite,

		start: time.Now(),

		rows:   defBasicQueryRows,
		time:   defBasicQueryTime,
		isAgg:  defBasicQueryIsAgg,
		isBack: defBasicQueryIsBack,
	}
}

func (c *basicQuerySuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          c.querySuite.Name(),
		Short:        "payload of " + c.querySuite.Name(),
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	cmd.Flags().IntVarP(&c.rows, flagRows, "", defBasicQueryRows, "the table's total rows")
	cmd.Flags().IntVarP(&c.time, flagTime, "", defBasicQueryTime, "total time running test (seconds)")
	cmd.Flags().BoolVarP(&c.isAgg, flagIsAgg, "", defBasicQueryIsAgg, "full scan with TiKV return all rows if false, or do some aggregation if true")
	cmd.Flags().BoolVarP(&c.isBack, flagIsBack, "", defBasicQueryIsBack, "whether or not is back table query")
	return cmd
}

func (c *basicQuerySuite) CurrentQPS() float64 {
	duration := defLogTime.Seconds()
	count := float64(atomic.LoadInt64(&c.currentCount))
	return count / duration
}

func (c *basicQuerySuite) AverageQPS() float64 {
	dur := time.Since(c.start)
	if dur <= time.Duration(0) {
		return 0.0 // no query performed
	}

	duration := float64(dur/time.Millisecond) / 1000.0 // preserve millisecond parts
	total := atomic.LoadInt64(&c.totalCount)
	aveQPS := float64(total) / float64(duration)
	return aveQPS
}

func (c *basicQuerySuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.querySuite.Name(), func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		case flagIsAgg:
			v, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			c.isAgg = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}

func (c *basicQuerySuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *basicQuerySuite) prepare() error {
	tableName := "t_" + strings.ReplaceAll(c.querySuite.Name(), "-", "_")
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, tableName, []data.ColumnDef{
		{
			Name: "a",
			Tp:   "bigint",
		},
		{
			Name: "b",
			Tp:   "bigint",
		},
		{
			Name: "c",
			Tp:   "timestamp(6)",
		},
		{
			Name: "d",
			Tp:   "varchar(100)",
		},
		{
			Name: "e",
			Tp:   "decimal(48,10)",
		},
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.NormalIndex,
			Columns: []string{"b"},
		},
		{
			Tp:      data.PrimaryKey,
			Columns: []string{"a"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.Prepare(tblInfo, c.rows, c.rows/100000)
}

func (c *basicQuerySuite) Run() error {
	fmt.Printf("%v config: %v: %v, %v: %vs, %v: %v, %v: %v\n",
		c.querySuite.Name(), flagRows, c.rows, flagTime, c.time, flagIsAgg, c.isAgg, flagIsBack, c.isBack)

	timeout := time.Duration(c.time) * time.Second
	timer := time.NewTicker(defLogTime)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	err := c.prepare()
	if err != nil {
		fmt.Println("prepare data meet error: ", err)
		cancel()
		return err
	}
	fmt.Printf("start to query: %v\n", c.querySuite.GenQueryPrepareStmt())
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Thread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := execPrepareStmtLoop(ctx, c.cfg, func() string {
				return c.querySuite.GenQueryPrepareStmt()
			}, func() []interface{} {
				defer atomic.AddInt64(&c.currentCount, 1)
				defer atomic.AddInt64(&c.totalCount, 1)

				return c.querySuite.GenQueryArgs()
			})
			if err != nil {
				fmt.Println(err.Error())
				cancel()
			}
		}()
	}

	// logging qps messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				fmt.Printf("%s\tQPS: %v.f\tAverage QPS: %v.f\n", time.Now().Format(defTimeLayout), c.CurrentQPS(), c.AverageQPS())
				// clear counter of the past second
				atomic.StoreInt64(&c.currentCount, 0)
			}
		}
	}()
	wg.Wait()
	return nil
}

func (c *basicQuerySuite) exec(genSQL func() string) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	for {
		var rows *sql.Rows
		var err error
		sqlStr := genSQL()
		if strings.HasPrefix(strings.ToLower(sqlStr), "select") {
			rows, err = db.Query(sqlStr)
		} else {
			_, err = db.Exec(sqlStr)
		}
		if err != nil {
			return err
		}
		if rows != nil {
			for rows.Next() {
			}
			rows.Close()
		}
	}
}
