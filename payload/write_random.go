package payload

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type WriteRandomSuite struct {
	cfg       *config.Config
	tableName string
	tblInfo   *data.TableInfo

	batchSize int
	rows      int
}

func NewWriteRandomSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &WriteRandomSuite{cfg: cfg, rows: defRowsOfBasicWrite}
	return suite
}

func (c *WriteRandomSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "write-random",
		Short:        "payload of write random in batchs",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.batchSize, "batch", "", 5000, "write in batch size")
	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfBasicWrite, "rows of data to randomly write in")
	return cmd
}

func (c *WriteRandomSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *WriteRandomSuite) Run() error {
	err := c.prepare()
	if err != nil {
		return err
	}
	fmt.Println("start to do write random load:", c.genSQL())

	// get threads really needs
	threads := func() int {
		occupations := c.rows / c.cfg.Thread
		if c.rows%c.batchSize != 0 {
			occupations += 1
		}
		if occupations > c.cfg.Thread {
			return c.cfg.Thread
		}
		return occupations
	}()
	step := c.rows / threads
	errCh := make(chan error, threads)
	wg := &sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		begin, end := i*step, (i+1)*step
		if end > c.rows {
			end = c.rows
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.randomInsert(begin, end)
			if err != nil {
				errCh <- err
			}
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case err = <-errCh:
				fmt.Println(err)
				cancel()
			}
		}
	}()
	wg.Wait()
	if err == nil {
		cancel()
	}
	fmt.Println("test finished")
	return nil
}

func (c *WriteRandomSuite) genSQL() string {
	return fmt.Sprintf("insert into %v (a,b) values (1, 1);", c.tableName)
}

func (c *WriteRandomSuite) genPrepareSQL() string {
	return "insert into " + c.tblInfo.DBTableName() + " (a,b) values (?, ?);"
}

func (c *WriteRandomSuite) genPrepareArgs(a, b int) []interface{} {
	return []interface{}{a, b}
}

func (c *WriteRandomSuite) genPrepareSQLSequential(rows int) string {
	var builder strings.Builder
	builder.WriteString("insert into ")
	builder.WriteString(c.tblInfo.DBTableName())
	builder.WriteString(" (a, b) values ")
	for row := 0; row < rows; row++ {
		if row > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?, ?)")
	}
	return builder.String()
}

func (c *WriteRandomSuite) genPrepareArgsSequential(randomSequence *[]int) []interface{} {
	if randomSequence == nil {
		return nil
	}
	args := make([]interface{}, 0, len(*randomSequence))
	for index, val := range *randomSequence {
		args = append(args, val, index)
	}
	return args
}

func (c *WriteRandomSuite) prepare() error {
	c.tableName = "t_write_random"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name:     "a",
			Tp:       "bigint",
			Property: "Primary Key",
		},
		{
			Name: "b",
			Tp:   "bigint",
		},
		{
			Name:         "c",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
	}, []data.IndexInfo{})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.CreateTable(c.tblInfo, false)
}

func (c *WriteRandomSuite) randomInsert(begin, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	batchSize := c.batchSize
	stmt, err := db.Prepare(c.genPrepareSQLSequential(batchSize))
	if err != nil {
		return err
	}
	var i int
	for i = begin; i+batchSize <= end; i += batchSize {
		batchBegin, batchEnd := i, i+batchSize
		if batchEnd > end {
			batchEnd = end
		}
		sequence := genShuffledSlice(batchBegin, batchEnd)
		_, err := stmt.Exec(c.genPrepareArgsSequential(sequence)...)
		if err != nil {
			return err
		}
	}

	if i == end {
		return nil
	}

	sequence := genShuffledSlice(i, end)
	stmt, err = db.Prepare(c.genPrepareSQL())
	if err != nil {
		return err
	}
	for index, val := range *sequence {
		_, err = stmt.Exec(c.genPrepareArgs(val, index)...)
		if err != nil {
			return err
		}
	}

	return nil
}

func genShuffledSlice(begin, end int) *[]int {
	size := end - begin
	if size <= 0 {
		return nil
	}
	rand.Seed(time.Now().Unix())
	nums := make([]int, size)
	for index := range nums {
		nums[index] = begin + index
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	return &nums
}
