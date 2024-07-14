package payload

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
	"github.com/crazycs520/loadgen/util"
)

type BankDealSuite struct {
	cfg     *config.Config
	tblInfo *data.TableInfo

	//probability int
	conflictErr   int64
	rows          int
	rowsPerRegion int
	dealCnt       int64
}

func NewBankDealSuite(cfg *config.Config) cmd.CMDGenerater {
	return &BankDealSuite{
		cfg: cfg,
	}
}

func (c *BankDealSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "bank-deal",
		Short:        `payload of brank deal`,
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", defBasicQueryRows, "total table rows")
	cmd.Flags().IntVarP(&c.rowsPerRegion, flagRowsPerRegion, "", 10000, "rows per table")
	return cmd
}

func (c *BankDealSuite) RunE(cmd *cobra.Command, args []string) error {
	fmt.Printf("thread: %v\n", c.cfg.Thread)
	return c.Run()
}

func (c *BankDealSuite) prepare() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	_, err := db.Exec("drop table if exists deal_history, bank_deal")
	if err != nil {
		return err
	}
	_, err = db.Exec("create table deal_history (mer_id varchar(20), order_id varchar(64), deal_state varchar(4))")
	if err != nil {
		return err
	}

	tableName := "bank_deal"
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, tableName, []data.ColumnDef{
		{
			Name: "id",
			Tp:   "bigint",
		},
		{
			Name: "trans_no",
			Tp:   "varchar(64)",
		},
		{
			Name: "mer_id",
			Tp:   "varchar(20)",
		},
		{
			Name: "order_id",
			Tp:   "varchar(64)",
		},
		{
			Name: "deal_state",
			Tp:   "varchar(4)",
		},
		{
			Name: "settle_amt",
			Tp:   "bigint",
		},
		{
			Name: "update_time",
			Tp:   "date",
		},
	}, []data.IndexInfo{
		{
			Tp:      data.PrimaryKey,
			Columns: []string{"id"},
		},
		{
			Name:    "idx1",
			Tp:      data.UniqueIndex,
			Columns: []string{"order_id", "mer_id"},
		},
		{
			Name:    "idx2",
			Tp:      data.UniqueIndex,
			Columns: []string{"trans_no"},
		},
	})
	tblInfo.GenRowArgs = func(num int) []interface{} {
		id := num
		transNo := strconv.Itoa(num)
		merID := "H00" + strconv.Itoa(num)
		orderID := "BWXH100000000000" + strconv.Itoa(num)
		dealState := "00"
		settleAmt := rand.Intn(1000)
		update_time := time.Now().Format(data.TimeFormatForDATE)
		return []interface{}{id, transNo, merID, orderID, dealState, settleAmt, update_time}
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.Prepare(c.tblInfo, c.rows, c.rowsPerRegion)
}

func (c *BankDealSuite) Run() error {
	err := c.prepare()
	if err != nil {
		return err
	}
	dbs := make([]*sql.DB, 0, c.cfg.Thread)
	for i := 0; i < c.cfg.Thread; i++ {
		db := util.GetSQLCli(c.cfg)
		setVarSQLs := []string{
			"set @@tidb_general_log=1",
			"set @@tidb_txn_mode = 'pessimistic'",
			"set @@tx_isolation = 'READ-COMMITTED'",
			"set @@transaction_isolation = 'READ-COMMITTED'",
			"set @@tidb_enable_async_commit=1",
			"set @@tidb_enable_1pc=1",
		}
		for _, sql := range setVarSQLs {
			_, err := db.Exec(sql)
			if err != nil {
				return err
			}
		}
		dbs = append(dbs, db)
	}
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()

	for num := 0; num < c.rows; num++ {
		var wg sync.WaitGroup
		for i := 0; i < c.cfg.Thread; i++ {
			wg.Add(1)
			db := dbs[i]
			go func(db *sql.DB, num int) {
				defer wg.Done()
				err := c.deal(db, num)
				if err != nil {
					fmt.Println(err.Error())
					os.Exit(1)
				}
			}(db, num)
		}
		wg.Wait()
	}
	db := dbs[0]
	_, rows, err := util.QueryAllRows(db, "select count(*), mer_id, order_id as cnt from deal_history group by mer_id, order_id having cnt > 1")
	if err != nil {
		return err
	}
	fmt.Printf("deal count: %v, repeat rows: %v\n", c.dealCnt, rows)
	if len(rows) > 0 {
		return errors.New(fmt.Sprintf("have repeat deal?: %v", rows))
	}
	return nil
}

func (c *BankDealSuite) deal(db *sql.DB, num int) (err error) {
	defer func() {
		_, err1 := db.Exec("commit")
		if err == nil {
			err = err1
		}
	}()
	_, err = db.Exec("set autocommit=0")
	if err != nil {
		return err
	}
	transNo := strconv.Itoa(num)

	_, rows, err := util.QueryAllRows(db, fmt.Sprintf("select mer_id, order_id, deal_state from %v where trans_no = '%v'", c.tblInfo.DBTableName(), transNo))
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 3 {
		//fmt.Printf("query result unexpected 0: %v, transNo: %v -----\n\n", rows, transNo)
		//return errors.New(fmt.Sprintf("query result unexpected, %v", rows))
		return nil
	}
	merID := rows[0][0]
	orderID := rows[0][1]
	dealState := rows[0][2]
	if dealState != "'00'" {
		return
	}
	_, rows, err = util.QueryAllRows(db, fmt.Sprintf("select mer_id, order_id, deal_state from %v where mer_id = %v and order_id = %v for update", c.tblInfo.DBTableName(), merID, orderID))
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 3 {
		//fmt.Printf("query result unexpected 1: %v , merID: %v, orderID: %v -----\n\n", rows, merID, orderID)
		//return errors.New(fmt.Sprintf("query result unexpected, %v", rows))
		return nil
	}
	dealState = rows[0][2]
	if dealState != "'00'" {
		return
	}
	_, err = db.Exec(fmt.Sprintf("update %v set deal_state = '01' where mer_id = %v and order_id = %v", c.tblInfo.DBTableName(), merID, orderID))
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("insert into deal_history (mer_id, order_id, deal_state) values (%v, %v, %v)", merID, orderID, dealState))
	atomic.AddInt64(&c.dealCnt, 1)
	return err
}
