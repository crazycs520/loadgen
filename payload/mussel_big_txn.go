package payload

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/util"
	"github.com/spf13/cobra"
)

type MusselBigTxnSuite struct {
	cfg *config.Config

	loop     bool
	interval *time.Duration
	txnSize  int
}

func NewMusselBigTxnSuite(cfg *config.Config) cmd.CMDGenerater {
	return &MusselBigTxnSuite{
		cfg: cfg,
	}
}

func (c *MusselBigTxnSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "mussel-big-txn",
		Short:        "mussel big txn load",
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	c.interval = cmd.Flags().DurationP(flagInterval, "", time.Minute*10, "interval duration")
	cmd.Flags().IntVarP(&c.txnSize, flagTxnSize, "", 200000, "Txn row size")
	cmd.Flags().BoolVarP(&c.loop, flagLoop, "", true, "do in loop?")
	return cmd
}

func (c *MusselBigTxnSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *MusselBigTxnSuite) Run() error {
	db := util.GetSQLCli(c.cfg)
	db2 := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
		db2.Close()
	}()
	fmt.Printf("[%v] starting mussel big txn, interval: %v, txn-size: %v, loop: %v\n", time.Now().Format(time.RFC3339), c.interval.String(), c.txnSize, c.loop)

	startTime := time.Now()
	lastLogTime := time.Now()
	loop := 0
	for c.loop || loop == 0 {
		loop++
		step := 0
		start := MusselRecord{
			pk: "",
			sk: "",
			ts: 0,
		}

		totalRows, err := c.getTotalRows(db)
		if err != nil {
			return err
		}
		regionCount := 1000
		updateBatch := c.txnSize / regionCount
		scanBatch := totalRows / regionCount
		db2.Exec("set tidb_txn_mode = 'pessimistic'")
		txn, err := db2.Begin()
		if err != nil {
			return err
		}
		totalUpdate := int64(0)
		totalScan := 0
		for {
			step++
			next, err := c.scan(db, start, scanBatch)
			if err != nil {
				return err
			}
			if start.pk > next.pk || (start.pk >= next.pk && start.sk > next.sk) || (start.pk >= next.pk && start.sk >= next.sk && start.ts > next.ts) || (next.pk == "" && next.sk == "" && next.ts == 0) {
				fmt.Printf("[%vs] loop %v finished, total updated: %v, scan: %v\n", int(time.Since(startTime).Seconds()), loop, totalUpdate, totalScan)
				break
			}

			updated, err := c.update(txn, start, next, updateBatch)
			if err != nil {
				return err
			}
			totalUpdate += updated
			totalScan += scanBatch
			start = next
			if time.Since(lastLogTime) > time.Second*10 {
				lastLogTime = time.Now()
				fmt.Printf("[%vs] total updated: %v, scan: %v, loop: %v\n", int(time.Since(startTime).Seconds()), totalUpdate, totalScan, loop)
			}
		}
		commitStart := time.Now()
		err = txn.Commit()
		if err != nil {
			return err
		}
		fmt.Printf("[%vs] commit txn, cost: %v, total update: %v, scan: %v, loop: %v\n", int(time.Since(startTime).Seconds()), time.Since(commitStart).String(), totalUpdate, totalScan, loop)
		time.Sleep(*c.interval)
	}
	return nil
}

func (c *MusselBigTxnSuite) getTotalRows(db *sql.DB) (int, error) {
	sql := fmt.Sprintf(`select count(*) from t1`)
	var err error
	var cnt int64
	err = util.QueryRows(db, sql, func(row, cols []string) error {
		cnt, err = strconv.ParseInt(row[0], 10, 64)
		return err
	})
	return int(cnt), err
}

func (c *MusselBigTxnSuite) scan(db *sql.DB, start MusselRecord, batch int) (MusselRecord, error) {
	sql := fmt.Sprintf(`select pk, sk, ts 
from  
  (select pk, sk, ts 
  from 
    t1
  where pk > '%[1]s' or (pk = '%[1]s' and sk > '%[2]s') or  (pk = '%[1]s' and sk = '%[2]s' and ts > %[3]d) order by pk asc, sk asc, ts asc Limit %[4]d) t order by t.pk desc, t.sk desc, t.ts desc limit 1;`,
		start.pk, start.sk, start.ts, batch)

	var record MusselRecord
	err := util.QueryRows(db, sql, func(row, cols []string) error {
		ts, err := strconv.ParseInt(row[2], 10, 64)
		if err != nil {
			return err
		}
		record = MusselRecord{
			pk: row[0],
			sk: row[1],
			ts: ts,
		}
		return nil
	})
	return record, err
}

func (c *MusselBigTxnSuite) update(txn *sql.Tx, start, next MusselRecord, limit int) (int64, error) {
	sql := fmt.Sprintf(`update t1 set ts=ts+1 where  
  (pk > '%[1]s' or (pk = '%[1]s' and sk > '%[2]s') or (pk = '%[1]s' and sk = '%[2]s' and ts >= %[3]d)) 
and  
  (pk < '%[4]s' or (pk = '%[4]s' and sk < '%[5]s') or (pk = '%[4]s' and sk = '%[5]s' and ts <= %[6]d)) 
limit %[7]d; `,
		start.pk, start.sk, start.ts, next.pk, next.sk, next.ts, limit)

	result, err := txn.Exec(sql)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()

}
