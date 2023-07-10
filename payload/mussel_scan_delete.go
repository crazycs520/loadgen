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

type MusselScanDeleteSuite struct {
	cfg *config.Config

	batch    int
	loop     bool
	ttl      *time.Duration
	interval *time.Duration
}

func NewMusselScanDeleteSuite(cfg *config.Config) cmd.CMDGenerater {
	return &MusselScanDeleteSuite{
		cfg: cfg,
	}
}

func (c *MusselScanDeleteSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "mussel-scan-delete",
		Short:        "mussel scan delete load",
		RunE:         c.RunE,
		SilenceUsage: true,
	}

	defTTL := time.Minute * 10
	c.ttl = cmd.Flags().DurationP(flagTTL, "", defTTL, "ttl duration")
	c.interval = cmd.Flags().DurationP(flagInterval, "", defTTL, "interval duration")
	cmd.Flags().IntVarP(&c.batch, flagBatch, "", 10000, "batch size")
	cmd.Flags().BoolVarP(&c.loop, flagLoop, "", true, "batch size")
	return cmd
}

func (c *MusselScanDeleteSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *MusselScanDeleteSuite) Run() error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()
	fmt.Printf("[%v] starting mussel scan delete, ttl: %s, interval: %v, batch: %v, loop: %v\n", time.Now().Format(time.RFC3339), c.ttl.String(), c.interval.String(), c.batch, c.loop)

	startTime := time.Now()
	start := MusselRecord{
		pk: "",
		sk: "",
		ts: 0,
	}

	totalDeleted := int64(0)
	totalScan := 0
	lastLogTime := time.Now()
	loop := 0
	for c.loop || loop == 0 {
		loop++
		step := 0
		for {
			step++
			next, err := c.scan(db, start, c.batch)
			if err != nil {
				return err
			}
			if start.pk > next.pk || (start.pk >= next.pk && start.sk > next.sk) || (start.pk >= next.pk && start.sk >= next.sk && start.ts > next.ts) || (next.pk == "" && next.sk == "" && next.ts == 0) {
				fmt.Printf("[%vs] loop %v finished, total deleted: %v, scan: %v\n", int(time.Since(startTime).Seconds()), loop, totalDeleted, totalScan)
				break
			}

			deleted, err := c.delete(db, start, next, *c.ttl)
			if err != nil {
				return err
			}
			totalDeleted += deleted
			totalScan += c.batch
			start = next
			if time.Since(lastLogTime) > time.Second*10 {
				lastLogTime = time.Now()
				fmt.Printf("[%vs] total deleted: %v, scan: %v, loop: %v\n", int(time.Since(startTime).Seconds()), totalDeleted, totalScan, loop)
			}
		}
		time.Sleep(*c.interval)
	}
	fmt.Printf("[%v] finish mussel scan delete, total-delete: %v, total-scan: %v  cost: %v\n",
		time.Now().Format(time.RFC3339), totalDeleted, totalScan, time.Since(startTime).String())
	return nil
}

type MusselRecord struct {
	pk string
	sk string
	ts int64
}

func (c *MusselScanDeleteSuite) scan(db *sql.DB, start MusselRecord, batch int) (MusselRecord, error) {
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

func (c *MusselScanDeleteSuite) delete(db *sql.DB, start, next MusselRecord, ttl time.Duration) (int64, error) {
	sql := fmt.Sprintf(`delete from t1 where  
  (pk > '%[1]s' or (pk = '%[1]s' and sk > '%[2]s') or (pk = '%[1]s' and sk = '%[2]s' and ts >= %[3]d)) 
and  
  (pk < '%[4]s' or (pk = '%[4]s' and sk < '%[5]s') or (pk = '%[4]s' and sk = '%[5]s' and ts <= %[6]d)) 
and  
  ts <=  %[7]d; `,
		start.pk, start.sk, start.ts, next.pk, next.sk, next.ts, time.Now().Add(-ttl).UnixNano())

	result, err := db.Exec(sql)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()

}

/*
func (c *MusselScanDeleteSuite) deleteFromParentTable(start, end int) error {
	db := util.GetSQLCli(c.cfg)
	defer func() {
		db.Close()
	}()

	for i := start; i < end; i += 1 {
		if c.manualCascade {
			txn, err := db.Begin()
			if err != nil {
				return err
			}
			sql := fmt.Sprintf("delete from fk_parent where id=%v;", i)
			_, err = txn.Exec(sql)
			if err != nil {
				return err
			}
			sql = fmt.Sprintf("delete from fk_child where pid=%v;", i)
			_, err = txn.Exec(sql)
			if err != nil {
				return err
			}
			err = txn.Commit()
			if err != nil {
				return err
			}
		} else {
			sql := fmt.Sprintf("delete from fk_parent where id=%v;", i)
			_, err := db.Exec(sql)
			if err != nil {
				return err
			}
		}
		atomic.AddInt64(&c.deletedRows, 1)
	}
	return nil
}
*/
