package cmd

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
)

type BenchSQL struct {
	*App
	query  string
	ignore bool

	valMin     int64
	valMax     int64
	currentVal int64

	totalQPS int64
}

const randValueStr = "#rand-val"
const seqValueStr = "#seq-val"

func (b *BenchSQL) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "bench",
		Short:        "bench the sql",
		Long:         `benchmark the sql statement`,
		RunE:         b.RunE,
		SilenceUsage: true,
	}

	//cmd.Flags().IntVar(&app.EstimateTableRows, "new-table-row", 0, "estimate need be split table rows")
	cmd.Flags().StringVarP(&b.query, "sql", "", "", "bench sql statement")
	cmd.Flags().BoolVarP(&b.ignore, "ignore", "", false, "should ignore error?")
	cmd.Flags().Int64VarP(&b.valMin, "valmin", "", 0, randValueStr+"/"+seqValueStr+" min val")
	cmd.Flags().Int64VarP(&b.valMax, "valmax", "", math.MaxInt64-1, randValueStr+"/"+seqValueStr+" max val")

	return cmd
}

func (b *BenchSQL) validateParas(cmd *cobra.Command) error {
	msg := "need specify `%s` parameter"
	var err error
	if b.query == "" {
		err = fmt.Errorf(msg, "sql")
	}
	return err
}

func (b *BenchSQL) RunE(cmd *cobra.Command, args []string) error {
	if err := b.validateParas(cmd); err != nil {
		fmt.Println(err.Error())
		fmt.Printf("-----------[ help ]-----------\n")
		return cmd.Help()
	}
	fmt.Printf("global config:\n%v\n", b.cfg.String())
	fmt.Printf("sql: %v\n", b.replaceSQL(b.query))
	for i := 0; i < b.cfg.Thread; i++ {
		go b.benchSql()
	}
	lastQPS := int64(0)
	lastTime := time.Now()
	for {
		time.Sleep(2 * time.Second)
		now := time.Now()
		totalQPS := atomic.LoadInt64(&b.totalQPS)
		qps := float64((totalQPS - lastQPS)) / now.Sub(lastTime).Seconds()
		lastQPS = totalQPS
		lastTime = now
		fmt.Printf("qps: %v\n", int64(qps))
	}
}

func (b *BenchSQL) benchSql() {
	db := b.GetSQLCli()
	sqlStr := b.query
	isQuery := strings.HasPrefix(strings.ToLower(sqlStr), "select") || strings.HasPrefix(strings.ToLower(sqlStr), "show")
	for {
		batch := 20
		var err error
		var rows *sql.Rows
		for i := 0; i < batch; i++ {
			sqlStr = b.replaceSQL(b.query)
			if isQuery {
				rows, err = db.Query(sqlStr)
			} else {
				_, err = db.Exec(sqlStr)
			}
			if err != nil && !b.ignore {
				fmt.Printf("exec: %v, err: %v\n", sqlStr, err)
				os.Exit(-1)
			}
			if rows != nil {
				for rows.Next() {
				}
				rows.Close()
			}
		}
		atomic.AddInt64(&b.totalQPS, int64(batch))
	}
}

func (b *BenchSQL) replaceSQL(sql string) string {
	if b.valMin == b.valMax {
		return sql
	}
	if strings.Contains(sql, randValueStr) {
		rand.Seed(time.Now().UnixNano())
		v := rand.Intn(int(b.valMax-b.valMin+1)) + int(b.valMin)
		return strings.Replace(sql, randValueStr, strconv.Itoa(v), -1)
	}
	if strings.Contains(sql, seqValueStr) {
		v := atomic.AddInt64(&b.currentVal, 1)
		if v > b.valMax {
			v = b.valMin
			atomic.StoreInt64(&b.currentVal, b.valMin)
		}
		return strings.Replace(sql, seqValueStr, strconv.Itoa(int(v)), -1)
	}

	return sql
}
