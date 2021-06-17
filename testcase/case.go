package testcase

import (
	"context"
	"database/sql"
	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
	"github.com/crazycs520/load/util"
	"strings"
)

func init() {
	cmd.RegisterCaseCmd(NewIndexLookUpWrongPlan)
	cmd.RegisterCaseCmd(NewWriteHotSuite)
	cmd.RegisterCaseCmd(NewNormalOLTPSuite)
	cmd.RegisterCaseCmd(NewFullTableScanSuite)
	cmd.RegisterCaseCmd(NewWriteConflictSuite)
}

func execSQLLoop(ctx context.Context, cfg *config.Config, genSQL func() string) error {
	db := util.GetSQLCli(cfg)
	defer func() {
		db.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		sql := genSQL()
		err := execSQL(db, sql)
		if err != nil {
			return err
		}
	}
}

func execSQL(db *sql.DB, sql string) error {
	if strings.HasPrefix(strings.ToLower(sql), "select") {
		rows, err := db.Query(sql)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		return rows.Close()
	}

	_, err := db.Exec(sql)
	return err
}
