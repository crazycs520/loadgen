package payload

import (
	"strings"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
)

type WriteAutoIncSuite struct {
	cft       *config.Config
	tableName string
	*basicWriteSuite
}

func (c *WriteAutoIncSuite) Name() string {
	return writeAutoIncSuiteName
}

func (c *WriteAutoIncSuite) UpdateTableDef(_ *data.TableInfo) {
}

func (c *WriteAutoIncSuite) prepare() error {
	c.tableName = "t_" + strings.ReplaceAll(c.Name(), "-", "_")
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, c.tableName, []data.ColumnDef{
		{
			Name:     "auto",
			Tp:       "bigint",
			Property: "auto_increment",
		},
		{
			Name: "a",
			Tp:   "bigint",
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
	}, []data.IndexInfo{
		{
			Name:    "idx0",
			Tp:      data.UniqueIndex,
			Columns: []string{"a"},
		},
	})
	if err != nil {
		return err
	}
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuit(c.cfg)
	return load.CreateTable(c.tblInfo, false)
}

func NewWriteAutoIncSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &WriteAutoIncSuite{}
	basic := NewBasicWriteSuite(cfg, suite)
	suite.basicWriteSuite = basic
	return suite
}
