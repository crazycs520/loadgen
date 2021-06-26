package payload

import (
	"fmt"
	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
)

type FullIndexScanSuite struct {
	*basicQuerySuite
}

func (c *FullIndexScanSuite) Name() string {
	return fullIndexScanSuiteName
}

func (c *FullIndexScanSuite) GenQuerySQL() string {
	if c.agg {
		return fmt.Sprintf("select sum(b), avg(b), min(b), max(b) from %v use index (idx0);", c.tblInfo.DBTableName())
	}
	return fmt.Sprintf("select b from %v use index (idx0);", c.tblInfo.DBTableName())
}

func NewFullIndexScanSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FullIndexScanSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
