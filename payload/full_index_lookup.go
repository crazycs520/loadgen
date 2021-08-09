package payload

import (
	"fmt"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FullIndexLookUpSuite struct {
	*basicQuerySuite
}

func (c *FullIndexLookUpSuite) Name() string {
	return fullIndexLookupSuiteName
}

func (c *FullIndexLookUpSuite) GenQuerySQL() string {
	if c.agg {
		return fmt.Sprintf("select sum(a+e), max(c) from %v use index (idx0)", c.tblInfo.DBTableName())
	}
	return fmt.Sprintf("select * from %v use index (idx0)", c.tblInfo.DBTableName())
}

func NewFullIndexLookUpSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FullIndexLookUpSuite{}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}
