package payload

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

type FixPointGetSuite struct {
	*basicQuerySuite

	rowID int
}

func (c *FixPointGetSuite) Name() string {
	return fixPointGetSuiteName
}

func (c *FixPointGetSuite) GenQueryPrepareStmt() string {
	return "select * from " + c.tblInfo.DBTableName() + " where a = ?;"
}

func (c *FixPointGetSuite) GenQueryArgs() []interface{} {
	return []interface{}{c.rowID}
}

func NewFixPointGetSuite(cfg *config.Config) cmd.CMDGenerater {
	suite := &FixPointGetSuite{
		rowID: 1,
	}
	basic := NewBasicQuerySuite(cfg, suite)
	suite.basicQuerySuite = basic
	return suite
}

func (c *FixPointGetSuite) Cmd() *cobra.Command {
	cmd := c.basicQuerySuite.Cmd()
	cmd.Flags().IntVarP(&c.rowID, flagRowID, "", 1, "the fix rowid of point get")
	return cmd
}

func (c *FixPointGetSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.querySuite.Name(), func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		case flagAgg:
			v, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			c.agg = v
		case flagRowID:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rowID = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}
