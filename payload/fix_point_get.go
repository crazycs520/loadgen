package payload

import (
	"fmt"
	"github.com/spf13/cobra"
	"strconv"

	"github.com/crazycs520/load/cmd"
	"github.com/crazycs520/load/config"
)

type FixPointGetSuite struct {
	*basicQuerySuite

	rowID int
}

func (c *FixPointGetSuite) Name() string {
	return fixPointGetSuiteName
}

func (c *FixPointGetSuite) GenQuerySQL() string {
	return fmt.Sprintf("select * from %v where a = %v", c.tblInfo.DBTableName(), c.rowID)
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
