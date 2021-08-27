package payload

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/config"
	"github.com/crazycs520/loadgen/data"
)

const (
	defRowsOfBasicWrite = 100000
	defAdditionalColCnt = 0
)

type WriteSuite interface {
	Name() string
	UpdateTableDef(*data.TableInfo)
}

type basicWriteSuite struct {
	cfg        *config.Config
	tblInfo    *data.TableInfo
	writeSuite WriteSuite

	rows             int
	additionalColCnt int
}

func NewBasicWriteSuite(cfg *config.Config, writeSuite WriteSuite) *basicWriteSuite {
	return &basicWriteSuite{
		cfg:              cfg,
		writeSuite:       writeSuite,
		rows:             defRowsOfBasicWrite,
		additionalColCnt: defAdditionalColCnt,
	}
}

func (c *basicWriteSuite) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:          c.writeSuite.Name(),
		Short:        "payload of " + c.writeSuite.Name(),
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	cmd.Flags().IntVarP(&c.rows, flagRows, "", defRowsOfBasicWrite, "total insert rows")
	cmd.Flags().IntVarP(&c.additionalColCnt, flagColCnt, "", defAdditionalColCnt, "the count of additional bigint column")
	return cmd
}

func (c *basicWriteSuite) ParseCmd(combinedCmd string) bool {
	return ParsePayloadCmd(combinedCmd, c.writeSuite.Name(), func(flag, value string) error {
		switch flag {
		case flagRows:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.rows = v
		case flagColCnt:
			v, err := strconv.Atoi(value)
			if err != nil {
				return err
			}
			c.additionalColCnt = v
		default:
			return fmt.Errorf("unknow flag %v", flag)
		}
		return nil
	})
}

func (c *basicWriteSuite) RunE(cmd *cobra.Command, args []string) error {
	return c.Run()
}

func (c *basicWriteSuite) prepare() error {
	tableName := "t_" + strings.ReplaceAll(c.writeSuite.Name(), "-", "_")
	cols := []data.ColumnDef{
		{
			Name: "c0",
			Tp:   "bigint",
		},
		{
			Name: "c1",
			Tp:   "bigint",
		},
		{
			Name:         "c2",
			Tp:           "timestamp(6)",
			DefaultValue: "current_timestamp(6)",
		},
		{
			Name: "c3",
			Tp:   "varchar(100)",
		},
		{
			Name: "c4",
			Tp:   "decimal(48,10)",
		},
	}
	for i := 0; i < c.additionalColCnt; i++ {
		cols = append(cols, data.ColumnDef{
			Name: "c" + strconv.Itoa(len(cols)),
			Tp:   "bigint",
		})
	}
	tblInfo, err := data.NewTableInfo(c.cfg.DBName, tableName, cols, nil)
	if err != nil {
		return err
	}
	c.writeSuite.UpdateTableDef(tblInfo)
	c.tblInfo = tblInfo
	load := data.NewLoadDataSuite(c.cfg)
	return load.CreateTable(tblInfo, true)
}

func (c *basicWriteSuite) Run() error {
	err := c.prepare()
	if err != nil {
		fmt.Println("prepare table meet error: ", err)
		return err
	}

	load := data.NewLoadDataSuite(c.cfg)
	load.SetBatchSize(500)
	err = load.LoadData(c.tblInfo, c.rows)
	if err != nil {
		fmt.Printf("insert data error: %v\n", err)
	}
	return nil
}
