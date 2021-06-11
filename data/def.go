package data

import (
	"bytes"
	"fmt"
	"strings"
)

type ColumnDef struct {
	Name         string
	Tp           string
	Property     string
	DefaultValue string
	MinValue     string
	MaxValue     string
}

func NewTableInfo(dbName, tableName string, colDefs []ColumnDef, indexs []IndexInfo) (*TableInfo, error) {
	colInfos := []*ColumnInfo{}
	for _, colDef := range colDefs {
		col, err := NewColumnInfo(colDef)
		if err != nil {
			return nil, err
		}
		colInfos = append(colInfos, col)
	}
	return &TableInfo{
		DBName:    dbName,
		TableName: tableName,
		Columns:   colInfos,
		Indexs:    indexs,
	}, nil
}

func (col *ColumnInfo) getDefinition() string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(col.fieldType)
	if len(col.Property) > 0 {
		buf.WriteString(" ")
		buf.WriteString(col.Property)
	}
	if col.DefaultValue != nil {
		buf.WriteString(" DEFAULT ")
		buf.WriteString(col.getDefaultValueString())
	}
	return buf.String()
}

func (col *ColumnInfo) getDefaultValueString() string {
	if col.Tp == KindBit {
		return fmt.Sprintf("b'%v'", col.DefaultValue)
	} else {
		str := fmt.Sprintf("%v", col.DefaultValue)
		if strings.Contains(strings.ToLower(str), "current_timestamp") {
			return str
		}
		return fmt.Sprintf("'%v'", str)
	}
}
