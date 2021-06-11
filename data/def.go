package data

import "fmt"

type ColumnDef struct {
	Name         string
	Tp           string
	DefaultValue string
	MinValue     string
	MaxValue     string
}

func NewTableInfo(dbName, tableName string, colDefs []ColumnDef, indexs []IndexInfo) (*TableInfo, error) {
	colInfos := []*ColumnInfo{}
	for _, colDef := range colDefs {
		col, err := NewColumnInfo(colDef.Name, colDef.Tp, colDef.DefaultValue, colDef.MinValue, colDef.MaxValue)
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
	if col.DefaultValue != nil {
		return fmt.Sprintf("%s NULL DEFAULT %v", col.fieldType, col.getDefaultValueString())
	} else {
		return fmt.Sprintf("%s NULL", col.fieldType)
	}
}

func (col *ColumnInfo) getDefaultValueString() string {
	if col.Tp == KindBit {
		return fmt.Sprintf("b'%v'", col.DefaultValue)
	} else {
		return fmt.Sprintf("'%v'", col.DefaultValue)
	}
}
