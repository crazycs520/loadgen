package payload

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/crazycs520/loadgen/config"
)

func TestCreateSplitTablesParseCmd(t *testing.T) {
	suite := NewCreateSplitTablesSuite(&config.Config{}).(*CreateSplitTablesSuite)

	if suite.rows != 10000 {
		t.Fatalf("default rows = %d, want 10000", suite.rows)
	}

	if !suite.ParseCmd("create-split-tables:tables=12:regions=88:rows=1234") {
		t.Fatalf("ParseCmd returned false")
	}
	if suite.tables != 12 {
		t.Fatalf("tables = %d, want 12", suite.tables)
	}
	if suite.regions != 88 {
		t.Fatalf("regions = %d, want 88", suite.regions)
	}
	if suite.rows != 1234 {
		t.Fatalf("rows = %d, want 1234", suite.rows)
	}
}

func TestCreateSplitTablesTableName(t *testing.T) {
	suite := &CreateSplitTablesSuite{}

	if got := suite.tableName(7); got != "t_7" {
		t.Fatalf("tableName(7) = %q, want %q", got, "t_7")
	}
}

func TestCreateSplitTablesCreateTableSQL(t *testing.T) {
	suite := &CreateSplitTablesSuite{}

	got := suite.createTableSQL(3)
	want := "create table t_3 (id integer not null auto_increment, k integer default '0' not null, c char(120) default '' not null, pad char(60) default '' not null, primary key (id))"
	if got != want {
		t.Fatalf("createTableSQL(3) = %q, want %q", got, want)
	}
}

func TestCreateSplitTablesSplitTableSQL(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 256}

	got := suite.splitTableSQL(9)
	want := "split table t_9 between (0) and (10000000) regions 256"
	if got != want {
		t.Fatalf("splitTableSQL(9) = %q, want %q", got, want)
	}
}

func TestCreateSplitTablesSplitTableSQLGrowsUpperBoundForRows(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 1000, rows: 10000001}

	got := suite.splitTableSQL(9)
	want := "split table t_9 between (0) and (10000002) regions 1000"
	if got != want {
		t.Fatalf("splitTableSQL(9) = %q, want %q", got, want)
	}
}

func TestCreateSplitTablesSplitTableSQLKeepsDefaultUpperBoundForDefaultRows(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 1000, rows: 10000000}

	got := suite.splitTableSQL(9)
	want := "split table t_9 between (0) and (10000000) regions 1000"
	if got != want {
		t.Fatalf("splitTableSQL(9) = %q, want %q", got, want)
	}
}

func TestCreateSplitTablesInsertTableSQL(t *testing.T) {
	suite := &CreateSplitTablesSuite{}

	got := suite.insertTableSQL(2, 3)
	want := "insert into t_2 (id, k, c, pad) values (?, ?, ?, ?),(?, ?, ?, ?),(?, ?, ?, ?)"
	if got != want {
		t.Fatalf("insertTableSQL(2, 3) = %q, want %q", got, want)
	}
}

func TestCreateSplitTablesInsertTableArgs(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 2, rows: 20}

	got := suite.insertTableArgs(5, 2)
	want := []interface{}{2750001, 5, "c-5", "pad-5", 3250001, 6, "c-6", "pad-6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("insertTableArgs(5, 2) = %#v, want %#v", got, want)
	}
}

func TestCreateSplitTablesRowIDStepUsesDefaultUpperBound(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 1000, rows: 10000}

	if got, want := suite.rowIDStep(), 1000; got != want {
		t.Fatalf("rowIDStep() = %d, want %d", got, want)
	}
	if got, want := suite.distributedRowID(3), 3501; got != want {
		t.Fatalf("distributedRowID(3) = %d, want %d", got, want)
	}
}

func TestCreateSplitTablesDistributedRowIDsCoverRegionsEvenly(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 4, rows: 9}
	regionWidth := suite.splitRegionWidth()
	counts := make([]int, suite.regions)

	for row := 0; row < suite.rows; row++ {
		id := suite.distributedRowID(row)
		region := id / regionWidth
		if region < 0 || region >= suite.regions {
			t.Fatalf("distributedRowID(%d) = %d maps to region %d, want 0..%d", row, id, region, suite.regions-1)
		}
		counts[region]++
	}

	want := []int{2, 2, 3, 2}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("distributed row counts = %#v, want %#v", counts, want)
	}
}

func TestCreateSplitTablesDistributedRowIDsAvoidSplitBoundaries(t *testing.T) {
	suite := &CreateSplitTablesSuite{regions: 4, rows: 12}
	regionWidth := suite.splitRegionWidth()
	counts := make([]int, suite.regions)

	for row := 0; row < suite.rows; row++ {
		id := suite.distributedRowID(row)
		if id%regionWidth == 0 {
			t.Fatalf("distributedRowID(%d) = %d hits split boundary %d", row, id, regionWidth)
		}
		counts[id/regionWidth]++
	}

	want := []int{3, 3, 3, 3}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("distributed row counts = %#v, want %#v", counts, want)
	}
}

func TestCreateSplitTablesRunTasksProcessesAllTables(t *testing.T) {
	suite := &CreateSplitTablesSuite{
		cfg:    &config.Config{Thread: 3},
		tables: 7,
	}

	var (
		mu        sync.Mutex
		processed = make(map[int]int)
	)

	err := suite.runTasks(func() (func(int) error, func(), error) {
		return func(idx int) error {
			mu.Lock()
			processed[idx]++
			mu.Unlock()
			return nil
		}, func() {}, nil
	})
	if err != nil {
		t.Fatalf("runTasks() error = %v, want nil", err)
	}

	for i := 0; i < suite.tables; i++ {
		if got := processed[i]; got != 1 {
			t.Fatalf("task %d processed %d times, want 1", i, got)
		}
	}
}

func TestCreateSplitTablesRunTasksReturnsWorkerError(t *testing.T) {
	suite := &CreateSplitTablesSuite{
		cfg:    &config.Config{Thread: 2},
		tables: 5,
	}
	wantErr := errors.New("boom")

	err := suite.runTasks(func() (func(int) error, func(), error) {
		return func(idx int) error {
			if idx == 2 {
				return wantErr
			}
			return nil
		}, func() {}, nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("runTasks() error = %v, want %v", err, wantErr)
	}
}
