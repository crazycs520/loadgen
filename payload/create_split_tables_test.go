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

func TestCreateSplitTablesInsertTableSQL(t *testing.T) {
	suite := &CreateSplitTablesSuite{}

	got := suite.insertTableSQL(2, 3)
	want := "insert into t_2 (k, c, pad) values (?, ?, ?),(?, ?, ?),(?, ?, ?)"
	if got != want {
		t.Fatalf("insertTableSQL(2, 3) = %q, want %q", got, want)
	}
}

func TestCreateSplitTablesInsertTableArgs(t *testing.T) {
	suite := &CreateSplitTablesSuite{}

	got := suite.insertTableArgs(5, 2)
	want := []interface{}{5, "c-5", "pad-5", 6, "c-6", "pad-6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("insertTableArgs(5, 2) = %#v, want %#v", got, want)
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
