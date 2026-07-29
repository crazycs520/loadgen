package payload

import (
	"reflect"
	"strings"
	"testing"
)

func TestInsertWideSchemaShape(t *testing.T) {
	if len(insertWideLiteralKinds) != insertWideColumnCount {
		t.Fatalf("literal kind count = %d, want %d", len(insertWideLiteralKinds), insertWideColumnCount)
	}
	if got := strings.Count(insertWideCreateTableTemplate, " NOT NULL"); got != insertWideColumnCount {
		t.Fatalf("NOT NULL column count = %d, want %d", got, insertWideColumnCount)
	}
	if got := strings.Count(insertWideCreateTableTemplate, "UNIQUE KEY"); got != 2 {
		t.Fatalf("unique index count = %d, want 2", got)
	}
	if got := strings.Count(insertWideCreateTableTemplate, "KEY `idx_"); got != 2 {
		t.Fatalf("normal index count = %d, want 2", got)
	}
	if !strings.Contains(insertWideCreateTableTemplate, "PRIMARY KEY (`id`) CLUSTERED") {
		t.Fatal("clustered primary key is missing")
	}
}

func TestInsertWidePreparedSQL(t *testing.T) {
	const rows = 100
	statement := buildInsertWidePreparedSQL("insert_bench", rows)
	if got := strings.Count(statement, "?"); got != rows*insertWideColumnCount {
		t.Fatalf("placeholder count = %d, want %d", got, rows*insertWideColumnCount)
	}
	if got := strings.Count(statement, "("); got != rows {
		t.Fatalf("row tuple count = %d, want %d", got, rows)
	}
	if !strings.HasPrefix(statement, "INSERT INTO `insert_bench`.`t_insert_wide` VALUES ") {
		t.Fatalf("unexpected statement prefix: %s", statement[:64])
	}
}

func TestInsertWideRowsAreDeterministicAndUnique(t *testing.T) {
	first := buildInsertWideArgs(0, 2, insertKeyScattered)
	second := buildInsertWideArgs(0, 2, insertKeyScattered)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("row generation is not deterministic")
	}
	if len(first) != 2*insertWideColumnCount {
		t.Fatalf("argument count = %d, want %d", len(first), 2*insertWideColumnCount)
	}
	if first[0] == first[insertWideColumnCount] {
		t.Fatal("primary keys are not unique")
	}
	if first[2] == first[insertWideColumnCount+2] {
		t.Fatal("order numbers are not unique")
	}
	if first[4] == first[insertWideColumnCount+4] {
		t.Fatal("emails are not unique")
	}

	monotonic := buildInsertWideArgs(0, 2, insertKeyMonotonic)
	if monotonic[0] != int64(1) || monotonic[insertWideColumnCount] != int64(2) {
		t.Fatalf("unexpected monotonic keys: %v, %v", monotonic[0], monotonic[insertWideColumnCount])
	}
	if monotonic[0] == first[0] {
		t.Fatal("scattered and monotonic keys unexpectedly match")
	}
}

func TestInsertWideTextAndArgsContainSameRows(t *testing.T) {
	args := buildInsertWideArgs(0, 2, insertKeyScattered)
	statement := buildInsertWideTextSQL("insert_bench", 0, 2, insertKeyScattered)
	if len(args) != 2*insertWideColumnCount {
		t.Fatalf("argument count = %d, want %d", len(args), 2*insertWideColumnCount)
	}
	if !strings.Contains(statement, args[2].(string)) || !strings.Contains(statement, args[4].(string)) {
		t.Fatal("text statement does not contain generated unique values")
	}
	if got := strings.Count(statement, "),(") + 1; got != 2 {
		t.Fatalf("text row tuple count = %d, want 2", got)
	}
}

func TestValidInsertIdentifier(t *testing.T) {
	for _, value := range []string{"insert_bench", "db123"} {
		if !validInsertIdentifier(value) {
			t.Fatalf("expected valid identifier %q", value)
		}
	}
	for _, value := range []string{"", "insert-bench", "db`; DROP DATABASE test"} {
		if validInsertIdentifier(value) {
			t.Fatalf("expected invalid identifier %q", value)
		}
	}
}
