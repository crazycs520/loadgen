package payload

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/crazycs520/loadgen/cmd"
	"github.com/crazycs520/loadgen/config"
)

const (
	insertWideBenchmarkSuiteName = "insert-wide-benchmark"
	insertWideTableName          = "t_insert_wide"
	insertWideColumnCount        = 50

	insertProtocolText        = "text"
	insertProtocolInterpolate = "interpolate"
	insertProtocolPrepare     = "prepare"

	insertTxnAutocommit  = "autocommit"
	insertTxnOptimistic  = "optimistic"
	insertTxnPessimistic = "pessimistic"

	insertKeyScattered = "scattered"
	insertKeyMonotonic = "monotonic"
)

const insertWideCreateTableTemplate = "CREATE TABLE `%s`.`" + insertWideTableName + "` (" +
	"`id` BIGINT NOT NULL," +
	"`tenant_id` INT UNSIGNED NOT NULL," +
	"`order_no` VARCHAR(32) NOT NULL," +
	"`user_id` BIGINT UNSIGNED NOT NULL," +
	"`email` VARCHAR(96) NOT NULL," +
	"`status` TINYINT UNSIGNED NOT NULL," +
	"`category_id` INT UNSIGNED NOT NULL," +
	"`region_id` SMALLINT UNSIGNED NOT NULL," +
	"`quantity` INT NOT NULL," +
	"`amount` DECIMAL(18,4) NOT NULL," +
	"`discount` DECIMAL(10,4) NOT NULL," +
	"`score` DOUBLE NOT NULL," +
	"`ratio` FLOAT NOT NULL," +
	"`is_active` BOOLEAN NOT NULL," +
	"`flags` BIGINT UNSIGNED NOT NULL," +
	"`created_at` DATETIME(6) NOT NULL," +
	"`updated_at` TIMESTAMP(6) NOT NULL," +
	"`biz_date` DATE NOT NULL," +
	"`biz_time` TIME(6) NOT NULL," +
	"`biz_year` YEAR NOT NULL," +
	"`name` VARCHAR(64) NOT NULL," +
	"`title` VARCHAR(128) NOT NULL," +
	"`description` VARCHAR(256) NOT NULL," +
	"`code` CHAR(16) NOT NULL," +
	"`tags` JSON NOT NULL," +
	"`metadata` JSON NOT NULL," +
	"`payload` VARBINARY(64) NOT NULL," +
	"`bit_flags` BIT(8) NOT NULL," +
	"`event_type` ENUM('create','update','delete') NOT NULL," +
	"`label_set` SET('a','b','c','d') NOT NULL," +
	"`note` TEXT NOT NULL," +
	"`c_tiny` TINYINT NOT NULL," +
	"`c_small` SMALLINT NOT NULL," +
	"`c_medium` MEDIUMINT NOT NULL," +
	"`c_int` INT NOT NULL," +
	"`c_big` BIGINT NOT NULL," +
	"`c_utiny` TINYINT UNSIGNED NOT NULL," +
	"`c_usmall` SMALLINT UNSIGNED NOT NULL," +
	"`c_umedium` MEDIUMINT UNSIGNED NOT NULL," +
	"`c_uint` INT UNSIGNED NOT NULL," +
	"`c_ubig` BIGINT UNSIGNED NOT NULL," +
	"`c_decimal` DECIMAL(30,8) NOT NULL," +
	"`c_float` FLOAT NOT NULL," +
	"`c_double` DOUBLE NOT NULL," +
	"`c_char` CHAR(32) NOT NULL," +
	"`c_varchar` VARCHAR(255) NOT NULL," +
	"`c_binary` BINARY(16) NOT NULL," +
	"`c_varbinary` VARBINARY(128) NOT NULL," +
	"`c_datetime` DATETIME NOT NULL," +
	"`c_timestamp` TIMESTAMP NOT NULL," +
	"PRIMARY KEY (`id`) CLUSTERED," +
	"UNIQUE KEY `uk_tenant_order` (`tenant_id`,`order_no`)," +
	"UNIQUE KEY `uk_email` (`email`)," +
	"KEY `idx_created_status` (`created_at`,`status`)," +
	"KEY `idx_category_score` (`category_id`,`score`)" +
	")"

type insertLiteralKind uint8

const (
	insertLiteralNumber insertLiteralKind = iota
	insertLiteralString
	insertLiteralBinary
)

var insertWideLiteralKinds = [...]insertLiteralKind{
	insertLiteralNumber, // id
	insertLiteralNumber, // tenant_id
	insertLiteralString, // order_no
	insertLiteralNumber, // user_id
	insertLiteralString, // email
	insertLiteralNumber, // status
	insertLiteralNumber, // category_id
	insertLiteralNumber, // region_id
	insertLiteralNumber, // quantity
	insertLiteralNumber, // amount
	insertLiteralNumber, // discount
	insertLiteralNumber, // score
	insertLiteralNumber, // ratio
	insertLiteralNumber, // is_active
	insertLiteralNumber, // flags
	insertLiteralString, // created_at
	insertLiteralString, // updated_at
	insertLiteralString, // biz_date
	insertLiteralString, // biz_time
	insertLiteralNumber, // biz_year
	insertLiteralString, // name
	insertLiteralString, // title
	insertLiteralString, // description
	insertLiteralString, // code
	insertLiteralString, // tags
	insertLiteralString, // metadata
	insertLiteralBinary, // payload
	insertLiteralBinary, // bit_flags
	insertLiteralString, // event_type
	insertLiteralString, // label_set
	insertLiteralString, // note
	insertLiteralNumber, // c_tiny
	insertLiteralNumber, // c_small
	insertLiteralNumber, // c_medium
	insertLiteralNumber, // c_int
	insertLiteralNumber, // c_big
	insertLiteralNumber, // c_utiny
	insertLiteralNumber, // c_usmall
	insertLiteralNumber, // c_umedium
	insertLiteralNumber, // c_uint
	insertLiteralNumber, // c_ubig
	insertLiteralNumber, // c_decimal
	insertLiteralNumber, // c_float
	insertLiteralNumber, // c_double
	insertLiteralString, // c_char
	insertLiteralString, // c_varchar
	insertLiteralBinary, // c_binary
	insertLiteralBinary, // c_varbinary
	insertLiteralString, // c_datetime
	insertLiteralString, // c_timestamp
}

var (
	insertWideDescriptionPrefix = strings.Repeat("description-", 12)
	insertWideNotePrefix        = strings.Repeat("note-", 36)
	insertWideVarcharPrefix     = strings.Repeat("varchar-", 20)
	insertWideBaseTime          = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
)

type insertEndpoint struct {
	host string
	port int
}

func (e insertEndpoint) String() string {
	return fmt.Sprintf("%s:%d", e.host, e.port)
}

// InsertWideBenchmarkSuite measures end-to-end insert time for a deterministic
// 50-column table with two unique and two non-unique secondary indexes.
type InsertWideBenchmarkSuite struct {
	cfg *config.Config

	rows          int
	batchSize     int
	txnStatements int
	protocol      string
	txnMode       string
	keyMode       string
	recreateTable bool
	caseName      string
	resultFile    string
}

// NewInsertWideBenchmarkSuite creates the wide insert benchmark command.
func NewInsertWideBenchmarkSuite(cfg *config.Config) cmd.CMDGenerater {
	return &InsertWideBenchmarkSuite{
		cfg:           cfg,
		rows:          1_000_000,
		batchSize:     100,
		txnStatements: 1,
		protocol:      insertProtocolPrepare,
		txnMode:       insertTxnAutocommit,
		keyMode:       insertKeyScattered,
		recreateTable: true,
		caseName:      "insert-wide",
	}
}

// Cmd builds the Cobra command.
func (c *InsertWideBenchmarkSuite) Cmd() *cobra.Command {
	command := &cobra.Command{
		Use:          insertWideBenchmarkSuiteName,
		Short:        "benchmark deterministic 100-row INSERTs into a 50-column indexed table",
		RunE:         c.RunE,
		SilenceUsage: true,
	}
	command.Flags().IntVar(&c.rows, flagRows, c.rows, "total rows; must be divisible by batch-size")
	command.Flags().IntVar(&c.batchSize, flagBatchSize, c.batchSize, "rows in each INSERT statement")
	command.Flags().IntVar(&c.txnStatements, "txn-statements", c.txnStatements, "INSERT statements in each explicit transaction")
	command.Flags().StringVar(&c.protocol, "protocol", c.protocol, "text, interpolate, or prepare")
	command.Flags().StringVar(&c.txnMode, "txn-mode", c.txnMode, "autocommit, optimistic, or pessimistic")
	command.Flags().StringVar(&c.keyMode, "key-mode", c.keyMode, "scattered or monotonic")
	command.Flags().BoolVar(&c.recreateTable, "recreate-table", c.recreateTable, "drop and recreate the benchmark table before timing")
	command.Flags().StringVar(&c.caseName, "case-name", c.caseName, "case identifier stored in the result")
	command.Flags().StringVar(&c.resultFile, "result-file", c.resultFile, "optional JSON result file")
	return command
}

// RunE validates flags and runs the benchmark.
func (c *InsertWideBenchmarkSuite) RunE(_ *cobra.Command, _ []string) error {
	return c.Run()
}

// Run executes the benchmark.
func (c *InsertWideBenchmarkSuite) Run() error {
	if err := c.validate(); err != nil {
		return err
	}

	endpoints := insertEndpoints(c.cfg)
	if c.recreateTable {
		if err := c.createTable(endpoints[0]); err != nil {
			return err
		}
	}

	result, runErr := c.runBenchmark(endpoints)
	if runErr != nil {
		result.Error = runErr.Error()
	}
	encoded, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		return marshalErr
	}
	fmt.Println(string(encoded))

	if c.resultFile != "" {
		if err := writeInsertWideResult(c.resultFile, encoded); err != nil {
			return err
		}
	}
	return runErr
}

func (c *InsertWideBenchmarkSuite) validate() error {
	switch c.protocol {
	case insertProtocolText, insertProtocolInterpolate, insertProtocolPrepare:
	default:
		return fmt.Errorf("unknown protocol %q", c.protocol)
	}
	switch c.txnMode {
	case insertTxnAutocommit, insertTxnOptimistic, insertTxnPessimistic:
	default:
		return fmt.Errorf("unknown txn-mode %q", c.txnMode)
	}
	switch c.keyMode {
	case insertKeyScattered, insertKeyMonotonic:
	default:
		return fmt.Errorf("unknown key-mode %q", c.keyMode)
	}
	if c.rows <= 0 {
		return errors.New("rows must be greater than zero")
	}
	if c.batchSize <= 0 {
		return errors.New("batch-size must be greater than zero")
	}
	if c.rows%c.batchSize != 0 {
		return fmt.Errorf("rows %d must be divisible by batch-size %d", c.rows, c.batchSize)
	}
	if c.batchSize*insertWideColumnCount > math.MaxUint16 {
		return fmt.Errorf("batch-size %d creates more than 65535 parameters", c.batchSize)
	}
	if c.txnStatements <= 0 {
		return errors.New("txn-statements must be greater than zero")
	}
	if c.txnMode == insertTxnAutocommit && c.txnStatements != 1 {
		return errors.New("autocommit requires txn-statements=1")
	}
	if c.cfg.Thread <= 0 {
		return errors.New("thread must be greater than zero")
	}
	if !validInsertIdentifier(c.cfg.DBName) {
		return fmt.Errorf("invalid database name %q", c.cfg.DBName)
	}
	return nil
}

func validInsertIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func (c *InsertWideBenchmarkSuite) createTable(endpoint insertEndpoint) error {
	db, err := openInsertWideDB(c.cfg, endpoint, "", false)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	databaseName := "`" + c.cfg.DBName + "`"
	statements := []string{
		"CREATE DATABASE IF NOT EXISTS " + databaseName,
		"DROP TABLE IF EXISTS " + databaseName + ".`" + insertWideTableName + "`",
		fmt.Sprintf(insertWideCreateTableTemplate, c.cfg.DBName),
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("execute DDL: %w", err)
		}
	}
	return nil
}

type insertWideResult struct {
	CaseName       string               `json:"case_name"`
	StartedAt      string               `json:"started_at"`
	FinishedAt     string               `json:"finished_at"`
	DurationSecond float64              `json:"duration_seconds"`
	Rows           int64                `json:"rows"`
	Statements     int64                `json:"statements"`
	Transactions   int64                `json:"transactions"`
	RowsPerSecond  float64              `json:"rows_per_second"`
	Protocol       string               `json:"protocol"`
	TxnMode        string               `json:"txn_mode"`
	TxnStatements  int                  `json:"txn_statements"`
	BatchSize      int                  `json:"batch_size"`
	Threads        int                  `json:"threads"`
	KeyMode        string               `json:"key_mode"`
	Endpoints      []string             `json:"endpoints"`
	SessionVars    string               `json:"session_variables"`
	TxnLatency     insertLatencySummary `json:"transaction_latency_ms"`
	Error          string               `json:"error,omitempty"`
}

type insertLatencySummary struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

func (c *InsertWideBenchmarkSuite) runBenchmark(endpoints []insertEndpoint) (insertWideResult, error) {
	result := insertWideResult{
		CaseName:      c.caseName,
		Protocol:      c.protocol,
		TxnMode:       c.txnMode,
		TxnStatements: c.txnStatements,
		BatchSize:     c.batchSize,
		Threads:       c.cfg.Thread,
		KeyMode:       c.keyMode,
		SessionVars:   c.cfg.SessionVars,
		Endpoints:     make([]string, len(endpoints)),
	}
	for i := range endpoints {
		result.Endpoints[i] = endpoints[i].String()
	}

	totalBatches := int64(c.rows / c.batchSize)
	var nextBatch int64
	var committedRows int64
	var committedStatements int64
	var committedTransactions int64
	latencies := &insertLatencyCollector{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, c.cfg.Thread)
	var wg sync.WaitGroup

	start := time.Now()
	result.StartedAt = start.Format(time.RFC3339Nano)
	for workerID := 0; workerID < c.cfg.Thread; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			endpoint := endpoints[id%len(endpoints)]
			err := c.runWorker(
				ctx,
				endpoint,
				totalBatches,
				&nextBatch,
				&committedRows,
				&committedStatements,
				&committedTransactions,
				latencies,
			)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("worker %d on %s: %w", id, endpoint.String(), err):
				default:
				}
				cancel()
			}
		}(workerID)
	}
	wg.Wait()
	finish := time.Now()

	result.FinishedAt = finish.Format(time.RFC3339Nano)
	result.DurationSecond = finish.Sub(start).Seconds()
	result.Rows = atomic.LoadInt64(&committedRows)
	result.Statements = atomic.LoadInt64(&committedStatements)
	result.Transactions = atomic.LoadInt64(&committedTransactions)
	if result.DurationSecond > 0 {
		result.RowsPerSecond = float64(result.Rows) / result.DurationSecond
	}
	result.TxnLatency = latencies.summary()

	select {
	case err := <-errCh:
		return result, err
	default:
	}
	if result.Rows != int64(c.rows) {
		return result, fmt.Errorf("committed %d rows, expected %d", result.Rows, c.rows)
	}
	return result, nil
}

func (c *InsertWideBenchmarkSuite) runWorker(
	ctx context.Context,
	endpoint insertEndpoint,
	totalBatches int64,
	nextBatch *int64,
	committedRows *int64,
	committedStatements *int64,
	committedTransactions *int64,
	latencies *insertLatencyCollector,
) error {
	db, err := openInsertWideDB(c.cfg, endpoint, c.cfg.DBName, c.protocol == insertProtocolInterpolate)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	if err := c.initializeSession(ctx, db); err != nil {
		return err
	}

	preparedSQL := buildInsertWidePreparedSQL(c.cfg.DBName, c.batchSize)
	var preparedStmt *sql.Stmt
	if c.protocol == insertProtocolPrepare {
		preparedStmt, err = db.PrepareContext(ctx, preparedSQL)
		if err != nil {
			return err
		}
		defer preparedStmt.Close()
	}

	if c.txnMode == insertTxnAutocommit {
		for {
			batchIndex := atomic.AddInt64(nextBatch, 1) - 1
			if batchIndex >= totalBatches {
				return nil
			}
			started := time.Now()
			if err := c.executeBatch(ctx, db, preparedStmt, preparedSQL, batchIndex); err != nil {
				return err
			}
			latencies.add(time.Since(started))
			atomic.AddInt64(committedRows, int64(c.batchSize))
			atomic.AddInt64(committedStatements, 1)
			atomic.AddInt64(committedTransactions, 1)
		}
	}

	groupSize := int64(c.txnStatements)
	for {
		firstBatch := atomic.AddInt64(nextBatch, groupSize) - groupSize
		if firstBatch >= totalBatches {
			return nil
		}
		lastBatch := firstBatch + groupSize
		if lastBatch > totalBatches {
			lastBatch = totalBatches
		}

		started := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		var txPreparedStmt *sql.Stmt
		if preparedStmt != nil {
			txPreparedStmt = tx.StmtContext(ctx, preparedStmt)
		}
		for batchIndex := firstBatch; batchIndex < lastBatch; batchIndex++ {
			if err := c.executeBatch(ctx, tx, txPreparedStmt, preparedSQL, batchIndex); err != nil {
				if txPreparedStmt != nil {
					txPreparedStmt.Close()
				}
				_ = tx.Rollback()
				return err
			}
		}
		if txPreparedStmt != nil {
			if err := txPreparedStmt.Close(); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		latencies.add(time.Since(started))

		statementCount := lastBatch - firstBatch
		atomic.AddInt64(committedRows, statementCount*int64(c.batchSize))
		atomic.AddInt64(committedStatements, statementCount)
		atomic.AddInt64(committedTransactions, 1)
	}
}

func (c *InsertWideBenchmarkSuite) initializeSession(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "SET SESSION autocommit=1"); err != nil {
		return err
	}
	if c.txnMode == insertTxnOptimistic || c.txnMode == insertTxnPessimistic {
		statement := "SET SESSION tidb_txn_mode='" + c.txnMode + "'"
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	for _, assignment := range c.cfg.GetSessionVars() {
		if assignment == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, "SET SESSION "+assignment); err != nil {
			return fmt.Errorf("set session %s: %w", assignment, err)
		}
	}
	return nil
}

type insertWideExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func (c *InsertWideBenchmarkSuite) executeBatch(
	ctx context.Context,
	execer insertWideExecer,
	preparedStmt *sql.Stmt,
	preparedSQL string,
	batchIndex int64,
) error {
	firstRow := batchIndex * int64(c.batchSize)
	switch c.protocol {
	case insertProtocolText:
		_, err := execer.ExecContext(ctx, buildInsertWideTextSQL(c.cfg.DBName, firstRow, c.batchSize, c.keyMode))
		return err
	case insertProtocolInterpolate:
		args := buildInsertWideArgs(firstRow, c.batchSize, c.keyMode)
		_, err := execer.ExecContext(ctx, preparedSQL, args...)
		return err
	case insertProtocolPrepare:
		if preparedStmt == nil {
			return errors.New("prepared statement is not initialized")
		}
		args := buildInsertWideArgs(firstRow, c.batchSize, c.keyMode)
		_, err := preparedStmt.ExecContext(ctx, args...)
		return err
	default:
		return fmt.Errorf("unsupported protocol %q", c.protocol)
	}
}

func openInsertWideDB(
	cfg *config.Config,
	endpoint insertEndpoint,
	dbName string,
	interpolate bool,
) (*sql.DB, error) {
	driverConfig := mysqlDriver.NewConfig()
	driverConfig.User = cfg.User
	driverConfig.Passwd = cfg.Password
	driverConfig.Net = "tcp"
	driverConfig.Addr = endpoint.String()
	driverConfig.DBName = dbName
	driverConfig.InterpolateParams = interpolate
	driverConfig.Timeout = 10 * time.Second
	driverConfig.ReadTimeout = 10 * time.Minute
	driverConfig.WriteTimeout = 10 * time.Minute
	driverConfig.Params = map[string]string{"charset": "utf8mb4"}

	db, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func insertEndpoints(cfg *config.Config) []insertEndpoint {
	hosts := append([]string(nil), cfg.Hosts...)
	ports := append([]int(nil), cfg.Ports...)
	if len(hosts) == 0 {
		hosts = []string{cfg.Host}
	}
	if len(ports) == 0 {
		ports = []int{cfg.Port}
	}
	if len(hosts) == 1 && hosts[0] == "127.0.0.1" && cfg.Host != "127.0.0.1" {
		hosts[0] = cfg.Host
	}
	if len(ports) == 1 && ports[0] == 4000 && cfg.Port != 4000 {
		ports[0] = cfg.Port
	}

	count := len(hosts)
	if len(ports) > count {
		count = len(ports)
	}
	endpoints := make([]insertEndpoint, 0, count)
	for i := 0; i < count; i++ {
		endpoints = append(endpoints, insertEndpoint{
			host: hosts[i%len(hosts)],
			port: ports[i%len(ports)],
		})
	}
	return endpoints
}

func buildInsertWidePreparedSQL(dbName string, rows int) string {
	var builder strings.Builder
	builder.Grow(rows * insertWideColumnCount * 2)
	builder.WriteString("INSERT INTO `")
	builder.WriteString(dbName)
	builder.WriteString("`.`")
	builder.WriteString(insertWideTableName)
	builder.WriteString("` VALUES ")
	for row := 0; row < rows; row++ {
		if row > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('(')
		for column := 0; column < insertWideColumnCount; column++ {
			if column > 0 {
				builder.WriteByte(',')
			}
			builder.WriteByte('?')
		}
		builder.WriteByte(')')
	}
	return builder.String()
}

func buildInsertWideTextSQL(dbName string, firstRow int64, rows int, keyMode string) string {
	var builder strings.Builder
	builder.Grow(rows * 1024)
	builder.WriteString("INSERT INTO `")
	builder.WriteString(dbName)
	builder.WriteString("`.`")
	builder.WriteString(insertWideTableName)
	builder.WriteString("` VALUES ")
	values := make([]interface{}, 0, insertWideColumnCount)
	for row := 0; row < rows; row++ {
		if row > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('(')
		values = values[:0]
		values = appendInsertWideRow(values, firstRow+int64(row), keyMode)
		for column, value := range values {
			if column > 0 {
				builder.WriteByte(',')
			}
			appendInsertWideLiteral(&builder, value, insertWideLiteralKinds[column])
		}
		builder.WriteByte(')')
	}
	return builder.String()
}

func buildInsertWideArgs(firstRow int64, rows int, keyMode string) []interface{} {
	args := make([]interface{}, 0, rows*insertWideColumnCount)
	for row := 0; row < rows; row++ {
		args = appendInsertWideRow(args, firstRow+int64(row), keyMode)
	}
	return args
}

func appendInsertWideRow(dst []interface{}, sequence int64, keyMode string) []interface{} {
	key := insertWideKey(sequence, keyMode)
	tenantID := key % 10_000
	timeValue := insertWideBaseTime.Add(time.Duration(key%94_608_000) * time.Second)
	dateTimeMicros := timeValue.Format("2006-01-02 15:04:05.000000")
	dateTime := timeValue.Format("2006-01-02 15:04:05")
	dateValue := timeValue.Format("2006-01-02")
	timeOnly := timeValue.Format("15:04:05.000000")
	orderNo := fmt.Sprintf("ord-%016x", key)
	email := fmt.Sprintf("u%016x@example.test", key)
	code := fmt.Sprintf("%016x", key)
	name := fmt.Sprintf("name-%016x", key)
	title := fmt.Sprintf("title-%016x", key)
	description := insertWideDescriptionPrefix + code
	tags := fmt.Sprintf(`["tag-%d","region-%d"]`, key%97, key%32)
	metadata := fmt.Sprintf(`{"id":%d,"tenant":%d,"active":%t}`, key, tenantID, key%2 == 0)
	note := insertWideNotePrefix + code
	charValue := fmt.Sprintf("char-%016x", key)
	varcharValue := insertWideVarcharPrefix + code
	decimalValue := fmt.Sprintf("%d.%04d", key%1_000_000_000_000, key%10_000)
	largeDecimalValue := fmt.Sprintf("%d.%08d", key%10_000_000_000_000, key%100_000_000)
	eventTypes := [...]string{"create", "update", "delete"}
	labelSets := [...]string{"a", "a,b", "b,c", "a,c,d"}

	var binary16 [16]byte
	binary.BigEndian.PutUint64(binary16[:8], key)
	binary.BigEndian.PutUint64(binary16[8:], uint64(sequence+1))
	payload := make([]byte, 32)
	copy(payload[:16], binary16[:])
	copy(payload[16:], binary16[:])
	varBinary := make([]byte, 48)
	copy(varBinary[:16], binary16[:])
	copy(varBinary[16:32], binary16[:])
	copy(varBinary[32:], binary16[:])

	return append(dst,
		int64(key),         // 1 id
		uint64(tenantID),   // 2 tenant_id
		orderNo,            // 3 order_no
		key,                // 4 user_id
		email,              // 5 email
		uint64(key%8),      // 6 status
		uint64(key%10_000), // 7 category_id
		uint64(key%128),    // 8 region_id
		int64(key%1000)+1,  // 9 quantity
		decimalValue,       // 10 amount
		fmt.Sprintf("%d.%04d", key%1000, key%10_000), // 11 discount
		float64(key%1_000_000)/100.0,                 // 12 score
		float32(key%10_000)/10_000.0,                 // 13 ratio
		int64(key%2),                                 // 14 is_active
		key,                                          // 15 flags
		dateTimeMicros,                               // 16 created_at
		dateTimeMicros,                               // 17 updated_at
		dateValue,                                    // 18 biz_date
		timeOnly,                                     // 19 biz_time
		int64(2024+key%10),                           // 20 biz_year
		name,                                         // 21 name
		title,                                        // 22 title
		description,                                  // 23 description
		code,                                         // 24 code
		tags,                                         // 25 tags
		metadata,                                     // 26 metadata
		payload,                                      // 27 payload
		[]byte{byte(key)},                            // 28 bit_flags
		eventTypes[key%3],                            // 29 event_type
		labelSets[key%4],                             // 30 label_set
		note,                                         // 31 note
		int64(key%255)-127,                           // 32 c_tiny
		int64(key%65_535)-32_767,                     // 33 c_small
		int64(key%16_777_215)-8_388_607,              // 34 c_medium
		int64(key%4_294_967_295)-2_147_483_647,       // 35 c_int
		-int64(key%1_000_000_000_000),                // 36 c_big
		uint64(key%256),                              // 37 c_utiny
		uint64(key%65_536),                           // 38 c_usmall
		uint64(key%16_777_216),                       // 39 c_umedium
		uint64(key%4_294_967_296),                    // 40 c_uint
		key,                                          // 41 c_ubig
		largeDecimalValue,                            // 42 c_decimal
		float32(key%100_000)/100.0,                   // 43 c_float
		float64(key%10_000_000)/1000.0,               // 44 c_double
		charValue,                                    // 45 c_char
		varcharValue,                                 // 46 c_varchar
		binary16[:],                                  // 47 c_binary
		varBinary,                                    // 48 c_varbinary
		dateTime,                                     // 49 c_datetime
		dateTime,                                     // 50 c_timestamp
	)
}

func insertWideKey(sequence int64, keyMode string) uint64 {
	value := uint64(sequence + 1)
	if keyMode == insertKeyMonotonic {
		return value
	}
	return bits.Reverse64(value) >> 1
}

func appendInsertWideLiteral(builder *strings.Builder, value interface{}, kind insertLiteralKind) {
	switch kind {
	case insertLiteralNumber:
		switch v := value.(type) {
		case string:
			builder.WriteString(v)
		case int:
			builder.WriteString(strconv.Itoa(v))
		case int64:
			builder.WriteString(strconv.FormatInt(v, 10))
		case uint64:
			builder.WriteString(strconv.FormatUint(v, 10))
		case float32:
			builder.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
		case float64:
			builder.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
		default:
			fmt.Fprint(builder, v)
		}
	case insertLiteralString:
		builder.WriteByte('\'')
		appendInsertEscapedString(builder, fmt.Sprint(value))
		builder.WriteByte('\'')
	case insertLiteralBinary:
		builder.WriteString("X'")
		switch v := value.(type) {
		case []byte:
			builder.WriteString(hex.EncodeToString(v))
		case string:
			builder.WriteString(hex.EncodeToString([]byte(v)))
		default:
			builder.WriteString(hex.EncodeToString([]byte(fmt.Sprint(v))))
		}
		builder.WriteByte('\'')
	}
}

func appendInsertEscapedString(builder *strings.Builder, value string) {
	for _, r := range value {
		switch r {
		case 0:
			builder.WriteString(`\0`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\\':
			builder.WriteString(`\\`)
		case '\'':
			builder.WriteString(`\'`)
		default:
			builder.WriteRune(r)
		}
	}
}

type insertLatencyCollector struct {
	mu     sync.Mutex
	values []time.Duration
}

func (c *insertLatencyCollector) add(value time.Duration) {
	c.mu.Lock()
	c.values = append(c.values, value)
	c.mu.Unlock()
}

func (c *insertLatencyCollector) summary() insertLatencySummary {
	c.mu.Lock()
	values := append([]time.Duration(nil), c.values...)
	c.mu.Unlock()
	if len(values) == 0 {
		return insertLatencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	var total time.Duration
	for _, value := range values {
		total += value
	}
	milliseconds := func(value time.Duration) float64 {
		return float64(value) / float64(time.Millisecond)
	}
	percentile := func(p float64) time.Duration {
		index := int(math.Ceil(float64(len(values))*p)) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(values) {
			index = len(values) - 1
		}
		return values[index]
	}
	return insertLatencySummary{
		Count: len(values),
		Mean:  milliseconds(total / time.Duration(len(values))),
		P50:   milliseconds(percentile(0.50)),
		P95:   milliseconds(percentile(0.95)),
		P99:   milliseconds(percentile(0.99)),
		Max:   milliseconds(values[len(values)-1]),
	}
}

func writeInsertWideResult(path string, data []byte) error {
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
