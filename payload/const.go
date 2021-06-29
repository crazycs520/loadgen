package payload

const (
	normalOLTPSuiteName        = "normal-oltp"
	fullTableScanSuiteName     = "full-table-scan"
	fullIndexScanSuiteName     = "full-index-scan"
	fullIndexLookupSuiteName   = "full-index-lookup"
	randPointGetSuiteName      = "rand-point-get"
	randBatchPointGetSuiteName = "rand-batch-point-get"
	fixPointGetSuiteName       = "fix-point-get"
	writeAutoIncSuiteName      = "write-auto-inc"
)

const (
	symbolSeparator  = ":"
	symbolAssignment = "="
	flagRows         = "rows"
	flagAgg          = "agg" // aggregation
	flagInsert       = "insert"
	flagUpdate       = "update"
	flagSelect       = "select"
	flagPointGet     = "point-get"
	flagIgnore       = "ignore" // ignore execute sql error
	flagBatchSize    = "batch-size"
	flagRowID        = "rowid"
	flagColCnt       = "col-cnt"
)
