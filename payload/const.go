package payload

const (
	normalOLTPSuiteName           = "normal-oltp"
	fullTableScanSuiteName        = "full-table-scan"
	fullIndexScanSuiteName        = "full-index-scan"
	fullIndexLookupSuiteName      = "full-index-lookup"
	randPointGetSuiteName         = "rand-point-get"
	randBatchPointGetSuiteName    = "rand-batch-point-get"
	fixPointGetSuiteName          = "fix-point-get"
	writeAutoIncSuiteName         = "write-auto-inc"
	pointGetForUpdateSuiteName    = "point-get-for-update"
	indexLookupForUpdateSuiteName = "index-lookup-for-update"
	writeWideTableSuiteName       = "write-wide-table"
)

const (
	symbolSeparator   = ":"
	symbolAssignment  = "="
	flagRows          = "rows"
	flagTables        = "tables"
	flagIsAgg         = "agg"  // aggregation
	flagIsBack        = "back" // is back table query
	flagInsert        = "insert"
	flagUpdate        = "update"
	flagSelect        = "select"
	flagTime          = "time" // time running test
	flagPointGet      = "point-get"
	flagIgnore        = "ignore" // ignore execute sql error
	flagBatchSize     = "batch-size"
	flagRowID         = "rowid"
	flagRandRowID     = "rand-rowid"
	flagColCnt        = "col-cnt"
	flagUsePrepare    = "use-prepare"
	flagIntCols       = "int-cols"
	flagDoubleCols    = "double-cols"
	flagVarcharCols   = "varchar-cols"
	flagVarcharSize   = "varchar-size"
	flagTimestampSize = "timestamp-size"
	flagFile          = "file"
	flagPrepare       = "prepare"
	flagExecute       = "execute"
)
