package payload

type payloadName = string

const (
	fullTableScanSuiteName payloadName = "full-table-scan"
)

const (
	symbolSeparator  = ":"
	symbolAssignment = "="
	flagRows         = "rows"
	flagAgg          = "agg" // aggregation
)
