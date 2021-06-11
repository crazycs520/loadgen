package data

import (
	"time"
)

// Kind constants.
const (
	KindTINYINT   int = iota
	KindSMALLINT      //int16
	KindMEDIUMINT     //int24
	KindInt32         //int32
	KindBigInt        //int64
	KindBit

	KindFloat
	KindDouble

	KindDECIMAL

	KindChar
	KindVarChar

	KindBLOB
	KindTINYBLOB
	KindMEDIUMBLOB
	KindLONGBLOB

	KindTEXT
	KindTINYTEXT
	KindMEDIUMTEXT
	KindLONGTEXT

	KindBool

	KindDATE
	KindTIME
	KindDATETIME
	KindTIMESTAMP
	KindYEAR

	KindJSON

	KindEnum
	KindSet
)

var ALLFieldType = map[int]string{
	KindTINYINT:   "TINYINT",
	KindSMALLINT:  "SMALLINT",
	KindMEDIUMINT: "MEDIUMINT",
	KindInt32:     "INT",
	KindBigInt:    "BIGINT",
	KindBit:       "BIT",

	KindFloat:  "FLOAT",
	KindDouble: "DOUBLE",

	KindDECIMAL: "DECIMAL",

	KindChar:    "CHAR",
	KindVarChar: "VARCHAR",

	KindBLOB:       "BLOB",
	KindTINYBLOB:   "TINYBLOB",
	KindMEDIUMBLOB: "MEDIUMBLOB",
	KindLONGBLOB:   "LONGBLOB",

	KindTEXT:       "TEXT",
	KindTINYTEXT:   "TINYTEXT",
	KindMEDIUMTEXT: "MEDIUMTEXT",
	KindLONGTEXT:   "LONGTEXT",

	KindBool: "BOOL",

	KindDATE:      "DATE",
	KindTIME:      "TIME",
	KindDATETIME:  "DATETIME",
	KindTIMESTAMP: "TIMESTAMP",
	KindYEAR:      "YEAR",

	KindJSON: "JSON",
	KindEnum: "ENUM",
	KindSet:  "SET",
}

const (
	valueNull string = "NULL"
)

const (
	TimeFormat        = "2006-01-02 15:04:05.000000"
	TimeFormatForDATE = "2006-01-02"
	TimeFormatForTIME = "15:04:05"

	MINDATETIME = "2000-01-01 00:00:00"
	MAXDATETIME = "2020-12-31 23:59:59"

	//MINTIMESTAMP = "1970-01-01 08:00:01" // TiDB have DST time problem
	MINTIMESTAMP = "2000-01-01 00:00:01"
	MAXTIMESTAMP = "2038-01-19 03:14:07"
)

var MinDATETIME time.Time
var MaxDATETIME time.Time
var GapDATETIMEUnix int64

var MinTIMESTAMP time.Time
var MaxTIMESTAMP time.Time
var GapTIMESTAMPUnix int64

type ambiguousTimeStr struct {
	start string
	end   string
}

type ambiguousTime struct {
	start int64
	end   int64
}

var ambiguousTimeStrSlice = []ambiguousTimeStr{
	// backward
	{
		start: "1900-12-31 23:54:17",
		end:   "1901-01-01 00:00:00",
	},
	// moved forward
	{
		start: "1940-06-02 23:59:59",
		end:   "1940-06-03 01:00:00",
	},
	// move backward
	{
		start: "1940-09-30 23:00:00",
		end:   "1940-10-01 00:00:00",
	},
	// moved forward
	{
		start: "1941-03-15 23:59:59",
		end:   "1941-03-16 01:00:00",
	},
	// move backward
	{
		start: "1941-09-30 23:00:00",
		end:   "1941-10-01 00:00:00",
	},
	// moved forward
	{
		start: "1986-05-03 23:59:59",
		end:   "1986-05-04 01:00:00",
	},
	// move backward
	{
		start: "1986-09-13 23:00:00",
		end:   "1986-09-14 00:00:00",
	},
	// moved forward
	{
		start: "1987-04-11 23:59:59",
		end:   "1987-04-12 01:00:00",
	},
	// move backward
	{
		start: "1987-09-12 23:00:00",
		end:   "1987-09-13 00:00:00",
	},
	// moved forward
	{
		start: "1988-04-09 23:59:59",
		end:   "1988-04-10 01:00:00",
	},
	// move backward
	{
		start: "1988-09-10 23:00:00",
		end:   "1988-09-11 00:00:00",
	},

	// moved forward
	{
		start: "1989-04-15 23:59:59",
		end:   "1989-04-16 01:00:00",
	},
	// move backward
	{
		start: "1989-09-16 23:00:00",
		end:   "1989-09-17 00:00:00",
	},
	// moved forward
	{
		start: "1990-04-14 23:59:59",
		end:   "1990-04-15 01:00:00",
	},
	// move backward
	{
		start: "1990-09-15 23:00:00",
		end:   "1990-09-16 00:00:00",
	},
	// moved forward
	{
		start: "1991-04-13 23:59:59",
		end:   "1991-04-14 01:00:00",
	},
	// move backward
	{
		start: "1991-09-14 23:00:00",
		end:   "1991-09-15 00:00:00",
	},
}

var ambiguousTimeSlice []ambiguousTime

var Local = time.Local

func init() {
	var err error
	Local, err = time.LoadLocation("Asia/Shanghai")
	if err != nil {
		Local = time.Local
	}
	for _, v := range ambiguousTimeStrSlice {
		start, _ := time.ParseInLocation(TimeFormat, v.start, Local)
		end, _ := time.ParseInLocation(TimeFormat, v.end, Local)
		amt := ambiguousTime{
			start: start.Unix(),
			end:   end.Unix(),
		}
		ambiguousTimeSlice = append(ambiguousTimeSlice, amt)
	}

	MinDATETIME, _ = time.ParseInLocation(TimeFormat, MINDATETIME, Local)
	MaxDATETIME, _ = time.ParseInLocation(TimeFormat, MAXDATETIME, Local)
	GapDATETIMEUnix = MaxDATETIME.Unix() - MinDATETIME.Unix()

	MinTIMESTAMP, _ = time.ParseInLocation(TimeFormat, MINTIMESTAMP, Local)
	MaxTIMESTAMP, _ = time.ParseInLocation(TimeFormat, MAXTIMESTAMP, Local)
	GapTIMESTAMPUnix = MaxTIMESTAMP.Unix() - MinTIMESTAMP.Unix()
}

func NotAmbiguousTime(t time.Time) bool {
	ok := true
	for _, amt := range ambiguousTimeSlice {
		if t.Unix() >= amt.start && t.Unix() <= amt.end {
			ok = false
			break
		}
	}
	return ok
}
