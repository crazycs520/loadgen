package data

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type TableInfo struct {
	DBName    string
	TableName string
	Columns   []*ColumnInfo
	Indexs    []IndexInfo

	ExpectedRows int
}

type ColumnInfo struct {
	Name         string
	Tp           int
	fieldType    string
	Unsigned     bool
	Property     string // such as auto_increment
	FiledTypeM   int    //such as:  VARCHAR(10) ,    FiledTypeM = 10
	FiledTypeD   int    //such as:  DECIMAL(10,5) ,  FiledTypeD = 5
	DefaultValue interface{}
	MinValue     interface{}
	MaxValue     interface{}
}

const (
	NormalIndex int = 0
	UniqueIndex int = 1
	PrimaryKey  int = 2
)

type IndexInfo struct {
	Tp      int
	Columns []string
}

func NewColumnInfo(colDef ColumnDef) (*ColumnInfo, error) {
	tp := strings.ToLower(colDef.Tp)
	tpPrefix := tp
	tpSuffix := ""
	unsigned := false
	if idx := strings.Index(tp, "unsigned"); idx > 0 {
		tp = strings.TrimSpace(tp[:idx])
		unsigned = true
	}
	if idx := strings.Index(tp, "("); idx > 0 {
		tpPrefix = tp[:idx]
		tpSuffix = strings.TrimSpace(tp[idx:])
	}

	k, ok := str2ColumnTP[tpPrefix]
	if !ok {
		return nil, fmt.Errorf("unknown column tp: %v of column %v", tp, colDef.Name)
	}
	col := &ColumnInfo{
		Tp:        k,
		fieldType: ALLFieldType[k],
		Property:  colDef.Property,
		Name:      colDef.Name,
		Unsigned:  unsigned,
	}
	defaultValue, err := col.convertValue(colDef.DefaultValue)
	if err != nil {
		return nil, fmt.Errorf("parse default value error, tp is %v, error is %v", tpPrefix, err)
	}
	minValue, err := col.convertValue(colDef.MinValue)
	if err != nil {
		return nil, fmt.Errorf("parse min value error, tp is %v, error is %v", tpPrefix, err)
	}
	maxValue, err := col.convertValue(colDef.MaxValue)
	if err != nil {
		return nil, fmt.Errorf("parse max value error, tp is %v, error is %v", tpPrefix, err)
	}
	col.DefaultValue = defaultValue
	col.MinValue = minValue
	col.MaxValue = maxValue

	if tpSuffix == "" {
		return col, nil
	}
	tpSuffix = strings.Trim(tpSuffix, "(")
	tpSuffix = strings.Trim(tpSuffix, ")")
	nums := strings.Split(tpSuffix, ",")
	if len(nums) == 0 {
		return col, nil
	}
	num, err := strconv.ParseInt(nums[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unknown column tp: %v of column %v", tp, colDef.Name)
	}
	col.FiledTypeM = int(num)
	col.fieldType = fmt.Sprintf("%s(%d)", ALLFieldType[col.Tp], col.FiledTypeM)
	if len(nums) < 2 {
		return col, nil
	}
	num, err = strconv.ParseInt(nums[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unknown column tp: %v of column %v", tp, colDef.Name)
	}
	col.FiledTypeD = int(num)
	col.fieldType = fmt.Sprintf("%s(%d,%d)", ALLFieldType[col.Tp], col.FiledTypeM, col.FiledTypeD)
	return col, nil
}

var str2ColumnTP = map[string]int{
	"bit":        KindBit,
	"text":       KindTEXT,
	"date":       KindDATE,
	"datetime":   KindDATETIME,
	"decimal":    KindDECIMAL,
	"double":     KindDouble,
	"enum":       KindEnum,
	"float":      KindFloat,
	"mediumint":  KindMEDIUMINT,
	"json":       KindJSON,
	"int":        KindInt32,
	"bigint":     KindBigInt,
	"longtext":   KindLONGTEXT,
	"mediumtext": KindMEDIUMTEXT,
	"set":        KindSet,
	"smallint":   KindSMALLINT,
	"char":       KindChar,
	"time":       KindTIME,
	"timestamp":  KindTIMESTAMP,
	"tinyint":    KindTINYINT,
	"tinytext":   KindTINYTEXT,
	"varchar":    KindVarChar,
	"year":       KindYEAR,
}

// randValue return a rand value of the column
func (col *ColumnInfo) randValue() interface{} {
	switch col.Tp {
	case KindTINYINT:
		if col.Unsigned {
			return rand.Int31n(1 << 8)
		}
		return rand.Int31n(1<<8) - 1<<7
	case KindSMALLINT:
		if col.Unsigned {
			return rand.Int31n(1 << 16)
		}
		return rand.Int31n(1<<16) - 1<<15
	case KindMEDIUMINT:
		if col.Unsigned {
			return rand.Int31n(1 << 24)
		}
		return rand.Int31n(1<<24) - 1<<23
	case KindInt32:
		if col.Unsigned {
			return rand.Int63n(1 << 32)
		}
		return rand.Int63n(1<<32) - 1<<31
	case KindBigInt:
		if rand.Intn(2) == 1 || col.Unsigned {
			return rand.Int63()
		}
		return -1 - rand.Int63()
	case KindBit:
		if col.FiledTypeM >= 64 {
			return fmt.Sprintf("%b", rand.Uint64())
		} else {
			m := col.FiledTypeM
			if col.FiledTypeM > 7 { // it is a bug
				m = m - 1
			}
			n := (int64)((1 << (uint)(m)) - 1)
			return fmt.Sprintf("%b", rand.Int63n(n))
		}
	case KindFloat:
		return rand.Float32() + 1
	case KindDouble:
		return rand.Float64() + 1
	case KindDECIMAL:
		if col.Unsigned {
			value := RandDecimal(col.FiledTypeM, col.FiledTypeD)
			if len(value) > 0 && value[0] == '-' {
				return value[1:]
			}
		}
		return RandDecimal(col.FiledTypeM, col.FiledTypeD)
	case KindChar, KindVarChar, KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		if col.FiledTypeM == 0 {
			return ""
		} else {
			return RandSeq(rand.Intn(col.FiledTypeM))
		}
	case KindBool:
		return rand.Intn(2)
	case KindDATE:
		randTime := time.Unix(MinDATETIME.Unix()+rand.Int63n(GapDATETIMEUnix), 0)
		return randTime.Format(TimeFormatForDATE)
	case KindTIME:
		randTime := time.Unix(MinTIMESTAMP.Unix()+rand.Int63n(GapTIMESTAMPUnix), 0)
		return randTime.Format(TimeFormatForTIME)
	case KindDATETIME:
		randTime := randTime(MinDATETIME, GapDATETIMEUnix)
		return randTime.Format(TimeFormat)
	case KindTIMESTAMP:
		randTime := randTime(MinTIMESTAMP, GapTIMESTAMPUnix)
		return randTime.Format(TimeFormat)
	case KindYEAR:
		return rand.Intn(254) + 1901 //1901 ~ 2155
	default:
		return nil
	}
}

func (col *ColumnInfo) seqValue(num int64) interface{} {
	switch col.Tp {
	case KindTINYINT, KindSMALLINT, KindMEDIUMINT, KindInt32, KindBigInt:
		if col.Unsigned {
			v := uint64(num)
			if col.MinValue != nil {
				min := col.MinValue.(uint64)
				v = min + uint64(num)
				if col.MaxValue != nil {
					max := col.MaxValue.(uint64)
					if v > max {
						v = v%max + min
					}
				}
			}
			return v
		}
		v := num
		if col.MinValue != nil {
			min := col.MinValue.(int64)
			v = min + num
			if col.MaxValue != nil {
				max := col.MaxValue.(int64)
				if v > max {
					v = v%max + min
				}
			}
		}
		return v
	case KindBit:
		//if col.FiledTypeM >= 64 {
		//	return fmt.Sprintf("%b", rand.Uint64())
		//} else {
		//	m := col.FiledTypeM
		//	if col.FiledTypeM > 7 { // it is a bug
		//		m = m - 1
		//	}
		//	n := (int64)((1 << (uint)(m)) - 1)
		//	return fmt.Sprintf("%b", rand.Int63n(n))
		//}
		return nil
	case KindFloat, KindDouble:
		v := float64(num)
		if col.MinValue != nil {
			min := col.MinValue.(float64)
			v = min + float64(num)
			if col.MaxValue != nil {
				max := col.MaxValue.(float64)
				if v > max {
					v = float64(int64(v)%int64(max)) + min
				}
			}
		}
		return v
	case KindDECIMAL:
		return nil
		//return RandDecimal(col.FiledTypeM, col.FiledTypeD)
	case KindChar, KindVarChar, KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		if col.FiledTypeM == 0 {
			return ""
		} else {
			return intToSeqString(int(num))
		}
	case KindBool:
		return num % 2
	case KindDATE:
		return time.Now().Format(TimeFormatForDATE)
	case KindTIME:
		return time.Now().Format(TimeFormatForTIME)
	case KindDATETIME:
		return time.Now().Format(TimeFormat)
	case KindTIMESTAMP:
		return time.Now().Format(TimeFormat)
	case KindYEAR:
		return num%254 + 1901 //1901 ~ 2155
	default:
		return nil
	}
}

func (col *ColumnInfo) convertValue(value string) (interface{}, error) {
	if value == "" {
		return nil, nil
	}
	switch col.Tp {
	case KindTINYINT, KindSMALLINT, KindMEDIUMINT, KindInt32, KindBigInt:
		if col.Unsigned {
			return strconv.ParseUint(value, 10, 64)
		}
		return strconv.ParseInt(value, 10, 64)
	case KindBit:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return v, err
		}
		return fmt.Sprintf("%b", v), nil
	case KindFloat, KindDouble, KindDECIMAL:
		return strconv.ParseFloat(value, 64)
	case KindChar, KindVarChar, KindBLOB, KindTINYBLOB, KindMEDIUMBLOB, KindLONGBLOB, KindTEXT, KindTINYTEXT, KindMEDIUMTEXT, KindLONGTEXT:
		return value, nil
	case KindBool:
		return strconv.ParseBool(value)
	case KindDATE:
		return time.ParseInLocation(TimeFormatForDATE, value, Local)
	case KindTIME:
		return time.ParseInLocation(TimeFormatForTIME, value, Local)
	case KindDATETIME:
		return time.ParseInLocation(TimeFormat, value, Local)
	case KindTIMESTAMP:
		if strings.Contains(strings.ToLower(value), "current_timestamp") {
			return value, nil
		}
		return time.ParseInLocation(TimeFormat, value, Local)
	case KindYEAR:
		return strconv.ParseInt(value, 10, 64) //1901 ~ 2155
	default:
		return nil, nil
	}
}

func randTime(minTime time.Time, gap int64) time.Time {
	// https://github.com/chronotope/chrono-tz/issues/23
	// see all invalid time: https://timezonedb.com/time-zones/Asia/Shanghai
	var randTime time.Time
	for {
		randTime = time.Unix(minTime.Unix()+rand.Int63n(gap), 0).In(Local)
		if NotAmbiguousTime(randTime) {
			break
		}
	}
	return randTime
}

func RandDecimal(m, d int) string {
	ms := randNum(m - d)
	ds := randNum(d)
	var i int
	for i = range ms {
		if ms[i] != byte('0') {
			break
		}
	}
	ms = ms[i:]
	l := len(ms) + len(ds) + 1
	flag := rand.Intn(2)
	//check for 0.0... avoid -0.0
	zeroFlag := true
	for i := range ms {
		if ms[i] != byte('0') {
			zeroFlag = false
		}
	}
	for i := range ds {
		if ds[i] != byte('0') {
			zeroFlag = false
		}
	}
	if zeroFlag {
		flag = 0
	}
	vs := make([]byte, 0, l+flag)
	if flag == 1 {
		vs = append(vs, '-')
	}
	vs = append(vs, ms...)
	if len(ds) == 0 {
		return string(vs)
	}
	vs = append(vs, '.')
	vs = append(vs, ds...)
	return string(vs)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz1234567890"

func RandSeq(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func intToSeqString(n int) string {
	b := make([]byte, 0)
	for n > 0 {
		b = append(b, byte(n%26)+'a')
		n = n / 26
	}
	return string(b)
}

const numberBytes = "0123456789"

func randNum(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = numberBytes[rand.Int63()%int64(len(numberBytes))]
	}
	return b
}
