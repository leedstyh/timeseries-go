package timeseries

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

//Get a column
func (ts TimeSeries) Get(colname string) []float64 {
	return ts.Columns[colname]
}

//GetIndex of series
func (ts TimeSeries) GetIndex() []time.Time {
	return ts.Index
}

//GetDataPointAtIndex returns a DataPoint at the index which can be either {time.Time, int, string}
func (ts TimeSeries) GetDataPointAtIndex(index interface{}) DataPoint {
	dp := NewDataPoint()
	switch index.(type) {
	case string:
		d, err := parseDate(index.(string))
		if err != nil {
			fmt.Println("parse date failed for getdatapoint:", err)
			return dp
		}
		i := ts.IndexOfTime(d)
		if i < 0 {
			fmt.Println("getdatapoint: did not find time in timeseries-", d)
			return dp
		}
		dp.Index = ts.Index[i]
		for k := range ts.Columns {
			dp.Columns[k] = ts.Columns[k][i]
		}
	case time.Time:
		i := ts.IndexOfTime(index.(time.Time))
		if i < 0 {
			fmt.Println("getdatapoint: did not find time in timeseries-", i)
			return dp
		}
		dp.Index = ts.Index[i]
		for k := range ts.Columns {
			dp.Columns[k] = ts.Columns[k][i]
		}
	case int:
		var i int
		if index.(int) < 0 {
			i = index.(int) + ts.Length()
		} else {
			i = index.(int)
		}
		dp.Index = ts.Index[i]
		for k := range ts.Columns {
			dp.Columns[k] = ts.Columns[k][i]
		}
	}
	return dp
}

//WriteToSetter writes a TimeSeries to any other type with a Set and SetIndex method
func (ts TimeSeries) WriteToSetter(dst TableSetter) TableSetter {
	dst.SetIndex(ts.Index)
	for k, v := range ts.Columns {
		dst.Set(k, v)
	}
	return dst
}

//utility func
func (ts TimeSeries) writeCsv(path string) error {
	var filename string
	if path[len(path)-3:] != "csv" {
		filename = filepath.Join(path, ts.Start().String()[:len(ts.Start().String())-10]+" "+ts.End().String()[:len(ts.Start().String())-10]+".csv")
	} else {
		filename = path
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(f)
	defer f.Close()
	defer writer.Flush()
	columns := append([]string{"timestamp"}, ts.ListColumns()...)
	writer.Write(columns)
	for i, t := range ts.Index {
		datapoint := make([]string, 0)
		datapoint = append(datapoint, t.String()[:len(t.String())-10])
		for _, col := range columns[1:] {
			datapoint = append(datapoint, strconv.FormatFloat(ts.Columns[col][i], 'f', 5, 64))
		}
		writer.Write(datapoint)
	}
	return nil
}

func (ts TimeSeries) writeJSON(path string) error {
	var filename string
	if path[len(path)-4:] != "json" {
		filename = filepath.Join(path, ts.Start().String()[:len(ts.Start().String())-10]+" "+ts.End().String()[:len(ts.Start().String())-10]+".csv")
	} else {
		filename = path
	}
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	defer f.Sync()
	data := split1{make([]string, 0), ts.Columns}
	for _, d := range ts.Index {
		data.Date = append(data.Date, d.String()[:len(d.String())-10])
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}
	f.Write(jsonData)
	return nil
}

//WriteAsJSON writes to a folderpath in batches of pagesize. if pagesize not provided
//it will write as a single file
func (ts TimeSeries) WriteAsJSON(folderpath string, pageSize ...int) error {
	_, err := os.Stat(folderpath)
	if err != nil {
		err := os.Mkdir(folderpath, 0766)
		if err != nil {
			return err
		}
	}
	if pageSize == nil {
		ts.writeJSON(folderpath)
	} else {
		tsSplit := ts.SplitByBatchSize(pageSize[0])
		for _, t := range tsSplit {
			t.writeJSON(folderpath)
		}
	}
	return nil
}

//WriteAsCSV writes timeseries to disk as csv
func (ts TimeSeries) WriteAsCSV(folderpath string, pageSize ...int) error {
	_, err := os.Stat(folderpath)
	if err != nil {
		err := os.Mkdir(folderpath, 0766)
		if err != nil {
			return err
		}
	}
	if pageSize == nil {
		ts.writeCsv(folderpath)
	} else {
		tsSplit := ts.SplitByBatchSize(pageSize[0])
		for _, t := range tsSplit {
			t.writeCsv(folderpath)
		}
	}
	return nil
}

//AppendToCSV opens `path` as CSV, writes to end, only supports OHLCV
//it also wont write column names
func (ts TimeSeries) AppendToCSV(path string, fromIndex ...interface{}) error {
	if fromIndex != nil {
		var err error
		ts, err = ts.Slice(fromIndex, ts.Length())
		if err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(f)
	defer f.Close()
	defer writer.Flush()
	columns := append([]string{"timestamp"}, "open", "high", "low", "close", "volume")
	writer.Write([]string{})
	for i, t := range ts.Index {
		datapoint := make([]string, 0)
		datapoint = append(datapoint, t.String()[:len(t.String())-10])
		for _, col := range columns[1:] {
			datapoint = append(datapoint, strconv.FormatFloat(ts.Columns[col][i], 'f', 4, 64))
		}
		writer.Write(datapoint)
	}
	return nil
}

func (ts TimeSeries) AppendDataPointToCSV(path string, dp DataPoint) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(f)
	defer f.Close()
	defer writer.Flush()
	columns := append([]string{"timestamp"}, "open", "high", "low", "close", "volume")
	writer.Write([]string{})
	datapoint := make([]string, 0)
	datapoint = append(datapoint, dp.Index.String()[:len(dp.Index.String())-10])
	for _, col := range columns[1:] {
		datapoint = append(datapoint, strconv.FormatFloat(dp.Columns[col], 'f', 4, 64))
	}
	writer.Write(datapoint)
	return nil
}

//ListColumns returns all numeric columns
func (ts TimeSeries) ListColumns() []string {
	cols := make([]string, 0)
	for k := range ts.Columns {
		cols = append(cols, k)
	}
	return cols
}

//Length of timeseries
func (ts TimeSeries) Length() int {
	return len(ts.Index)
}

//Start time of timeseries
func (ts TimeSeries) Start() time.Time {
	return ts.Index[0]
}

//End time of timeseries
func (ts TimeSeries) End() time.Time {
	return ts.Index[len(ts.Index)-1]
}

//Interval is difference of first two time index elements
func (ts TimeSeries) Interval() time.Duration {
	return ts.Index[1].Sub(ts.Index[0])
}

//IsEmpty returns true if no elements in timeseries
func (ts TimeSeries) IsEmpty() bool {
	if ts.Length() == 0 {
		return true
	}
	return false
}

//IndexOfTime returns index of time provided. searches linearly
//if time not in index, return -1
func (ts TimeSeries) IndexOfTime(t interface{}) int {
	switch t.(type) {
	case time.Time:
		for i, v := range ts.Index {
			if v.Equal(t.(time.Time)) {
				return i
			}
		}
		return -1
	case string:
		t, err := parseDate(t.(string))
		if err != nil {
			logrus.Errorln("could not find index at time:", err)
		}
		for i, v := range ts.Index {
			if v.Equal(t) {
				return i
			}
		}
		return -1
	default:
		logrus.Errorln("could not find index: invalid type for time")
		return -1
	}
}

//Validate equal lengths of all columns, withNonCritical is default false
func (ts TimeSeries) Validate(withNonCritical ...bool) error {
	var logAll bool
	if withNonCritical != nil {
		logAll = withNonCritical[0]
	}
	zeroRows := []time.Time{}
	zeroCols := []string{}
	for k := range ts.Columns {
		if len(ts.Columns[k]) != len(ts.Index) {
			log.Fatalln("validation failed: TimeSeries column lengths do not match! cannot recover")
		}
		isZeroColumn := true
		for _, j := range ts.Columns[k] {
			if j != 0 {
				isZeroColumn = false
				break
			}
		}
		if isZeroColumn {
			zeroCols = append(zeroCols, k)
		}
	}
	if logAll {
		for k := range ts.Index {
			if k != 0 {
				if ts.Index[k].Before(ts.Index[k-1]) {
					log.Warnln("validation warning: unsorted time Index: run ts.Sort()")
				}
				if ts.Index[k].Equal(ts.Index[k-1]) {
					log.Warnf("validation warning: duplicate Index keys found for %s\n", ts.Index[k].String())
				}
			}
			isZeroRow := true
			for col := range ts.Columns {
				if ts.Columns[col][k] != 0 {
					isZeroRow = false
					break
				}
			}
			if isZeroRow {
				zeroRows = append(zeroRows, ts.Index[k])
			}

		}
		if len(zeroRows) != 0 {
			for _, row := range zeroRows {
				logrus.Warnf("validation warning: row at index %v is empty/all zeroes\n", row)
			}
		}
		if len(zeroCols) != 0 {
			for _, col := range zeroCols {
				logrus.Warnf("validation warning: column %v is empty/all zeroes\n", col)
			}
		}
	}
	return nil
}

//Swap two indices
func (ts *TimeSeries) Swap(i, j int) {
	ts.Index[i], ts.Index[j] = ts.Index[j], ts.Index[i]
	for k := range ts.Columns {
		ts.Columns[k][i], ts.Columns[k][j] = ts.Columns[k][j], ts.Columns[k][i]
	}
}

//ConvertToDataPointArray converts TimeSeries to DataPointArray
func (ts TimeSeries) ConvertToDataPointArray() DataPointArray {
	dpa := make(DataPointArray, 0)
	for i := range ts.Index {
		dp := NewDataPoint()
		dp.Index = ts.Index[i]
		for k := range ts.Columns {
			dp.Columns[k] = ts.Columns[k][i]
		}
		dpa = append(dpa, dp)
	}
	return dpa
}

//ConvertToTimeSeries converts a datapointarray to timeseries
func (dpa DataPointArray) ConvertToTimeSeries() TimeSeries {
	ts := NewTimeSeries()
	for i := range dpa {
		ts.Index = append(ts.Index, dpa[i].Index)
		for k, v := range dpa[i].Columns {
			ts.Columns[k] = append(ts.Columns[k], v)
		}
	}
	return ts
}

//Sort a `TimeSeries`, if no by provided, sort by index
func (ts TimeSeries) Sort(by ...string) TimeSeries {
	dpa := ts.ConvertToDataPointArray()
	if by == nil {
		sort.Slice(dpa, func(i, j int) bool {
			return dpa[i].Index.Before(dpa[j].Index)
		})
	} else {
		sort.Slice(dpa, func(i, j int) bool {
			return dpa[i].Columns[by[0]] < dpa[j].Columns[by[0]]
		})
	}
	return dpa.ConvertToTimeSeries()
}

//Resample converts source timeseries interval into different interval using criteria provided
func (ts TimeSeries) Resample(interval string, criteriaMap ...map[string]string) (TimeSeries, error) {
	targetDuration, err := parseInterval(interval) //convert string interval to duration
	sourceDuration := ts.Index[1].Sub(ts.Index[0])
	var applyMap map[string](func([]float64) float64)
	Resampledts := NewTimeSeries()
	if criteriaMap == nil {
		applyMap, err = functionMapper(nil)
	} else {
		applyMap, err = functionMapper(criteriaMap[0])
	}
	if err != nil {
		log.Warnln(err)
	}
	if targetDuration < sourceDuration {
		log.Fatalln("Resample failed: cannot Resample to lower duration %")
	}
	var batchHeadIndex, batchTailIndex int
	for batchTailIndex <= len(ts.Index)-1 {
		startTime := ts.Index[batchHeadIndex]
		endTime := startTime.Add(targetDuration)
		if ts.Index[batchTailIndex].Before(endTime) == true {
			batchTailIndex++
		} else {
			if batchTailIndex == batchHeadIndex {
				batchTailIndex++
				batchHeadIndex++
			} else {
				Resampledts.Index = append(Resampledts.Index, ts.Index[batchHeadIndex])
				for k, v := range ts.Columns {
					Resampledts.Columns[k] = append(Resampledts.Columns[k], applyMap[k](v[batchHeadIndex:batchTailIndex]))
				}
				batchHeadIndex = batchTailIndex
			}
		}
	}
	Resampledts.Index = append(Resampledts.Index, ts.Index[batchHeadIndex])
	for k, v := range ts.Columns {
		Resampledts.Columns[k] = append(Resampledts.Columns[k], applyMap[k](v[batchHeadIndex:batchTailIndex]))
	}
	return Resampledts, err
}

//Split separates by interval. for ex:-Split("1day") would yield an array of `TimeSeries` at day level
func (ts TimeSeries) Split(interval string) []TimeSeries {
	var splitList []TimeSeries
	duration, _ := parseInterval(interval)
	batchHeadIndex := 0
	batchTailIndex := 0
	for batchTailIndex < len(ts.Index) {
		startTime := ts.Index[batchHeadIndex]
		endTime := startTime.Add(duration)
		if ts.Index[batchTailIndex].Before(endTime) == true {
			batchTailIndex++
		} else {
			if batchTailIndex == batchHeadIndex {
				batchTailIndex++
				batchHeadIndex++
			} else {
				splitTs := NewTimeSeries()
				splitTs.Index = append(splitTs.Index, ts.Index[batchHeadIndex:batchTailIndex]...)
				for k, v := range ts.Columns {
					splitTs.Columns[k] = append(splitTs.Columns[k], v[batchHeadIndex:batchTailIndex]...)
				}
				splitList = append(splitList, splitTs)
				batchHeadIndex = batchTailIndex
			}
		}
	}
	return splitList
}

//SplitByBatchSize splits `TimeSeries` into array of `TimeSeries` where each has `batchsize` elements
func (ts TimeSeries) SplitByBatchSize(batchsize int) []TimeSeries {
	var splitList []TimeSeries
	for k := 0; k < ts.Length(); k = k + batchsize {
		splitTs := NewTimeSeries()
		upperbound := int(math.Min(float64(k+batchsize), float64(ts.Length())))
		splitTs.Index = ts.Index[k:upperbound]
		for col := range ts.Columns {
			splitTs.Columns[col] = ts.Columns[col][k:upperbound]
		}
		splitList = append(splitList, splitTs)
	}
	return splitList
}

//SplitByDay splits `TimeSeries` into array of `TimeSeries` where each element contains data for a single day
func (ts TimeSeries) SplitByDay() []TimeSeries {
	var splitList []TimeSeries
	startIndex := 0
	splitTs := NewTimeSeries()
	for k := 1; ; k++ {
		if k == len(ts.Index)-1 {
			splitTs = NewTimeSeries()
			splitTs.Index = ts.Index[startIndex : k+1]
			for col := range ts.Columns {
				splitTs.Columns[col] = ts.Columns[col][startIndex : k+1]
			}
			splitList = append(splitList, splitTs)
			break
		}
		if ts.Index[k-1].Day() != ts.Index[k].Day() || k == ts.Length()-1 {
			splitTs = NewTimeSeries()
			splitTs.Index = ts.Index[startIndex:k]
			for col := range ts.Columns {
				splitTs.Columns[col] = ts.Columns[col][startIndex:k]
			}
			startIndex = k
			splitList = append(splitList, splitTs)
		}

	}
	return splitList
}

//Slice can slice either by integer or time.Time
func (ts TimeSeries) Slice(i1 interface{}, i2 ...interface{}) (TimeSeries, error) {
	var lowerIndex, upperIndex int
	var lower, upper interface{}
	lower = i1
	if i2 == nil {
		upper = int(-1)
	} else {
		upper = i2[0]
	}

	switch lower.(type) {
	case int:
		lowerIndex = lower.(int)
	case time.Time:
		for i, v := range ts.Index {
			if v.After(lower.(time.Time)) {
				lowerIndex = i
				break
			}
		}
	case string:
		date, err := parseDate(lower.(string))
		if err != nil {
			return ts, err
		}
		for i, v := range ts.Index {
			if v.After(date) {
				lowerIndex = i
				break
			}
		}
	default:
		log.Errorf("invalid type for lower bound while slicing `TimeSeries` `%v`", lower)
		return NewTimeSeries(), fmt.Errorf("invalid type for lower bound while slicing `TimeSeries` `%v`", lower)
	}

	switch upper.(type) {
	case int:
		upperIndex = upper.(int)
	case time.Time:
		for i, v := range ts.Index {
			if v.After(upper.(time.Time)) {
				upperIndex = i
				break
			}
		}
		if upperIndex == 0 {
			upperIndex = len(ts.Index) - 1
		}
	case string:
		date, err := parseDate(upper.(string))
		if err != nil {
			log.Error(err)
			return ts, err
		}
		for i, v := range ts.Index {
			if v.After(date) {
				upperIndex = i
				break
			}
		}
	default:
		log.Errorf("invalid type for upper bound while slicing TimeSeries: `%v`", upper)
		return NewTimeSeries(), fmt.Errorf("invalid type for upper bound while slicing TimeSeries: `%v`", upper)
	}

	if lowerIndex > upperIndex {
		upperIndex, lowerIndex = lowerIndex, upperIndex
	}

	if lowerIndex < 0 {
		lowerIndex = len(ts.Index) + lowerIndex
	}
	if upperIndex < 0 {
		upperIndex = len(ts.Index) + upperIndex + 1
	}

	SlicedTimeSeries := NewTimeSeries()
	SlicedTimeSeries.Index = ts.Index[lowerIndex:upperIndex]
	for k, v := range ts.Columns {
		SlicedTimeSeries.Columns[k] = v[lowerIndex:upperIndex]
	}
	return SlicedTimeSeries, nil
}

//Append 2 timeseries together
func (ts TimeSeries) Append(ts1 TimeSeries) (TimeSeries, error) {
	if ts.IsEmpty() {
		return ts1, nil
	}
	ts.Index = append(ts.Index, ts1.Index...)
	if ts.Start().After(ts1.Start()) {
		log.Errorf("Append failed: ts2 is before ts1")
		return ts, fmt.Errorf("Append failed: ts2 is before ts1")
	}
	for col := range ts.Columns {
		ok := false
		for col1 := range ts1.Columns {
			if col == col1 {
				ok = true
				ts.Columns[col] = append(ts.Columns[col], ts1.Columns[col]...)
			}
		}
		if !ok {
			return ts, fmt.Errorf("Append failed: column `%s` in ts1 but not in ts2", col)
		}
	}
	return ts, ts.Validate()
}

//AppendDataPoint to timeseries at end
func (ts TimeSeries) AppendDataPoint(dp DataPoint) (TimeSeries, error) {
	ts.Index = append(ts.Index, dp.Index)
	cols := ts.ListColumns()
	for k := range dp.Columns {
		if !aInB(k, cols) {
			return ts, fmt.Errorf("failed to append datapoint to timeseries: field mismatch %v", k)
		}
	}
	for k, v := range dp.Columns {
		ts.Columns[k] = append(ts.Columns[k], v)

	}
	return ts, ts.Validate()
}

//Map a function to the columns provided
func (ts TimeSeries) Map(fn func(float64) float64, columns ...string) TimeSeries {
	if columns == nil {
		columns = ts.ListColumns()
	}
	empty := NewTimeSeries()
	for _, col := range columns {
		for k, v := range ts.Columns[col] {
			empty.Columns[col][k] = fn(v)
		}
	}
	return empty
}

//Filter using a truth function on columns provided
func (ts TimeSeries) Filter(fn func(float64) bool, columns ...string) TimeSeries {
	if columns == nil {
		columns = ts.ListColumns()
	}
	empty := NewTimeSeries()
	for i := range ts.Index {
		result := true
		for _, column := range columns {
			if fn(ts.Columns[column][i]) == false {
				result = false
				break
			}
		}
		if result == false {
			continue
		}
		empty.Index = append(empty.Index, ts.Index[i])
		for k := range ts.Columns {
			empty.Columns[k] = append(empty.Columns[k], ts.Columns[k][i])
		}
	}
	return empty
}

//Reduce applies a function continuously on a row to return a single value
func (ts TimeSeries) Reduce(fn func(float64, float64) float64, column string) float64 {
	reduced := ts.Columns[column][0]
	for i := range ts.Columns[column] {
		if i != 0 {
			reduced = fn(reduced, ts.Columns[column][i])
		}
	}
	return reduced
}

//FilterByTruthTable returns only those samples in the `TimeSeries` that match `truthArray`==matchingBool at corresponding index
func (ts TimeSeries) FilterByTruthTable(truthArray []bool, matchingBool bool) (TimeSeries, []int) {
	matchedTs := NewTimeSeries()
	matchingIndices := make([]int, 0)
	if ts.Length() != len(truthArray) {
		fmt.Println("cannot match truth table, unequal sizes! ", ts.Length(), len(truthArray))
		return matchedTs, matchingIndices
	}
	for k, v := range truthArray {
		if v == matchingBool {
			matchingIndices = append(matchingIndices, k)
			matchedTs.Index = append(matchedTs.Index, ts.Index[k])
			for key := range ts.Columns {
				matchedTs.Columns[key] = append(matchedTs.Columns[key], ts.Columns[key][k])
			}
		}
	}
	return matchedTs, matchingIndices
}

// func (ts TimeSeries) Reduce(fn func(float64, float64) float64, columns ...string) {

// }

//Print prints nicely, level indicates how many columns up/down to print
//default 5
func (ts TimeSeries) Print(level ...int) {
	var printLevel int
	if level != nil {
		printLevel = level[0]
	} else {
		printLevel = 5
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)
	titles := table.Row{"timestamp"}
	for k := range ts.Columns {
		titles = append(titles, k)
	}
	t.AppendHeader(titles)

	if len(ts.Index) <= printLevel*2 {
		var allRows []table.Row
		for k := range ts.Index {
			var row = table.Row{ts.Index[k]}
			for _, colname := range titles {
				if colname != "timestamp" {
					row = append(row, ts.Columns[colname.(string)][k])
				}
			}
			allRows = append(allRows, row)

		}
		t.AppendSeparator()
		t.AppendRows(allRows)
		t.Render()
		return
	}
	printUp, _ := ts.Slice(0, printLevel)
	printDown, _ := ts.Slice(-printLevel-1, -1)
	var allRows []table.Row
	for k := range printUp.Index {
		var row = table.Row{printUp.Index[k]}
		for _, colname := range titles {
			if colname != "timestamp" {
				row = append(row, printUp.Columns[colname.(string)][k])
			}
		}
		allRows = append(allRows, row)
	}

	allRows = append(allRows, table.Row{"..."})
	allRows = append(allRows, table.Row{"..."})
	allRows = append(allRows, table.Row{"..."})

	for k := range printDown.Index {
		var row = table.Row{printDown.Index[k]}
		for _, colname := range titles {
			if colname != "timestamp" {
				row = append(row, printDown.Columns[colname.(string)][k])
			}
		}
		allRows = append(allRows, row)
	}
	t.AppendRows(allRows)
	t.AppendSeparator()
	t.Render()
	return
}

//Print a datapoint pretty pretty
func (dp DataPoint) Print() {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)
	titles := table.Row{"timestamp"}
	for k := range dp.Columns {
		titles = append(titles, k)
	}
	t.AppendHeader(titles)
	row := table.Row{}
	row = append(row, dp.Index)
	for _, col := range titles {
		if col.(string) != "timestamp" {
			row = append(row, dp.Columns[col.(string)])
		}
	}
	t.AppendRow(row)
	t.Render()
}
