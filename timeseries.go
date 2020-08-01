package timeseries

import (
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/jedib0t/go-pretty/table"
	log "github.com/sirupsen/logrus"
)

//WriteToSetter writes a TimeSeries to any other type with a Set and SetIndex method
func (ts TimeSeries) WriteToSetter(dst TableSetter) TableSetter {
	dst.SetIndex(ts.Index)
	for k, v := range ts.Columns {
		dst.Set(k, v)
	}
	return dst
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

//Validate equal lengths of all columns
func (ts TimeSeries) Validate() error {
	for k := range ts.Columns {
		if len(ts.Columns[k]) != len(ts.Index) {
			log.Fatalln("validation failed: TimeSeries column lengths do not match! cannot recover")
		}
	}
	for k := range ts.Index {
		if k != 0 {
			if ts.Index[k].Before(ts.Index[k-1]) {
				log.Warnln("validation warning: unsorted time Index")
			}
			if ts.Index[k].Equal(ts.Index[k-1]) {
				log.Warnf("validation warning: duplicate Index keys found for %s\n", ts.Index[k].String())
			}
		}
	}
	return nil
}

//Sort by index
func (ts TimeSeries) Sort() TimeSeries {
	sort.Slice(ts.Index, func(i, j int) bool {
		if ts.Index[j].Before(ts.Index[i]) {
			for _, v := range ts.Columns {
				v[i], v[j] = v[j], v[i]
			}
		}
		return ts.Index[j].Before(ts.Index[i])
	})
	return ts
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
func (ts TimeSeries) Split(interval string) ([]TimeSeries, error) {
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
	return splitList, nil
}

//SplitByBatchSize splits `TimeSeries` into array of `TimeSeries` where each has `batchsize` elements
func (ts TimeSeries) SplitByBatchSize(batchsize int) ([]TimeSeries, error) {
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
	return splitList, nil
}

//SplitByDay splits `TimeSeries` into array of `TimeSeries` where each element contains data for a single day
func (ts TimeSeries) SplitByDay() ([]TimeSeries, error) {
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
	return splitList, nil
}

//Slice can slice either by integer or time.Time
func (ts TimeSeries) Slice(lower, upper interface{}) (TimeSeries, error) {
	var lowerIndex, upperIndex int
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
		if upperIndex == 0 {
			upperIndex = len(ts.Index) - 1
		}
	default:
		log.Errorf("invalid type for upper bound while slicing TimeSeries: `%v`", upper)
		return NewTimeSeries(), fmt.Errorf("invalid type for upper bound while slicing TimeSeries: `%v`", upper)
	}

	if lowerIndex < 0 {
		lowerIndex = len(ts.Index) + lowerIndex + 1
	}
	if upperIndex < 0 {
		upperIndex = len(ts.Index) + upperIndex + 1
	}

	if lowerIndex > upperIndex {
		swap := upperIndex
		upperIndex = lowerIndex
		lowerIndex = swap
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
	printDown, _ := ts.Slice(-1-printLevel, -1)
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
