package timeseries

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

//TableGetter is timeseries getter.
type TableGetter interface {
	GetIndex() ([]time.Time, error)
	Get(string) ([]float64, error)
	ListColumns() []string //is this necessary?
}

//TableSetter is timeseries setter, numeric columns with time index
type TableSetter interface {
	SetIndex([]time.Time) error
	Set(string, []float64) error
}

//yahoo for data from yahoo finance
type yahoo struct {
	Date   []string  `json:"Date" csv:"Date"`
	Open   []float64 `json:"Open" csv:"Open"`
	High   []float64 `json:"High" csv:"High"`
	Low    []float64 `json:"Low" csv:"Low"`
	Close  []float64 `json:"Close" csv:"Close"`
	Volume []float64 `json:"Volume" csv:"Volume"`
	OI     []float64 `json:"OI" csv:"OI"`
	IV     []float64 `json:"IV" csv:"IV"`
}

//Generic tohlcv
type generic struct {
	Date   []string  `json:"timestamp" csv:"timestamp"`
	Open   []float64 `json:"open" csv:"pen"`
	High   []float64 `json:"high" csv:"igh"`
	Low    []float64 `json:"low" csv:"low"`
	Close  []float64 `json:"close" csv:"close"`
	Volume []float64 `json:"volume" csv:"volume"`
	OI     []float64 `json:"OI" csv:"OI"`
	IV     []float64 `json:"IV" csv:"IV"`
}

//split for nested column json
type split1 struct {
	Date    []string             `json:"timestamp"`
	Columns map[string][]float64 `json:"columns"`
}

//split0 same as split but different name
type split0 struct {
	Date    []string             `json:"TimeIndex"`
	Columns map[string][]float64 `json:"Columns"`
}

type split struct {
	Date    []string             `json:"index"`
	Columns map[string][]float64 `json:"columns"`
}

//DataPoint holds a single point of data
type DataPoint struct {
	Index   time.Time
	Columns map[string]float64
}

//DataPointArray holds an array of `DataPoint`
type DataPointArray []DataPoint

type changelog struct {
	operation      string
	index          time.Time
	indexFrom      time.Time
	indexTo        time.Time
	commitedToDisk bool
}

//TimeSeries one time Index and a map of Columns. Central datastruct
type TimeSeries struct {
	Index   []time.Time `json:"index"`
	Columns map[string][]float64
	MaxSize int
	Meta    map[string]string
	changes []changelog
}

//NewTimeSeries returns empty `TimeSeries`
func NewTimeSeries() TimeSeries {
	return TimeSeries{make([]time.Time, 0), make(map[string][]float64), 0, make(map[string]string), make([]changelog, 0)}
}

//NewTimeSeriesFromGetter reads a `TableGetter` if no TableGetter provided, then will return empty TimeSeries
func NewTimeSeriesFromGetter(ts ...TableGetter) TimeSeries {
	var err error
	emptyts := TimeSeries{make([]time.Time, 0), make(map[string][]float64), 0, make(map[string]string), make([]changelog, 0)}
	if ts == nil {
		return emptyts
	}
	emptyts.Index, err = ts[0].GetIndex()
	if err != nil {
		log.Warn("failed internal load:", err)
	}
	for _, v := range ts[0].ListColumns() {
		emptyts.Columns[v], err = ts[0].Get(v)
		if err != nil {
			log.Warn("failed internal load:", err)
		}
	}
	return emptyts
}

//NewTimeSeriesFromCSV reads a CSV file, if offset is provided, those many bytes are read from EOF
func NewTimeSeriesFromCSV(filepath string, offset ...int64) (TimeSeries, error) {
	stat, err := os.Stat(filepath)
	if err != nil {
		return TimeSeries{}, err
	}
	logrus.Infof("Total file size: %v KB", stat.Size()/1024)
	if offset == nil || float64(stat.Size()) < math.Abs(float64(offset[0])) {
		logrus.Infof("Reading entire file: %v KB", stat.Size()/1024)
		return NewTimeSeriesFromFile(filepath)
	}
	logrus.Infof("Reading last %v KB...", math.Abs(float64(offset[0]))/1024)
	ts := NewTimeSeries()
	file, _ := os.Open(filepath)

	columnNames, err := csv.NewReader(file).Read()
	if offset[0] > 0 {
		offset[0] = -offset[0]
	}
	file.Seek(offset[0], 2)
	eol := make([]byte, 0) //find end of line
	for !bytes.Contains(eol, []byte("\n")) {
		b := make([]byte, 1)
		file.Read(b)
		eol = append(eol, b...)
	}
	columns, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return TimeSeries{}, err
	}
	var indexCol int
	for index, col := range columnNames {
		columnNames[index] = strings.ToLower(col)
		if strings.Contains(col, "date") || strings.Contains(col, "time") {
			indexCol = index
			break
		}
	}
	for i := range columns {
		for j := range columns[i] {
			if j == indexCol {
				datapoint, err := parseDate(columns[i][j])
				if err != nil {
					logrus.Errorln("date parse failed while loading timeseries from file: ", err)
				}
				ts.Index = append(ts.Index, datapoint)
			} else {
				if columns[i][j] == "" {
					columns[i][j] = "0"
				}
				datapoint, err := strconv.ParseFloat(columns[i][j], 64)
				if err != nil {
					logrus.Errorln("float parse failed while loading timeseries from file at index ", i, j, err)
				}
				ts.Columns[columnNames[j]] = append(ts.Columns[columnNames[j]], datapoint)
			}
		}
	}
	return ts, nil
}

//NewTimeSeriesFromFile reads a json or csv file.
//schema types yahoo, generic
func NewTimeSeriesFromFile(filepath string, sourceSchema ...string) (TimeSeries, error) {
	var schema string
	ts := NewTimeSeries()
	if sourceSchema == nil {
		schema = "split"
	} else {
		schema = sourceSchema[0]
	}
	if filepath[len(filepath)-4:] == ".csv" {
		schema = "auto"
	}
	file, err := ioutil.ReadFile(filepath)
	if err != nil {
		return NewTimeSeries(), err
	}

	switch filepath[len(filepath)-4:] {
	case ".csv":
		f, err := os.Open(filepath)
		defer f.Close()
		if err != nil {
			return ts, err
		}
		csvdata := csv.NewReader(f)
		columnNames, err := csvdata.Read()
		var indexCol int
		for index, col := range columnNames {
			columnNames[index] = strings.ToLower(col)
			if strings.Contains(col, "date") || strings.Contains(col, "time") {
				indexCol = index
				break
			}
		}
		columns, err := csvdata.ReadAll()
		if err != nil {
			return ts, err
		}
		for i := range columns {
			for j := range columns[i] {
				if j == indexCol {
					datapoint, err := parseDate(columns[i][j])
					if err != nil {
						logrus.Errorln("date parse failed while loading timeseries from file: ", err)
					}
					ts.Index = append(ts.Index, datapoint)
				} else {
					if columns[i][j] == "" {
						columns[i][j] = "0"
					}
					datapoint, err := strconv.ParseFloat(columns[i][j], 64)
					if err != nil {
						logrus.Errorln("float parse failed while loading timeseries from file at index ", i, j, err)
					}
					ts.Columns[columnNames[j]] = append(ts.Columns[columnNames[j]], datapoint)
				}
			}
		}

	case "json":
		if schema == "yahoo" {
			var data yahoo
			json.Unmarshal(file, &data)
			index, err := parseDateArray(data.Date)
			if err != nil {
				logrus.Errorln("date parse failed while loading timeseries from file: ", err)
			}
			ts.Index = index
			ts.Columns["open"] = data.Open
			ts.Columns["high"] = data.High
			ts.Columns["low"] = data.Low
			ts.Columns["close"] = data.Close
			ts.Columns["volume"] = data.Volume
		} else if schema == "generic" {
			var data generic
			json.Unmarshal(file, &data)
			index, err := parseDateArray(data.Date)
			if err != nil {
				logrus.Errorln("date parse failed while loading timeseries from file: ", err)
			}
			ts.Index = index
			ts.Columns["open"] = data.Open
			ts.Columns["high"] = data.High
			ts.Columns["low"] = data.Low
			ts.Columns["close"] = data.Close
			ts.Columns["volume"] = data.Volume

		} else if schema == "split" {
			var data split
			json.Unmarshal(file, &data)
			index, err := parseDateArray(data.Date)
			if err != nil {
				logrus.Errorln("date parse failed while loading timeseries from file: ", err)
			}
			ts.Index = index
			ts.Columns = data.Columns
		} else if schema == "split0" {
			var data split0
			json.Unmarshal(file, &data)
			index, err := parseDateArray(data.Date)
			if err != nil {
				logrus.Errorln("date parse failed while loading timeseries from file: ", err)
			}
			ts.Index = index
			ts.Columns = data.Columns
		} else if schema == "split1" {
			var data split1
			json.Unmarshal(file, &data)
			index, err := parseDateArray(data.Date)
			if err != nil {
				logrus.Errorln("date parse failed while loading timeseries from file: ", err)
			}
			ts.Index = index
			ts.Columns = data.Columns
		}
	}
	if ts.Length() == 0 {
		logrus.Errorln("load failed, probably wrong schema provided")
		return ts, fmt.Errorf("load failed, probably wrong schema provided")
	}
	ts.changes = append(ts.changes, changelog{"load", ts.End(), ts.Start(), ts.End(), true})
	// for col := range ts.Columns {
	// 	allzero := true
	// 	for i := 0; i < len(ts.Columns[col]); i++ {
	// 		if ts.Columns[col][i] != 0 {
	// 			allzero = false
	// 		}
	// 	}
	// 	if allzero {
	// 		continue
	// 	}
	// 	for i := 0; i < len(ts.Columns[col])-2; {
	// 		if ts.Columns[col][i+1] == 0 && col != "volume" {
	// 			substitute := ts.Columns[col][i]
	// 			for ts.Columns[col][i+1] == 0 {
	// 				ts.Columns[col][i+1] = substitute
	// 				i++
	// 			}
	// 		}
	// 		i++
	// 	}
	//}
	return ts, nil
}

//NewTimeSeriesFromDirectory reads entire directory
func NewTimeSeriesFromDirectory(directory string, sourceSchema ...string) (TimeSeries, error) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return NewTimeSeries(), err
	}
	var schema string
	if sourceSchema == nil {
		schema = "split"
	} else {
		schema = sourceSchema[0]
	}
	ts := NewTimeSeries()
	for _, f := range files {
		fullpath := filepath.Join(directory, f.Name())
		if fullpath[len(fullpath)-4:] != "json" && fullpath[len(fullpath)-3:] != "csv" {
			continue
		}
		presentRead, err := NewTimeSeriesFromFile(fullpath, schema)
		if err != nil {
			return NewTimeSeries(), err
		}
		ts, err = ts.Append(presentRead)
		if err != nil {
			return NewTimeSeries(), err
		}
	}
	ts.changes = append(ts.changes, changelog{"load", ts.End(), ts.Start(), ts.End(), true})
	return ts, nil
}

//NewTimeSeriesFromData loads a timeseries from data given
//timeindex can be a string array or
func NewTimeSeriesFromData(timeindex interface{}, columns map[string][]float64) (TimeSeries, error) {
	ts := NewTimeSeries()
	var err error
	switch timeindex.(type) {
	case []time.Time:
		ts.Index = timeindex.([]time.Time)
	case []string:
		ts.Index, err = parseDateArray(timeindex.([]string))
		if err != nil {
			return ts, err
		}
	default:
		return ts, fmt.Errorf("invalid timeindex type")
	}
	for name, data := range columns {
		ts.Columns[name] = data
	}
	ts.changes = append(ts.changes, changelog{"load", ts.End(), ts.Start(), ts.End(), true})
	return ts, nil
}

//NewDataPoint creates new datapoint
func NewDataPoint() DataPoint {
	return DataPoint{time.Time{}, make(map[string]float64, 0)}
}

//NewDataPointFromData uses arguments to build a datapoint
func NewDataPointFromData(timeindex interface{}, columns map[string]float64) DataPoint {
	dp := NewDataPoint()
	switch timeindex.(type) {
	case time.Time:
		dp.Index = timeindex.(time.Time)
	case string:
		t, err := parseDate(timeindex.(string))
		if err != nil {
			log.Warn("datapoint creation warning: could not parse time ", err)
		}
		dp.Index = t
	default:
		log.Warnf("datapoint creation warning: invalid type for timeindex `%T`\n", timeindex)
	}
	for k, v := range columns {
		dp.Columns[k] = v
	}
	return dp
}
