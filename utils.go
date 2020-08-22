package timeseries

import (
	"fmt"
	"io/ioutil"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// var regexDate, _ = regexp.Compile("[0-9]{4}-[0-9]{2}-[0-9]{2}")
// var regexTime, _ = regexp.Compile("[0-9]{2}:[0-9]{2}:[0-9]{2}")

//AnyOfAInB checks if any element of a is in b
//utility func
func anyOfAInB(a []string, b []string) bool {
	for _, i := range a {
		for _, j := range b {
			if i == j {
				return true
			}
		}
	}
	return false
}

//AInB checks if string a is in string array b
func aInB(a string, b []string) bool {
	for _, i := range b {
		if i == a {
			return true
		}
	}
	return false
}

//IsInt returns 1 if string is an integer
func isInt(num string) bool {
	_, err := strconv.ParseInt(num, 10, 0)
	if err != nil {
		return false
	}
	return true
}

//IsFloat returns 1 if string is a float
func isFloat(num string) bool {
	_, err := strconv.ParseFloat(num, 32)
	if err != nil {
		return false
	}
	return true
}

//ParseDate parses datetime
//Rules: dates must be delimited by "-"
//times with : RFC3339
//Will not parse timezones
func parseDate(datetime string) (time.Time, error) {
	var d, t string
	datetime = strings.Split(strings.Replace(datetime, "T", " ", 1), "+")[0]
	datetimeSplit := strings.Split(datetime, " ")[:2]
	if len(datetimeSplit) != 1 && len(datetimeSplit) != 2 {
		return time.Time{}, fmt.Errorf("could not find time OR date in provided string %v", datetime)
	} else if len(datetimeSplit) == 1 {
		if strings.Contains(datetimeSplit[0], ":") {
			t = datetimeSplit[0]
		} else {
			d = datetimeSplit[0]
		}
	} else if len(datetimeSplit) == 2 {
		d = datetimeSplit[0]
		t = datetimeSplit[1]
	}
	// d := regexDate.FindString(datetime)
	// t := regexTime.FindString(datetime)
	if d != "" && t != "" {
		return time.Parse("2006-01-02 15:04:05", d+" "+t)
	} else if d != "" && t == "" {
		return time.Parse("2006-01-02", d)
	} else if d == "" && t != "" {
		return time.Parse("15:04:05", t)
	} else {
		return time.Time{}, fmt.Errorf("could not find time OR date in provided string")
	}

}

func parseDateArray(dates []string) ([]time.Time, error) {
	d := make([]time.Time, 0)
	for _, i := range dates {
		parsed, err := parseDate(i)
		if err != nil {
			return []time.Time{}, err
		}
		d = append(d, parsed)
	}
	return d, nil
}

var regexDuration, _ = regexp.Compile("[0-9]+[a-zA-Z]{1}")

//ParseInterval can be minute, hour, day
//If absolute is set, it wont parse or check. Just direct convert.
//Max is 1 week, because month is not rigorously defined.
func parseInterval(interval string, absolute ...bool) (time.Duration, error) {
	if absolute != nil {
		return time.ParseDuration(interval)
	}
	match := regexDuration.FindString(interval)
	if match == "" {
		switch interval {
		case "hour":
			return time.ParseDuration("1h")
		case "minute":
			return time.ParseDuration("1m")
		case "day":
			return time.ParseDuration("24h")
		}
		d, _ := time.ParseDuration("0s")
		return d, fmt.Errorf("parsing interval %v failed.. max interval is hour", interval)
	}
	switch match[len(match)-1] {
	case 'h':
		if len(match) == 2 {
			match = "1h"
		}
	case 'd':
		hours, _ := strconv.Atoi(match[:len(match)-1])
		match = strconv.Itoa(hours*24) + "h"
	case 'w':
		weeks, _ := strconv.Atoi(match[:len(match)-1])
		match = strconv.Itoa(weeks*24*7) + "h"
	}
	return time.ParseDuration(match)
}

//ExtractTimeFromDatetime returns the time in a date
func extractTimeFromDatetime(t time.Time) (time.Time, error) {
	return time.Parse("15:04:05", strings.Split(t.String(), " ")[1])
}

//ListFilesInDir returns a list of the files in dir. Non absolute.
func listFilesInDir(dirname string) []string {
	iter, _ := ioutil.ReadDir(dirname)
	files := make([]string, 0)
	for _, f := range iter {
		files = append(files, f.Name())
	}
	return files
}

//FunctionMapper maps a string:string map to a string:func map.
//the func here is a numerical reduce function which takes a float64 array and returns a single value
//available functions: min, max, sd, first, last, sum, mean ::: which must be passed as string keys
func functionMapper(criteria map[string]string) (map[string]func(arr []float64) float64, error) {
	applyMap := make(map[string]func([]float64) float64)
	if criteria == nil {
		criteria = map[string]string{
			"open":   "first",
			"high":   "max",
			"low":    "min",
			"close":  "last",
			"volume": "sum",
		}
	}

	for k, v := range criteria {
		switch v {
		case "first":
			applyMap[k] = func(arr []float64) float64 {
				return arr[0]
			}
		case "last":
			applyMap[k] = func(arr []float64) float64 {
				return arr[len(arr)-1]
			}
		case "sum":
			applyMap[k] = func(arr []float64) float64 {
				s := 0.0
				for _, value := range arr {
					s = s + value
				}
				return s
			}
		case "max":
			applyMap[k] = func(arr []float64) float64 {
				maximum := arr[0]
				for _, value := range arr {
					if value > maximum {
						maximum = value
					}
				}
				return maximum
			}
		case "min":
			applyMap[k] = func(arr []float64) float64 {
				minimum := arr[0]
				for _, value := range arr {
					if value < minimum {
						minimum = value
					}
				}
				return minimum
			}
		case "mean":
			applyMap[k] = func(arr []float64) float64 {
				sum := 0.0
				for _, value := range arr {
					sum += value
				}
				return sum / float64(len(arr))
			}
		case "sd": //standard deviation
			applyMap[k] = func(arr []float64) float64 {
				sum := 0.0
				for _, value := range arr {
					sum += value
				}
				mean := sum / float64(len(arr))
				stddev := 0.0
				for _, value := range arr {
					stddev += (value - mean)
				}
				stddev = math.Sqrt(stddev / float64(len(arr)))
				return stddev
			}
		default:
			return nil, fmt.Errorf("could not resample field %s by %s: no such field or function", k, v)
		}
	}

	return applyMap, nil
}
