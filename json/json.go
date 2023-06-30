package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	//"github.com/go-gota/gota/dataframe"
)

var file_path = "./."
var json_path = "./metadata/."

type Stats struct {
	Min                int      `json:"min"`
	Max                int      `json:"max"`
	Mean               int      `json:"mean"`
	Std                int      `json:"std"`
	NullValueCounts    int      `json:"nullValueCounts"`
	PresentValueCounts int      `json:"present_value_counts"`
	UniqueValueCounts  int      `json:"uniqueValueCounts"`
	Sample_value       []string `json:"sample_value"`
	NullProportion     int      `json:"nullProportion"`
	UniqueProportion   int      `json:"uniqueProportion"`
}

type Constraints struct {
	Required string `json:"required"`
	Unique   string `json:"unique"`
}

type Fields struct {
	Name        string      `json:"name"`
	Types       string      `json:"types"`
	Format      string      `json:"format"`
	Description string      `json:"description"`
	Constraints Constraints `json:"constraints"`
	Stats       Stats       `json:"stats"`
}

type Schema struct {
	Fields []Fields `json:"fields"`
}

type Resources []struct {
	Profile     string `json:"profile"`
	Name        string `json:"name"`
	Path        string `json:"path"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Format      string `json:"format"`
	Mediatype   string `json:"mediatype"`
	Encoding    string `json:"encoding"`
	Bytes       string `json:"bytes"`
	Hash        string `json:"hash"`
	Schema      Schema `json:"schema"`
	Dialect     struct {
		CaseSensitiveHeader string `json:"caseSensitiveHeader"`
		Delimiter           string `json:"delimiter"`
		DoubleQuote         string `json:"doubleQuote"`
		Header              string `json:"header"`
		LineTerminator      string `json:"lineTerminator"`
		QuoteChar           string `json:"quoteChar"`
		SkipInitialSpace    string `json:"skipInitialSpace"`
		RowsCount           int    `json:"rowsCount"`
		ColumnsCount        int    `json:"columnsCount"`
	} `json:"dialect"`
	Version string `json:"version"`
}

type frictionless_struct struct {
	Profile     string    `json:"profile"`
	Name        string    `json:"name"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Resources   Resources `json:"resources"`
}

var frictionless_schema = `{
	"profile": "tabular-data-package",
	"name": "Experiment Name",
	"title": "Title of Experiment",
	"description": "Description of Experiment",
	"resources": [
		{
			"profile": "data-resource",
			"name": "",
			"path": "path of dataset",
			"title": "JSON File Data Resource",
			"description": "An example of a JSON file",
			"format": "json",
			"mediatype": "application/json",
			"encoding": "UTF-8",
			"bytes": "2082",
			"hash": "",
			"schema": {
				"fields": [
					{
						"name": "column",
						"types": "integer",
						"format": "default",
						"description": "column description",
						"constraints": {
							"required": "True",
							"unique": "True"
						},
						"stats": {
							"min": 0,
							"max": 10,
							"mean": 5,
							"std": 6,
							"nullValueCounts": 4,
							"present_value_counts": 6,
							"uniqueValueCounts": 3
						}
					}
				]
			},
			"dialect": {
				"caseSensitiveHeader": "false",
				"delimiter": ",",
				"doubleQuote": "true",
				"header": "true",
				"lineTerminator": "\r\n",
				"quoteChar": "",
				"skipInitialSpace": "true",
				"rowsCount": 0,
				"columnsCount": 0
			},
			"version": "1.0.0"
		}
	]
}`

func main() {
	var data_file_path []string

	err := filepath.Walk(file_path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return err
		}
		data_file_path = append(data_file_path, path)
		return nil
	})

	if err != nil {
		fmt.Println(err)
	}

	var frictionless_data frictionless_struct
	err1 := json.Unmarshal([]byte(frictionless_schema), &frictionless_data)
	if err1 != nil {
		log.Fatal("error unmarshaling json: ", err)
	}

	for _, v := range data_file_path {
		fi, err := os.Stat(v)
		if err != nil {
			fmt.Println(err)
			return
		}

		Extension := filepath.Ext(v)

		if fi.Mode().IsDir() {
			continue
		} else {
			if Extension == ".json" {
				generate_schema(v, frictionless_data)
				frictionless_data.Resources[0].Path = v
				frictionless_data.Resources[0].Name = filepath.Base(v)
				frictionless_data.Resources[0].Bytes = strconv.Itoa(int(fi.Size()))

				file, _ := json.MarshalIndent(frictionless_data, "", "\t")
				json_file_path := json_path + "/" + strings.TrimSuffix(filepath.Base(v), ".json") + ".json"
				e := ioutil.WriteFile(json_file_path, file, 0644)
				if e != nil {
					print(e)
				}
			}
		}
	}
}

func generate_schema(file_name string, frictionless_data frictionless_struct) {
	jsonFile, err := os.Open(file_name)
	if err != nil {
		log.Fatal(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var data []map[string]interface{}
	err = json.Unmarshal(byteValue, &data)
	if err != nil {
		log.Fatal(err)
	}

	field := []Fields{}
	n_rows := len(data)
	n_cols := 0

	for _, obj := range data {
		if len(obj) > n_cols {
			n_cols = len(obj)
		}
	}

	for key := range data[0] {
		dat_type := getDataType(key, data)

		var newStats Stats
		uniq_count, uniq_list := uniqueValueCount(key, data)

		if dat_type {
			newStats = Stats{
				Min:  getMinValue(key, data),
				Max:  getMaxValue(key, data),
				Mean: getMeanValue(key, data),
				Std:  getStdValue(key, data),
			}
		}

		newStats.NullValueCounts = getNullValueCount(key, data)
		newStats.PresentValueCounts = getPresentValueCount(key, data)
		newStats.UniqueValueCounts = uniq_count
		newStats.NullProportion = getNullProportion(key, data)
		newStats.UniqueProportion = getUniqueProportion(key, data)
		newStats.Sample_value = getsamplevalues(uniq_list)

		dat_map := get_type_mapping(key, data)
		newFields := Fields{
			Name:        key,
			Types:       dat_map,
			Format:      "default",
			Description: key,
			Constraints: Constraints{},
			Stats:       newStats,
		}

		field = append(field, newFields)
	}

	frictionless_data.Resources[0].Schema.Fields = field
	frictionless_data.Resources[0].Dialect.RowsCount = n_rows
	frictionless_data.Resources[0].Dialect.ColumnsCount = n_cols
}

func getDataType(key string, data []map[string]interface{}) bool {
	for _, obj := range data {
		val := obj[key]
		switch val.(type) {
		case int, int8, int16, int32, int64, float32, float64:
			return true
		}
	}
	return false
}

func uniqueValueCount(key string, data []map[string]interface{}) (int, []string) {
	found := false
	var uniquelist []string

	for _, obj := range data {
		value := fmt.Sprintf("%v", obj[key])
		found = false
		for _, k := range uniquelist {
			if value == k {
				found = true
			}
		}

		if !found {
			uniquelist = append(uniquelist, value)
		}
	}

	return len(uniquelist), uniquelist
}

func getMinValue(key string, data []map[string]interface{}) int {
	minValue := math.MaxInt64

	for _, obj := range data {
		value := obj[key]
		switch value.(type) {
		case int, int8, int16, int32, int64:
			if value.(int) < minValue {
				minValue = value.(int)
			}
		case float32, float64:
			if int(value.(float64)) < minValue {
				minValue = int(value.(float64))
			}
		}
	}

	return minValue
}

func getMaxValue(key string, data []map[string]interface{}) int {
	maxValue := math.MinInt64

	for _, obj := range data {
		value := obj[key]
		switch value.(type) {
		case int, int8, int16, int32, int64:
			if value.(int) > maxValue {
				maxValue = value.(int)
			}
		case float32, float64:
			if int(value.(float64)) > maxValue {
				maxValue = int(value.(float64))
			}
		}
	}

	return maxValue
}

func getMeanValue(key string, data []map[string]interface{}) int {
	sum := 0
	count := 0

	for _, obj := range data {
		value := obj[key]
		switch value.(type) {
		case int, int8, int16, int32, int64:
			sum += value.(int)
			count++
		case float32, float64:
			sum += int(value.(float64))
			count++
		}
	}

	if count > 0 {
		return sum / count
	}

	return 0
}

func getStdValue(key string, data []map[string]interface{}) int {
	mean := getMeanValue(key, data)
	sumSquaredDiff := 0
	count := 0

	for _, obj := range data {
		value := obj[key]
		switch value.(type) {
		case int, int8, int16, int32, int64:
			diff := value.(int) - mean
			sumSquaredDiff += diff * diff
			count++
		case float32, float64:
			diff := int(value.(float64)) - mean
			sumSquaredDiff += diff * diff
			count++
		}
	}

	if count > 0 {
		return int(math.Sqrt(float64(sumSquaredDiff / count)))
	}

	return 0
}

func getNullValueCount(key string, data []map[string]interface{}) int {
	count := 0

	for _, obj := range data {
		value := obj[key]
		if value == nil {
			count++
		}
	}

	return count
}

func getPresentValueCount(key string, data []map[string]interface{}) int {
	count := 0

	for _, obj := range data {
		value := obj[key]
		if value != nil {
			count++
		}
	}

	return count
}

func getNullProportion(key string, data []map[string]interface{}) int {
	nullCount := getNullValueCount(key, data)
	presentCount := getPresentValueCount(key, data)

	if presentCount > 0 {
		return int((float64(nullCount) / float64(presentCount)) * 100)
	}

	return 0
}

func getUniqueProportion(key string, data []map[string]interface{}) int {
	uniqueCount, _ := uniqueValueCount(key, data)
	presentCount := getPresentValueCount(key, data)

	if presentCount > 0 {
		return int((float64(uniqueCount) / float64(presentCount)) * 100)
	}

	return 0
}

func get_type_mapping(key string, data []map[string]interface{}) string {
	for _, obj := range data {
		value := obj[key]
		switch value.(type) {
		case string:
			return "string"
		case int, int8, int16, int32, int64:
			return "integer"
		case float32, float64:
			return "number"
		case bool:
			return "boolean"
		}
	}

	return ""
}

func getsamplevalues(sample_list []string) []string {
	var sample []string
	for i, k := range sample_list {
		sample = append(sample, k)
		if i == 2 {
			break
		}
	}
	return sample
}

