package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"github.com/go-gota/gota/dataframe"
	"bufio"
	"io"
)

type DatabaseCredentials struct {
	SourceDirectory string `json:"sourceDirectory"`
}

var json_path = "./output"

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

type FileInfo struct {
	Path       string
	Size       int64
	ModTime    time.Time
	IsCSV      bool
	IsTSV      bool
	IsDelimiterEmpty bool
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
            "title": "CSV File Data Resource",
            "description": "An example of a CSV file",
            "format": "csv",
            "mediatype": "text/csv",
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

type Config struct {
	PluginType          string `json:"plugin_type"`
	SourceDirectory     string `json:"source_directory"`
	
}

func csv_plugin(config DatabaseCredentials) {
	
	

	// Count CSV files in the source directory and its subdirectories
	totalCount, tsvCount, tsvFiles, emptyFiles, err := countCSVFiles(config.SourceDirectory, true)
	if err != nil {
		fmt.Println("Error counting CSV files:", err)
		return
	}

	fmt.Println("Total CSV files found (including subdirectories):", totalCount)
	fmt.Println("Total TSV files found (including subdirectories):", tsvCount)
	fmt.Println("TSV files found (including subdirectories):", tsvFiles)
	fmt.Println("Empty CSV files found (including subdirectories):", (len(emptyFiles)-(totalCount+tsvCount)))
	fmt.Println("Total files found (including subdirectories):", len(emptyFiles))

	file_path := config.SourceDirectory
	var data_file_path []string
	err = filepath.Walk(file_path, func(path string, info os.FileInfo, err error) error {
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
	// ***************************************************

	var frictionless_data frictionless_struct
	err1 := json.Unmarshal([]byte(frictionless_schema), &frictionless_data)
	if err1 != nil {
		log.Fatal("error unmarshaling json: ", err)
	}
	// log.Printf("frictionless_data: %+v", frictionless_data.Resources)

	// ***************************************************
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
			if Extension == ".csv" {

				generate_schema(v, frictionless_data)
				frictionless_data.Resources[0].Path = v
				frictionless_data.Resources[0].Name = filepath.Base(v)
				frictionless_data.Resources[0].Bytes = strconv.Itoa(int(fi.Size()))
				// fmt.Printf("***************%+v\n", frictionless_data)
				file, _ := json.MarshalIndent(frictionless_data, "", "\t")
				json_file_path := json_path + "/" + strings.TrimSuffix(filepath.Base(v), ".csv") + ".json"
				// fmt.Println(json_file_path)
				e := ioutil.WriteFile(json_file_path, file, 0644)
				if e != nil {
					print(e)
				}

			}
		}

	}

}

func generate_schema(file_name string, frictionless_data frictionless_struct) {
	csvfile, err := os.Open(file_name)
	if err != nil {
		log.Fatal(err)

	}
	df := dataframe.ReadCSV(csvfile)
	df_col := []string(df.Names())
	n_rows, n_cols := df.Dims()
	// fmt.Println(n_cols, n_rows)
	field := []Fields{}
	// fmt.Println(df.Types())
	// count := 0

	for _, col := range df_col {
		dat_type := is_numeric_type(col, df)
		// fmt.Println("dat_type of column", dat_type)
		var newStats Stats
		uniq_count, uniq_list := uniqueValueCount(col, df)
		// fmt.Println("count", col, uniq_count)
		if dat_type {

			newStats = Stats{
				Min:  int(df.Col(col).Min()),
				Max:  int(df.Col(col).Max()),
				Mean: int(math.Round(df.Col(col).Mean())),
				Std:  int(math.Round(df.Col(col).StdDev())),
			}
		}
		newStats.NullValueCounts = len(df.Col(col).IsNaN())
		newStats.PresentValueCounts = len(df.Col(col).Records())
		newStats.UniqueValueCounts = uniq_count
		newStats.NullProportion = (len(df.Col(col).IsNaN()) / len(df.Col(col).Records()))
		newStats.UniqueProportion = (uniq_count / len(df.Col(col).Records()))
		newStats.Sample_value = getsamplevalues(uniq_list)
		dat_map := get_type_mapping(col, df)
		// fmt.Println(dat_map)
		newFields := Fields{
			Name:        col,
			Types:       dat_map,
			Format:      "default",
			Description: col,
			Constraints: Constraints{},
			Stats:       newStats,
		}

		field = append(field, newFields)

	}
	frictionless_data.Resources[0].Schema.Fields = field
	frictionless_data.Resources[0].Dialect.RowsCount = n_rows
	frictionless_data.Resources[0].Dialect.ColumnsCount = n_cols
}

func is_numeric_type(col string, df dataframe.DataFrame) bool {

	dat_type := df.Col(col).Type()
	data_types := []string{"int", "float", "int64", "int8", "float64"}
	for _, i := range data_types {
		if i == string(dat_type) {
			return true
		}
	}
	return false
}

func uniqueValueCount(col string, df dataframe.DataFrame) (int, []string) {
	found := false
	var uniquelist []string
	records := (df.Col(col).Records())
	// fmt.Println("\nrecords", len(records))
	for _, i := range records {
		found = false
		for _, k := range uniquelist {
			if i == k {
				// fmt.Println(i, k)
				found = true
			}
		}
		if !found {
			uniquelist = append(uniquelist, i)
		}
	}
	// fmt.Println("\ncount****", col, len(uniquelist))
	return len(uniquelist), uniquelist
}

// func (s *SeriesGeneric) NilCount(opts ...NilCountOptions) (int, error)
func getsamplevalues(uniq_list []string) []string {
	var sample_value []string
	if len(uniq_list) > 0 {
		if len(uniq_list) > 2 {
			sample_value = append(sample_value, uniq_list[0])
			sample_value = append(sample_value, uniq_list[1])
		} else {
			sample_value = append(sample_value, uniq_list[0])
		}
	}
	return sample_value
}

func get_type_mapping(col string, df dataframe.DataFrame) string {
	var value_dat string
	type_mappings := map[string]string{"int64": "integer", "int32": "integer", "float64": "integer", "float32": "integer",
		"float": "integer", "string": "string", "object": "string", "datetime64": "date", "category": "string", "uint32": "integer", "uint64": "integer"}
	dat_type := df.Col(col).Type()
	for key, value := range type_mappings {
		if key == string(dat_type) {
			value_dat = value
		}
	}
	return value_dat

}


// countCSVFiles counts the number of CSV files in a directory and its subdirectories
func countCSVFiles1(dirPath string, includeSubdirs bool) (int, error) {
	fileCount := 0

	// Traverse the directory recursively
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check if it is a regular file with a .csv extension
		if info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ".csv") {
			fileCount++
			// Get file details
			fileSize := info.Size()
			fileModTime := info.ModTime()

			// Print file details
			fmt.Printf("File: %s\n", path)
			fmt.Printf("Size: %d bytes\n", fileSize)
			fmt.Printf("Last Modified: %s\n", fileModTime.Format(time.RFC3339))
			fmt.Println("-----------------------------")
		}

		if !includeSubdirs && info.IsDir() && path != dirPath {
			return filepath.SkipDir // Skip subdirectories
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return fileCount, nil
}

// readJSONConfig reads and parses the JSON configuration file
func readJSONConfig(filePath string) (Config, error) {
	var config Config

	// Read the JSON file
	file, err := os.Open(filePath)
	if err != nil {
		return config, err
	}
	defer file.Close()

	// Parse the JSON data
	byteValue, err := ioutil.ReadAll(file)
	if err != nil {
		return config, err
	}

	// Unmarshal JSON into the config struct
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		return config, err
	}

	return config, nil
}


func countCSVFiles(dirPath string, includeSubdirs bool) (int, int, []string, []FileInfo, error) {
	csvFileCount := 0
	tsvFileCount := 0
	tsvFiles := []string{}
	filesInfo := []FileInfo{}

	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.Mode().IsRegular() && strings.HasSuffix(info.Name(), ".csv") {
			// Get the delimiter and append the path to the TSV files array if the delimiter is a tab
			delimiter, err := getCSVFileDelimiter(path)
			if err != nil {
				// Handle empty delimiter files
				fmt.Printf("Empty file: %s\n", path)
				fmt.Println("-----------------------------")
				filesInfo = append(filesInfo, FileInfo{
					Path:       path,
					Size:       info.Size(),
					ModTime:    info.ModTime(),
					IsCSV:      true,
					IsTSV:      false,
					IsDelimiterEmpty: true,
				})
				return nil
			}

			if delimiter == "\t" {
				tsvFiles = append(tsvFiles, path)
				tsvFileCount++
			}

			csvFileCount++

			filesInfo = append(filesInfo, FileInfo{
				Path:       path,
				Size:       info.Size(),
				ModTime:    info.ModTime(),
				IsCSV:      true,
				IsTSV:      delimiter == "\t",
				IsDelimiterEmpty: false,
			})

			fmt.Printf("File: %s\n", path)
			fmt.Printf("Size: %d bytes\n", info.Size())
			fmt.Printf("Last Modified: %s\n", info.ModTime().Format(time.RFC3339))
			if delimiter == "\t"{
				fmt.Printf("Delimeter: TAB\n")	
			}else if (delimiter == "," || delimiter == "") {
				fmt.Println("Delimeter: COMMA\n")
			}else {
				fmt.Println("Invalid delimeter file")
			}
			// fmt.Printf("Delimeter: %s\n",delimiter)
			fmt.Println("-----------------------------")
		}

		if !includeSubdirs && info.IsDir() && path != dirPath {
			return filepath.SkipDir
		}

		return nil
	})

	if err != nil {
		return 0, 0, nil, nil, err
	}

	return csvFileCount, tsvFileCount, tsvFiles, filesInfo, nil
}

func getCSVFileDelimiter(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	firstLine, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}

	delimiter := detectDelimiter(firstLine)
	if delimiter == "" {
		return "", fmt.Errorf("unable to detect delimiter in file: %s", filePath)
	}

	return delimiter, nil
}

func detectDelimiter(line string) string {
	delimiters := [3]string{",", "\t"}

	for _, delimiter := range delimiters {
		if strings.Contains(line, delimiter) {
			return delimiter
		}
	}

	return ""
}



type MyRPCServer struct{}

func (s *MyRPCServer) GetData(args DatabaseCredentials, reply *DatabaseCredentials) error {
	// Print received data on the console
	fmt.Println("Received data:", args)

	// You can perform additional processing or save the data in the server
	

	*reply = args // Set the reply value
	csv_plugin(args)
	return nil
}

func main() {
	server := rpc.NewServer()
	myRPCServer := &MyRPCServer{}
	server.Register(myRPCServer)

	// Start the server to listen for RPC requests
	listener, err := net.Listen("tcp", ":3400")
	if err != nil {
		log.Fatal("Listen error:", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
		}

		go server.ServeConn(conn)
	}
}

