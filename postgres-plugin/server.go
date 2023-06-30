package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"math"

	_ "github.com/lib/pq"

)

type DatabaseCredentials struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	User            string `json:"user"`
	Password        string `json:"password"`
	DBName          string `json:"dbname"`
	PluginType      string `json:"pluginType"`
	SourceDirectory string `json:"sourceDirectory"`
}

type MyRPCServer struct{}


func ReadCredentialsFromFile(filepath string) (DatabaseCredentials, error) {
	var credentials DatabaseCredentials

	data, err := ioutil.ReadFile(filepath)
	if err != nil {
		return credentials, err
	}

	err = json.Unmarshal(data, &credentials)
	if err != nil {
		return credentials, err
	}

	return credentials, nil
}

type Stats struct {
	Min                int      `json:"min"`
	Max                int      `json:"max"`
	Mean               int      `json:"mean"`
	Std                int      `json:"std"`
	NullValueCounts    int      `json:"nullValueCounts"`
	PresentValueCounts int      `json:"present_value_counts"`
	UniqueValueCounts  int      `json:"uniqueValueCounts"`
	SampleValue        []string `json:"sample_value"`
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

type FrictionlessStruct struct {
	Profile     string    `json:"profile"`
	Name        string    `json:"name"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Resources   Resources `json:"resources"`
}

var (
	jsonPath = "./json"
)

func postgres_plugin(credentials DatabaseCredentials) {
	//credentials, err := ReadCredentialsFromFile("credentials.json")
	//if err != nil {
	//	log.Fatal("Failed to read database credentials:", err)
	//}

	dbHost := credentials.Host
	dbPort := credentials.Port
	dbUser := credentials.User
	dbPassword := credentials.Password
	dbName := credentials.DBName
	// Connect to the PostgreSQL database
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Failed to connect to the database:", err)
	}
	defer db.Close()
	log.Println("Connected to the database successfully.")

	// Retrieve the list of tables from the database
	tables, err := getTables(db)
	if err != nil {
		log.Fatal("Failed to retrieve tables:", err)
	}

	// Unmarshal the predefined JSON schema
	frictionlessData, err := getFrictionlessSchema()
	if err != nil {
		log.Fatal(err)
	}

	// Iterate over the tables and generate metadata for each table
	for _, table := range tables {
		err = generateSchema(db, table, &frictionlessData)
		if err != nil {
			log.Println(err)
			continue
		}

		// Generate the JSON file path and name
		jsonFilePath := fmt.Sprintf("%s/%s.json", jsonPath, table)

		// Marshal the frictionlessData into JSON format
		jsonData, err := json.MarshalIndent(frictionlessData, "", "  ")
		if err != nil {
			log.Println(err)
			continue
		}

		// Write the JSON data to a file
		err = ioutil.WriteFile(jsonFilePath, jsonData, 0644)
		if err != nil {
			log.Println(err)
			continue
		}

		log.Printf("Metadata generated for table: %s\n", table)
	}
}

func getTables(db *sql.DB) ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public'
		AND table_type = 'BASE TABLE';
	`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	log.Println("Retrieved tables successfully.")
	return tables, nil
}

func generateSchema(db *sql.DB, table string, frictionlessData *FrictionlessStruct) error {
	// Generate schema metadata for the table
	fields, err := getFields(db, table)
	if err != nil {
		return err
	}

	// Update the frictionlessData with table metadata
	frictionlessData.Resources[0].Name = table
	frictionlessData.Resources[0].Path = ""
	frictionlessData.Resources[0].Title = table
	frictionlessData.Resources[0].Description = fmt.Sprintf("Metadata for the table: %s", table)
	frictionlessData.Resources[0].Schema.Fields = fields

	return nil
}

func getFields(db *sql.DB, table string) ([]Fields, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1;", table)

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dataTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	var fields []Fields
	for i, column := range columns {
		dataType := dataTypes[i].DatabaseTypeName()

		stats, err := getColumnStats(db, table, column)
		if err != nil {
			return nil, err
		}

		fields = append(fields, Fields{
			Name:        column,
			Types:       getFieldType(dataType),
			Format:      "",
			Description: "",
			Constraints: Constraints{
				Required: "",
				Unique:   "",
			},
			Stats: stats,
		})
	}

	return fields, nil
}

func getColumnStats(db *sql.DB, table, column string) (Stats, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NULL;", table, column)

	var nullCount int
	err := db.QueryRow(query).Scan(&nullCount)
	if err != nil {
		return Stats{}, err
	}

	query = fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s;", column, table)

	var uniqueCount int
	err = db.QueryRow(query).Scan(&uniqueCount)
	if err != nil {
		return Stats{}, err
	}

	query = fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)

	var rowCount int
	err = db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		return Stats{}, err
	}

	stats := Stats{
		NullValueCounts:    nullCount,
		PresentValueCounts: rowCount - nullCount,
		UniqueValueCounts:  uniqueCount,
		NullProportion:     int(math.Round(float64(nullCount) / float64(rowCount) * 100)),
		UniqueProportion:   int(math.Round(float64(uniqueCount) / float64(rowCount) * 100)),
		SampleValue:        []string{},
	}

	query = fmt.Sprintf("SELECT DISTINCT %s FROM %s LIMIT 5;", column, table)

	rows, err := db.Query(query)
	if err != nil {
		return Stats{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return Stats{}, err
		}
		stats.SampleValue = append(stats.SampleValue, value)
	}

	return stats, nil
}

func getFieldType(dataType string) string {
	switch dataType {
	case "int", "int2", "int4", "int8", "serial", "smallint", "bigint":
		return "integer"
	case "float4", "float8", "numeric", "decimal":
		return "number"
	case "date", "timestamp", "timestamptz":
		return "datetime"
	case "bool":
		return "boolean"
	default:
		return "string"
	}
}

func getFrictionlessSchema() (FrictionlessStruct, error) {
	// Replace this code with your own JSON unmarshaling logic
	frictionlessData := FrictionlessStruct{}
	jsonData := `
		{
			"profile": "tabular-data-resource",
			"name": "",
			"title": "",
			"description": "",
			"resources": [
				{
					"profile": "tabular-data-resource",
					"name": "",
					"path": "",
					"title": "",
					"description": "",
					"format": "",
					"mediatype": "",
					"encoding": "",
					"bytes": "",
					"hash": "",
					"schema": {
						"fields": []
					},
					"dialect": {
						"caseSensitiveHeader": "",
						"delimiter": "",
						"doubleQuote": "",
						"header": "",
						"lineTerminator": "",
						"quoteChar": "",
						"skipInitialSpace": "",
						"rowsCount": 0,
						"columnsCount": 0
					}
				}
			],
			"version": ""
		}
	`

	err := json.Unmarshal([]byte(jsonData), &frictionlessData)
	if err != nil {
		return FrictionlessStruct{}, err
	}

	return frictionlessData, nil
}

func (s *MyRPCServer) GetData(args DatabaseCredentials, reply *DatabaseCredentials) error {
	// Print received data on the console
	fmt.Println("Received data:", args)

	// You can perform additional processing or save the data in the server
	

	*reply = args // Set the reply value
	postgres_plugin(args)
	return nil
}

func main() {
	server := rpc.NewServer()
	myRPCServer := &MyRPCServer{}
	server.Register(myRPCServer)

	// Start the server to listen for RPC requests
	listener, err := net.Listen("tcp", ":3402")
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

