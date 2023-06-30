package main

import (
	"database/sql"

	"fmt"

	"io"

	"io/ioutil"

	"log"

	"net/http"

	"os"

	"path/filepath"
	
	"net/rpc"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/gin-gonic/gin"

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




































type Credentials struct {
	DataSource string `json:"data_source"`

	Username string `json:"username"`

	Password string `json:"password"`

	DatabaseName string `json:"database_name"`

	Host string `json:"host"`

	Port string `json:"port"`

	URL string `json:"url"`

	PublicKey string `json:"public_key"`

	RequestDatetime string `json:"request_datetime"`

	BucketName string `json:"bucket_name"`

	Region string `json:"region"`

	SecretKey string `json:"secretkey"`

	AccessKey string `json:"accesskey"`

	Endpoint string `json:"endpoint"`
}

type Response struct {
	Message string `json:"message"`

	S3Connection bool `json:"s3_connection,omitempty"`

	S3ErrorMessage string `json:"s3_error_message,omitempty"`

	DownloadedFiles []string `json:"downloaded_files,omitempty"`

	CopiedFiles []string `json:"copiedFiles"`

	CopiedFolder string `json:"copiedFolder"`
	DownloadedCount int      `json:"downloaded_count,omitempty"`
	CSVFiles        int      `json:"csv_files"`
	JSONFiles       int      `json:"json_files"`
}

func createTable(db *sql.DB) error {

	_, err := db.Exec(`

		CREATE TABLE IF NOT EXISTS credentials (

			id SERIAL PRIMARY KEY,

			data_source TEXT,

			username TEXT,

			password TEXT,

			database_name TEXT,

			host TEXT,

			port TEXT,

			url TEXT,

			public_key TEXT,

			request_datetime TEXT,

			bucket_name TEXT,

			region TEXT,

			secret_key TEXT,

			access_key TEXT,

			endpoint TEXT

		)`)

	return err

}

func handleCredentials(c *gin.Context) {

	// Parse the JSON request body

	var creds Credentials

	err := c.ShouldBindJSON(&creds)

	if err != nil {

		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})

		return

	}

	// Connect to the PostgreSQL database

	connStr := "host=localhost port=5431 user=postgres password=postgres dbname=postgres sslmode=disable"

	db, err := sql.Open("postgres", connStr)

	if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	defer db.Close()

	// Create the table if it doesn't exist

	err = createTable(db)

	if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}

	// Insert the credentials into the database

	_, err = db.Exec(`

		INSERT INTO credentials (

			data_source, username, password, database_name, host, port,

			url, public_key, request_datetime, bucket_name, region,

			secret_key, access_key, endpoint

		)

		VALUES (

			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14

		)`,

		creds.DataSource, creds.Username, creds.Password, creds.DatabaseName,

		creds.Host, creds.Port, creds.URL, creds.PublicKey, creds.RequestDatetime,

		creds.BucketName, creds.Region, creds.SecretKey, creds.AccessKey, creds.Endpoint)

	if err != nil {

		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

		return

	}
	c.JSON(http.StatusOK, gin.H{"message": "Credentials saved successfully!"})

	if creds.DataSource != "s3" && creds.DataSource != "local" && creds.DataSource != "postgres" {

		c.JSON(http.StatusBadRequest, gin.H{"message": "Enter a valid DataSource"})

		return

	}

	// If the data source is "s3", connect to the S3 bucket and download the files

	if creds.DataSource == "s3" {

		// Create an AWS session

		awsConfig := &aws.Config{

			Region: aws.String(creds.Region),

			Credentials: credentials.NewStaticCredentials(creds.AccessKey, creds.SecretKey, ""),
		}

		sess, err := session.NewSession(awsConfig)

		if err != nil {

			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})

			return

		}

		// Create an S3 client

		s3Client := s3.New(sess)

		// Perform a simple S3 operation to test the connection

		_, err = s3Client.ListObjectsV2(&s3.ListObjectsV2Input{

			Bucket: aws.String(creds.BucketName),
		})

		if err != nil {

			c.JSON(http.StatusOK, Response{

				Message: "Cannot Connect to S3",

				S3Connection: false,

				S3ErrorMessage: err.Error(),
			})

			return

		}

		// Download the files from the S3 bucket

		if creds.DataSource == "s3" {
			// ...
			downloadedCount, downloadedFiles, err := downloadFilesFromS3(s3Client, creds.BucketName)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		
			downloadedCount, downloadedFiles, err = downloadFilesFromS3(s3Client, creds.BucketName)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}

			// Filter the files based on their extensions
			csvFiles := 0
			jsonFiles := 0
			for _, file := range downloadedFiles {
				ext := filepath.Ext(file)
				switch ext {
				case ".csv":
					csvFiles++
				case ".json":
					jsonFiles++
				}
			}

			c.JSON(http.StatusOK, Response{
				Message:         "Connection to S3 Successfull",
				S3Connection:    true,
				S3ErrorMessage:  "",
				DownloadedFiles: downloadedFiles,
				DownloadedCount: downloadedCount,
				CSVFiles:        csvFiles,
				JSONFiles:       jsonFiles,
			})
			if csvFiles > 0 {
			
				client, err := rpc.Dial("tcp", "localhost:3400")
				if err != nil {
					log.Fatal("Dial error:", err)
				}

				// Prepare the data to be sent
				data := DatabaseCredentials{
					SourceDirectory: "/home/swati/api/downloads",
				}

				var reply DatabaseCredentials
				err = client.Call("MyRPCServer.GetData", data, &reply)
				if err != nil {
					log.Fatal("RPC error:", err)
				}
			fmt.Println("Calling csv:")
				}


			if jsonFiles > 0 {
			
				client, err := rpc.Dial("tcp", "localhost:3401")
				if err != nil {
					log.Fatal("Dial error:", err)
				}

				// Prepare the data to be sent
				data := DatabaseCredentials{
					SourceDirectory: "/home/swati/api/downloads",
				}

				var reply DatabaseCredentials
				err = client.Call("MyRPCServer.GetData", data, &reply)
				if err != nil {
					log.Fatal("RPC error:", err)
				}
			fmt.Println("Calling json:")
				}
			return
		}
		
	}

	// If the data source is "local", copy files from the specified directory

	if creds.DataSource == "local" {
		copyDir := "./copied_files"
		destDir, err := copyFilesFromLocal(creds.URL, copyDir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Get the list of copied files
		copiedFiles, err := getFilesList(destDir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		

		
    		// Count the file types
    		csvFiles := 0
    		jsonFiles := 0
    		for _, file := range copiedFiles {
		ext := filepath.Ext(file)
			switch ext {
			case ".csv":
			    csvFiles++
			case ".json":
			    jsonFiles++
				}
			    }

		    response := Response{
			Message:      "Data copied Successfully",
			CopiedFiles:  copiedFiles,
			CopiedFolder: destDir,
			CSVFiles:     csvFiles,
			JSONFiles:    jsonFiles,
		}


		// Return the response
		c.JSON(http.StatusOK, response)
		
					if csvFiles > 0 {
			
				client, err := rpc.Dial("tcp", "localhost:3400")
				if err != nil {
					log.Fatal("Dial error:", err)
				}

				// Prepare the data to be sent
				data := DatabaseCredentials{
					SourceDirectory: "/home/swati/api/downloads",
				}

				var reply DatabaseCredentials
				err = client.Call("MyRPCServer.GetData", data, &reply)
				if err != nil {
					log.Fatal("RPC error:", err)
				}
			fmt.Println("Calling csv:")
				}


			if jsonFiles > 0 {
			
				client, err := rpc.Dial("tcp", "localhost:3401")
				if err != nil {
					log.Fatal("Dial error:", err)
				}

				// Prepare the data to be sent
				data := DatabaseCredentials{
					SourceDirectory: "/home/swati/api/downloads",
				}

				var reply DatabaseCredentials
				err = client.Call("MyRPCServer.GetData", data, &reply)
				if err != nil {
					log.Fatal("RPC error:", err)
				}
			fmt.Println("Calling json:")
				}
		
		
		return
	}

	if creds.DataSource == "postgres" {

		// Return the success response with only the required PostgreSQL credentials

		c.JSON(http.StatusOK, gin.H{

			"host": creds.Host,

			"dbname": creds.DatabaseName,

			"user": creds.Username,

			"password": creds.Password,

			"port": creds.Port,
		})
	client, err := rpc.Dial("tcp", "localhost:3402")
	if err != nil {
		log.Fatal("Dial error:", err)
	}

	// Prepare the data to be sent
	data := DatabaseCredentials{
		Host:            creds.Host,
		Port:            5431,
		User:            creds.Username,
		Password:        creds.Password,
		DBName:          creds.DatabaseName,
		PluginType:      "postgres",
		//SourceDirectory: "/home/swati/Documents/shared_folder/cred.json",
	}

	var reply DatabaseCredentials
	err = client.Call("MyRPCServer.GetData", data, &reply)
	if err != nil {
		log.Fatal("RPC error:", err)
	}
		return

	}

	// Return a success response for other data sources

	c.JSON(http.StatusOK, Response{

		Message: "Credentials saved successfully!",
	})

}

func downloadFilesFromS3(s3Client *s3.S3, bucketName string) (int, []string, error) {
	// Retrieve the list of objects in the bucket
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	listResult, err := s3Client.ListObjectsV2(listInput)
	if err != nil {
		return 0, nil, err
	}

	// Create a directory to store the downloaded files
	downloadDir := "./downloads"
	err = os.MkdirAll(downloadDir, 0755)
	if err != nil {
		return 0, nil, err
	}

	// Download each object in the bucket
	downloadedFiles := make([]string, 0)
	count := 0

	for _, object := range listResult.Contents {
		downloadInput := &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    object.Key,
		}

		// Construct the file path for the downloaded file
		filePath := filepath.Join(downloadDir, *object.Key)

		// Create the file
		file, err := os.Create(filePath)
		if err != nil {
			return count, nil, err
		}
		defer file.Close()

		// Download the object and write it to the file
		result, err := s3Client.GetObject(downloadInput)
		if err != nil {
			return count, nil, err
		}

		_, err = io.Copy(file, result.Body)
		if err != nil {
			return count, nil, err
		}

		downloadedFiles = append(downloadedFiles, *object.Key)
		count++
	}

	return count, downloadedFiles, nil
}

func copyFilesFromLocal(sourceDir, destinationDir string) (string, error) {
	files, err := ioutil.ReadDir(sourceDir)
	if err != nil {
		return "", err
	}

	err = os.MkdirAll(destinationDir, 0755)
	if err != nil {
		return "", err
	}

	for _, file := range files {
		sourcePath := filepath.Join(sourceDir, file.Name())
		destinationPath := filepath.Join(destinationDir, file.Name())

		err = copyFile(sourcePath, destinationPath)
		if err != nil {
			return "", err
		}
	}

	return destinationDir, nil
}

func copyFile(sourcePath, destinationPath string) error {

	sourceFile, err := os.Open(sourcePath)

	if err != nil {

		return err

	}

	defer sourceFile.Close()

	destinationFile, err := os.Create(destinationPath)

	if err != nil {

		return err

	}

	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)

	if err != nil {

		return err

	}

	return nil

}

func getFilesList(directory string) ([]string, error) {

	files, err := ioutil.ReadDir(directory)

	if err != nil {

		return nil, err

	}

	fileNames := make([]string, 0)

	for _, file := range files {

		fileNames = append(fileNames, file.Name())

	}

	return fileNames, nil

}

func main() {

	r := gin.Default()

	r.POST("/credentials", handleCredentials)

	fmt.Println("Server listening on port 8080...")

	log.Fatal(r.Run(":8080"))

}


