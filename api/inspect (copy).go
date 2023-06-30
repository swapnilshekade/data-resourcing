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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

type Credentials struct {
	DataSource      string `json:"data_source"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	DatabaseName    string `json:"database_name"`
	Host            string `json:"host"`
	Port            string `json:"port"`
	URL             string `json:"url"`
	PublicKey       string `json:"public_key"`
	RequestDatetime string `json:"request_datetime"`
	BucketName      string `json:"bucket_name"`
	Region          string `json:"region"`
	SecretKey       string `json:"secretkey"`
	AccessKey       string `json:"accesskey"`
	Endpoint        string `json:"endpoint"`
}

type Response struct {
	Message         string   `json:"message"`
	S3Connection    bool     `json:"s3_connection"`
	S3ErrorMessage  string   `json:"s3_error_message"`
	DownloadedFiles []string `json:"downloaded_files"`
	CopiedFiles []string `json:"copiedFiles"` 
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


	if creds.DataSource != "s3" && creds.DataSource != "local" && creds.DataSource != "postgres" {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Enter a valid DataSource"})
		return
	}
	
	
	
		// If the data source is "s3", connect to the S3 bucket and download the files
	if creds.DataSource == "s3" {
		// Create an AWS session
		awsConfig := &aws.Config{
			Region:      aws.String(creds.Region),
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
				Message:        "Credentials saved successfully!",
				S3Connection:   false,
				S3ErrorMessage: err.Error(),
			})
			return
		}

		// Download the files from the S3 bucket
		downloadedFiles, err := downloadFilesFromS3(s3Client, creds.BucketName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Return the success response with downloaded file list
		c.JSON(http.StatusOK, Response{
			Message:         "Credentials saved successfully!",
			S3Connection:    true,
			S3ErrorMessage:  "",
			DownloadedFiles: downloadedFiles,
		})

		return
	}

	// If the data source is "local", copy files from the specified directory
	if creds.DataSource == "local" {
		copyDir := "./copied_files"
		err = copyFilesFromLocal(creds.URL, copyDir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Get the list of copied files
		copiedFiles, err := getFilesList(copyDir)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Return the success response with the list of copied files
		c.JSON(http.StatusOK, Response{
			Message:         "Data copied Successfully",
			CopiedFiles: copiedFiles,
		})

		return
	}


	if creds.DataSource == "postgres" {
		// Return the success response with only the required PostgreSQL credentials
		c.JSON(http.StatusOK, gin.H{
			"host":     creds.Host,
			"dbname":   creds.DatabaseName,
			"user":     creds.Username,
			"password": creds.Password,
			"port":     creds.Port,
		})
		return
	}
	// Return a success response for other data sources
	c.JSON(http.StatusOK, Response{
		Message:         "Credentials saved successfully!",

	})
}

func downloadFilesFromS3(s3Client *s3.S3, bucketName string) ([]string, error) {
	// Retrieve the list of objects in the bucket
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}

	listResult, err := s3Client.ListObjectsV2(listInput)
	if err != nil {
		return nil, err
	}

	// Create a directory to store the downloaded files
	downloadDir := "./downloads"
	err = os.MkdirAll(downloadDir, 0755)
	if err != nil {
		return nil, err
	}

	// Download each object in the bucket
	downloadedFiles := make([]string, 0)
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
			return nil, err
		}
		defer file.Close()

		// Download the object and write it to the file
		result, err := s3Client.GetObject(downloadInput)
		if err != nil {
			return nil, err
		}

		_, err = io.Copy(file, result.Body)
		if err != nil {
			return nil, err
		}

		downloadedFiles = append(downloadedFiles, *object.Key)
	}

	return downloadedFiles, nil
}

func copyFilesFromLocal(sourceDir, destinationDir string) error {
	files, err := ioutil.ReadDir(sourceDir)
	if err != nil {
		return err
	}

	err = os.MkdirAll(destinationDir, 0755)
	if err != nil {
		return err
	}

	for _, file := range files {
		sourcePath := filepath.Join(sourceDir, file.Name())
		destinationPath := filepath.Join(destinationDir, file.Name())

		err = copyFile(sourcePath, destinationPath)
		if err != nil {
			return err
		}
	}

	return nil
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

