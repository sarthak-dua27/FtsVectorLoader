package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"log"
	"main/internal"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {

	var nodeAddress string
	var bucketName string
	var scopeName string
	var username string
	var password string
	var fieldName string
	var collectionName string
	var documentIdPrefix string
	var startIndex int
	var endIndex int
	var batchSize int
	var datasetName string
	var xattrFlag bool

	var percentagesToResize []float32
	var dimensionsForResize []int
	var floatListStr string
	var intListStr string
	var base64Flag bool
	var capella bool
	var provideDefaultDocs bool
	// var vectorCategory string

	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "", "Bucket name")
	flag.StringVar(&scopeName, "scopeName", "", "Scope name")
	flag.StringVar(&collectionName, "collectionName", "_default", "Collection name")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.StringVar(&fieldName, "fieldName", "vector_data", "fieldName")
	flag.StringVar(&documentIdPrefix, "documentIdPrefix", "", "documentIdPrefix")
	flag.IntVar(&startIndex, "startIndex", 0, "startIndex")
	flag.IntVar(&endIndex, "endIndex", 50, "endIndex")
	flag.IntVar(&batchSize, "batchSize", 600, "batchSize")
	flag.BoolVar(&provideDefaultDocs, "provideDefaultDocs", true, "provideDefaultDocs = true will upsert docs and then update docs for xattr (metadata)")
	flag.BoolVar(&capella, "capella", false, "pushing docs to capella?")
	flag.StringVar(&datasetName, "datasetName", "", "Name of the dataset ('sift', 'siftsmall', 'gist')")
	flag.BoolVar(&xattrFlag, "xattrFlag", false, "xattrFlag = true will upsert vectors into xattr (metadata) and false will upsert vectors into document")
	flag.StringVar(&floatListStr, "percentagesToResize", "", "Comma-separated list of float32 values")
	flag.StringVar(&intListStr, "dimensionsForResize", "", "Comma-separated list of int values")
	flag.BoolVar(&base64Flag, "base64Flag", false, "true results in, embeddings get uploaded as base64 strings")
	// flag.StringVar(&vectorCategory, "vectorCategory", "learn", "Available categories are learn, base, groundtruth and query")

	flag.Parse()

	var cluster *gocb.Cluster
	var er error
	if capella {
		options := gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
			SecurityConfig: gocb.SecurityConfig{
				TLSSkipVerify: true,
			},
		}
		if err := options.ApplyProfile(gocb.
			ClusterConfigProfileWanDevelopment); err != nil {
			log.Fatal(err)
		}
		cluster, er = gocb.Connect(nodeAddress, options)
	} else {
		cluster, er = gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
		})
	}

	if er != nil {
		panic(fmt.Errorf("error creating cluster object : %v", er))
	}
	bucket := cluster.Bucket(bucketName)

	err := bucket.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		panic(err)
	}

	scope := bucket.Scope(scopeName)

	collection := scope.Collection(collectionName)

	if floatListStr != "" {
		// isResize = true

		var floatList []float32

		floatStrList := strings.Split(floatListStr, ",")
		for _, floatStr := range floatStrList {
			floatVal, err := strconv.ParseFloat(floatStr, 32)
			if err != nil {
				fmt.Printf("Error parsing float value: %v\n", err)
				return
			}
			floatList = append(floatList, float32(floatVal))
		}
		percentagesToResize = floatList

		var intList []int
		intStrList := strings.Split(intListStr, ",")
		for _, intStr := range intStrList {
			intVal, err := strconv.Atoi(intStr)
			if err != nil {
				fmt.Printf("Error parsing integer value: %v\n", err)
				return
			}
			intList = append(intList, intVal)
		}

		dimensionsForResize = intList

		fmt.Println(dimensionsForResize)
		fmt.Println(percentagesToResize)

	}

	//dataset downloading and extraction
	baseUrl := "ftp://ftp.irisa.fr/local/texmex/corpus/"
	datasetUrl := baseUrl + datasetName + ".tar.gz"

	// Check if the dataset file already exists in the "raw/" folder
	if internal.DatasetExists("raw/" + datasetName + ".tar.gz") {
		fmt.Println("Dataset file already exists. Skipping download.")
	} else {
		fmt.Println("Downloading the dataset")
		internal.DownloadDataset(datasetUrl, datasetName)
	}

	var learnVecs = "source/" + datasetName + "/" + datasetName + "_base.fvecs"

	vectors, err := internal.ReadVectorsFromFile(learnVecs)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if floatListStr != "" {
		err = internal.ResizeVectors(vectors, percentagesToResize, dimensionsForResize)
		if err != nil {
			log.Printf("Error resizing vectors %v\n", err)
		}
	}

	var encodedVectors []string
	if base64Flag {
		for _, vector := range vectors {
			byteSlice := internal.FloatsToLittleEndianBytes(vector)
			base64String := base64.StdEncoding.EncodeToString(byteSlice)
			encodedVectors = append(encodedVectors, base64String)
		}
	}

	var wg sync.WaitGroup
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			if xattrFlag {
				if base64Flag {
					vectArr := encodedVectors[j]
					go internal.UpdateDocumentsXattrbase64(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr)
				} else {
					vectArr := vectors[j]
					go internal.UpdateDocumentsXattr(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr, j, provideDefaultDocs)
				}

			} else {
				if base64Flag {
					vectArr := encodedVectors[j]
					go internal.UpdateDocumentsXattrbase64field(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr)
				} else {
					vectArr := vectors[j]
					go internal.UpdateDocumentsField(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr, j)
				}

			}

		}
		wg.Wait()
		startIndex = end
	}
}