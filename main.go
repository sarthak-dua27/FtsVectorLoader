package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"sync"
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
	flag.BoolVar(&provideDefaultDocs, "provideDefaultDocs", false, "provideDefaultDocs = true will upsert docs and then update docs for xattr (metadata)")
	flag.BoolVar(&capella, "capella", false, "pushing docs to capella?")
	flag.StringVar(&datasetName, "datasetName", "", "Name of the dataset ('sift', 'siftsmall', 'gist')")
	flag.BoolVar(&xattrFlag, "xattrFlag", true, "xattrFlag = true will upsert vectors into xattr (metadata) and false will upsert vectors into document")
	flag.StringVar(&floatListStr, "percentagesToResize", "", "Comma-separated list of float32 values")
	flag.StringVar(&intListStr, "dimensionsForResize", "", "Comma-separated list of int values")
	flag.BoolVar(&base64Flag, "base64Flag", false, "true results in, embeddings get uploaded as base64 strings")

	flag.Parse()

	_, _, collection := couchbaseConnect(capella, username, password, nodeAddress, bucketName, scopeName, collectionName)

	if floatListStr != "" { percentagesToResize, dimensionsForResize = resizeInit(floatListStr,intListStr) }

	vector_path := databaseInit(datasetName)

	vectors, err := readVectorsFromFile(vector_path)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if floatListStr != "" {
		err = resizeVectors(vectors, percentagesToResize, dimensionsForResize)
		if err != nil {
			log.Printf("Error resizing vectors %v\n", err)
		}
	}

	var encodedVectors []string
	if base64Flag {
		for _, vector := range vectors {
			byteSlice := floatsToLittleEndianBytes(vector)
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
					go updateDocumentsXattrbase64(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr)
				} else {
					vectArr := vectors[j]
					go updateDocumentsXattr(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr, j, provideDefaultDocs)
				}

			} else {
				if base64Flag {
					vectArr := encodedVectors[j]
					go updateDocumentsXattrbase64field(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr)
				} else {
					vectArr := vectors[j]
					go updateDocumentsField(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr, j)
				}

			}

		}
		wg.Wait()
		startIndex = end
	}
}