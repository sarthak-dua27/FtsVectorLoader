package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
)

func main() {
	var nodeAddress string
	var bucketName string
	var username string
	var password string
	var fieldName string
	var collectionName string
	var documentIdPrefix string
	var startIndex int
	var endIndex int
	var batchSize int

	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "", "Bucket name")
	flag.StringVar(&collectionName, "collectionName", "_default", "Collection name")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.StringVar(&fieldName, "fieldName", "vector_data", "fieldName")
	flag.StringVar(&documentIdPrefix, "documentIdPrefix", "", "documentIdPrefix")
	flag.IntVar(&startIndex, "startIndex", 0, "startIndex")
	flag.IntVar(&endIndex, "endIndex", 50, "endIndex")
	flag.IntVar(&batchSize, "batchSize", 100, "batchSize")

	flag.Parse()

	// Initialize the Connection
	cluster, err := gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})

	if err != nil {
		panic(fmt.Errorf("error creating cluster object : %v", err))
	}
	bucket := cluster.Bucket(bucketName)

	err = bucket.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		panic(err)
	}

	collection := bucket.DefaultCollection()
	vectArr := []float32{1, 2.3, 1.24, 3.25}

	var wg sync.WaitGroup
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			go updateDocumentsXattr(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr)
		}
		wg.Wait()
		startIndex = end
	}
}

func updateDocumentsXattr(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData []float32) {
	defer waitGroup.Done()
	for i := 0; i < 3; i++ {
		_, err := collection.MutateIn(documentID, []gocb.MutateInSpec{
			gocb.UpsertSpec("vector", vectorData, &gocb.UpsertSpecOptions{
				CreatePath: true,
				IsXattr:    true,
			}),
		},
			nil,
		)
		if err != nil {
			fmt.Printf("Error mutating document: %v Retrying\n", err)
		} else {
			fmt.Print("Done")
			break
		}
	}

}
