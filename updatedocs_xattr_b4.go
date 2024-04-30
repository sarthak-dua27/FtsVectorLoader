package main

import (
	"sync"
	"github.com/couchbase/gocb/v2"
	"fmt"
	"time"
	"log"
)

func updateDocumentsXattrbase64(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData string) {
	defer waitGroup.Done()

	for i := 0; i < 3; i++ {
		mops := []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data_base64", vectorData, &gocb.UpsertSpecOptions{
				CreatePath: true,
				IsXattr:    true,
			}),
		}
		_, err := collection.MutateIn(documentID, mops, &gocb.MutateInOptions{
			Timeout: 10050 * time.Millisecond,
		})
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Document ID:%v got updated. (xattrs, base 64 vector)\n",documentID)
		}
	}
}
