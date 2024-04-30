package main

import (
	"sync"
	"github.com/couchbase/gocb/v2"
	"fmt"
	"time"
)




func updateDocumentsXattrbase64field(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData string) {
	defer waitGroup.Done()

	for i := 0; i < 3; i++ {
		mops := []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data_base64", vectorData, &gocb.UpsertSpecOptions{}),
		}
		_, err := collection.MutateIn(documentID, mops, &gocb.MutateInOptions{
			Timeout: 10050 * time.Millisecond,
		})
		if err != nil {
			panic(err)
		}  else {
			fmt.Printf("Document ID:%v got updated. (no xattrs,  base64 vector)\n",documentID)
		}
	} 
}
