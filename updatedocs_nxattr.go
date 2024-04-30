package main

import (
	"log"
	"sync"
	"github.com/couchbase/gocb/v2"
	"github.com/go-faker/faker/v4"
	"fmt"
)


func updateDocumentsField(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData []float32, ind int) {
	defer waitGroup.Done()

	type Data struct {
		Sno    int       `json:"sno"`
		Sname  string    `json:"sname"`
		Id     string    `json:"id"`
		Vector []float32 `json:"vector_data"`
	}

	for i := 0; i < 3; i++ {
		var _, err = collection.Upsert(documentID,
			Data{
				Sno:    ind,
				Sname:  faker.Name(),
				Id:     documentID,
				Vector: vectorData,
			}, nil)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Document ID:%v got updated. (no xattrs, vector)\n",documentID)
		}

	}
}