package main

import (
	"sync"
	"github.com/couchbase/gocb/v2"
	"github.com/go-faker/faker/v4"
	"fmt"
)

func updateDocumentsXattr(waitGroup *sync.WaitGroup,collection *gocb.Collection, documentID string, vectorData []float32, ind int, provideDefaultDocs bool) {
	defer waitGroup.Done()

	type Data struct {
		Sno      int   `json:"sno"`
		Sname     string   `json:"sname"`
		Id string `json:"id"`
	}


	for i := 0; i < 3; i++ {
		if provideDefaultDocs {
			var _, err = collection.Upsert(documentID ,
			Data{
				Sno : ind, 
				Sname : faker.Name(),
				Id  : documentID,
			}, nil)
			if err != nil {
				fmt.Printf("Document %v upsert failed\n",documentID)
			} else {
				fmt.Printf("Document %v upserted successfully\n",documentID)
				break
			}
		} else {
			var _, errr = collection.MutateIn(documentID, []gocb.MutateInSpec{
				gocb.UpsertSpec("vector_data", vectorData, &gocb.UpsertSpecOptions{
					CreatePath: true,
					IsXattr:    true,
				}),
			},
				nil,
			)
			if errr != nil {
				fmt.Printf("Error mutating document %v : %v Retrying\n",documentID, errr)
			} else {
				fmt.Printf("Document ID:%v got updated. (xattrs,  vector)\n",documentID)
				break
			}
		}
	}
}