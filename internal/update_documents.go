package internal

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/go-faker/faker/v4"
	"log"
	"sync"
	"time"
)

func UpdateDocumentsXattr(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData []float32, ind int, provideDefaultDocs bool) {
	defer waitGroup.Done()

	type Data struct {
		Sno   int    `json:"sno"`
		Sname string `json:"sname"`
		Id    string `json:"id"`
	}

	for i := 0; i < 3; i++ {
		if provideDefaultDocs {
			var _, err = collection.Upsert(documentID,
				Data{
					Sno:   ind,
					Sname: faker.Name(),
					Id:    documentID,
				}, nil)
			if err != nil {
				log.Fatalf("Unable to upsert doc %v", err)
				return
			}
		}

		var _, errr = collection.MutateIn(documentID, []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data", vectorData, &gocb.UpsertSpecOptions{
				CreatePath: true,
				IsXattr:    true,
			}),
		},
			nil,
		)
		if errr != nil {
			fmt.Printf("Error mutating document %v : %v Retrying\n", documentID, errr)
		} else {
			// fmt.Println("Done")
			//fmt.Printf("Document ID %v got updated with vector data in xattrs\n", documentID)
			break
		}

	}
}

func UpdateDocumentsField(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData []float32, ind int) {
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
			//fmt.Printf("Document ID %v got upserted with vector in doc.\n", documentID)
			break
		}

	}
}

func UpdateDocumentsXattrbase64(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData string) {
	defer waitGroup.Done()

	for i := 0; i < 3; i++ {
		mops := []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_encoded", vectorData, &gocb.UpsertSpecOptions{
				CreatePath: true,
				IsXattr:    true,
			}),
		}
		_, err := collection.MutateIn(documentID, mops, &gocb.MutateInOptions{
			Timeout: 10050 * time.Millisecond,
		})
		if err != nil {
			panic(err)
		} else {
			//fmt.Printf("Document ID %v got upserted.\n", documentID)
			break
		}
	}
}

func UpdateDocumentsXattrbase64field(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData string) {
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
		} else {
			//fmt.Printf("Document ID %v got upserted.\n", documentID)
			break
		}
	}
}
