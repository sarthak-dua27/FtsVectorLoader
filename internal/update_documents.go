package internal

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/go-faker/faker/v4"
	"log"
	// "sync"
	"time"
)

func UpdateDocumentsXattr(collection *gocb.Collection, documentID string, vectorData []float32, provideDefaultDocs bool, ind int) (int){
	// defer waitGroup.Done()

	type Data struct {
		Sno   int    `json:"sno"`
		Sname string `json:"sname"`
		Id    string `json:"id"`
	}

	for i := 0; i < 3; i++ {
		g:=0
		if provideDefaultDocs {
			var _, err = collection.Upsert(documentID,
				Data{
					Sno:   ind,
					Sname: faker.Name(),
					Id:    documentID,
				}, nil)
			if err != nil {
				log.Printf("Unable to upsert doc %v", err)
			} else {
				fmt.Printf("Sample doc upserted with id %v.\n",documentID)
				g = 1
			}
		}

		if provideDefaultDocs && g==1 {
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
				fmt.Printf("Document ID %v got updated with vector data in xattrs\n", documentID)
				return 1
			}
		}

	
}
return 0
}

func UpdateDocumentsField(collection *gocb.Collection, documentID string, vectorData []float32, provideDefaultDocs bool,ind int) (int) {
	// defer waitGroup.Done()

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
				log.Printf("Unable to upsert doc %v", err)
			}
		}

		var _, errr = collection.MutateIn(documentID, []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data", vectorData, &gocb.UpsertSpecOptions{}),
		},nil,)

		if errr != nil {
			fmt.Printf("Error mutating document %v : %v Retrying\n", documentID, errr)
		} else {
			fmt.Printf("Document ID %v got updated with vector data in xattrs\n", documentID)
			return 1
		}
	}
	return 0
}

func UpdateDocumentsXattrbase64(collection *gocb.Collection, documentID string, vectorData string,provideDefaultDocs bool,ind int) (int){
	// defer waitGroup.Done()

	type Data struct {
		Sno   int    `json:"sno"`
		Sname string `json:"sname"`
		Id    string `json:"id"`
	}
	for i:=0 ;i<3;i++ {
		if provideDefaultDocs {
			for i:=0;i<3;i++ {
				var _, err = collection.Upsert(documentID,
					Data{
						Sno:   ind,
						Sname: faker.Name(),
						Id:    documentID,
					}, nil)
				if err != nil {
					log.Printf("Unable to upsert doc %v", err)
				} else {
					break
				}
			}
		}

		mops := []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_encoded", vectorData, &gocb.UpsertSpecOptions{
				CreatePath: true,
				IsXattr:    true,
			}),
		}
		_, err := collection.MutateIn(documentID, mops, &gocb.MutateInOptions{
		})
		if err != nil {
			log.Printf("Unable to upsert doc %v", err)
		} else {
			fmt.Printf("Document ID %v got upserted.\n", documentID)
			return 1
		}
	}
	return 0
}

func UpdateDocumentsXattrbase64field(collection *gocb.Collection, documentID string, vectorData string,provideDefaultDocs bool,ind int) (int){
	// defer waitGroup.Done()

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
		} else {
			break
		}
			
		}

		mops := []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data_base64", vectorData, &gocb.UpsertSpecOptions{}),
		}
		_, err := collection.MutateIn(documentID, mops, &gocb.MutateInOptions{
			Timeout: 10050 * time.Millisecond,
		})
		if err != nil {
			log.Printf("Unable to upsert doc %v", err)
		} else {
			fmt.Printf("Document ID %v got upserted.\n", documentID)
			return 1
		}
	}
	return 0
}
