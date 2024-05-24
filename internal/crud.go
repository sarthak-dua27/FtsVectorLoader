// package internal

// import (
// 	"fmt"
// 	"math/rand"
// 	"sync"
// 	"time"
// 	"sync/atomic"

// 	"github.com/couchbase/gocb/v2"
// )
// func CrudOperations(startIndex int, endIndex int, upsertPercent float64, encodedVectors []string, vectors [][]float32,
// 	xattrFlag bool, base64Flag bool, documentIdPrefix string, provideDefaultDocs bool, collection *gocb.Collection, batchSize int) {

// 	source := rand.NewSource(time.Now().UnixNano())
// 	rand := rand.New(source)

// 	totalItems := endIndex - startIndex
// 	numUpserts := int(float64(totalItems) * upsertPercent)
// 	numDeletes := totalItems - numUpserts

// 	// upsertCount := 0
// 	// deleteCount := 0
// 	// attempts := 0

// 	var upsertCount atomic.Uint64
// 	var deleteCount atomic.Uint64
// 	var attempts atomic.Uint64

// 	// Combine all items and shuffle them
// 	selectedIndices := rand.Perm(totalItems)

// 	for batchStart := 0; batchStart < totalItems; batchStart += batchSize {
// 		batchEnd := batchStart + batchSize
// 		if batchEnd > totalItems {
// 			batchEnd = totalItems
// 		}

// 		var wg sync.WaitGroup

// 		for _, offset := range selectedIndices[batchStart:batchEnd] {
// 			if int(upsertCount.Load()) >= numUpserts && int(deleteCount.Load()) >= numDeletes {
// 				break
// 			}

// 			j := startIndex + offset

// 			if int(upsertCount.Load()) < numUpserts && (int(deleteCount.Load()) >= numDeletes || rand.Float64() < upsertPercent) {
// 				wg.Add(1)
// 				go func(index int) {
// 					if xattrFlag {
// 						if base64Flag {
// 							vectArr := encodedVectors[index%len(encodedVectors)]
// 							UpdateDocumentsXattrbase64(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, index+1), vectArr, provideDefaultDocs, index+1)
// 						} else {
// 							vectArr := vectors[index%len(vectors)]
// 							UpdateDocumentsXattr(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, index+1), vectArr, provideDefaultDocs, index+1)
// 						}
// 					} else {
// 						if base64Flag {
// 							vectArr := encodedVectors[index%len(encodedVectors)]
// 							UpdateDocumentsXattrbase64field(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, index+1), vectArr, provideDefaultDocs, index+1)
// 						} else {
// 							vectArr := vectors[index%len(vectors)]
// 							UpdateDocumentsField(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, index+1), vectArr, provideDefaultDocs, index+1)
// 						}
// 					}
// 					upsertCount.Add(1)
// 					attempts.Add(1)
// 				}(j)
// 			} else {
// 				wg.Add(1)
// 				go func(index int) {
// 					defer wg.Done()
// 					_, err := collection.Remove(fmt.Sprintf("%s%d", documentIdPrefix, index+1), &gocb.RemoveOptions{})
// 					if err == nil {
// 						deleteCount.Add(1)
// 						fmt.Printf("Document ID %v got deleted.\n", fmt.Sprintf("%s%d", documentIdPrefix, index+1))
// 					}
// 					attempts.Add(1)
// 				}(j)
// 			}
// 		}

// 		wg.Wait()
// 	}

// 	fmt.Printf("CRUD Operation: [Upserts]: %d\n", upsertCount.Load())
// 	fmt.Printf("CRUD Operation: [Deletes]: %d\n", deleteCount.Load())
// 	fmt.Printf("Total attempts: %d\n", attempts.Load())
// }

package internal

import (
	"fmt"
	"github.com/couchbase/gocb/v2"
	"sync"
	"math/rand"
	"time"
	// "sync/atomic"
)


func CrudOperations(startIndex int, endIndex int, upsertPercent float64, encodedVectors []string, vectors [][]float32,
	xattrFlag bool, base64Flag bool, documentIdPrefix string, provideDefaultDocs bool, collection *gocb.Collection, batchSize int){

	
	source := rand.NewSource(time.Now().UnixNano())
	rand := rand.New(source)

	totalDocs := endIndex - startIndex
	upsertCount := int(float64(totalDocs) * (upsertPercent))
	// deleteCount := totalDocs - upsertCount
	

	goRoutines := 100
	batchCount := int(upsertCount/ goRoutines)
	marker := rand.Perm(goRoutines)


	startTime := time.Now()

	var wg sync.WaitGroup
	for i:=0;i<goRoutines;i++ {
	
	markerPos := (marker[i])
	startIndex := markerPos*batchCount
	end := (markerPos*batchCount) + batchCount
	wg.Add(1)
	go func(start, end int) {
		defer wg.Done()
		batchSuffle := rand.Perm(end-start)

		for k := 0; k < batchCount; k++ {
			j:= batchSuffle[k] + start

			if xattrFlag {
				if base64Flag {
					vectArr := encodedVectors[j % len(encodedVectors)]
					UpdateDocumentsXattrbase64(collection, fmt.Sprintf("%s%d", documentIdPrefix, j+1), vectArr, provideDefaultDocs, j+1)
				} else {
					vectArr := vectors[j % len(vectors)]
					UpdateDocumentsXattr(collection, fmt.Sprintf("%s%d", documentIdPrefix, j+1), vectArr, provideDefaultDocs, j+1)
				}
			} else {
				if base64Flag {
					vectArr := encodedVectors[j % len(encodedVectors)]
					UpdateDocumentsXattrbase64field(collection, fmt.Sprintf("%s%d", documentIdPrefix, j+1), vectArr, provideDefaultDocs, j+1)
				} else {
					vectArr := vectors[j % len(vectors)]
					UpdateDocumentsField(collection, fmt.Sprintf("%s%d", documentIdPrefix, j+1), vectArr, provideDefaultDocs, j+1)
				}
			}
		}
	}(startIndex, end)
	}
	wg.Wait()
	fmt.Print(time.Since(startTime))

}