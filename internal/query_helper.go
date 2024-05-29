package internal

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

func callAPI(username string, password string, url string, payload map[string]interface{}) (map[string]interface{}, error) {
	//fmt.Printf("Executing query on url %s with payload %v", url, payload)
	apiClient := NewAPIClient(url)
	resp, err := apiClient.DoRequest("POST", username, password, payload)
	if err != nil {
		return nil, err
	}
	postResult, err := ProcessResponse(resp)
	if err != nil {
		return nil, err
	}
	return postResult, nil
}

func SimulateQuery(nodeAddress string, indexName string, vector []float32, username string, password string, xattr bool, base64 bool) {

	url := fmt.Sprintf("http://%s:8094/api/index/%s/query", nodeAddress, indexName)

	var field = "vector_data"
	if xattr {
		field = "_$xattrs.vector_data"
	}
	if base64 {
		field = "vector_data_base64"
	}

	payload := map[string]interface{}{
		"query": map[string]interface{}{
			"match_none": struct{}{},
		},
		"explain": true,
		"fields":  []string{"*"},
		"knn": []map[string]interface{}{
			{
				"field":  field,
				"k":      10,
				"vector": vector,
			},
		},
	}

	result, err := callAPI(username, password, url, payload)
	if err != nil {
		fmt.Printf("Error running query %v\n", err)
	}
	if result["status"] == "fail" {
		fmt.Println(result)
	} else {
		fmt.Println(result["status"], "Total Hits:", result["total_hits"])
	}

}

func getAllIndexes(nodeAddress string, username string, password string) (map[string]interface{}, error) {
	url := fmt.Sprintf("http://%s:8094/api/index", nodeAddress)
	apiClient := NewAPIClient(url)
	resp, err := apiClient.DoRequest("GET", username, password, nil)
	if err != nil {
		return nil, err
	}
	postResult, err := ProcessResponse(resp)
	if err != nil {
		return nil, err
	}
	return postResult, nil
}

func getIndexNames(nodeAddress string, username string, password string, indexNames *[]string) {
	indexes, err := getAllIndexes(nodeAddress, username, password)
	if err != nil {
		fmt.Printf("Error retriving index names %v", indexes)
		return
	}
	indexDefs, _ := indexes["indexDefs"].(map[string]interface{})
	indexDefs2, _ := indexDefs["indexDefs"].(map[string]interface{})
	for index := range indexDefs2 {
		*indexNames = append(*indexNames, index)
	}
}

func run(nodeAddress string, indexName string, vector [][]float32, username string, password string, n int, duration time.Duration, xattr bool, base64 bool, wg *sync.WaitGroup) {
	defer wg.Done()
	startTime := time.Now()
	for time.Since(startTime) < duration {
		timeB4 := time.Now()
		for i := 0; i < n; i++ {
			go SimulateQuery(nodeAddress, indexName, vector[rand.Intn(len(vector)-1)], username, password, xattr, base64)
		}
		timeToSleep := time.Second - time.Since(timeB4)
		if timeToSleep > 0 {
			time.Sleep(timeToSleep)
		}
	}
}
func RunQueriesPerSecond(nodeAddress string, indexName string, vector [][]float32, username string, password string, n int, duration time.Duration, xattr bool, base64 bool) {
	var indexNames []string
	var customizeXattrandBase64params bool
	if indexName != "" {
		customizeXattrandBase64params = true
		indexNames = append(indexNames, indexName)
	} else {
		getIndexNames(nodeAddress, username, password, &indexNames)
	}
	var wg sync.WaitGroup
	for _, index := range indexNames {
		wg.Add(1)
		if customizeXattrandBase64params {
			if strings.Contains(index, "xattr") {
				go run(nodeAddress, index, vector, username, password, n, duration, true, base64, &wg)
			} else if strings.Contains(index, "base") {
				go run(nodeAddress, index, vector, username, password, n, duration, xattr, true, &wg)
			} else {
				go run(nodeAddress, index, vector, username, password, n, duration, xattr, base64, &wg)
			}
		} else {
			go run(nodeAddress, index, vector, username, password, n, duration, xattr, base64, &wg)
		}

	}
	wg.Wait()

}
