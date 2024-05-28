package internal

import (
	"fmt"
	"math/rand"
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
		field = "$xattrs.vector_data"
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
	fmt.Println(result["status"], "Total Hits:", result["total_hits"])

}

func RunQueriesPerSecond(nodeAddress string, indexName string, vector [][]float32, username string, password string, n int, duration time.Duration, xattr bool, base64 bool) {
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
