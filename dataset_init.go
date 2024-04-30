package main

import (
	"fmt"

)

func databaseInit(datasetName string) string{
	//dataset downloading and extraction
	baseUrl := "ftp://ftp.irisa.fr/local/texmex/corpus/"
	datasetUrl := baseUrl + datasetName + ".tar.gz"

	// Check if the dataset file already exists in the "raw/" folder
	if datasetExists("raw/" + datasetName + ".tar.gz") {
		fmt.Println("Dataset file already exists. Skipping download.")
	} else {
		fmt.Println("Downloading the dataset")
		downloadDataset(datasetUrl, datasetName)
	}

	return "source/" + datasetName + "/" + datasetName + "_base.fvecs"
}