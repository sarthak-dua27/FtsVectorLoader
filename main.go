package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"io"
	"log"
	"os"
	"os/exec"
	"archive/tar"
	"compress/gzip"
)


func datasetExists(filePath string) bool {
	// Check if the file exists
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func extractDataset(source string) {

	// Destination directory where the contents will be extracted
	destination := "source/"

	// Open the source file
	file, err := os.Open(source)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create a gzip reader
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		fmt.Println("Error creating gzip reader:", err)
		return
	}
	defer gzipReader.Close()

	// Create a tar reader
	tarReader := tar.NewReader(gzipReader)

	// Iterate through each file in the tar archive
	for {
		header, err := tarReader.Next()

		// If no more files in the archive, break the loop
		if err == io.EOF {
			break
		}

		if err != nil {
			fmt.Println("Error reading tar:", err)
			return
		}

		// Construct the full path for the file
		target := destination + header.Name

		// Create directory if it doesn't exist
		if header.Typeflag == tar.TypeDir {
			err = os.MkdirAll(target, os.FileMode(header.Mode))
			if err != nil {
				fmt.Println("Error creating directory:", err)
				return
			}
			continue
		}

		// Create the file
		file, err := os.Create(target)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer file.Close()

		// Copy file contents from tar to the newly created file
		_, err = io.Copy(file, tarReader)
		if err != nil {
			fmt.Println("Error extracting file:", err)
			return
		}
	}

	fmt.Println("Files extracted successfully!")
}

func downloadDataset(url string, datasetName string) error {
	saveName := datasetName + ".tar.gz"
	// Destination file path
	destination := "raw/" + saveName
	log.Printf("Executing command wget -O %s %s\n", destination, url)
	// Execute wget command
	cmd := exec.Command("wget", "-O", destination, url)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return err
	}
	// Print wget command output
	fmt.Println(string(output))
	fmt.Println("File downloaded successfully!")
	extractDataset(destination)
	return nil
}

func readVectorsFromFile(filepath string) ([][]float32, error) {

	// Open the file for reading
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the dimension of the vector type
	var dimension int32
	err = binary.Read(file, binary.LittleEndian, &dimension)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Dimension is: %d\n", dimension)

	// Calculate the number of vectors in the dataset
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()
	numVectors := fileSize / (4 + int64(dimension*4))
	fmt.Printf("Total number of vectors in dataset: %d\n", numVectors)

	// Reset file cursor to start
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	// Initialize the output vector slice
	outVector := make([][]float32, numVectors)

	// Read vectors from the file
	for i := 0; i < int(numVectors); i++ {
		// Skip the dimension bytes
		_, err := file.Seek(4, 1)
		if err != nil {
			return nil, err
		}

		// Read float values of size 4 bytes of length dimension
		vector := make([]float32, dimension)
		for j := 0; j < int(dimension); j++ {
			var value float32
			err := binary.Read(file, binary.LittleEndian, &value)
			if err != nil {
				return nil, err
			}
			vector[j] = value
		}

		outVector[i] = vector
	}

	return outVector, nil
}


func main()  {
	
	var nodeAddress string
	var bucketName string
	var scopeName string
	var username string
	var password string
	var fieldName string
	var scopeName string
	var collectionName string
	var documentIdPrefix string
	var startIndex int
	var endIndex int
	var batchSize int

	//new additions
	var datasetName string
	var xattrFlag bool
	// var vectorCategory string 
	

	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "", "Bucket name")
	flag.StringVar(&collectionName, "collectionName", "_default", "Collection name")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.StringVar(&fieldName, "fieldName", "vector_data", "fieldName")
	flag.StringVar(&documentIdPrefix, "documentIdPrefix", "", "documentIdPrefix")
	flag.IntVar(&startIndex, "startIndex", 0, "startIndex")
	flag.IntVar(&endIndex, "endIndex", 50, "endIndex")
	flag.IntVar(&batchSize, "batchSize", 100, "batchSize")
	//new additions
	flag.StringVar(&datasetName, "datasetName", "", "Name of the dataset ('sift', 'siftsmall', 'gist')")
	flag.BoolVar(&xattrFlag,"xattrFlag",true,"xattrFlag = true will upsert vectors into xattr (metadata) and false will upsert vectors into document")
	// flag.StringVar(&vectorCategory, "vectorCategory", "learn", "Available categories are learn, base, groundtruth and query")
	flag.Parse()
	// Initialize the Connection
	cluster, err := gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})

	if er != nil {
		panic(fmt.Errorf("error creating cluster object : %v", er))
	}
	bucket := cluster.Bucket(bucketName)

	err := bucket.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		panic(err)
	}

	collection := bucket.DefaultCollection()

	collection := scope.Collection(collectionName)

	//dataset downloading and extraction
	baseUrl := "ftp://ftp.irisa.fr/local/texmex/corpus/"
	datasetUrl := baseUrl + datasetName + ".tar.gz"

	// Check if the dataset file already exists in the "raw/" folder
    if datasetExists("raw/" + datasetName + ".tar.gz") {
        fmt.Println("Dataset file already exists. Skipping download.")
    } else {
        downloadDataset(datasetUrl, datasetName)
    }



	var learn_vecs string = "source/" + datasetName + "/" + datasetName + "_learn.fvecs"

	vectors, err := readVectorsFromFile(learn_vecs)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	//printing a sample value from the output vectors
	fmt.Println("Sample Vector read from output vector list:", vectors[0])


	var wg sync.WaitGroup
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			if xattrFlag {
				go updateDocumentsXattr(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr)
			}else{
				go updateDocumentsField(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr,bucket)
			}

		}
		wg.Wait()
		startIndex = end
	}
}


func updateDocumentsXattr(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData []float32) {
	defer waitGroup.Done()

	type Data struct {
		Sno      int   `json:"sno"`
		Sname     string   `json:"sname"`
		Id string `json:"id"`
	}


	for i := 0; i < 3; i++ {
		_, err := collection.MutateIn(documentID, []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data", vectorData, &gocb.UpsertSpecOptions{
				CreatePath: true,
				IsXattr:    true,
			}),
		},
			nil,
		)
		if err != nil {
			fmt.Printf("Error mutating document: %v Retrying\n", err)
		} else {
			fmt.Println("Done")
			break
		}
	}
}

func updateDocumentsField(waitGroup *sync.WaitGroup, collection *gocb.Collection, documentID string, vectorData []float32, bucket *gocb.Bucket, ind int) {
	defer waitGroup.Done()

	for i := 0; i < 3; i++ {
		mops := []gocb.MutateInSpec{
			gocb.UpsertSpec("vector_data", vectorData, &gocb.UpsertSpecOptions{}),
		}
		_, err := collection.MutateIn(documentID, mops, &gocb.MutateInOptions{
			Timeout: 10050 * time.Millisecond,
		})
		if err != nil {
			panic(err)
		} else {
			fmt.Printf("document ID %v got updated\n",documentID)
		}
	}
}



// waitGroup *sync.WaitGroup
func updateDocumentsXattrbase64(waitGroup *sync.WaitGroup,collection *gocb.Collection, documentID string, vectorData string, ind int) {
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
			panic(err)
		}
	}
}

func updateDocumentsXattrbase64field(waitGroup *sync.WaitGroup,collection *gocb.Collection, documentID string, vectorData string, ind int) {
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
		}
	}
}
