package main

import (
	"fmt"
	"os"
	"encoding/binary"
	"flag"
	"sync"
	"time"
	"github.com/couchbase/gocb/v2"
	"io"
	"os/exec"
	"archive/tar"
	"compress/gzip"
	"math/rand"
	"strconv"
	"strings"
	"github.com/go-faker/faker/v4"
	"log"
	"encoding/base64"
	"math"
	"unsafe"
)

func floatsToLittleEndianBytes(floats []float32) []byte {
    byteSlice := make([]byte, len(floats)*4)
    for i, num := range floats {
        bits := math.Float32bits(num)
        *(*uint32)(unsafe.Pointer(&byteSlice[i*4])) = bits
    }
    return byteSlice
}


func datasetExists(filePath string) bool {
    // Check if the file exists
    _, err := os.Stat(filePath)
    return !os.IsNotExist(err)
}


func extractDataset(source string){

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

func downloadDataset(url string, datasetName string){
	saveName := datasetName + ".tar.gz"
    // Destination file path
    destination := "raw/" + saveName
    // Execute wget command
    cmd := exec.Command("wget", "-O", destination, url)
    output, err := cmd.CombinedOutput()
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    // Print wget command output
    fmt.Println(string(output))
    fmt.Println("File downloaded successfully!")
	extractDataset(destination)
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


func resizeVectors(trainVecs [][]float32, percentages []float32, dims []int) error {
	totalVectors := len(trainVecs)

	// Get random indices for vectors to resize
	indicesToResize := rand.Perm(totalVectors)

	if len(percentages) != len(dims) {
		return fmt.Errorf("percentages and dims lists must have the same length")
	}

	var totalPercentage float32
	for _, per := range percentages {
		totalPercentage += per
	}

	if totalPercentage > 1 {
		return fmt.Errorf("total percentage of docs to update should be less than 1")
	}

	for idx, percentage := range percentages {
		vectorsToResize := int(percentage * float32(totalVectors))

		currentIndices := indicesToResize[:vectorsToResize]
		indicesToResize = indicesToResize[vectorsToResize:]

		fmt.Printf("Number of docs resized with dimension %d is %d\n", dims[idx], len(currentIndices))

		for _, index := range currentIndices {
			vector := trainVecs[index]
			currentDim := len(vector)

			// Resize the vector to the desired dimension
			if currentDim < dims[idx] {
				// If the current dimension is less than the desired dimension, repeat the values
				trainVecs[index] = repeatValues(vector, dims[idx])
			} else if currentDim > dims[idx] {
				// If the current dimension is greater than the desired dimension, truncate the vector
				trainVecs[index] = vector[:dims[idx]]
			}
		}
	}

	return nil
}

func repeatValues(vector []float32, targetDim int) []float32 {
	repeatedValues := make([]float32, 0, targetDim)
	for i := 0; i < (targetDim+len(vector)-1)/len(vector); i++ {
		repeatedValues = append(repeatedValues, vector...)
	}
	return repeatedValues[:targetDim]
}

func main()  {
	
	var nodeAddress string
	var bucketName string
	var scopeName string
	var username string
	var password string
	var fieldName string
	var collectionName string
	var documentIdPrefix string
	var startIndex int
	var endIndex int
	var batchSize int

	//new additions
	var datasetName string
	var xattrFlag bool

	var percentagesToResize []float32
	var dimensionsForResize []int
	var floatListStr string
	var intListStr string
	var base64Flag bool
	var capella bool
	// var vectorCategory string 
	


	flag.StringVar(&nodeAddress, "nodeAddress", "", "IP address of the node")
	flag.StringVar(&bucketName, "bucketName", "", "Bucket name")
	flag.StringVar(&scopeName, "scopeName", "", "Scope name")
	flag.StringVar(&collectionName, "collectionName", "_default", "Collection name")
	flag.StringVar(&username, "username", "", "username")
	flag.StringVar(&password, "password", "", "password")
	flag.StringVar(&fieldName, "fieldName", "vector_data", "fieldName")
	flag.StringVar(&documentIdPrefix, "documentIdPrefix", "", "documentIdPrefix")
	flag.IntVar(&startIndex, "startIndex", 0, "startIndex")
	flag.IntVar(&endIndex, "endIndex", 50, "endIndex")
	flag.IntVar(&batchSize, "batchSize", 600, "batchSize")
	flag.BoolVar(&capella, "capella", false, "pushing docs to capella?")
	//new additions
	flag.StringVar(&datasetName, "datasetName", "", "Name of the dataset ('sift', 'siftsmall', 'gist')")
	flag.BoolVar(&xattrFlag,"xattrFlag",true,"xattrFlag = true will upsert vectors into xattr (metadata) and false will upsert vectors into document")
	flag.StringVar(&floatListStr, "percentagesToResize", "", "Comma-separated list of float32 values")
	flag.StringVar(&intListStr, "dimensionsForResize", "", "Comma-separated list of int values")
	flag.BoolVar(&base64Flag,"base64Flag",false,"true results in, embeddings get uploaded as base64 strings")
	// flag.StringVar(&vectorCategory, "vectorCategory", "learn", "Available categories are learn, base, groundtruth and query")

	flag.Parse()



	// connectionString := "couchbases://cb.us-mkjdqvlcpxghs.nonprod-project-avengers.com"
	// Initialize the Connection
	// cluster, err := gocb.Connect(connectionString, gocb.ClusterOptions{
	// 	Authenticator: gocb.PasswordAuthenticator{
	// 		Username: username,
	// 		Password: password,
	// 	},
	// })

	// if err != nil {
	// 	panic(fmt.Errorf("error creating cluster object : %v", err))
	// }
	// bucket := cluster.Bucket(bucketName)

	// err = bucket.WaitUntilReady(15*time.Second, nil)
	// if err != nil {
	// 	panic(err)
	// }
	// collection := bucket.Scope(scopeName).Collection(collectionName)
	// collection := bucket.DefaultCollection()



	var cluster *gocb.Cluster
	var er error
	if capella {
		options := gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
			SecurityConfig: gocb.SecurityConfig{
				TLSSkipVerify: true,
			},
		}
		if err := options.ApplyProfile(gocb.
			ClusterConfigProfileWanDevelopment); err != nil {
			log.Fatal(err)
		}
		cluster, er = gocb.Connect(nodeAddress, options)
	} else {
		cluster, er = gocb.Connect("couchbase://"+nodeAddress, gocb.ClusterOptions{
			Authenticator: gocb.PasswordAuthenticator{
				Username: username,
				Password: password,
			},
		})
	}

	if er != nil {
		panic(fmt.Errorf("error creating cluster object : %v", er))
	}
	bucket := cluster.Bucket(bucketName)

	err := bucket.WaitUntilReady(15*time.Second, nil)
	if err != nil {
		panic(err)
	}

	scope := bucket.Scope(scopeName)

	collection := scope.Collection(collectionName)




	if floatListStr != "" {
		// isResize = true

		var floatList []float32

		floatStrList := strings.Split(floatListStr, ",")
		for _, floatStr := range floatStrList {
			floatVal, err := strconv.ParseFloat(floatStr, 32)
			if err != nil {
				fmt.Printf("Error parsing float value: %v\n", err)
				return
			}
			floatList = append(floatList, float32(floatVal))
		}
		percentagesToResize = floatList

		var intList []int
		intStrList := strings.Split(intListStr, ",")
		for _, intStr := range intStrList {
			intVal, err := strconv.Atoi(intStr)
			if err != nil {
				fmt.Printf("Error parsing integer value: %v\n", err)
				return
			}
			intList = append(intList, intVal)
		}

		dimensionsForResize = intList

		fmt.Println(dimensionsForResize)
		fmt.Println(percentagesToResize)

	}


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



	var learn_vecs string = "source/" + datasetName + "/" + datasetName + "_base.fvecs"

	vectors, err := readVectorsFromFile(learn_vecs)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}


	if floatListStr != ""{
		resizeVectors(vectors,percentagesToResize,dimensionsForResize)
	}


	var encodedVectors []string
	if base64Flag {
		for _, vector := range vectors {
			byteSlice := floatsToLittleEndianBytes(vector)
   			base64String := base64.StdEncoding.EncodeToString(byteSlice)
			encodedVectors = append(encodedVectors, base64String)
		}
	}

	var wg sync.WaitGroup
	for startIndex != endIndex {
		end := startIndex + batchSize
		if end > endIndex {
			end = endIndex
		}
		wg.Add(end - startIndex)
		for j := startIndex; j < end; j++ {
			if xattrFlag {
				if base64Flag {
					vectArr := encodedVectors[j]
					go updateDocumentsXattrbase64(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr,j)
				} else {
					vectArr := vectors[j]
					go updateDocumentsXattr(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr,j)
				}
				
			}else{
				if base64Flag {
					vectArr := encodedVectors[j]
					go updateDocumentsXattrbase64field(&wg, collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr,j)
				} else {
					vectArr := vectors[j]
					go updateDocumentsField(&wg,collection, fmt.Sprintf("%s%d", documentIdPrefix, j), vectArr,bucket,j)
				}	
				
			}
			
		}
		wg.Wait()
		startIndex = end
	}
}

// waitGroup *sync.WaitGroup
func updateDocumentsXattr(waitGroup *sync.WaitGroup,collection *gocb.Collection, documentID string, vectorData []float32, ind int) {
	defer waitGroup.Done()

	type Data struct {
		Sno      int   `json:"sno"`
		Sname     string   `json:"sname"`
		Id string `json:"id"`
	}


	for i := 0; i < 3; i++ {
		var _, err = collection.Upsert(documentID ,
		Data{
			Sno : ind, 
			Sname : faker.Name(),
			Id  : documentID,
		}, nil)
		if err != nil {
			log.Fatal(err)
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
				// fmt.Println("Done")
				fmt.Printf("xattrs of the document ID %v got updated\n", documentID)
				break
			}
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