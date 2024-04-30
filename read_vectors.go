package main

import (
	"fmt"
	"os"
	"encoding/binary"
)

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