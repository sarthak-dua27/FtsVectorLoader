package internal

import (
	"fmt"
	"math/rand"
)

func ResizeVectors(trainVecs [][]float32, percentages []float32, dims []int) error {
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
