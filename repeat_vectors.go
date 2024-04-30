package main


func repeatValues(vector []float32, targetDim int) []float32 {
	repeatedValues := make([]float32, 0, targetDim)
	for i := 0; i < (targetDim+len(vector)-1)/len(vector); i++ {
		repeatedValues = append(repeatedValues, vector...)
	}
	return repeatedValues[:targetDim]
}