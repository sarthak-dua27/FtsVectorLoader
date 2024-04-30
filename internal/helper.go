package internal

import (
	"math"
	"os"
	"unsafe"
)

func FloatsToLittleEndianBytes(floats []float32) []byte {
	byteSlice := make([]byte, len(floats)*4)
	for i, num := range floats {
		bits := math.Float32bits(num)
		*(*uint32)(unsafe.Pointer(&byteSlice[i*4])) = bits
	}
	return byteSlice
}

func DatasetExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}
