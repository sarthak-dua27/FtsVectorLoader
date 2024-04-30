package main

import (
	"unsafe"
	"math"
)

func floatsToLittleEndianBytes(floats []float32) []byte {
	byteSlice := make([]byte, len(floats)*4)
	for i, num := range floats {
		bits := math.Float32bits(num)
		*(*uint32)(unsafe.Pointer(&byteSlice[i*4])) = bits
	}
	return byteSlice
}