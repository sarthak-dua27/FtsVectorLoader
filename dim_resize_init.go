package main

import (
	"strconv"
	"strings"
	"fmt"
)

func resizeInit(floatListStr string, intListStr string) ([]float32,[]int) {

		var floatList []float32

		floatStrList := strings.Split(floatListStr, ",")
		for _, floatStr := range floatStrList {
			floatVal, err := strconv.ParseFloat(floatStr, 32)
			if err != nil {
				fmt.Printf("Error parsing float value: %v\n", err)
				break 
			}
			floatList = append(floatList, float32(floatVal))
		}
		percentagesToResize := floatList

		var intList []int
		intStrList := strings.Split(intListStr, ",")
		for _, intStr := range intStrList {
			intVal, err := strconv.Atoi(intStr)
			if err != nil {
				fmt.Printf("Error parsing integer value: %v\n", err)
				break
			}
			intList = append(intList, intVal)
		}

		dimensionsForResize := intList

		fmt.Println(dimensionsForResize)
		fmt.Println(percentagesToResize)

		return percentagesToResize, dimensionsForResize

}