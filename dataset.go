package main 

import (
	"os"
	"fmt"
	"os/exec"
	"io"
	"archive/tar"
	"compress/gzip"

)

func datasetExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}


func extractDataset(source string) {
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




func downloadDataset(url string, datasetName string) {
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