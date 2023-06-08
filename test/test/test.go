package main

import (
	"fmt"
	"os"
)

func main() {

	var str = "s3://" + "xxx"
	targetDir := "s3:" + string(os.PathSeparator) + string(os.PathSeparator) + str
	fmt.Println(targetDir)

	//test_es7()
	//test_es8()
}
