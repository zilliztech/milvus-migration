package util

import (
	"fmt"
	"testing"
)

func TestGenerateUUID(t *testing.T) {
	uuid := GenerateUUID("load-")
	fmt.Println(uuid)
}
