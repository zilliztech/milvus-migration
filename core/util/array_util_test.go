package util

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestSplitArray(t *testing.T) {

	origin_arr := []string{"1", "2", "3", "4"}

	splitArray := SplitArray(origin_arr, 2)

	var g errgroup.Group
	for _, arr := range splitArray {
		var finalArr = arr
		g.Go(func() error {
			return print(finalArr)
		})
	}

	assert.NoError(t, g.Wait())
}

func print(arr []string) error {
	fmt.Println(arr)

	return nil
}

func TestSplitArray1(t *testing.T) {

	origin_arr := []string{"1", "2", "3", "4"}

	splitArray := SplitArray(origin_arr, 1)

	var g errgroup.Group
	for _, arr := range splitArray {
		var finalArr = arr
		g.Go(func() error {
			return print(finalArr)
		})
	}

	assert.NoError(t, g.Wait())
}
