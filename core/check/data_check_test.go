package check

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func Test_VerifyInt32(t *testing.T) {
	data := []int32{0, 1, 2, 3}
	for _, value := range data {
		err := VerifyInt32(value)
		assert.NoError(t, err)
	}

	data = []int32{math.MinInt32, math.MaxInt32}
	for _, value := range data {
		err := VerifyInt32(value)
		assert.NoError(t, err)
	}
}

func Test_VerifyInt64(t *testing.T) {
	data := []int64{0, 1, 2, 3}
	for _, value := range data {
		err := VerifyInt64(value)
		assert.NoError(t, err)
	}

	data = []int64{math.MinInt64, math.MaxInt64}
	for _, value := range data {
		err := VerifyInt64(value)
		assert.NoError(t, err)
	}
}

func Test_VerifyFloats32(t *testing.T) {
	data := []float32{2.5, 32.2, 53.254}
	for _, value := range data {
		err := VerifyFloat32(value)
		assert.NoError(t, err)
	}

	data = []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))}
	for _, value := range data {
		err := VerifyFloat32(value)
		assert.Error(t, err)
	}
}

func Test_VerifyFloats64(t *testing.T) {
	data := []float64{2.5, 32.2, 53.254}
	for _, value := range data {
		err := VerifyFloat64(value)
		assert.NoError(t, err)
	}

	data = []float64{math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, value := range data {
		err := VerifyFloat64(value)
		assert.Error(t, err)
	}

}
