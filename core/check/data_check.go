package check

import (
	"fmt"
	"math"
)

func VerifyInt32(i int32) error {
	if i > math.MaxInt32 || i < math.MinInt32 {
		return fmt.Errorf("int32 value is out of range")
	}

	return nil
}

func VerifyInt64(i int64) error {
	if i > math.MaxInt64 || i < math.MinInt64 {
		return fmt.Errorf("int64 value is out of range")
	}

	return nil
}

func VerifyFloat64(value float64) error {
	if math.IsNaN(value) {
		return fmt.Errorf("float value is not a number")
	}

	if math.IsInf(value, -1) || math.IsInf(value, 1) {
		return fmt.Errorf("float value is infinity")
	}

	return nil
}

func VerifyFloat32(value float32) error {
	return VerifyFloat64(float64(value))
}
