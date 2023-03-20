package gstore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBasic(t *testing.T) {

	Init()
	err := Add("1", "2")
	assert.NoError(t, err)

	val, err := GetString("1")
	assert.NoError(t, err)

	assert.Equal(t, val, "2")

}
