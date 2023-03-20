package util

import (
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"testing"
)

var (
	segColTest = &milvustype.SegColInfo{
		CollectionName: "col",
		SegmentName:    "seg",
	}
	talesDir  = "tables"
	outputDir = "target/"
)

func TestGetAddressAndPortFromEndpoint(t *testing.T) {

	endPoint := "localhost:9001"

	address, port, err := GetAddressAndPortFromEndpoint(endPoint)

	assert.NoError(t, err)

	assert.Equal(t, address, "localhost")
	assert.Equal(t, port, "9001")
}

func TestGenerateFaissDataFilePath(t *testing.T) {
	collection := "test"

	dir, file := GenerateFaissDataFilePath(outputDir, collection)
	assert.Equal(t, dir, "target/test")
	assert.Equal(t, file, "target/test/data.npy")
}

func TestGenerateFaissIdFilePath(t *testing.T) {
	collection := "test"

	dir, file := GenerateFaissIdFilePath(outputDir, collection)
	assert.Equal(t, dir, "target/test")
	assert.Equal(t, file, "target/test/id.npy")
}

func TestGetSourceRVFilePath(t *testing.T) {
	path := GetSourceRVFilePath(talesDir, segColTest)
	assert.Equal(t, "tables/col/seg/seg.rv", path)
}

func TestGetSourceDeletedDocsFilePath(t *testing.T) {
	path := GetSourceDeletedDocsFilePath(talesDir, segColTest)
	assert.Equal(t, "tables/col/seg/deleted_docs", path)
}

func TestGetOutputUIDFilePath(t *testing.T) {
	targetDir, targetFileName := GetOutputUIDFilePath(outputDir, segColTest)
	assert.Equal(t, "target/tables/col/seg", targetDir)
	assert.Equal(t, "target/tables/col/seg/id.npy", targetFileName)
}

func TestGetOutputRVFilePath(t *testing.T) {
	targetDir, targetFileName := GetOutputRVFilePath(outputDir, segColTest)
	assert.Equal(t, "target/tables/col/seg", targetDir)
	assert.Equal(t, "target/tables/col/seg/data.npy", targetFileName)
}
