package util

import (
	"encoding/json"
	"github.com/zilliztech/milvus-migration/core/type/milvustype"
	"io"
)

func GetMetaCols(r io.Reader) (*milvustype.MetaJSON, error) {
	jsonData, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var metaJson milvustype.MetaJSON
	if err := json.Unmarshal(jsonData, &metaJson); err != nil {
		return nil, err
	}
	return &metaJson, nil
}
