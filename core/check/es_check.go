package check

import (
	"errors"
	esconvert "github.com/zilliztech/milvus-migration/core/convert/es"
	"github.com/zilliztech/milvus-migration/core/type/estype"
)

func VerifyESField(metaJson *estype.MetaJSON) error {

	for _, idx := range metaJson.IdxCfgs {
		if len(idx.FilterFields) <= 0 {
			return errors.New("ES Meta file Index migration Field is empty, IndexName:" + idx.Index)
		}
		for _, f := range idx.FilterFields {
			if _, ok := esconvert.SupportESTypeMap[f.Type]; !ok {
				return errors.New("ES Meta file Index migration Field not support type: " + f.Type)
			}
			if f.Type == string(esconvert.DenseVector) && f.Dims <= 0 {
				return errors.New("ES Meta file Index migration dense_vector type Field dims need > 0")
			}
		}
	}
	return nil
}
