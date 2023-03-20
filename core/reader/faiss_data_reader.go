package reader

import (
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
)

type FaissDataReader struct {
	FaissIdReader
}

// construction
func NewFaissDataReader(fileParam *common.FileParam, bufSize int) *FaissDataReader {
	fir := NewFaissIdReader(fileParam, bufSize)
	fdr := FaissDataReader{
		FaissIdReader: *fir,
	}
	return &fdr
}

func (this *FaissDataReader) readHead() error {
	err := this.readAutoIndexHeader(true)
	if err != nil {
		return err
	}

	// reshape
	this.head.Type = "float32"
	this.head.Dim = this.dataDim
	return nil
}

func (this *FaissDataReader) PublishTo(w io.Writer) error {
	defer log.Info("[FaissDataReader] write faiss-data file success", zap.String("file", this.FileFullName()))

	err := this.readHead()
	if err != nil {
		return err
	}

	// write head
	err = this.pushHeadTo(w)
	if err != nil {
		return err
	}

	return this.pushDataList(w)
}

// write data list
func (this *FaissDataReader) pushDataList(w io.Writer) error {
	log.Info("[FaissDataReader] begin to write data list")

	int64Byte := 8
	for _, objectCount := range this.clusterArray {
		// get real data
		allFloats := objectCount * this.dataDim
		for i := 0; i < allFloats; i++ {
			_, err := w.Write(this.getFloat32Bytes())
			if err != nil {
				return err
			}
		}

		// skip id
		this.skipKByte(objectCount * int64Byte)
	}

	log.Info("[FaissDataReader] end to write data list")
	return nil
}

// Deprecated
// async to store dim
func (this *FaissDataReader) storeDim() {
	gstore.AddTempCollectionDim(this.head.Dim)
}
