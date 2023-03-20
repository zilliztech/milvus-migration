package reader

import (
	"github.com/zilliztech/milvus-migration/core/common"
)

type DeletedDocsReader struct {
	BaseReader
	deleteCount int
}

const deleteBufferSize = 1024

func (this *DeletedDocsReader) readHead() error {

	// 前8位代表数组长度
	dataSize := this.order.Uint64(this.getInt64Bytes())
	total := dataSize / 4
	this.head = common.CMeta{
		Total:    int(total),
		Type:     "int32",
		NeedRead: int(total),
	}

	this.deleteCount = int(total)
	return nil
}

func (this *DeletedDocsReader) NeedSkipDelete() bool {
	return this.deleteCount != 0
}

func (this *DeletedDocsReader) GetDeleteRows() int {
	return this.deleteCount
}

func (this *DeletedDocsReader) getDeleteIndex() int {
	this.BaseReader.readCnt++
	return int(this.BaseReader.readInt32())
}

func (this *DeletedDocsReader) GetValidDeleteIndex() (int, bool) {
	if !this.hasNext() {
		return 0, false
	}
	return this.getDeleteIndex(), true
}

func NewDeletedDocsReader(fileParam *common.FileParam) *DeletedDocsReader {
	base := NewBaseReader(*fileParam, deleteBufferSize)

	rv := DeletedDocsReader{
		BaseReader: *base,
	}

	return &rv
}
