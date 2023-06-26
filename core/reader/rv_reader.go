package reader

import (
	"errors"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
)

type RVReader struct {
	BaseReader
	dim           int
	currentIndex  int
	arrayN        int
	deletedReader *DeletedDocsReader
}

func (this *RVReader) readHead() error {
	dataSize := this.order.Uint64(this.getInt64Bytes())
	total := dataSize / 4
	this.head = common.CMeta{
		Total:    int(total),
		Dim:      this.dim,
		Row:      int(total) / this.dim,
		Type:     "float32",
		NeedRead: int(total),
	}

	if this.head.Total != (this.head.Row * this.dim) {
		msg := fmt.Sprintf("check total != row*dim, total=%d, rows*dim=%d", this.head.Total, this.head.Row)
		log.Error(msg)
		return errors.New(msg)
	}

	// scalar dim is zero, but need read by 1
	if this.dim == 0 {
		this.arrayN = 1
	} else {
		this.arrayN = this.dim
	}

	return nil
}

func (this *RVReader) SetReadSources(source ReadSource, deleteSource ReadSource) {
	this.setFileSource(source)
	this.deletedReader.setFileSource(deleteSource)
}

func (this *RVReader) NextData() []byte {
	this.BaseReader.readCnt++
	return this.BaseReader.getFloat32Bytes()
}

func (this *RVReader) pushNData(w io.Writer) error {
	for i := 0; i < this.arrayN; i++ {
		_, err := w.Write(this.NextData())
		if err != nil {
			log.Error("[RVReader] push data error", zap.Error(err))
			return err
		}
	}

	return nil
}

func (this *RVReader) skipNData() {
	for i := 0; i < this.arrayN; i++ {
		this.NextData()
	}
}

func NewRVReaderWithDelete(fileParam *common.FileParam, deleteFile *common.FileParam, bufSize int, dim int) *RVReader {
	base := NewBaseReader(*fileParam, bufSize)

	rv := RVReader{
		BaseReader: *base,
		dim:        dim,
	}

	deletedReader := NewDeletedDocsReader(deleteFile)
	rv.deletedReader = deletedReader

	return &rv
}

func (this *RVReader) BeforePublish() error {
	// init file source
	err := this.initFileSource()
	if err != nil {
		return err
	}

	// init delete file source
	return this.deletedReader.initFileSource()
}

func (this *RVReader) AfterPublish() error {
	// close file
	err := this.closeFileSource()
	if err != nil {
		return err
	}
	err = this.deletedReader.closeFileSource()
	if err != nil {
		return err
	}
	return nil
}

func (this *RVReader) PublishTo(w io.Writer) (error, *PublishResponse) {
	defer log.Info("[RVReader] write file success", zap.String("file", this.FileFullName()))

	// read delete info
	err := this.deletedReader.readHead()
	if err != nil {
		return err, nil
	}

	// high speed read
	if !this.deletedReader.NeedSkipDelete() {
		log.Warn("[RVReader] find empty deleted_docs, will write all data", zap.String("file", this.deletedReader.FileFullName()))
		return this.publishDirectTo(w), nil
	}

	// slow speed read
	rows := this.deletedReader.GetDeleteRows()

	err = this.readHead()
	if err != nil {
		return err, nil
	}
	this.head.DeleteOffset = rows

	// write head
	err = this.pushHeadTo(w)
	if err != nil {
		return err, nil
	}

	// write body
	for {
		skipIndex, exist := this.deletedReader.GetValidDeleteIndex()
		if exist {
			if this.currentIndex == skipIndex {
				// skip
				this.skipNData()
				this.currentIndex++
			} else {
				err := this.pushNData(w)
				if err != nil {
					return err, nil
				}
			}
		} else {
			return this.pushDataLoopTo(w), nil
		}
	}
}

func (this *RVReader) publishDirectTo(w io.Writer) error {
	err := this.readHead()
	if err != nil {
		return err
	}

	// write head
	head, err := this.convertHead()
	if err != nil {
		return err
	}
	_, err = w.Write(head)
	if err != nil {
		log.Error("[RVReader] push head error", zap.Error(err))
		return err
	}
	log.Info("[RVReader] publish head finish", zap.String("fileName", this.FileFullName()))

	// write body
	for this.hasNext() {
		_, err := w.Write(this.NextData())
		if err != nil {
			log.Error("[RVReader] push data error", zap.Error(err))
			return err
		}
	}
	log.Info("[RVReader] publish body finish", zap.String("fileName", this.FileFullName()))

	return nil
}

func (this *RVReader) pushHeadTo(w io.Writer) error {
	// write head
	head, err := this.convertHead()
	if err != nil {
		return err
	}
	_, err = w.Write(head)
	if err != nil {
		log.Error("[RVReader] push head error", zap.String("fileName", this.FileFullName()), zap.Error(err))
		return err
	}

	log.Info("[RVReader] publish head finish", zap.String("fileName", this.FileFullName()))
	return nil
}

func (this *RVReader) pushDataLoopTo(w io.Writer) error {
	// write body
	for this.hasNext() {
		_, err := w.Write(this.NextData())
		if err != nil {
			log.Error("[RVReader] push body error", zap.String("fileName", this.FileFullName()), zap.Error(err))
			return err
		}
	}

	log.Info("[RVReader] publish body finish", zap.String("fileName", this.FileFullName()))
	return nil
}
