package reader

import (
	"errors"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
)

type UIDReader struct {
	BaseReader
	currentIndex  int
	deletedReader *DeletedDocsReader
}

func (this *UIDReader) readHead() error {
	dataSize := this.order.Uint64(this.getInt64Bytes())
	total := dataSize / 8
	this.head = common.CMeta{
		Total:    int(total),
		Dim:      0,
		Row:      int(total),
		Type:     "int64",
		NeedRead: int(total),
	}

	if this.head.Total != this.head.Row {
		msg := fmt.Sprintf("check total != row, total=%d, rows=%d", this.head.Total, this.head.Row)
		log.Error(msg)
		return errors.New(msg)
	}

	return nil
}

func (this *UIDReader) SetReadSources(source ReadSource, deleteSource ReadSource) {
	this.setFileSource(source)
	this.deletedReader.setFileSource(deleteSource)
}

func (this *UIDReader) NextData() []byte {
	this.BaseReader.readCnt++
	return this.BaseReader.getInt64Bytes()
}

func (this *UIDReader) pushData(w io.Writer) error {
	_, err := w.Write(this.NextData())
	if err != nil {
		log.Error("[UIDReader] push data error", zap.Error(err))
		return err
	}
	return nil
}

func (this *UIDReader) skipData() {
	this.NextData()
}

func NewUidReaderWithDelete(fileParam *common.FileParam, deleteFile *common.FileParam, bufSize int) *UIDReader {
	base := NewBaseReader(*fileParam, bufSize)

	rv := UIDReader{
		BaseReader:   *base,
		currentIndex: 0,
	}

	deletedReader := NewDeletedDocsReader(deleteFile)
	rv.deletedReader = deletedReader

	return &rv
}

func (this *UIDReader) BeforePublish() error {
	// init file source
	err := this.initFileSource()
	if err != nil {
		return err
	}

	// init delete file source
	return this.deletedReader.initFileSource()
}

func (this *UIDReader) AfterPublish() error {
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
func (this *UIDReader) PublishTo(w io.Writer) (error, *PublishResponse) {
	return this.publishTo(w), nil
}
func (this *UIDReader) publishTo(w io.Writer) error {
	defer log.Info("[UIDReader] write file success", zap.String("file", this.FileFullName()))

	// read delete info
	err := this.deletedReader.readHead()
	if err != nil {
		return err
	}

	// high speed read
	if !this.deletedReader.NeedSkipDelete() {
		log.Warn("[UIDReader] find empty deleted_docs, will write all data", zap.String("file", this.deletedReader.FileFullName()))
		return this.publishDirectTo(w)
	}

	// slow speed read
	rows := this.deletedReader.GetDeleteRows()

	err = this.readHead()
	if err != nil {
		return err
	}
	this.head.DeleteOffset = rows

	// write head
	err = this.pushHeadTo(w)
	if err != nil {
		return err
	}

	// write body
	for {
		skipIndex, exist := this.deletedReader.GetValidDeleteIndex()
		if exist {
			if this.currentIndex == skipIndex {
				// skip
				this.skipData()
				this.currentIndex++
			} else {
				err := this.pushData(w)
				if err != nil {
					return err
				}
			}
		} else {
			return this.pushDataLoopTo(w)
		}
	}
}

func (this *UIDReader) publishDirectTo(w io.Writer) error {
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
		log.Error("[UIDReader] push head error", zap.Error(err))
		return err
	}
	log.Info("[UIDReader] publish head finish", zap.String("fileName", this.FileFullName()))

	// write body
	for this.hasNext() {
		_, err := w.Write(this.NextData())
		if err != nil {
			log.Error("[UIDReader] push body error", zap.Error(err))
			return err
		}
	}
	log.Info("[UIDReader] publish body finish", zap.String("fileName", this.FileFullName()))

	return nil
}

func (this *UIDReader) pushHeadTo(w io.Writer) error {
	// write head
	head, err := this.convertHead()
	if err != nil {
		return err
	}
	_, err = w.Write(head)
	if err != nil {
		log.Error("[UIDReader] push head error", zap.String("fileName", this.FileFullName()), zap.Error(err))
		return err
	}

	log.Info("[RVReader] publish head finish", zap.String("fileName", this.FileFullName()))
	return nil
}

func (this *UIDReader) pushDataLoopTo(w io.Writer) error {
	// write body
	for this.hasNext() {
		_, err := w.Write(this.NextData())
		if err != nil {
			log.Error("[UIDReader] push data error", zap.Error(err))
			return err
		}
	}

	log.Info("[UIDReader] publish body finish", zap.String("fileName", this.FileFullName()))
	return nil
}
