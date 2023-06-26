package reader

import (
	"bufio"
	"encoding/binary"
	"github.com/zilliztech/milvus-migration/core/check"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/transform/numpy"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"math"
)

const defaultKBSize = 1024

const oneKB = 1024

type PublishResponse struct {
	NoData     bool //本次生成文件是否有数据
	RemainData bool //还有剩余数据没读取完
}
type Publisher interface {
	BeforePublish() error
	PublishTo(w io.Writer) (error, *PublishResponse)
	AfterPublish() error
}

type ReadSource interface {
	GetReader() (io.Reader, error)
	Close() error
}

type BaseReader struct {
	reader  *bufio.Reader
	readCnt int

	// self
	fileParam  common.FileParam
	fileSource ReadSource
	bufSize    int
	head       common.CMeta
	byte1      []byte
	byte4      []byte
	byte8      []byte
	order      binary.ByteOrder
}

func (this *BaseReader) FileFullName() string {
	return this.fileParam.FileFullName
}

func (this *BaseReader) BucketName() string {
	return this.fileParam.BucketName
}

func (this *BaseReader) FileDir() string {
	return this.fileParam.FileDir
}

// base Method
func (this *BaseReader) getBoolBytes() []byte {
	return this.read1Byte()
}

func (this *BaseReader) getFloat32Bytes() []byte {
	bs := this.read4Byte()
	data := math.Float32frombits(this.order.Uint32(bs))
	if err := check.VerifyFloat32(data); err != nil {
		panic(err)
	}
	return bs
}

func (this *BaseReader) getFloat64Bytes() []byte {
	bs := this.read8Byte()
	data := math.Float64frombits(this.order.Uint64(bs))
	if err := check.VerifyFloat64(data); err != nil {
		panic(err)
	}
	return bs
}

func (this *BaseReader) getInt32Bytes() []byte {
	bs := this.read4Byte()
	data := int32(this.order.Uint32(bs))
	if err := check.VerifyInt32(data); err != nil {
		panic(err)
	}
	return bs
}

func (this *BaseReader) getInt64Bytes() []byte {
	bs := this.read8Byte()
	data := int64(this.order.Uint64(bs))
	if err := check.VerifyInt64(data); err != nil {
		panic(err)
	}
	return bs
}

func (this *BaseReader) readInt32() int32 {
	ret := this.read4Byte()
	data := int32(this.order.Uint32(ret))
	return data
}

func (this *BaseReader) readUint8() uint8 {
	return this.read1Byte()[0]
}

func (this *BaseReader) readUint32() uint32 {
	return this.order.Uint32(this.read4Byte())
}

func (this *BaseReader) readUint64() uint64 {
	return this.order.Uint64(this.read8Byte())
}

func (this *BaseReader) readInt64() int64 {
	return int64(this.readUint64())
}

func (this *BaseReader) readFloat32() float32 {
	return math.Float32frombits(this.readUint32())
}

func (this *BaseReader) read4Byte() (ret []byte) {
	_, err := io.ReadFull(this.reader, this.byte4)
	if err != nil {
		log.Error("read4Byte error", zap.Error(err))
		panic(err)
	}
	return this.byte4
}

func (this *BaseReader) getKByte(k int) []byte {
	kBytes := make([]byte, k)
	_, err := io.ReadFull(this.reader, this.byte4)
	if err != nil {
		log.Error("readKByte error", zap.Int("kBytes", k), zap.Error(err))
		panic(err)
	}
	return kBytes
}

func (this *BaseReader) read8Byte() (ret []byte) {
	_, err := io.ReadFull(this.reader, this.byte8)
	if err != nil {
		log.Error("read8Byte error", zap.Error(err))
		panic(err)
	}
	return this.byte8
}

func (this *BaseReader) read1Byte() (ret []byte) {
	_, err := io.ReadFull(this.reader, this.byte1)
	if err != nil {
		log.Error("read1Byte error", zap.Error(err))
		panic(err)
	}
	return this.byte1
}

func (this *BaseReader) skipKByte(num int) {
	for {
		if num == 0 {
			return
		}
		if num > 0 {
			skip, err := this.reader.Discard(num)
			if err != nil {
				log.Error("baseReader Discard error", zap.Error(err), zap.Int("num", num))
				panic(err)
			}
			num -= skip
		}
	}
}

func (this *BaseReader) convertHead() ([]byte, error) {
	return npconvert.ConvertToNumpyHead(this.head)
}

func (this *BaseReader) SetReadSources(source ReadSource, deleteSource ReadSource) {
	panic("Must implement by child")
}

func (this *BaseReader) NextData() []byte {
	panic("Must implement by child")
}

func (this *BaseReader) hasNext() bool {
	return this.readCnt != this.head.NeedRead
}

func (this *BaseReader) setFileSource(source ReadSource) {
	this.fileSource = source
}

func (this *BaseReader) initFileSource() error {
	r, err := this.fileSource.GetReader()
	if err != nil {
		return err
	}

	this.reader = bufio.NewReaderSize(r, this.bufSize)
	return nil
}

func (this *BaseReader) closeFileSource() error {
	err := this.fileSource.Close()
	if err != nil {
		return err
	}
	return nil
}

func NewBaseReader(fileParam common.FileParam, bufSize int) *BaseReader {

	if bufSize == 0 {
		bufSize = defaultKBSize
	}

	base := BaseReader{
		readCnt:   0,
		byte1:     make([]byte, 1),
		byte4:     make([]byte, 4),
		byte8:     make([]byte, 8),
		order:     binary.LittleEndian, //serialize byte order
		fileParam: fileParam,
		bufSize:   bufSize * oneKB,
	}

	return &base
}
