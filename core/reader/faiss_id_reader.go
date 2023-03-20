package reader

import (
	"fmt"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
)

type FaissIdReader struct {
	BaseReader
	dataDim      int
	clusterArray []int
}

// construction
func NewFaissIdReader(fileParam *common.FileParam, bufSize int) *FaissIdReader {
	base := NewBaseReader(*fileParam, bufSize)
	fr := FaissIdReader{
		BaseReader: *base,
	}
	return &fr
}

// vital function
func (this *FaissIdReader) BeforePublish() error {
	// init file source
	return this.initFileSource()
}

func (this *FaissIdReader) AfterPublish() error {
	// close file
	return this.closeFileSource()
}

func (this *FaissIdReader) PublishTo(w io.Writer) error {
	defer log.Info("[FaissIdReader] write faiss-id file success", zap.String("file", this.FileFullName()))

	err := this.readHead()
	if err != nil {
		return err
	}

	// write head
	err = this.pushHeadTo(w)
	if err != nil {
		return err
	}

	return this.pushIdList(w)
}

func (this *FaissIdReader) pushHeadTo(w io.Writer) error {
	// write head
	head, err := this.convertHead()
	if err != nil {
		return err
	}
	_, err = w.Write(head)
	if err != nil {
		log.Error("[FaissIdReader] push head error", zap.String("fileName", this.FileFullName()), zap.Error(err))
		return err
	}

	log.Info("[FaissIdReader] publish head finish", zap.String("fileName", this.FileFullName()))
	return nil
}

// write id list
func (this *FaissIdReader) pushIdList(w io.Writer) error {
	log.Info("[FaissDataReader] begin to write id list")

	float32Byte := 4
	for _, objectCount := range this.clusterArray {
		// skip data
		this.skipKByte(objectCount * this.dataDim * float32Byte)

		// get real data
		for i := 0; i < objectCount; i++ {
			_, err := w.Write(this.getInt64Bytes())
			if err != nil {
				return err
			}
		}
	}

	log.Info("[FaissDataReader] end to write id list")
	return nil
}

func (this *FaissIdReader) SetReadSources(source ReadSource) {
	this.setFileSource(source)
}

func (this *FaissIdReader) readHead() error {
	err := this.readAutoIndexHeader(true)
	if err != nil {
		return err
	}

	// reshape
	this.head.Type = "int64"
	this.head.Dim = 0
	return nil
}

func (this *FaissIdReader) readAutoIndexHeader(recordHead bool) error {
	// read header
	headType := string(this.read4Byte())
	log.Info("[FaissIdReader] headType is ", zap.String("headType", headType))
	switch headType {
	case "IwFl":
		return this.readIvfHeader(recordHead)
	case "IxFI", "IxF2", "IxFl":
		return this.readIndexHeader(recordHead)
	default:
		return fmt.Errorf("this tool only supports faiss flat and ivf_flat index files")
	}
}

func (this *FaissIdReader) readIndexHeader(recordHead bool) error {
	dim := this.readUint32()
	log.Info("[FaissIdReader] readIndexHeader dim", zap.Uint32("dim", dim))

	ntotal := this.readUint64()
	log.Info("[FaissIdReader] readIndexHeader ntotal ", zap.Uint64("ntotal", ntotal))
	if recordHead {
		this.dataDim = int(dim)
		this.head.Row = int(ntotal)
		this.head.Total = this.head.Dim * this.head.Row
		this.head.NeedRead = this.head.Total
	}

	// dummy
	this.skipKByte(8)
	// dummy
	this.skipKByte(8)
	// isTraind
	this.skipKByte(1)
	// metricType
	this.skipKByte(4)

	return nil
}

func (this *FaissIdReader) readIvfHeader(recordHead bool) error {
	// read common
	err := this.readIndexHeader(recordHead)
	if err != nil {
		return err
	}

	nlist := this.readUint64()
	log.Info("[FaissIdReader] readIvfHeader nlist", zap.Uint64("nlist", nlist))
	nprobe := this.readUint64()
	log.Info("[FaissIdReader] readIvfHeader nprobe", zap.Uint64("nprobe", nprobe))

	err = this.skipClusterIndex()
	if err != nil {
		return err
	}
	err = this.skipDirectMap()
	if err != nil {
		return err
	}
	return this.readInvertedLists()
}

func (this *FaissIdReader) skipClusterIndex() error {
	// skipByte header
	err := this.readAutoIndexHeader(false)
	if err != nil {
		return err
	}

	codeSize := this.readUint64()
	log.Info("[FaissIdReader] skipClusterIndex", zap.Uint64("codeSize", codeSize))

	// skipByte clusterData
	this.skipKByte(int(codeSize) * 4)

	return nil
}

func (this *FaissIdReader) skipDirectMap() error {
	// directMap.dmType
	dmType := this.readUint8()
	if dmType != 0 {
		return fmt.Errorf("[FaissIdReader] directMap dmType must be 0")
	}
	dmSize := this.readUint64()
	if dmSize != 0 {
		return fmt.Errorf("[FaissIdReader] directMap dmSize must be 0")
	}

	return nil
}

func (this *FaissIdReader) readInvertedLists() error {
	head := string(this.read4Byte())
	log.Info("[FaissIdReader] readInvertedLists head", zap.String("head", head))
	if head != "ilar" {
		return fmt.Errorf("[FaissIdReader] readInvertedLists not support headType %s", head)
	}

	nlist := this.readUint64()
	log.Info("[FaissIdReader] readInvertedLists nlist", zap.Uint64("nlist", nlist))
	listSize := this.readUint64()
	log.Info("[FaissIdReader] readInvertedLists listSize", zap.Uint64("listSize", listSize))

	return this.readClusterArray()
}

// get cluster data array
func (this *FaissIdReader) readClusterArray() error {
	clusterType := string(this.read4Byte())
	log.Info("[FaissIdReader] readClusterArray clusterType", zap.String("clusterType", clusterType))
	switch clusterType {
	case "full":
		return this.readClusterFullType()
	case "sprs":
		return this.readClusterSprsType()
	default:
		return fmt.Errorf("not support invlist clusterType", clusterType)
	}
}

func (this *FaissIdReader) readClusterFullType() error {
	clusterSize := int(this.readUint64())
	log.Info("[FaissIdReader] readClusterFullType clusterSize", zap.Int("clusterSize", clusterSize))

	clusters := make([]int, clusterSize)
	for i := 0; i < clusterSize; i++ {
		clusters[i] = int(this.readUint64())
	}

	this.clusterArray = clusters
	log.Info("[FaissIdReader] readClusterFullType clusterArraySize", zap.Int("clusterArraySize", len(this.clusterArray)))
	if len(this.clusterArray) == 0 {
		return fmt.Errorf("[FaissIdReader] readClusterFullType error, find clusterArray is empty, means no data")
	}
	return nil
}

// @Beta
func (this *FaissIdReader) readClusterSprsType() error {
	clusterSize := int(this.readUint64())
	log.Info("[FaissIdReader] readClusterSprsType clusterSize", zap.Int("clusterSize", clusterSize))

	clusters := make([]int, clusterSize)
	for i := 0; i < clusterSize; i++ {
		clusters[i] = int(this.readUint64())
	}

	// start from index 1, step is 2
	for i := 1; i < clusterSize; i = i + 2 {
		this.clusterArray = append(this.clusterArray, clusters[i])
	}
	log.Info("[FaissIdReader] readClusterSprsType clusterArraySize", zap.Int("clusterArraySize", len(this.clusterArray)))
	if len(this.clusterArray) == 0 {
		return fmt.Errorf("[FaissIdReader] readClusterSprsType error, find clusterArray is empty, means no data")
	}
	return nil
}
