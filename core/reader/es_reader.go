package reader

import (
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/reader/source"
	"github.com/zilliztech/milvus-migration/core/transform/es/parser"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"strings"
	"time"
)

// ESReader :
type ESReader struct {
	//not read from file(local, remote), is from ES server, so don't use BaseReader params.
	//Cfg   *config.ESConfig
	//BufSize  int
	ESSource *source.ESSource
}

//const PrintSize = 100

func NewESReader(esSource *source.ESSource) *ESReader {
	esr := ESReader{
		//BufSize:  bufSize,
		ESSource: esSource,
	}
	return &esr
}

func (esr *ESReader) BeforePublish() error {
	/*
		in others file/s3 Reader here need to get file/s3 io.reader
		but in es reader, just need es client, so here need to do nothing
			ps: create es client will happen the step of create ESSource.
	*/
	return nil
}

func (esr *ESReader) AfterPublish() error {
	/*
		in others file/s3 Reader here need to close file/s3 io.reader
		here in es reader we need close the es scroll
	*/
	// cannot close, because import the subtask to split big index json file, will close by ESSource self. by: 2023/06/19
	//return esr.ESSource.Close()
	return nil
}

func (esr *ESReader) PublishTo(w io.Writer) (error, *PublishResponse) {
	defer log.Info("[ESReader] write ES success",
		zap.String("urls", strings.Join(esr.ESSource.Cfg.Urls, ",")),
		zap.String("cloudId", esr.ESSource.Cfg.CloudId),
		zap.String("version", esr.ESSource.Cfg.Version),
		//zap.String("security", esr.ESSource.Cfg.Security),
	)
	return esr.writeAll(w)
}

func (esr *ESReader) writeAll(writer io.Writer) (error, *PublishResponse) {
	log.Info("[ESReader] begin to write json data...")
	start := time.Now()

	fileSize := 0 //当前写入json文件的大小
	//var batch = 0       //循环获取数据的次数，debug模式下查看运行速度
	remainData := false // 是否还需要生成新的小json文件
	noData := true      //是否是首次获取数据
	finishRows := 0

	for data := range esr.ESSource.DataChannel {
		if data.IsEmpty {
			break
		}
		var startParseDataTime time.Time
		if common.DEBUG {
			startParseDataTime = time.Now()
		}
		var b []byte
		if noData {
			b = esparser.First2JsonData(&data.Hits, esr.ESSource.IdxCfg)
			noData = false
		} else {
			b = esparser.Next2JsonData(&data.Hits, esr.ESSource.IdxCfg)
		}
		writer.Write(b)
		fileSize += len(b)
		finishRows += esr.ESSource.BatchSize

		if fileSize >= common.SUB_FILE_SIZE {
			remainData = true
			log.Info("[ESReader] Es data reach total size > Max json file size, will create new json file", zap.Int("fileSize", fileSize))
			break
		}
		if common.DEBUG {
			log.Info("[ESReader] 3 Es data parser to Writer ======>", zap.Float64("Cost", time.Since(startParseDataTime).Seconds()))
		}
	}
	if !noData {
		writer.Write(esparser.EndCharacter())
	}
	log.Info("[ESReader] success end to write json data=======>", zap.Float64("Cost", time.Since(start).Seconds()))
	return nil, &PublishResponse{RemainData: remainData, NoData: noData, FinishDataRows: finishRows}
}
