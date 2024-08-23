package data

import (
	"context"
	"github.com/shopspring/decimal"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const One_Percent = 1
const Half_Percent = 50
const Hundred_Percent = 100

type ProcessHandler struct {
	DumpFinish     bool
	DumpTotalSize  int64
	DumpFinishSize *atomic.Int64 //record rows

	LoadFinish bool
	//es record files
	LoadTotalFiles    *atomic.Int32 //record files
	LoadUnFinishFiles int32
	//milvus2x record load nums
	LoadTotalSize  int64
	LoadFinishSize *atomic.Int64

	lastPercent int
	mode        string
}

func NewProcessHandler(mode string) *ProcessHandler {
	return &ProcessHandler{
		DumpFinishSize: atomic.NewInt64(0),
		LoadTotalFiles: atomic.NewInt32(0),
		LoadFinishSize: atomic.NewInt64(0),
		mode:           mode,
	}
}

func (p *ProcessHandler) SetLoadFinished() {
	p.LoadFinish = true
}
func (p *ProcessHandler) SetDumpTotalSize(totalSize int64) {
	p.DumpTotalSize = totalSize
}
func (p *ProcessHandler) AddDumpedSize(increment int, ctx context.Context) {
	p.DumpFinishSize.Add(int64(increment))
	p.LoadTotalFiles.Inc()
	log.LL(ctx).Info("=================>JobProcess!", zap.Int("Percent", p.CalcProcess()))
}

func (p *ProcessHandler) SetDumpFinished() {
	p.DumpFinish = true
}
func (p *ProcessHandler) SetUnLoadSize(processingCount int32, ctx context.Context) {
	if p.DumpFinish {
		p.LoadUnFinishFiles = processingCount
		log.LL(ctx).Info("=================>JobProcess", zap.Int("Percent", p.CalcProcess()))
	}
}

func (p *ProcessHandler) SetLoadTotalSize(totalSize int64) {
	p.LoadTotalSize = totalSize
}
func (p *ProcessHandler) AddLoadSize(increment int, ctx context.Context) {
	p.LoadFinishSize.Add(int64(increment))
	log.LL(ctx).Info("=================>JobProcess!!", zap.Int("Percent", p.CalcProcess()))
}

func (p *ProcessHandler) CalcProcess() int {
	percent := p.calcProcess0()
	if percent > p.lastPercent {
		p.lastPercent = percent
	}
	return p.lastPercent
}
func (p *ProcessHandler) calcProcess0() int {
	if p.LoadFinish {
		return Hundred_Percent
	}
	if p.DumpFinish {
		if common.DumpMode(p.mode) == common.Elasticsearch {
			return calcByLoadFilesProc(p)
		} else if common.DumpMode(p.mode) == common.Milvus2x {
			return calcByInsertDataProc(p)
		} else {
			return Half_Percent
		}
	} else {
		return calcDumpProc(p)
	}
}

func calcDumpProc(p *ProcessHandler) int {
	if p.DumpTotalSize == 0 {
		return One_Percent
	}
	up := decimal.NewFromInt(p.DumpFinishSize.Load())
	down := decimal.NewFromInt(p.DumpTotalSize)
	percent := int(up.DivRound(down, 2).Mul(decimal.NewFromInt(Half_Percent)).IntPart())
	if percent > Half_Percent {
		return Half_Percent
	}
	return percent
}

func calcByInsertDataProc(p *ProcessHandler) int {
	//fmt.Printf("xxxxxxxx: (%d) ", p.LoadTotalSize)
	//数据量很少情况下，count milvus collection可能会返回为0
	if p.LoadTotalSize <= 0 {
		return One_Percent
	}

	loadSize := p.LoadFinishSize.Load()
	if loadSize == 0 {
		return Half_Percent
	}
	up := decimal.NewFromInt(loadSize)
	down := decimal.NewFromInt(p.LoadTotalSize)
	percent := Half_Percent + int(up.DivRound(down, 2).Mul(decimal.NewFromInt(Half_Percent)).IntPart())
	//milvus count 接口不是准确的rows, 这样的话可能出现percent > 100情况
	if percent > Hundred_Percent {
		return Hundred_Percent
	}
	return percent
}

func calcByLoadFilesProc(p *ProcessHandler) int {
	var unLoad = p.LoadUnFinishFiles
	if unLoad == 0 {
		return Half_Percent
	}
	totalLoad := p.LoadTotalFiles.Load()
	up := decimal.NewFromInt(int64(totalLoad - unLoad))
	down := decimal.NewFromInt(int64(totalLoad))
	return Half_Percent + int(up.DivRound(down, 2).Mul(decimal.NewFromInt(Half_Percent)).IntPart())
}
