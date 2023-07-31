package data

import (
	"context"
	"github.com/shopspring/decimal"
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

	LoadFinish        bool
	LoadTotalFiles    *atomic.Int32 //record files
	LoadUnFinishFiles int32

	lastPercent int
}

func NewProcessHandler() *ProcessHandler {
	return &ProcessHandler{
		DumpFinishSize: atomic.NewInt64(0),

		LoadTotalFiles: atomic.NewInt32(0),
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
		var unLoad = p.LoadUnFinishFiles
		if unLoad == 0 {
			return Half_Percent
		}
		totalLoad := p.LoadTotalFiles.Load()
		up := decimal.NewFromInt(int64(totalLoad - unLoad))
		down := decimal.NewFromInt(int64(totalLoad))
		return Half_Percent + int(up.DivRound(down, 2).Mul(decimal.NewFromInt(Half_Percent)).IntPart())
	} else {
		if p.DumpTotalSize == 0 {
			return One_Percent
		}
		up := decimal.NewFromInt(p.DumpFinishSize.Load())
		down := decimal.NewFromInt(p.DumpTotalSize)
		return int(up.DivRound(down, 2).Mul(decimal.NewFromInt(Half_Percent)).IntPart())
	}
}
