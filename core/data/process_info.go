package data

import (
	"github.com/shopspring/decimal"
	"go.uber.org/atomic"
)

type ProcessHandler struct {
	DumpFinish     bool
	DumpTotalSize  int64
	DumpFinishSize *atomic.Int64

	LoadFinish      bool
	LoadTotalFiles  int
	LoadFinishFiles *atomic.Int64
}

func NewProcessHandler() *ProcessHandler {
	return &ProcessHandler{
		DumpFinishSize:  atomic.NewInt64(0),
		LoadFinishFiles: atomic.NewInt64(0),
	}
}
func (p *ProcessHandler) SetDumpFinished() {
	p.DumpFinish = true
}
func (p *ProcessHandler) SetLoadFinished() {
	p.LoadFinish = true
}
func (p *ProcessHandler) SetDumpTotalSize(totalSize int64) {
	p.DumpTotalSize = totalSize
}
func (p *ProcessHandler) AddDumpFinishSize(increment int) {
	p.DumpFinishSize.Add(int64(increment))
}

func (p *ProcessHandler) SetLoadTotalSize(totalSize int) {
	p.LoadTotalFiles = totalSize
}
func (p *ProcessHandler) AddLoadFinishSize(increment int) {
	if p.DumpFinish {
		p.LoadFinishFiles.Add(int64(increment))
	}
}

func (p *ProcessHandler) CalProcess() int {
	if p.LoadFinish {
		return 100
	}
	if p.DumpFinish {
		if p.LoadTotalFiles == 0 {
			return 50
		}
		up := decimal.NewFromInt(p.LoadFinishFiles.Load())
		down := decimal.NewFromInt(int64(p.LoadTotalFiles))
		return 50 + int(up.DivRound(down, 2).Mul(decimal.NewFromInt(50)).IntPart())
	} else {
		if p.DumpTotalSize == 0 {
			return 1
		}
		up := decimal.NewFromInt(p.DumpFinishSize.Load())
		down := decimal.NewFromInt(p.DumpTotalSize)
		return int(up.DivRound(down, 2).Mul(decimal.NewFromInt(50)).IntPart())
	}
}
