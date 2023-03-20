package data

import (
	"github.com/shopspring/decimal"
	"go.uber.org/atomic"
)

type JobStatus string

const (
	JobStatusInit    JobStatus = "init"
	JobStatusRunning JobStatus = "running"
	JobStatusSuccess JobStatus = "success"
	JobStatusFail    JobStatus = "fail"
)

type JobInfo struct {
	JobId       string        `json:"jobId"`
	JobStatus   JobStatus     `json:"jobStatus"`
	JobProcess  int           `json:"jobProcess"`
	Msg         string        `json:"msg"`
	TotalTasks  int           `json:"totalTasks"`
	FinishTasks *atomic.Int64 `json:"finishTasks"`
}

func NewJobInfo(jobId string) *JobInfo {
	return &JobInfo{
		JobId:       jobId,
		JobStatus:   JobStatusInit,
		TotalTasks:  0,
		FinishTasks: atomic.NewInt64(0),
	}
}

func (this *JobInfo) SetJobStatus(status JobStatus, err error) {
	this.JobStatus = status
	if err != nil {
		this.Msg = err.Error()
	}
}

func (this *JobInfo) SetTotalTasks(totalTasks int) {
	this.TotalTasks = totalTasks
	this.JobStatus = JobStatusRunning
}

func (this *JobInfo) AddFinishTasks(increment int) {
	this.FinishTasks.Add(int64(increment))
}

func (this *JobInfo) CalculateJobProcess() {
	if this.TotalTasks == 0 {
		this.JobProcess = 1
		return
	}

	up := decimal.NewFromInt(this.FinishTasks.Load())
	down := decimal.NewFromInt(int64(this.TotalTasks))

	ret := up.DivRound(down, 2).Mul(decimal.NewFromInt(100))
	this.JobProcess = int(ret.IntPart())
}
