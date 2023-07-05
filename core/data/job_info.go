package data

import (
	"github.com/shopspring/decimal"
	"go.uber.org/atomic"
	"sync"
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

	//key: collectionName/indexName, val: taskInfo,  一个task(index/Collection)可以由多个subTask来生成多个json文件
	TaskMap     map[string]*TaskInfo
	ProcHandler *ProcessHandler
}

type TaskInfo struct {
	//主要是一个collection对应的Json文件处理情况
	//Finish        bool            `json:"Finish"`
	Total         int             `json:"Total"`
	TotalFinish   int             `json:"TotalFinish"`
	NoFinishFiles map[string]bool `json:"NoFinishFiles"` //key: fileName, val: dont care
	FinishFiles   map[string]bool `json:"FinishFiles"`   //key: fileName, val: dont care
	FileSort      *atomic.Int32   `json:"FileSort"`
}

var lockTask = sync.RWMutex{}

func NewJobInfo(jobId string) *JobInfo {
	return &JobInfo{
		JobId:       jobId,
		JobStatus:   JobStatusInit,
		TotalTasks:  0,
		FinishTasks: atomic.NewInt64(0),
	}
}

func NewJobInfoWithSubTask(jobId string) *JobInfo {
	return &JobInfo{
		JobId:       jobId,
		JobStatus:   JobStatusInit,
		TotalTasks:  0,
		FinishTasks: atomic.NewInt64(0),
		TaskMap:     make(map[string]*TaskInfo),
	}
}

func (jobInfo *JobInfo) GetFileSort(collection string) int32 {
	lockTask.Lock()
	defer lockTask.Unlock()
	val := jobInfo.TaskMap[collection]
	if val == nil {
		val = newSubTask()
		jobInfo.TaskMap[collection] = val
	}
	return val.FileSort.Add(1)
}

func (jobInfo *JobInfo) AddFileTask(collection string, fileName string) {
	lockTask.Lock()
	defer lockTask.Unlock()
	val := jobInfo.TaskMap[collection]
	if val == nil {
		val = newSubTask()
		jobInfo.TaskMap[collection] = val
	}
	val.NoFinishFiles[fileName] = true
	val.Total++
}

func (jobInfo *JobInfo) FinishFileTask(collection string, fileName string) {
	lockTask.Lock()
	defer lockTask.Unlock()
	subTask := jobInfo.TaskMap[collection]
	subTask.TotalFinish++
	subTask.FinishFiles[fileName] = true
	delete(subTask.NoFinishFiles, fileName)
}

func newSubTask() *TaskInfo {
	return &TaskInfo{
		FileSort:      atomic.NewInt32(0),
		NoFinishFiles: make(map[string]bool),
		FinishFiles:   make(map[string]bool),
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

func (this *JobInfo) SetProcessInfo(processInfo *ProcessHandler) {
	this.ProcHandler = processInfo
}

func (this *JobInfo) AddFinishTasks(increment int) {
	this.FinishTasks.Add(int64(increment))
}

func (this *JobInfo) CalculateJobProcess() {

	if this.ProcHandler != nil {
		this.JobProcess = this.ProcHandler.CalProcess()
		return
	}

	if this.TotalTasks == 0 {
		this.JobProcess = 1
		return
	}

	up := decimal.NewFromInt(this.FinishTasks.Load())
	down := decimal.NewFromInt(int64(this.TotalTasks))

	ret := up.DivRound(down, 2).Mul(decimal.NewFromInt(100))
	this.JobProcess = int(ret.IntPart())
}
