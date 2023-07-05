package gstore

import (
	"github.com/zilliztech/milvus-migration/core/data"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
)

func mustGetJobInfo(jobId string) *data.JobInfo {
	val, err := Get(jobId)
	if err != nil {
		log.Error("can not get expect job", zap.String("jobId", jobId))
		panic(err)
	}

	return val.(*data.JobInfo)
}

func GetJobInfo(jobId string) (*data.JobInfo, error) {
	val, err := Get(jobId)
	if err != nil {
		return nil, err
	}

	return val.(*data.JobInfo), nil
}

func NewJobInfo(jobId string) error {
	jobInfo := data.NewJobInfo(jobId)
	err := Add(jobId, jobInfo)
	if err != nil {
		return err
	}
	return nil
}

func NewJobInfoWithSubTask(jobId string) error {
	jobInfo := data.NewJobInfoWithSubTask(jobId)
	err := Add(jobId, jobInfo)
	if err != nil {
		return err
	}
	return nil
}

func RecordJobError(jobId string, err error) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.SetJobStatus(data.JobStatusFail, err)
}

func RecordJobSuccess(jobId string) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.SetJobStatus(data.JobStatusSuccess, nil)
}

func SetTotalTasks(jobId string, totalTasks int) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.SetTotalTasks(totalTasks)
}

func AddFinishTasks(jobId string, increment int) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.AddFinishTasks(increment)
}

func SetProcessInfo(jobId string, processInfo *data.ProcessHandler) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.SetProcessInfo(processInfo)
}

func GetFileSort(jobId string, collection string) int32 {
	jobInfo := mustGetJobInfo(jobId)
	return jobInfo.GetFileSort(collection)
}
func AddFileTask(jobId string, collection string, fileName string) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.AddFileTask(collection, fileName)
}
func FinishFileTask(jobId string, collection string, fileName string) {
	jobInfo := mustGetJobInfo(jobId)
	jobInfo.FinishFileTask(collection, fileName)
}
