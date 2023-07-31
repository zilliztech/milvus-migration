package gstore

import "github.com/zilliztech/milvus-migration/core/data"

func InitFileTask(jobId string) {
	allTask := &data.FileTask{
		TaskMap: make(map[string]*data.SubFileTask),
	}
	Add(getFileTaskKey(jobId), allTask)
}
func GetFileTask(jobId string) *data.FileTask {
	val, err := Get(getFileTaskKey(jobId))
	if err != nil {
		return nil
	}
	return val.(*data.FileTask)
}

func getFileTaskKey(jobId string) string {
	return jobId + "_file"
}

func GetFileSort(jobId string, collection string) int32 {
	return GetFileTask(jobId).GetFileSort(collection)
}
func AddFileSubTask(jobId string, collection string, fileName string) {
	GetFileTask(jobId).AddFileTask(collection, fileName)
}
func FinishFileSubTask(jobId string, collection string, fileName string) {
	GetFileTask(jobId).FinishFileTask(collection, fileName)
}
