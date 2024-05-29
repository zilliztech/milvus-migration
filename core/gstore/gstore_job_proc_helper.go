package gstore

import (
	"github.com/zilliztech/milvus-migration/core/data"
)

func InitProcessHandler(jobId string, mode string) {
	ph := data.NewProcessHandler(mode)
	SetProcessHandler(jobId, ph)
}

func SetProcessHandler(jobId string, processHandler *data.ProcessHandler) {
	Add(getProcKey(jobId), processHandler)
}
func GetProcessHandler(jobId string) *data.ProcessHandler {
	val, err := Get(getProcKey(jobId))
	if err != nil {
		return nil
	}
	return val.(*data.ProcessHandler)
}

func getProcKey(jobId string) string {
	return jobId + "_proc"
}
