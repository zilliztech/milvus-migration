package migration

import (
	"encoding/json"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/gstore"
)

func PrintStartJobMessage(jobId string) {
	jobInfo, _ := gstore.GetJobInfo(jobId)
	val, _ := json.Marshal(&jobInfo)
	fmt.Printf("Migration JobInfo: %s\n", string(val))

	//milvus, faiss not these info:
	//procInfo := gstore.GetProcessHandler(jobId)
	//val, _ = json.Marshal(&procInfo)
	//fmt.Printf("Migration ProcessInfo: %s, Process:%d\n", string(val), procInfo.CalcProcess())
	//
	//fileTaskInfo := gstore.GetFileTask(jobId)
	//val, _ = json.Marshal(&fileTaskInfo)
	//fmt.Printf("Migration FileTaskInfo:  %s\n", string(val))
}
