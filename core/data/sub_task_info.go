package data

import (
	"go.uber.org/atomic"
)

type FileTask struct {
	//key: collectionName/indexName, val: taskInfo,  一个task(index/Collection)可以由多个subTask来生成多个json文件
	TaskMap map[string]*SubFileTask
}

type SubFileTask struct {
	//主要是一个collection对应的Json文件处理情况
	//Finish        bool            `json:"Finish"`
	Total         int             `json:"Total"`
	TotalFinish   int             `json:"TotalFinish"`
	NoFinishFiles map[string]bool `json:"NoFinishFiles"` //key: fileName, val: dont care
	FinishFiles   map[string]bool `json:"FinishFiles"`   //key: fileName, val: dont care
	FileSort      *atomic.Int32   `json:"FileSort"`
}

func (all *FileTask) GetFileSort(collection string) int32 {
	lockTask.Lock()
	defer lockTask.Unlock()
	val := all.TaskMap[collection]
	if val == nil {
		val = newSubTask()
		all.TaskMap[collection] = val
	}
	return val.FileSort.Add(1)
}

func (all *FileTask) AddFileTask(collection string, fileName string) {
	lockTask.Lock()
	defer lockTask.Unlock()
	val := all.TaskMap[collection]
	if val == nil {
		val = newSubTask()
		all.TaskMap[collection] = val
	}
	val.NoFinishFiles[fileName] = true
	val.Total++
}

func (all *FileTask) FinishFileTask(collection string, fileName string) {
	lockTask.Lock()
	defer lockTask.Unlock()
	subTask := all.TaskMap[collection]
	subTask.TotalFinish++
	subTask.FinishFiles[fileName] = true
	delete(subTask.NoFinishFiles, fileName)
}
