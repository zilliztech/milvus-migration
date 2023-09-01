package task

import (
	"context"
)

type FileInfo struct {
	fn     string //file name
	cn     string // collection name
	taskId int64  //milvus task Id
}

type InitTasker interface {
	//Init(ctx context.Context, loader *loader.CustomMilvus2xLoader) error
	Init(ctx context.Context) error
}

type LoadTasker interface {
	CloseDataChannel()
	CloseCheckChannel()
	// Commit : commit a data file to BaseLoadTasker chan for wait to write to milvus2.x
	CommitData(fileInfo *FileInfo)
	CommitCheck(task *FileInfo, taskId int64)
	incTaskCount(ctx context.Context, task *FileInfo, taskId int64)
	// Check : check task progress
	Check(ctx context.Context) error
	GetDataChannel() chan *FileInfo
	//GetMilvusLoader() *loader.CustomMilvus2xLoader
	Write(ctx context.Context, fileName string, collection string) (int64, error)
	LoopCheckBacklog() error
	LoopCheckStateUntilSuc(ctx context.Context, task *FileInfo) error
}
