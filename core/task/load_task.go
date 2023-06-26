package task

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"go.uber.org/atomic"
)

type TaskLoader struct {
	DataChannel      chan *FileInfo
	CheckChannel     chan *FileInfo
	CusFieldLoader   *loader.CusFieldMilvus2xLoader
	JobId            string
	ProcessTaskCount *atomic.Int32
}
type FileInfo struct {
	fn     string //file name
	cn     string // collection name
	taskId int64  //milvus task Id
}

type CfgInfo struct {
}

type Tasker interface {
	Commit(fileName string, collection string)
	Start(ctx context.Context, idxCfgs []*estype.IdxCfg) error
	Check(ctx context.Context) error
	LoopCheckStateUntilSuc(ctx context.Context, task *FileInfo) error
	LoopCheckBacklog() error
	Close()
}
