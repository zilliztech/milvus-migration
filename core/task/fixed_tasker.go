package task

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"time"
)

/*
FixedBaseLoadTasker : fixed collection dump&load task
*/
type FixedBaseLoadTasker struct {
	DataChannel       chan *FileInfo
	CheckChannel      chan *FileInfo
	Loader            *loader.Milvus2xLoader
	JobId             string
	BulkInsertingNums *atomic.Int32
}

func NewFixedBaseLoadTasker(loader *loader.Milvus2xLoader, jobId string) *FixedBaseLoadTasker {
	loadTasker := &FixedBaseLoadTasker{
		DataChannel:       make(chan *FileInfo, 100),
		CheckChannel:      make(chan *FileInfo, 100),
		Loader:            loader,
		JobId:             jobId,
		BulkInsertingNums: atomic.NewInt32(0),
	}
	return loadTasker
}

func (tasker FixedBaseLoadTasker) CloseDataChannel() {
	close(tasker.DataChannel)
	//当dump结束后
	//gstore.GetProcessHandler(tasker.JobId).SetDumpFinished()
}
func (tasker FixedBaseLoadTasker) CloseCheckChannel() {
	close(tasker.CheckChannel)
}

func (tasker FixedBaseLoadTasker) GetDataChannel() chan *FileInfo {
	return tasker.DataChannel
}

// Commit : commit a data file to BaseLoadTasker chan for wait to write to milvus2.x
func (tasker FixedBaseLoadTasker) CommitData(fileInfo *FileInfo) {
	tasker.DataChannel <- fileInfo
}

func (tasker FixedBaseLoadTasker) CommitCheck(task *FileInfo, taskId int64) {
	task.taskId = taskId
	tasker.CheckChannel <- task
}

func (tasker FixedBaseLoadTasker) incTaskCount(ctx context.Context, task *FileInfo, taskId int64) {
	count := tasker.BulkInsertingNums.Inc()
	//gstore.GetProcessHandler(tasker.JobId).SetUnLoadSize(count, ctx)
	log.Info("[LoadTasker] Inc Task Processing-------------->", zap.Int32("Count", count),
		zap.String("fileName", task.fn), zap.Int64("taskId", taskId))
}

//func (tasker FixedBaseLoadTasker) GetMilvusLoader() *loader.CustomMilvus2xLoader {
//	return tasker.Loader
//}

func (tasker FixedBaseLoadTasker) Write(ctx context.Context, fileName string, collection string) (int64, error) {
	//return tasker.Loader
	return 0, nil
}

// Check : check task progress
func (tasker FixedBaseLoadTasker) Check(ctx context.Context) error {
	for task := range tasker.CheckChannel {
		stateErr := tasker.LoopCheckStateUntilSuc(ctx, task)
		if stateErr != nil {
			return stateErr
		}
		log.Info("[LoadTasker] Progress Task --------------->",
			zap.String("fileName", task.fn), zap.Int64("taskId", task.taskId))
	}
	//tasker.Loader.After(ctx)
	gstore.GetProcessHandler(tasker.JobId).SetLoadFinished()
	return nil
}

func (tasker FixedBaseLoadTasker) LoopCheckStateUntilSuc(ctx context.Context, task *FileInfo) error {
	//stateErr := tasker.Loader.CheckMilvusState(ctx, task.taskId)
	//for errors.Is(stateErr, dbclient.InBulkLoadProcess) {
	//	time.Sleep(common.LOAD_CHECK_BULK_STATE_INTERVAL)
	//	stateErr = tasker.Loader.CheckMilvusState(ctx, task.taskId)
	//}
	//if stateErr == nil {
	//	gstore.FinishFileSubTask(tasker.JobId, task.cn, task.fn) //finish
	//	count := tasker.BulkInsertingNums.Dec()
	//	gstore.GetProcessHandler(tasker.JobId).SetUnLoadSize(count, ctx)
	//	log.Info("[LoadTasker] Dec Task Processing-------------->", zap.Int32("Count", count),
	//		zap.String("fileName", task.fn), zap.Int64("taskId", task.taskId))
	//	return nil
	//}
	//return stateErr
	return nil
}
func (tasker FixedBaseLoadTasker) LoopCheckBacklog() error {
	count := tasker.BulkInsertingNums.Load()
	for count > 20 {
		time.Sleep(common.LOAD_CHECK_BACKLOG_INTERVAL)
		count = tasker.BulkInsertingNums.Load()
	}
	return nil
}
