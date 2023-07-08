package task

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/dbclient"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"time"
)

type BaseLoadTasker struct {
	DataChannel       chan *FileInfo
	CheckChannel      chan *FileInfo
	CusFieldLoader    *loader.CustomMilvus2xLoader
	JobId             string
	BulkInsertingNums *atomic.Int32
}

func NewBaseLoadTasker(cusFieldLoader *loader.CustomMilvus2xLoader, jobId string) *BaseLoadTasker {
	loadTasker := &BaseLoadTasker{
		DataChannel:       make(chan *FileInfo, 100),
		CheckChannel:      make(chan *FileInfo, 100),
		CusFieldLoader:    cusFieldLoader,
		JobId:             jobId,
		BulkInsertingNums: atomic.NewInt32(0),
	}
	return loadTasker
}

func (tasker BaseLoadTasker) CloseDataChannel() {
	close(tasker.DataChannel)
	//当dump结束后
	gstore.GetProcessHandler(tasker.JobId).SetDumpFinished()
}
func (tasker BaseLoadTasker) CloseCheckChannel() {
	close(tasker.CheckChannel)
}

func (tasker BaseLoadTasker) GetDataChannel() chan *FileInfo {
	return tasker.DataChannel
}

// Commit : commit a data file to BaseLoadTasker chan for wait to write to milvus2.x
func (tasker BaseLoadTasker) CommitData(fileInfo *FileInfo) {
	tasker.DataChannel <- fileInfo
}

func (tasker BaseLoadTasker) CommitCheck(task *FileInfo, taskId int64) {
	task.taskId = taskId
	tasker.CheckChannel <- task
}

func (tasker BaseLoadTasker) incTaskCount(ctx context.Context, task *FileInfo, taskId int64) {
	count := tasker.BulkInsertingNums.Inc()
	gstore.GetProcessHandler(tasker.JobId).SetUnLoadSize(count, ctx)
	log.Info("[LoadTasker] Inc Task Processing-------------->", zap.Int32("Count", count),
		zap.String("fileName", task.fn), zap.Int64("taskId", taskId))
}

func (tasker BaseLoadTasker) GetMilvusLoader() *loader.CustomMilvus2xLoader {
	return tasker.CusFieldLoader
}

// Check : check task progress
func (tasker BaseLoadTasker) Check(ctx context.Context) error {
	for task := range tasker.CheckChannel {
		stateErr := tasker.LoopCheckStateUntilSuc(ctx, task)
		if stateErr != nil {
			return stateErr
		}
		log.Info("[LoadTasker] Progress Task --------------->",
			zap.String("fileName", task.fn), zap.Int64("taskId", task.taskId))
	}
	tasker.CusFieldLoader.After(ctx)
	gstore.GetProcessHandler(tasker.JobId).SetLoadFinished()
	return nil
}

func (tasker BaseLoadTasker) LoopCheckStateUntilSuc(ctx context.Context, task *FileInfo) error {
	stateErr := tasker.CusFieldLoader.CheckMilvusState(ctx, task.taskId)
	for errors.Is(stateErr, dbclient.InBulkLoadProcess) {
		time.Sleep(common.LOAD_CHECK_BULK_STATE_INTERVAL)
		stateErr = tasker.CusFieldLoader.CheckMilvusState(ctx, task.taskId)
	}
	if stateErr == nil {
		gstore.FinishFileSubTask(tasker.JobId, task.cn, task.fn) //finish
		count := tasker.BulkInsertingNums.Dec()
		gstore.GetProcessHandler(tasker.JobId).SetUnLoadSize(count, ctx)
		log.Info("[LoadTasker] Dec Task Processing-------------->", zap.Int32("Count", count),
			zap.String("fileName", task.fn), zap.Int64("taskId", task.taskId))
		return nil
	}
	return stateErr
}
func (tasker BaseLoadTasker) LoopCheckBacklog() error {
	count := tasker.BulkInsertingNums.Load()
	for count > 20 {
		time.Sleep(common.LOAD_CHECK_BACKLOG_INTERVAL)
		count = tasker.BulkInsertingNums.Load()
	}
	return nil
}

//func (tasker LoadESTasker) LoopCheckBacklog(ctx context.Context, task *FileInfo) error {
//	err, backlog := BacklogProcessTask(cusFieldLoader, ctx, task)
//	if err != nil {
//		return err
//	}
//	for backlog {
//		time.Sleep(time.Second * 10)
//		err, backlog = BacklogProcessTask(cusFieldLoader, ctx, task)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}

//func BacklogProcessTask(cusFieldLoader *Loader.CustomMilvus2xLoader, ctx context.Context, task *FileInfo) (error, bool) {
//	stateList, err := cusFieldLoader.CusMilvus2x.Milvus2x.GetMilvus().ListBulkInsertTasks(ctx, task.cn, 30)
//	if err != nil {
//		return err, true
//	}
//	var processCount = 0
//	for _, state := range stateList {
//		if state.State != entity.BulkInsertCompleted {
//			processCount++
//		}
//		if processCount >= 20 {
//			return err, true
//		}
//	}
//	return nil, false
//}
