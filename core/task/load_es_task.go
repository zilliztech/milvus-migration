package task

import (
	"context"
	"errors"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/dbclient"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"time"
)

func NewESTaskLoader(cfg *config.MigrationConfig, jobId string) (*TaskLoader, error) {
	cusFieldLoader, err := loader.NewCusFieldMilvus2xLoader(cfg)
	if err != nil {
		return nil, err
	}
	return &TaskLoader{
		DataChannel:      make(chan *FileInfo, 100),
		CheckChannel:     make(chan *FileInfo, 100),
		CusFieldLoader:   cusFieldLoader,
		JobId:            jobId,
		ProcessTaskCount: atomic.NewInt32(0),
	}, nil
}

func (tasker TaskLoader) Close() {
	close(tasker.DataChannel)
}

// Commit : commit a data file to TaskLoader chan for wait to write to milvus2.x
func (tasker TaskLoader) Commit(fileName string, collection string) {
	tasker.DataChannel <- &FileInfo{fn: fileName, cn: collection}
}

// Start : start write data to milvus2.x
func (tasker TaskLoader) Start(ctx context.Context, idxCfgs []*estype.IdxCfg) error {

	defer close(tasker.CheckChannel)

	err := tasker.CusFieldLoader.Before(ctx, idxCfgs)
	if err != nil {
		return err
	}
	for task := range tasker.DataChannel {
		err := tasker.LoopCheckBacklog()
		if err != nil {
			return err
		}
		taskId, err := tasker.CusFieldLoader.Write2Milvus(ctx, task.fn, task.cn)
		if err != nil {
			return err
		}

		count := tasker.ProcessTaskCount.Inc()
		log.Info("[LoadESTasker] Processing Task -------------->", zap.Int32("Count", count),
			zap.String("fileName", task.fn), zap.Int64("taskId", taskId))

		task.taskId = taskId
		tasker.CheckChannel <- task
	}
	return nil
}

// Check : check task progress
func (tasker TaskLoader) Check(ctx context.Context) error {
	for task := range tasker.CheckChannel {
		stateErr := tasker.LoopCheckStateUntilSuc(ctx, task)
		if stateErr != nil {
			return stateErr
		}
		log.Info("[LoadESTasker] Check Task --------------->",
			zap.String("fileName", task.fn), zap.Int64("taskId", task.taskId))
	}
	tasker.CusFieldLoader.After(ctx)
	return nil
}

func (tasker TaskLoader) LoopCheckStateUntilSuc(ctx context.Context, task *FileInfo) error {
	stateErr := tasker.CusFieldLoader.CheckMilvusState(ctx, task.taskId)
	for errors.Is(stateErr, dbclient.InBulkLoadProcess) {
		time.Sleep(common.LOAD_CHECK_BULK_STATE_INTERVAL)
		stateErr = tasker.CusFieldLoader.CheckMilvusState(ctx, task.taskId)
	}
	if stateErr == nil {
		gstore.FinishFileTask(tasker.JobId, task.cn, task.fn) //finish
		tasker.ProcessTaskCount.Dec()
		return nil
	}
	return stateErr
}
func (tasker TaskLoader) LoopCheckBacklog() error {
	count := tasker.ProcessTaskCount.Load()
	for count > 20 {
		time.Sleep(common.LOAD_CHECK_BACKLOG_INTERVAL)
		count = tasker.ProcessTaskCount.Load()
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

//func BacklogProcessTask(cusFieldLoader *loader.CusFieldMilvus2xLoader, ctx context.Context, task *FileInfo) (error, bool) {
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
