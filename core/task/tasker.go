package task

import (
	"context"
)

func NewTasker(loader LoadTasker, initer InitTasker) *Tasker {
	return &Tasker{
		Loader: loader,
		Initer: initer,
	}
}

func (tasker Tasker) Close() {
	tasker.Loader.CloseDataChannel()
}
func (tasker Tasker) Commit(fileName string, collection string) {
	tasker.Loader.CommitData(&FileInfo{fn: fileName, cn: collection})
}
func (tasker Tasker) Progress(ctx context.Context) error {
	return tasker.Loader.Check(ctx)
}

// Start : start write data to milvus2.x
func (tasker Tasker) Start(ctx context.Context) error {

	defer tasker.Loader.CloseCheckChannel()

	err := tasker.Initer.Init(ctx, tasker.Loader.GetMilvusLoader())
	if err != nil {
		return err
	}
	for task := range tasker.Loader.GetDataChannel() {

		err := tasker.Loader.LoopCheckBacklog()
		if err != nil {
			return err
		}
		taskId, err := tasker.Loader.GetMilvusLoader().Write2Milvus(ctx, task.fn, task.cn)
		if err != nil {
			return err
		}

		tasker.Loader.incTaskCount(task, taskId)

		tasker.Loader.CommitCheck(task, taskId)
	}
	return nil
}
