package task

import (
	"context"
)

type Submitter struct {
	Loader LoadTasker
	Initer InitTasker
}

func NewSubmitter(loader LoadTasker, initer InitTasker) *Submitter {
	return &Submitter{
		Loader: loader,
		Initer: initer,
	}
}

func (submiter Submitter) Close() {
	submiter.Loader.CloseDataChannel()
}
func (submiter Submitter) Commit(fileName string, collection string) {
	submiter.Loader.CommitData(&FileInfo{fn: fileName, cn: collection})
}
func (submiter Submitter) Progress(ctx context.Context) error {
	return submiter.Loader.Check(ctx)
}

// Start : start write data to milvus2.x
func (submiter Submitter) Start(ctx context.Context) error {

	defer submiter.Loader.CloseCheckChannel()

	err := submiter.Initer.Init(ctx)
	if err != nil {
		return err
	}
	for task := range submiter.Loader.GetDataChannel() {

		err := submiter.Loader.LoopCheckBacklog()
		if err != nil {
			return err
		}
		taskId, err := submiter.Loader.Write(ctx, task.fn, task.cn)
		if err != nil {
			return err
		}

		submiter.Loader.incTaskCount(ctx, task, taskId)

		submiter.Loader.CommitCheck(task, taskId)
	}
	return nil
}
