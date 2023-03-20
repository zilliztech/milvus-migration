package worker

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/dataqueue"
	"github.com/zilliztech/milvus-migration/core/reader"
	"github.com/zilliztech/milvus-migration/core/writer"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"runtime"
)

// 负责拉通读写
type DumperWorker struct {
	dataQueue *dataqueue.IOQueue
	reader    reader.Publisher
	writer    writer.Receiver
}

func NewDumperWorker(config config.DumperWorkConfig) (*DumperWorker, error) {
	rr, err := newReader(config.InnerReadCfg)
	if err != nil {
		return nil, err
	}

	wr, err := newWriter(config.InnerWriteCfg)
	if err != nil {
		return nil, err
	}

	dq := dataqueue.NewIOQueue()

	wok := &DumperWorker{
		dataQueue: dq,
		reader:    rr,
		writer:    wr,
	}
	return wok, nil
}

func (this *DumperWorker) Work(ctx context.Context) error {
	err := this.pipeline4ReadAndWrite(ctx)
	if err != nil {
		return err
	}

	// async to gc
	go runtime.GC()
	return nil
}

func (this *DumperWorker) pipeline4ReadAndWrite(ctx context.Context) error {

	dq := this.dataQueue

	g, subCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return produce(subCtx, this.reader, dq)
	})

	g.Go(func() error {
		return consume(subCtx, this.writer, dq)
	})

	return g.Wait()
}

func produce(ctx context.Context, pb reader.Publisher, dq *dataqueue.IOQueue) error {
	defer func(dq *dataqueue.IOQueue) {
		err := dq.Close()
		if err != nil {
			log.Error("close produce error", zap.Error(err))
		}
	}(dq)

	err := pb.BeforePublish()
	if err != nil {
		return err
	}

	err = pb.PublishTo(dq)
	if err != nil {
		return err
	}

	err = pb.AfterPublish()
	if err != nil {
		return err
	}

	return err
}

func consume(ctx context.Context, cv writer.Receiver, dq *dataqueue.IOQueue) error {
	if err := cv.Execute(ctx, dq.GetReader()); err != nil {
		log.Error("consume execute fail", zap.Error(err))
		return err
	}

	return nil
}
