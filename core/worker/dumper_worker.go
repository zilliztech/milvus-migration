package worker

import (
	"context"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/dataqueue"
	"github.com/zilliztech/milvus-migration/core/reader"
	"github.com/zilliztech/milvus-migration/core/reader/source"
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
	return NewDumperWorkerWithChannel(config, nil)
}

func NewDumperWorkerWithChannel(config config.DumperWorkConfig, channel *source.ChannelSource) (*DumperWorker, error) {
	rr, err := newReader(config.InnerReadCfg, channel)
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
	err, _ := this.WorkWithResponse(ctx)
	return err
}

func (this *DumperWorker) WorkWithResponse(ctx context.Context) (error, *reader.PublishResponse) {
	err, response := this.pipeline4ReadAndWrite(ctx)
	// async to gc
	go runtime.GC() // todo:  need GC ??
	return err, response
}

func (this *DumperWorker) pipeline4ReadAndWrite(ctx context.Context) (error, *reader.PublishResponse) {

	dq := this.dataQueue

	g, subCtx := errgroup.WithContext(ctx) //todo: subCtx?
	var response *reader.PublishResponse
	g.Go(func() error {
		err, response0 := produce(subCtx, this.reader, dq)
		response = response0
		return err
	})

	g.Go(func() error {
		return consume(subCtx, this.writer, dq)
	})

	return g.Wait(), response
}

func produce(ctx context.Context, pb reader.Publisher, dq *dataqueue.IOQueue) (error, *reader.PublishResponse) {
	defer func(dq *dataqueue.IOQueue) {
		//flush pipe writer buffer
		err := dq.Close()
		if err != nil {
			log.Error("Flush and Close pipe writer error!", zap.Error(err))
		}
		log.Info("Flush and Close pipe writer.")
	}(dq)

	defer func() {
		//close source reader
		err := pb.AfterPublish()
		if err != nil {
			log.Error("close source reader error", zap.Error(err))
		}
		log.Info("Closed source reader.")
	}()

	err := pb.BeforePublish()
	if err != nil {
		return err, nil
	}

	//write to pipe writer buffer
	err, response := pb.PublishTo(dq)
	if err != nil {
		return err, response
	}
	return nil, response
}

func consume(ctx context.Context, cv writer.Receiver, dq *dataqueue.IOQueue) error {
	if err := cv.Execute(ctx, dq.GetReader()); err != nil {
		log.Error("consume execute fail", zap.Error(err))
		return err
	}

	return nil
}
