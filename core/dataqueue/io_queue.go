package dataqueue

import (
	"bufio"
	"io"
)

const (
	oneKB         = 1024
	defaultMBSize = 4
)

type IOQueue struct {
	reader    *io.PipeReader
	writer    *io.PipeWriter
	bufWriter *bufio.Writer
}

func NewIOQueue() *IOQueue {
	return NewIOQueueSize(oneKB * defaultMBSize)
}

func NewIOQueueSize(bufSize int) *IOQueue {
	reader, writer := io.Pipe()
	bufW := bufio.NewWriterSize(writer, bufSize)

	return &IOQueue{
		reader:    reader,
		writer:    writer,
		bufWriter: bufW,
	}
}

func (this *IOQueue) Write(p []byte) (n int, err error) {
	return this.bufWriter.Write(p)
}

func (this *IOQueue) GetReader() io.Reader {
	return this.reader
}

func (this *IOQueue) GetWriter() io.Writer {
	return this.bufWriter
}

func (this *IOQueue) Close() error {
	err := this.bufWriter.Flush()
	if err != nil {
		return err
	}

	return this.writer.Close()
}
