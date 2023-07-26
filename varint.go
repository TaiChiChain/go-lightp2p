package network

import (
	"bufio"
	"encoding/binary"
	"io"
)

type Writer interface {
	WriteMsg([]byte) error
}

type WriteCloser interface {
	Writer
	io.Closer
}

type Reader interface {
	ReadMsg() ([]byte, error)
}

type ReadCloser interface {
	Reader
	io.Closer
}

func NewDelimitedWriter(w io.Writer) WriteCloser {
	return &varintWriter{w}
}

type varintWriter struct {
	w io.Writer
}

func (w *varintWriter) WriteMsg(data []byte) (err error) {
	length := uint64(len(data))
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, length)
	_, err = w.w.Write(lenBuf[:n])
	if err != nil {
		return err
	}
	_, err = w.w.Write(data)
	return err
}

func (w *varintWriter) Close() error {
	if closer, ok := w.w.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func NewDelimitedReader(r io.Reader, maxSize int) ReadCloser {
	var closer io.Closer
	if c, ok := r.(io.Closer); ok {
		closer = c
	}
	return &varintReader{bufio.NewReader(r), maxSize, closer}
}

type varintReader struct {
	r       *bufio.Reader
	maxSize int
	closer  io.Closer
}

func (r *varintReader) ReadMsg() ([]byte, error) {
	length64, err := binary.ReadUvarint(r.r)
	if err != nil {
		return nil, err
	}
	length := int(length64)
	if length < 0 || length > r.maxSize {
		return nil, io.ErrShortBuffer
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *varintReader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}
