package network

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

type Direction int

const (
	// DirInbound is for when the remote peer initiated a stream.
	DirInbound = iota
	// DirOutbound is for when the local peer initiated a stream.
	DirOutbound
)

type stream struct {
	stream      network.Stream
	pid         protocol.ID
	sendTimeout time.Duration
	readTimeout time.Duration
}

func newStream(s network.Stream, pid protocol.ID, sendTimeout time.Duration, readTimeout time.Duration) *stream {
	return &stream{
		stream:      s,
		pid:         pid,
		sendTimeout: sendTimeout,
		readTimeout: readTimeout,
	}
}

func (s *stream) close() error {
	return s.stream.Close()
}

func (s *stream) getStream() network.Stream {
	return s.stream
}

func (s *stream) getProtocolID() protocol.ID {
	return s.pid
}

func (s *stream) reset() error {
	return s.stream.Reset()
}

func (s *stream) RemotePeerID() string {
	return s.stream.Conn().RemotePeer().String()
}

func (s *stream) RemotePeerAddr() ma.Multiaddr {
	return s.stream.Conn().RemoteMultiaddr()
}

func (s *stream) AsyncSend(msg []byte) error {
	if err := s.getStream().SetWriteDeadline(time.Now().Add(s.sendTimeout)); err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	writer := NewDelimitedWriter(s.getStream())
	if err := writer.WriteMsg(msg); err != nil {
		return fmt.Errorf("write msg: %w", err)
	}

	return nil
}

func (s *stream) Send(msg []byte) ([]byte, error) {
	if err := s.AsyncSend(msg); err != nil {
		return nil, errors.Wrap(err, "failed on send msg")
	}

	recvMsg, err := waitMsg(s.getStream(), s.readTimeout)
	if err != nil {
		return nil, err
	}

	return recvMsg, nil
}

func (s *stream) Read(timeout time.Duration) ([]byte, error) {
	recvMsg, err := waitMsg(s.getStream(), timeout)
	if err != nil {
		return nil, err
	}

	return recvMsg, nil
}
