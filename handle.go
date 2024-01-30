package network

import (
	"fmt"
	"io"
	"time"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-msgio"
	"github.com/pkg/errors"
)

func (p2p *P2P) handleMessage(s *stream) error {
	if err := s.getStream().SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("set read deadline failed: %w", err)
	}
	reader := msgio.NewVarintReaderSize(s.getStream(), network.MessageSizeMax)
	msg, err := reader.ReadMsg()
	if err != nil {
		if err != io.EOF {
			if err := s.reset(); err != nil {
				p2p.logger.WithField("error", err).Error("Reset stream")
			}

			return errors.Wrap(err, "failed on read msg")
		}

		return nil
	}

	msg, err = decompressMsg(msg)
	if err != nil {
		p2p.logger.WithField("error", err).Error("Handle msg decompress error")
		return errors.Errorf("Decompress receive msg error")
	}

	if p2p.config.enableMetrics {
		recvDataSize.Add(float64(len(msg)))
	}

	if p2p.messageHandler != nil {
		p2p.messageHandler(s, msg)
	}

	return nil
}

func (p2p *P2P) handleNewStream(s network.Stream) {
	err := p2p.handleMessage(newStream(s, p2p.config.sendTimeout, p2p.config.readTimeout, p2p.config.enableCompression, p2p.config.enableMetrics))
	if err != nil {
		if err != io.EOF {
			p2p.logger.WithField("error", err).Error("Handle message failed")
		}
		return
	}
}

// waitMsg wait the incoming messages within time duration.
func waitMsg(stream network.Stream, timeout time.Duration, enableMetrics bool) ([]byte, error) {
	if err := stream.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline failed: %w", err)
	}
	reader := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)

	msg, err := reader.ReadMsg()
	if err != nil {
		return nil, err
	}
	msg, err = decompressMsg(msg)
	if err != nil {
		return nil, err
	}

	if enableMetrics {
		recvDataSize.Add(float64(len(msg)))
	}

	return msg, nil
}

func (p2p *P2P) send(s *stream, msg []byte) error {
	if len(msg) > network.MessageSizeMax {
		return msgio.ErrMsgTooLarge
	}

	return s.AsyncSend(msg)
}

func compressMsg(msg []byte, enableCompression, enableMetrics bool) ([]byte, error) {
	var dstData []byte
	if enableCompression {
		compressionData := snappy.Encode(nil, msg)
		dstData = make([]byte, 0, len(compressionData)+1)
		dstData = append(dstData, snappyCompressionFlag)
		dstData = append(dstData, compressionData...)
	} else {
		dstData = make([]byte, 0, len(msg)+1)
		dstData = append(dstData, noCompressionFlag)
		dstData = append(dstData, msg...)
	}

	if enableMetrics {
		if reduceDataSize := (len(msg) - len(dstData)); reduceDataSize > 0 {
			compressionReduceDataSize.Add(float64(reduceDataSize))
		} else {
			compressionIncreaseDataSize.Add(-float64(reduceDataSize))
		}
	}

	return dstData, nil
}

func decompressMsg(msg []byte) ([]byte, error) {
	if len(msg) < 2 {
		return nil, fmt.Errorf("decompress msg error, msg length < 2")
	}

	var dstData []byte
	var err error
	switch msg[0] {
	case noCompressionFlag:
		dstData = msg[1:]
	case snappyCompressionFlag:
		dstData, err = snappy.Decode(nil, msg[1:])
		if err != nil {
			return nil, fmt.Errorf("can't decode msg data, error: %s", err)
		}
	}

	return dstData, nil
}
