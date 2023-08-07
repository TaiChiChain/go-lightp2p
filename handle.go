package network

import (
	"fmt"
	"io"
	"time"

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

	if p2p.messageHandler != nil {
		p2p.messageHandler(s, msg)
	}

	return nil
}

func (p2p *P2P) handleNewStream(s network.Stream) {
	err := p2p.handleMessage(newStream(s, p2p.config.protocolID, p2p.config.sendTimeout, p2p.config.readTimeout))
	if err != nil {
		if err != io.EOF {
			p2p.logger.WithField("error", err).Error("Handle message failed")
		}
		return
	}
}

// waitMsg wait the incoming messages within time duration.
func waitMsg(stream network.Stream, timeout time.Duration) ([]byte, error) {
	if err := stream.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("set read deadline failed: %w", err)
	}
	reader := msgio.NewVarintReaderSize(stream, network.MessageSizeMax)
	return reader.ReadMsg()
}

func (p2p *P2P) send(s *stream, msg []byte) error {
	if err := s.getStream().SetWriteDeadline(time.Now().Add(p2p.config.sendTimeout)); err != nil {
		return fmt.Errorf("set write deadline failed: %w", err)
	}

	writer := msgio.NewVarintWriter(s.getStream())
	if err := writer.WriteMsg(msg); err != nil {
		return fmt.Errorf("write msg: %w", err)
	}

	return nil
}
