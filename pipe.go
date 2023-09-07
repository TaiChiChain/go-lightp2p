package network

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/gammazero/workerpool"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	seqnoValidatorPeerMetadataStoreKey = "gossip_validator_seqno"
)

type PipeManagerImpl struct {
	ctx    context.Context
	host   host.Host
	pubsub *pubsub.PubSub
	config *Config

	lock  *sync.RWMutex
	pipes map[string]*PipeImpl
}

func NewPipeManager(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, config *Config) (PipeManager, error) {
	return &PipeManagerImpl{
		ctx:    ctx,
		host:   host,
		pubsub: pubsub,
		config: config,
		lock:   &sync.RWMutex{},
		pipes:  map[string]*PipeImpl{},
	}, nil
}

func (m *PipeManagerImpl) CreatePipe(ctx context.Context, pipeID string) (Pipe, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, ok := m.pipes[pipeID]; ok {
		return nil, errors.Errorf("pipe[%s] alerady exists", pipeID)
	}

	p, err := newPipe(ctx, m.host, m.pubsub, m.config, pipeID)
	if err != nil {
		return nil, err
	}

	if err := p.init(); err != nil {
		return nil, err
	}
	m.pipes[pipeID] = p
	return p, nil
}

type pipeBroadcastWorker func()

type PipeImpl struct {
	ctx               context.Context
	host              host.Host
	pubsub            *pubsub.PubSub
	config            *Config
	pipeID            string
	selfPeerID        string
	topic             *pubsub.Topic
	msgCh             chan PipeMsg
	broadcastWorkerCh chan pipeBroadcastWorker
}

func newPipe(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, config *Config, pipeID string) (*PipeImpl, error) {
	return &PipeImpl{
		ctx:               ctx,
		host:              host,
		pubsub:            pubsub,
		config:            config,
		pipeID:            pipeID,
		selfPeerID:        host.ID().String(),
		msgCh:             make(chan PipeMsg, config.pipeReceiveMsgCacheSize),
		broadcastWorkerCh: make(chan pipeBroadcastWorker, config.pipeBroadcastWorkerCacheSize),
	}, nil
}

func (p *PipeImpl) String() string {
	return fmt.Sprintf("Pipe<%s>", p.fullProtocolID())
}

func (p *PipeImpl) fullProtocolID() protocol.ID {
	return protocol.ID(fmt.Sprintf("%s/pipe/%s", p.config.protocolID, p.pipeID))
}

func (p *PipeImpl) init() error {
	p.host.SetStreamHandler(p.fullProtocolID(), func(s network.Stream) {
		remote := s.Conn().RemotePeer().String()
		reader := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

		for {
			msg, err := reader.ReadMsg()
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, network.ErrReset) {
					return
				}

				p.config.logger.WithFields(logrus.Fields{
					"error": err,
					"pipe":  p.fullProtocolID(),
				}).Warn("Read msg failed")

				// release stream
				if err := s.Close(); err != nil {
					p.config.logger.WithFields(logrus.Fields{
						"error": err,
						"pipe":  p.fullProtocolID(),
					}).Warn("Release stream failed")
				}
				return
			}
			select {
			case <-p.ctx.Done():
				if err := s.Close(); err != nil {
					p.config.logger.WithFields(logrus.Fields{
						"error": err,
						"pipe":  p.fullProtocolID(),
					}).Warn("Release stream failed")
				}

				return
			case p.msgCh <- PipeMsg{
				From: remote,
				Data: msg,
			}:
			}
		}
	})

	if p.pubsub != nil {
		topic, err := p.pubsub.Join(string(p.fullProtocolID()))
		if err != nil {
			return err
		}
		p.topic = topic

		sub, err := topic.Subscribe(pubsub.WithBufferSize(p.config.pipeGossipSubBufferSize))
		if err != nil {
			return err
		}
		go func() {
			for {
				msg, err := sub.Next(p.ctx)
				if err != nil {
					if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, network.ErrReset) {
						return
					}

					p.config.logger.WithFields(logrus.Fields{
						"error": err,
						"pipe":  p.fullProtocolID(),
					}).Warn("Read msg from pubsub failed")
					continue
				}

				from := msg.ReceivedFrom.String()
				if from == p.selfPeerID {
					continue
				}

				select {
				case <-p.ctx.Done():
					return
				case p.msgCh <- PipeMsg{
					From: from,
					Data: msg.Data,
				}:
				}
			}
		}()
	}

	go p.processBroadcastWorkers()
	return nil
}

func (p *PipeImpl) processBroadcastWorkers() {
	wp := workerpool.New(p.config.pipeBroadcastWorkerConcurrencyLimit)
	for {
		select {
		case <-p.ctx.Done():
			wp.StopWait()
			return
		case worker := <-p.broadcastWorkerCh:
			wp.Submit(worker)
		}
	}
}

func (p *PipeImpl) Send(ctx context.Context, to string, data []byte) error {
	peerID, err := peer.Decode(to)
	if err != nil {
		return fmt.Errorf("failed on decode peer id: %v", err)
	}

	switch p.host.Network().Connectedness(peerID) {
	case network.CannotConnect:
		return fmt.Errorf("cannot connect to %q", to)
	default:
		stream, err := p.getStream(peerID)
		if err != nil {
			return err
		}

		writer := msgio.NewVarintWriter(stream)
		if err = writer.WriteMsg(data); err != nil {
			return err
		}
	}
	return nil
}

func (p *PipeImpl) getStream(peerID peer.ID) (network.Stream, error) {
	conns := p.host.Network().ConnsToPeer(peerID)
	pid := p.fullProtocolID()
	if len(conns) > 0 {
		for cidx := range conns {
			streams := conns[cidx].GetStreams()
			for sidx := range streams {
				stream := streams[sidx]
				if stream.Protocol() == pid && stream.Stat().Direction == network.DirOutbound {
					// reuse stream
					return stream, nil
				}
			}
		}
	}

	return p.host.NewStream(p.ctx, peerID, pid)
}

func (p *PipeImpl) Broadcast(ctx context.Context, targets []string, data []byte) error {
	if p.pubsub != nil {
		return p.topic.Publish(ctx, data)
	}

	worker := func() {
		wg := &sync.WaitGroup{}
		for _, id := range targets {
			if id == p.selfPeerID {
				continue
			}

			wg.Add(1)
			go func(id string) {
				defer wg.Done()

				err := retry.Retry(func(attempt uint) error {
					return p.Send(ctx, id, data)
				}, strategy.Backoff(backoff.BinaryExponential(p.config.pipeBroadcastRetryBaseTime)), strategy.Limit(uint(p.config.pipeBroadcastRetryNumber)))
				if err != nil {
					p.config.logger.WithFields(logrus.Fields{
						"error": err,
						"id":    id,
					}).Error("Broadcast message failed")
				}
			}(id)
		}
		wg.Wait()
	}
	select {
	case <-ctx.Done():
	case <-p.ctx.Done():
	case p.broadcastWorkerCh <- worker:
	}

	return nil
}

func (p *PipeImpl) Receive(ctx context.Context) *PipeMsg {
	select {
	case <-ctx.Done():
		return nil
	case <-p.ctx.Done():
		return nil
	case msg := <-p.msgCh:
		return &msg
	}
}

type SeqnoValidatorPeerMetadataStoreAdaptor struct {
	ps peerstore.Peerstore
}

func (a *SeqnoValidatorPeerMetadataStoreAdaptor) Get(ctx context.Context, p peer.ID) ([]byte, error) {
	res, err := a.ps.Get(p, seqnoValidatorPeerMetadataStoreKey)
	if err != nil {
		if errors.Is(err, peerstore.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return res.([]byte), nil
}

func (a *SeqnoValidatorPeerMetadataStoreAdaptor) Put(ctx context.Context, p peer.ID, v []byte) error {
	return a.ps.Put(p, seqnoValidatorPeerMetadataStoreKey, v)
}
