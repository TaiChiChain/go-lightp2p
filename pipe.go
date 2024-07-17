package network

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/gammazero/workerpool"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-msgio"
	"github.com/minio/highwayhash"
	b58 "github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	msgNeedSplitSize = 4 * 1000 * 1024

	sendSmallMsgFlag uint8 = 1
	sendLargeMsgFlag uint8 = 2
	ackMsg           uint8 = 111
)

type PipeManagerImpl struct {
	ctx    context.Context
	host   host.Host
	router routing.Routing
	pubsub *pubsub.PubSub
	config *Config

	lock  *sync.RWMutex
	pipes map[string]*PipeImpl
}

func NewPipeManager(ctx context.Context, host host.Host, router routing.Routing, pubsub *pubsub.PubSub, config *Config) (PipeManager, error) {
	return &PipeManagerImpl{
		ctx:    ctx,
		host:   host,
		router: router,
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

	p, err := newPipe(ctx, m.host, m.router, m.pubsub, m.config, pipeID)
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
	router            routing.Routing
	pubsub            *pubsub.PubSub
	config            *Config
	pipeID            string
	selfPeerID        string
	topic             *pubsub.Topic
	cancelRelay       pubsub.RelayCancelFunc
	msgCh             chan PipeMsg
	broadcastWorkerCh chan pipeBroadcastWorker
	compressionAlgo   CompressionAlgo
	enableMetrics     bool
}

func newPipe(ctx context.Context, host host.Host, router routing.Routing, pubsub *pubsub.PubSub, config *Config, pipeID string) (*PipeImpl, error) {
	return &PipeImpl{
		ctx:               ctx,
		host:              host,
		router:            router,
		pubsub:            pubsub,
		config:            config,
		pipeID:            pipeID,
		selfPeerID:        host.ID().String(),
		msgCh:             make(chan PipeMsg, config.pipe.ReceiveMsgCacheSize),
		broadcastWorkerCh: make(chan pipeBroadcastWorker, config.pipe.SimpleBroadcast.WorkerCacheSize),
		compressionAlgo:   config.compressionAlgo,
		enableMetrics:     config.enableMetrics,
	}, nil
}

func (p *PipeImpl) String() string {
	return fmt.Sprintf("Pipe<%s>", p.fullProtocolID())
}

func (p *PipeImpl) fullProtocolID() protocol.ID {
	return protocol.ID(fmt.Sprintf("%s/pipe/%s", p.config.protocolID, p.pipeID))
}

func (p *PipeImpl) setStreamHandler() {
	p.host.SetStreamHandler(p.fullProtocolID(), func(s network.Stream) {
		remote := s.Conn().RemotePeer().String()

		err := func() error {
			msg, err := p.readLargeMsg(s)
			if err != nil {
				return err
			}

			select {
			case <-p.ctx.Done():
				if err := s.Close(); err != nil {
					p.config.logger.WithFields(logrus.Fields{
						"error": err,
						"from":  remote,
						"pipe":  p.fullProtocolID(),
					}).Warn("Release stream failed")
				}
				return nil
			case p.msgCh <- PipeMsg{
				From: remote,
				Data: msg,
			}:
			}
			return nil
		}()
		if err != nil {
			p.config.logger.WithFields(logrus.Fields{
				"err":  err,
				"from": remote,
				"pipe": p.fullProtocolID(),
			}).Warn("Read msg failed")
		}
	})
}

func (p *PipeImpl) subscribe() error {
	if p.pubsub != nil {
		topic, err := p.pubsub.Join(string(p.fullProtocolID()))
		if err != nil {
			return err
		}
		p.topic = topic
		if p.cancelRelay, err = p.topic.Relay(); err != nil {
			_ = topic.Close()
			return fmt.Errorf("p2p: failed to relay topic '%s': %w", string(p.fullProtocolID()), err)
		}

		sub, err := topic.Subscribe(pubsub.WithBufferSize(p.config.pipe.Gossipsub.SubBufferSize))
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

				from := b58.Encode(msg.From)
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
	} else {
		go p.processBroadcastWorkers()
	}
	return nil
}

func (p *PipeImpl) init() error {
	p.setStreamHandler()
	if err := p.subscribe(); err != nil {
		return err
	}
	return nil
}

func (p *PipeImpl) processBroadcastWorkers() {
	wp := workerpool.New(p.config.pipe.SimpleBroadcast.WorkerConcurrencyLimit)
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

func (p *PipeImpl) Send(ctx context.Context, to string, data []byte) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "pipe[%s] send msg failed", p.fullProtocolID())
		}
	}()

	peerID, err := peer.Decode(to)
	if err != nil {
		return fmt.Errorf("failed on decode peer id: %v", err)
	}

	p.host.ConnManager().Protect(peerID, p.pipeID)
	defer p.host.ConnManager().Unprotect(peerID, p.pipeID)

	// check has peer addr
	if len(p.host.Peerstore().Addrs(peerID)) == 0 {
		var err error
		func() {
			timedCtx, cancel := context.WithTimeout(ctx, p.config.pipe.FindPeerTimeout)
			defer cancel()
			// try to find the peer by dht
			_, err = p.router.FindPeer(timedCtx, peerID)
		}()
		if err != nil {
			p.config.logger.WithError(err).Warn("address not found in both peer store and routing system")
		}
	}

	data, err = compressMsg(data, p.compressionAlgo, p.config.enableMetrics)
	if err != nil {
		return err
	}

	return retry.Retry(func(attempt uint) error {
		s, err := p.getStream(peerID)
		if err != nil {
			return err
		}

		if err := p.sendLargeMsg(s, data); err != nil {
			if resetErr := s.Reset(); resetErr != nil {
				p.config.logger.WithError(resetErr).WithField("to", peerID).Error("Failed to reset stream")
			}

			return err
		}

		if p.enableMetrics {
			sendDataSize.Add(float64(len(data)))
		}

		if err = s.Close(); err != nil {
			p.config.logger.WithError(err).WithField("to", peerID).Error("Failed to close stream")
		}
		return nil
	}, strategy.Backoff(backoff.BinaryExponential(p.config.pipe.UnicastSendRetryBaseTime)), strategy.Limit(uint(p.config.pipe.UnicastSendRetryNumber)))
}

func (p *PipeImpl) getStream(peerID peer.ID) (network.Stream, error) {
	pid := p.fullProtocolID()
	return p.host.NewStream(p.ctx, peerID, pid)
}

func (p *PipeImpl) Broadcast(ctx context.Context, targets []string, data []byte) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "pipe[%s] broadcast msg failed", p.fullProtocolID())
		}
	}()

	if len(data) > network.MessageSizeMax {
		return msgio.ErrMsgTooLarge
	}

	if p.pubsub != nil {
		data, err = compressMsg(data, p.compressionAlgo, p.config.enableMetrics)
		if err != nil {
			return err
		}

		err := p.topic.Publish(ctx, data)
		if err != nil {
			return err
		}

		if p.enableMetrics {
			sendDataSize.Add(float64(len(data)))
		}
		return nil
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

				err := p.Send(ctx, id, data)
				if err != nil {
					p.config.logger.WithFields(logrus.Fields{
						"error": err,
						"to":    id,
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

func (p *PipeImpl) Receive(ctx context.Context) (pmsg *PipeMsg) {
	defer func() {
		if pmsg != nil {
			data, err := decompressMsg(pmsg.Data)
			if err == nil {
				pmsg.Data = data
			}
		}

		if p.enableMetrics && pmsg != nil {
			recvDataSize.Add(float64(len(pmsg.Data)))
		}
	}()

	select {
	case <-ctx.Done():
		return nil
	case <-p.ctx.Done():
		return nil
	case msg := <-p.msgCh:
		return &msg
	}
}

type messageIDGenerator struct {
	key []byte
}

func newMessageIDGenerater(key []byte) (*messageIDGenerator, error) {
	if len(key) != 32 {
		return nil, errors.Errorf("highwayhash key length must be 32, get %v", len(key))
	}
	return &messageIDGenerator{key: key}, nil
}

func (g *messageIDGenerator) generateMessageID(pmsg *pb.Message) string {
	h := highwayhash.Sum64(pmsg.Data, g.key)
	return strconv.FormatUint(h, 10)
}

func (p *PipeImpl) retryDo(do func() error) (pErr error) {
	return retry.Retry(func(attempt uint) error {
		return do()
	}, strategy.Backoff(backoff.BinaryExponential(p.config.pipe.UnicastSendRetryBaseTime)), strategy.Limit(uint(p.config.pipe.UnicastSendRetryNumber)))
}

type messageSliceInfo struct {
	Num  int
	Size int
}

func (p *PipeImpl) sendStreamMsg(s network.Stream, msg []byte) error {
	deadline := time.Now().Add(p.config.sendTimeout)
	if err := s.SetWriteDeadline(deadline); err != nil {
		return errors.Wrap(err, "failed to set write deadline")
	}
	return msgio.NewVarintWriter(s).WriteMsg(msg)
}

func (p *PipeImpl) readStreamMsg(stream network.Stream) ([]byte, error) {
	deadline := time.Now().Add(p.config.readTimeout)
	if err := stream.SetReadDeadline(deadline); err != nil {
		return nil, errors.Wrap(err, "failed to set read deadline")
	}
	return msgio.NewVarintReaderSize(stream, network.MessageSizeMax).ReadMsg()
}

func (p *PipeImpl) sendLargeMsg(s network.Stream, msg []byte) error {
	size := len(msg)
	if size <= msgNeedSplitSize {
		return p.sendStreamMsg(s, append([]byte{sendSmallMsgFlag}, msg...))
	}

	num := size / msgNeedSplitSize
	if size%msgNeedSplitSize != 0 {
		num++
	}
	raw, err := json.Marshal(&messageSliceInfo{
		Num:  num,
		Size: size,
	})
	if err != nil {
		return err
	}
	if err := p.sendStreamMsg(s, append([]byte{sendLargeMsgFlag}, raw...)); err != nil {
		return err
	}

	sendChunk := func(data []byte) error {
		// send
		if err := p.sendStreamMsg(s, data); err != nil {
			return errors.Wrap(err, "failed on send chunk msg")
		}
		// wait ack
		ack, err := p.readStreamMsg(s)
		if err != nil {
			return errors.Wrap(err, "failed on read ack")
		}
		if len(ack) != 1 || ack[0] != ackMsg {
			return errors.New("invalid ack for send chunk msg")
		}

		return nil
	}

	for i := 0; i < num-1; i++ {
		if err := sendChunk(msg[i*msgNeedSplitSize : (i+1)*msgNeedSplitSize]); err != nil {
			return err
		}
	}
	if num != 0 {
		if err := sendChunk(msg[(num-1)*msgNeedSplitSize:]); err != nil {
			return err
		}
	}

	return nil
}

func (p *PipeImpl) readLargeMsg(s network.Stream) ([]byte, error) {
	firstMsg, err := p.readStreamMsg(s)
	if err != nil {
		return nil, errors.Wrap(err, "failed on read first msg")
	}
	if len(firstMsg) == 0 {
		return nil, errors.New("empty first msg")
	}

	switch firstMsg[0] {
	case sendSmallMsgFlag:
		return firstMsg[1:], nil
	case sendLargeMsgFlag:
		break
	default:
		return nil, errors.Errorf("unknown first msg flag %v", firstMsg[0])
	}

	var info messageSliceInfo
	if err := json.Unmarshal(firstMsg[1:], &info); err != nil {
		return nil, err
	}

	readChunk := func() ([]byte, error) {
		// read msg
		raw, err := p.readStreamMsg(s)
		if err != nil {
			return nil, errors.Wrap(err, "failed on read chunk msg")
		}
		// send ack msg
		if err = p.sendStreamMsg(s, []byte{ackMsg}); err != nil {
			return nil, errors.Wrap(err, "failed on send ack msg")
		}
		return raw, nil
	}

	body := make([]byte, 0, info.Size)
	for i := 0; i < info.Num; i++ {
		raw, err := readChunk()
		if err != nil {
			return nil, err
		}
		body = append(body, raw...)
	}

	return body, nil
}
