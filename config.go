package network

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type SecurityType uint8

const (
	SecurityDisable SecurityType = iota
	SecurityNoise
	SecurityTLS
)

type PipeBroadcastType uint8

const (
	PipeBroadcastSimple   PipeBroadcastType = iota
	PipeBroadcastGossip   PipeBroadcastType = iota
	PipeBroadcastFloodSub PipeBroadcastType = iota
)

type connMgr struct {
	enabled bool
	lo      int
	hi      int
	grace   time.Duration
}

type Config struct {
	localAddr               string
	privKey                 crypto.PrivKey
	protocolID              protocol.ID
	logger                  logrus.FieldLogger
	bootstrap               []string
	connMgr                 *connMgr
	gater                   connmgr.ConnectionGater
	securityType            SecurityType
	disableAutoBootstrap    bool
	connectTimeout          time.Duration
	sendTimeout             time.Duration
	readTimeout             time.Duration
	pipeBroadcastType       PipeBroadcastType
	pipeReceiveMsgCacheSize int

	// pipeGossipSubBufferSize is the size of subscribe output buffer in go-libp2p-pubsub
	// we should have enough capacity of the queue
	// because when queue is full, if the consumer does not read fast enough, new messages are dropped
	pipeGossipSubBufferSize int

	// pipeGossipPeerOutboundBufferSize is the size of outbound messages to a peer buffers in go-libp2p-pubsub
	// we should have enough capacity of the queue
	// because we start dropping messages to a peer if the outbound queue is full
	pipeGossipPeerOutboundBufferSize int

	// pipeGossipValidateBufferSize is the size of validate buffers in go-libp2p-pubsub
	// we should have enough capacity of the queue
	// because when queue is full, validation is throttled and new messages are dropped.
	pipeGossipValidateBufferSize int

	pipeBroadcastWorkerCacheSize        int
	pipeBroadcastWorkerConcurrencyLimit int
	pipeBroadcastRetryNumber            int
	pipeBroadcastRetryBaseTime          time.Duration
}

type Option func(*Config)

func WithSecurity(t SecurityType) Option {
	return func(config *Config) {
		config.securityType = t
	}
}

func WithPipeBroadcastType(t PipeBroadcastType) Option {
	return func(config *Config) {
		config.pipeBroadcastType = t
	}
}

func WithDisableAutoBootstrap() Option {
	return func(config *Config) {
		config.disableAutoBootstrap = true
	}
}

func WithPrivateKey(privKey crypto.PrivKey) Option {
	return func(config *Config) {
		config.privKey = privKey
	}
}

func WithLocalAddr(addr string) Option {
	return func(config *Config) {
		config.localAddr = addr
	}
}

func WithProtocolID(id string) Option {
	return func(config *Config) {
		config.protocolID = protocol.ID(id)
	}
}

func WithBootstrap(peers []string) Option {
	return func(config *Config) {
		config.bootstrap = peers
	}
}

func WithConnectionGater(gater connmgr.ConnectionGater) Option {
	return func(config *Config) {
		config.gater = gater
	}
}

//   - enable is the enable signal of the connection manager module.
//   - lo and hi are watermarks governing the number of connections that'll be maintained.
//     When the peer count exceeds the 'high watermark', as many peers will be pruned (and
//     their connections terminated) until 'low watermark' peers remain.
//   - grace is the amount of time a newly opened connection is given before it becomes
//     subject to pruning.
func WithConnMgr(enable bool, lo int, hi int, grace time.Duration) Option {
	return func(config *Config) {
		config.connMgr = &connMgr{
			enabled: enable,
			lo:      lo,
			hi:      hi,
			grace:   grace,
		}
	}
}

func WithTimeout(connectTimeout, sendTimeout, readTimeout time.Duration) Option {
	return func(config *Config) {
		config.connectTimeout = connectTimeout
		config.sendTimeout = sendTimeout
		config.readTimeout = readTimeout
	}
}

func WithLogger(logger logrus.FieldLogger) Option {
	return func(config *Config) {
		config.logger = logger
	}
}

func WithPipeReceiveMsgCacheSize(s int) Option {
	return func(config *Config) {
		config.pipeReceiveMsgCacheSize = s
	}
}

func WithPipeBroadcastWorkerCacheSize(pipeBroadcastWorkerCacheSize int) Option {
	return func(config *Config) {
		config.pipeBroadcastWorkerCacheSize = pipeBroadcastWorkerCacheSize
	}
}

func WithPipeBroadcastWorkerConcurrencyLimit(pipeBroadcastWorkerConcurrencyLimit int) Option {
	return func(config *Config) {
		config.pipeBroadcastWorkerConcurrencyLimit = pipeBroadcastWorkerConcurrencyLimit
	}
}

func WithPipeBroadcastRetryNumber(pipeBroadcastRetryNumber int) Option {
	return func(config *Config) {
		config.pipeBroadcastRetryNumber = pipeBroadcastRetryNumber
	}
}

func WithPipeBroadcastRetryBaseTime(pipeBroadcastRetryBaseTime time.Duration) Option {
	return func(config *Config) {
		config.pipeBroadcastRetryBaseTime = pipeBroadcastRetryBaseTime
	}
}

func WithPipeGossipSubBufferSize(pipeGossipSubBufferSize int) Option {
	return func(config *Config) {
		config.pipeGossipSubBufferSize = pipeGossipSubBufferSize
	}
}

func WithPipeGossipPeerOutboundBufferSize(pipeGossipPeerOutboundBufferSize int) Option {
	return func(config *Config) {
		config.pipeGossipPeerOutboundBufferSize = pipeGossipPeerOutboundBufferSize
	}
}

func WithPipeGossipValidateBufferSize(pipeGossipValidateBufferSize int) Option {
	return func(config *Config) {
		config.pipeGossipValidateBufferSize = pipeGossipValidateBufferSize
	}
}

func checkConfig(config *Config) error {
	if config.logger == nil {
		config.logger = logrus.New()
	}

	if config.localAddr == "" {
		return errors.New("empty local address")
	}

	return nil
}

func generateConfig(opts ...Option) (*Config, error) {
	conf := &Config{
		securityType:                        SecurityTLS,
		disableAutoBootstrap:                false,
		connectTimeout:                      10 * time.Second,
		sendTimeout:                         5 * time.Second,
		readTimeout:                         5 * time.Second,
		pipeBroadcastType:                   PipeBroadcastSimple,
		pipeReceiveMsgCacheSize:             1024,
		pipeGossipSubBufferSize:             1024,
		pipeGossipPeerOutboundBufferSize:    1024,
		pipeGossipValidateBufferSize:        1024,
		pipeBroadcastWorkerCacheSize:        10000,
		pipeBroadcastWorkerConcurrencyLimit: 20,
		pipeBroadcastRetryNumber:            5,
		pipeBroadcastRetryBaseTime:          100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(conf)
	}

	if err := checkConfig(conf); err != nil {
		return nil, fmt.Errorf("create p2p: %w", err)
	}

	return conf, nil
}
