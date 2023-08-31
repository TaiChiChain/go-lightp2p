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
	pipeBroadcastType       PipeBroadcastType
	pipeReceiveMsgCacheSize int
	disableAutoBootstrap    bool
	connectTimeout          time.Duration
	sendTimeout             time.Duration
	readTimeout             time.Duration
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

func WithPipeReceiveMsgCacheSize(s int) Option {
	return func(config *Config) {
		config.pipeReceiveMsgCacheSize = s
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
		securityType:            SecurityTLS,
		pipeBroadcastType:       PipeBroadcastSimple,
		pipeReceiveMsgCacheSize: defaultPipeReceiveMsgCacheSize,
		disableAutoBootstrap:    false,
		connectTimeout:          10 * time.Second,
		sendTimeout:             5 * time.Second,
		readTimeout:             5 * time.Second,
	}
	for _, opt := range opts {
		opt(conf)
	}

	if err := checkConfig(conf); err != nil {
		return nil, fmt.Errorf("create p2p: %w", err)
	}

	return conf, nil
}
