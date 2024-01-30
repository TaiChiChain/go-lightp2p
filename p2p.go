package network

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	discoveryrouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	connmgrimpl "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"
)

const (
	noCompressionFlag uint8 = iota
	snappyCompressionFlag
)

var (
	reconnectInterval = 10 * time.Second
	newStreamTimeout  = 5 * time.Second
)

var _ Network = (*P2P)(nil)

type dhtValidator struct{}

// Validate validates the given record, returning an error if it's
// invalid (e.g., expired, signed by the wrong key, etc.).
func (v *dhtValidator) Validate(key string, value []byte) error {
	return nil
}

// Select selects the best record from the set of records (e.g., the
// newest).
//
// Decisions made by select should be stable.
func (v *dhtValidator) Select(key string, values [][]byte) (int, error) {
	return 0, nil
}

type P2P struct {
	config             *Config
	host               host.Host // manage all connections
	connectCallback    ConnectCallback
	disconnectCallback DisconnectCallback
	messageHandler     MessageHandler
	logger             logrus.FieldLogger
	router             routing.Routing
	pingServer         *ping.PingService
	ctx                context.Context

	PipeManager
}

func New(ctx context.Context, options ...Option) (*P2P, error) {
	conf, err := generateConfig(options...)
	if err != nil {
		return nil, fmt.Errorf("generate config: %w", err)
	}

	// disable resource limit
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits))
	if err != nil {
		return nil, err
	}
	opts := []libp2p.Option{
		libp2p.Identity(conf.privKey),
		libp2p.NoListenAddrs,
		libp2p.ConnectionGater(conf.gater),
		libp2p.ResourceManager(rm),
		libp2p.Transport(tcp.NewTCPTransport, tcp.DisableReuseport()),
	}

	switch conf.securityType {
	case SecurityDisable:
		opts = append(opts, libp2p.NoSecurity)
	case SecurityNoise:
		opts = append(opts, libp2p.Security(libp2ptls.ID, noise.New))
	case SecurityTLS:
		opts = append(opts, libp2p.Security(libp2ptls.ID, libp2ptls.New))
	}

	if conf.connMgr != nil && conf.connMgr.enabled {
		c, err := newConnManager(conf.connMgr)
		if err != nil {
			return nil, fmt.Errorf("failed to new conn manager: %w", err)
		}
		opts = append(opts, libp2p.ConnectionManager(c))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed on create p2p host")
	}

	pingServer := ping.NewPingService(h)

	dhtopts := []dht.Option{
		dht.Mode(dht.ModeServer),
		dht.Validator(&dhtValidator{}),
		dht.ProtocolPrefix(conf.protocolID),
	}
	dynamicRouting, err := dht.New(ctx, h, dhtopts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed on create dht")
	}

	var ps *pubsub.PubSub
	switch conf.pipe.BroadcastType {
	case PipeBroadcastSimple:
	case PipeBroadcastGossip:
		skBytes, err := conf.privKey.Raw()
		if err != nil {
			return nil, err
		}
		harsher := sha3.New256()
		_, err = harsher.Write(skBytes)
		if err != nil {
			return nil, err
		}
		g, err := newMessageIDGenerater(harsher.Sum(nil))
		if err != nil {
			return nil, err
		}
		opts := []pubsub.Option{
			pubsub.WithDiscovery(discoveryrouting.NewRoutingDiscovery(dynamicRouting)),
			pubsub.WithPeerOutboundQueueSize(conf.pipe.Gossipsub.PeerOutboundBufferSize),
			pubsub.WithValidateQueueSize(conf.pipe.Gossipsub.ValidateBufferSize),
			pubsub.WithSeenMessagesTTL(conf.pipe.Gossipsub.SeenMessagesTTL),
			pubsub.WithMaxMessageSize(4 << 20),
			pubsub.WithMessageIdFn(g.generateMessageID),
			pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		}
		if conf.pipe.Gossipsub.EventTracer != nil {
			opts = append(opts, pubsub.WithEventTracer(conf.pipe.Gossipsub.EventTracer))
		} else {
			if conf.pipe.Gossipsub.EnableMetrics {
				opts = append(opts, pubsub.WithRawTracer(&MetricsTracer{}))
			}
		}

		ps, err = pubsub.NewGossipSub(ctx, h, opts...)
		if err != nil {
			return nil, errors.Wrap(err, "failed on create p2p gossip pubsub")
		}
	default:
		return nil, errors.Errorf("unsupported broadcast type: %v", conf.pipe.BroadcastType)
	}

	pipeManager, err := NewPipeManager(ctx, h, dynamicRouting, ps, conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed on create p2p pipe manager")
	}

	p2p := &P2P{
		config:      conf,
		host:        h,
		logger:      conf.logger,
		router:      dynamicRouting,
		pingServer:  pingServer,
		ctx:         ctx,
		PipeManager: pipeManager,
	}

	return p2p, nil
}

func newConnManager(cfg *connMgr) (connmgr.ConnManager, error) {
	if cfg == nil || !cfg.enabled {
		return nil, nil
	}

	return connmgrimpl.NewConnManager(cfg.lo, cfg.hi, connmgrimpl.WithGracePeriod(cfg.grace))
}

func (p2p *P2P) baseProtocol() protocol.ID {
	return protocol.ID(fmt.Sprintf("%s/base", p2p.config.protocolID))
}

func (p2p *P2P) Ping(ctx context.Context, peerID string) (<-chan ping.Result, error) {
	peerInfo, err := p2p.FindPeer(peerID)
	if err != nil {
		return nil, errors.Wrap(err, "failed on find peer")
	}

	ch := p2p.pingServer.Ping(ctx, peerInfo.ID)
	return ch, nil
}

// Start start the network service.
func (p2p *P2P) Start() error {
	p2p.host.SetStreamHandler(p2p.baseProtocol(), p2p.handleNewStream)
	p2p.host.Network().Notify(p2p)

	a, err := ma.NewMultiaddr(p2p.config.localAddr)
	if err != nil {
		return errors.Wrapf(err, "listen address[%s] format error", p2p.config.localAddr)
	}
	err = p2p.host.Network().Listen(a)
	if err != nil {
		return errors.Wrapf(err, "listen on %s failed", p2p.config.localAddr)
	}

	if err := p2p.router.Bootstrap(p2p.ctx); err != nil {
		return errors.Wrap(err, "failed on bootstrap kad dht")
	}

	if !p2p.config.disableAutoBootstrap {
		err := p2p.BootstrapConnect()
		if err != nil {
			p2p.logger.WithFields(logrus.Fields{"error": err}).Warn("Connect all bootstrap node failed")
		}
	}

	p2p.logger.Info("Start p2p success")
	return nil
}

// BootstrapConnect refer to ipfs bootstrap
// connect to bootstrap peers concurrently
func (p2p *P2P) BootstrapConnect() error {
	// construct Bootstrap node's peer info
	var peers []peer.AddrInfo
	for _, maAddr := range p2p.config.bootstrap {
		pi, err := AddrToPeerInfo(maAddr)
		if err != nil {
			return err
		}
		peers = append(peers, *pi)
	}

	if len(peers) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, p := range peers {
		// performed asynchronously because when performed synchronously, if
		// one `Connect` call hangs, subsequent calls are more likely to
		// fail/abort due to an expiring context.
		// Also, performed asynchronously for dial speed.

		wg.Add(1)
		go func(p peer.AddrInfo) {
			defer wg.Done()
			if err := p2p.Connect(p); err != nil {
				p2p.logger.WithFields(logrus.Fields{"node": p.ID.String(), "err": err}).Info("Connect bootstrap node failed")
				return
			}

			p2p.logger.WithFields(logrus.Fields{"node": p.ID.String()}).Info("Connect bootstrap node success")
		}(p)
	}
	wg.Wait()

	go p2p.reconnectBootstrap()
	return nil
}

// reBootstrap regularly attempts to bootstrap .
// This should ensure that we auto-recover from situations in
// which the network was completely gone and we lost all peers.
func (p2p *P2P) reconnectBootstrap() {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	bootstrapPeers := make([]peer.AddrInfo, 0, len(p2p.config.bootstrap))
	for _, pAddr := range p2p.config.bootstrap {
		addr, err := ma.NewMultiaddr(pAddr)
		if err != nil {
			p2p.logger.WithFields(logrus.Fields{"node": pAddr, "err": err}).Error("Invalid bootstrap address")
			return
		}

		addrInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			p2p.logger.WithFields(logrus.Fields{"node": pAddr, "err": err}).Error("Failed on get address info")
			return
		}

		bootstrapPeers = append(bootstrapPeers, *addrInfo)
	}

	for {
		select {
		case <-p2p.ctx.Done():
			return
		case <-ticker.C:
			connected := p2p.bootstrap(bootstrapPeers)
			for _, p := range connected {
				p2p.logger.WithFields(logrus.Fields{"node": p.String()}).Info("Reconnect to bootstrap node success")
			}
		}
	}
}

func (p2p *P2P) bootstrap(bootstrapPeers []peer.AddrInfo) []peer.ID {
	var connectedPeers []peer.ID
	for _, bootstrap := range bootstrapPeers {
		if p2p.IsConnected(bootstrap.ID.String()) {
			// We are connected, assume success and do not try
			// to re-connect
			continue
		}

		err := p2p.Connect(bootstrap)
		if err != nil {
			p2p.logger.WithFields(logrus.Fields{"node": bootstrap.ID, "err": err}).Error("Reconnect to bootstrap node failed")
			continue
		}
		connectedPeers = append(connectedPeers, bootstrap.ID)
	}

	return connectedPeers
}

// Connect peer.
func (p2p *P2P) Connect(addr peer.AddrInfo) error {
	ctx, cancel := context.WithTimeout(p2p.ctx, p2p.config.connectTimeout)
	defer cancel()

	if err := p2p.host.Connect(ctx, addr); err != nil {
		return err
	}

	p2p.host.Peerstore().AddAddrs(addr.ID, addr.Addrs, peerstore.PermanentAddrTTL)

	return nil
}

func (p2p *P2P) SetConnectCallback(callback ConnectCallback) {
	p2p.connectCallback = callback
}

func (p2p *P2P) SetDisconnectCallback(callback DisconnectCallback) {
	p2p.disconnectCallback = callback
}

func (p2p *P2P) SetMessageHandler(handler MessageHandler) {
	p2p.messageHandler = handler
}

// AsyncSend message to peer with specific id.
func (p2p *P2P) AsyncSend(peerID string, msg []byte) error {
	s, err := p2p.newStream(peerID)
	if err != nil {
		return err
	}
	defer p2p.ReleaseStream(s)

	if err := p2p.send(s, msg); err != nil {
		return err
	}
	return nil
}

func (p2p *P2P) AsyncSendWithStream(s Stream, msg []byte) error {
	return p2p.send(s.(*stream), msg)
}

func (p2p *P2P) Send(peerID string, msg []byte) ([]byte, error) {
	s, err := p2p.newStream(peerID)
	if err != nil {
		return nil, err
	}
	defer p2p.ReleaseStream(s)

	if err := p2p.send(s, msg); err != nil {
		return nil, errors.Wrap(err, "failed on send msg")
	}

	recvMsg, err := s.Read(p2p.config.readTimeout)
	if err != nil {
		return nil, err
	}

	return recvMsg, nil
}

func (p2p *P2P) Broadcast(ids []string, msg []byte) error {
	for _, id := range ids {
		go func(id string) {
			if err := p2p.AsyncSend(id, msg); err != nil {
				p2p.logger.WithFields(logrus.Fields{
					"error": err,
					"id":    id,
				}).Error("Async Send message")
			}
		}(id)
	}
	return nil
}

// Stop stop the network service.
func (p2p *P2P) Stop() error {
	return p2p.host.Close()
}

// AddrToPeerInfo transfer addr to PeerInfo
// addr example: "/ip4/104.236.76.40/tcp/4001/ipfs/QmSoLV4Bbm51jM9C4gDYZQ9Cy3U6aXMJDAbzgu2fzaDs64"
func AddrToPeerInfo(multiAddr string) (*peer.AddrInfo, error) {
	maddr, err := ma.NewMultiaddr(multiAddr)
	if err != nil {
		return nil, err
	}

	return peer.AddrInfoFromP2pAddr(maddr)
}

func (p2p *P2P) Disconnect(peerID string) error {
	pid, err := peer.Decode(peerID)
	if err != nil {
		return errors.Wrap(err, "failed on decode peerID")
	}

	return p2p.host.Network().ClosePeer(pid)
}

func (p2p *P2P) PeerID() string {
	return p2p.host.ID().String()
}

func (p2p *P2P) PrivKey() crypto.PrivKey {
	return p2p.config.privKey
}

func (p2p *P2P) GetPeers() []peer.AddrInfo {
	var peers []peer.AddrInfo

	peersID := p2p.host.Peerstore().Peers()
	for _, peerID := range peersID {
		addrs := p2p.host.Peerstore().Addrs(peerID)
		peers = append(peers, peer.AddrInfo{ID: peerID, Addrs: addrs})
	}

	return peers
}

func (p2p *P2P) LocalAddr() string {
	localhostAddr := ""
	for _, addr := range p2p.host.Addrs() {
		addrStr := addr.String()
		if strings.HasPrefix(addrStr, "/ip4/") {
			if !strings.Contains(addrStr, "127.0.0.1") {
				// priority use of intranet addresses
				return addrStr
			}
			localhostAddr = addrStr
		}
	}
	if localhostAddr != "" {
		return localhostAddr
	}
	return p2p.config.localAddr
}

func (p2p *P2P) listenPort() (int, error) {
	strs := strings.Split(p2p.host.Addrs()[0].String(), "/")
	return strconv.Atoi(strs[len(strs)-1])
}

func (p2p *P2P) GetStream(peerID string) (Stream, error) {
	return p2p.newStream(peerID)
}

func (p2p *P2P) ReleaseStream(s Stream) {
	stream, ok := s.(*stream)
	if !ok {
		return
	}

	if stream.getProtocolID() == p2p.baseProtocol() {
		if err := stream.close(); err != nil {
			p2p.logger.WithField("err", err).Warn("Failed to release stream")
		}
		return
	}
}

func (p2p *P2P) StorePeer(addr peer.AddrInfo) error {
	p2p.host.Peerstore().AddAddrs(addr.ID, addr.Addrs, peerstore.AddressTTL)
	return nil
}

func (p2p *P2P) PeerInfo(peerID string) (peer.AddrInfo, error) {
	pid, err := peer.Decode(peerID)
	if err != nil {
		return peer.AddrInfo{}, errors.Wrap(err, "failed on get get peer id from string")
	}

	return p2p.host.Peerstore().PeerInfo(pid), nil
}

func (p2p *P2P) GetRemotePubKey(id peer.ID) (crypto.PubKey, error) {
	conns := p2p.host.Network().ConnsToPeer(id)

	for _, conn := range conns {
		return conn.RemotePublicKey(), nil
	}

	return nil, errors.New("get remote pub key: not found")
}

func (p2p *P2P) PeersNum() int {
	return len(p2p.host.Network().Peers())
}

func (p2p *P2P) IsConnected(peerID string) bool {
	id, err := peer.Decode(peerID)
	if err != nil {
		p2p.logger.WithFields(logrus.Fields{"error": err, "node": id}).Error("Decode node id failed")
		return false
	}
	return p2p.host.Network().Connectedness(id) == network.Connected
}

func (p2p *P2P) FindPeer(peerID string) (peer.AddrInfo, error) {
	id, err := peer.Decode(peerID)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("failed on decode peer id:%v", err)
	}

	return p2p.router.FindPeer(p2p.ctx, id)
}

func (p2p *P2P) Provider(peerID string, passed bool) error {
	_, err := peer.Decode(peerID)
	if err != nil {
		return errors.Wrap(err, "failed on decode peer id")
	}
	ccid, err := cid.Decode(peerID)
	if err != nil {
		return fmt.Errorf("failed on cast cid: %v", err)
	}
	return p2p.router.Provide(p2p.ctx, ccid, passed)
}

func (p2p *P2P) FindProvidersAsync(peerID string, i int) (<-chan peer.AddrInfo, error) {
	ccid, err := cid.Decode(peerID)
	if err != nil {
		return nil, fmt.Errorf("failed on cast cid: %v", err)
	}
	peerInfoC := p2p.router.FindProvidersAsync(p2p.ctx, ccid, i)
	return peerInfoC, nil
}

func (p2p *P2P) newStream(peerID string) (*stream, error) {
	if _, err := p2p.FindPeer(peerID); err != nil {
		// if network busy cause disconnected, try to connect
		if errors.Cause(err) != kbucket.ErrLookupFailure {
			return nil, errors.Wrap(err, "failed on find peer")
		}

		// try to connect
		peerInfo, err := p2p.PeerInfo(peerID)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get peer info")
		}
		if err := p2p.Connect(peerInfo); err != nil {
			return nil, errors.Wrap(err, "try to connect peer failed")
		}
		p2p.logger.WithFields(logrus.Fields{"node": peerID}).Info("Reconnect node success")
	}

	pid, err := peer.Decode(peerID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(p2p.ctx, newStreamTimeout)
	defer cancel()
	s, err := p2p.host.NewStream(ctx, pid, p2p.baseProtocol())
	if err != nil {
		return nil, errors.Wrap(err, "failed on create stream")
	}

	return newStream(s, p2p.config.sendTimeout, p2p.config.readTimeout, p2p.config.enableCompression, p2p.config.enableMetrics), nil
}

// called when network starts listening on an addr
func (p2p *P2P) Listen(net network.Network, addr ma.Multiaddr) {}

// called when network stops listening on an addr
func (p2p *P2P) ListenClose(net network.Network, addr ma.Multiaddr) {}

// called when a connection opened
func (p2p *P2P) Connected(net network.Network, conn network.Conn) {
	p2p.logger.WithFields(logrus.Fields{"node": conn.RemotePeer().String(), "direction": conn.Stat().Direction.String()}).Info("Node connected")

	if p2p.connectCallback != nil {
		if err := p2p.connectCallback(net, conn); err != nil {
			p2p.logger.WithFields(logrus.Fields{"error": err, "node": conn.RemotePeer().String()}).Error("Failed on node connected callback")
		}
	}
}

// called when a connection closed
func (p2p *P2P) Disconnected(net network.Network, conn network.Conn) {
	p2p.logger.WithFields(logrus.Fields{"node": conn.RemotePeer().String()}).Warn("Node disconnected")
	if p2p.disconnectCallback != nil {
		p2p.disconnectCallback(conn.RemotePeer().String())
	}
}
