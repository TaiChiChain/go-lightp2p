package network

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	protocolID string = "/test/1.0.0" // magic protocol
)

func getAddr(p2p *P2P) (peer.AddrInfo, error) {
	realAddr := fmt.Sprintf("%s/p2p/%s", p2p.LocalAddr(), p2p.PeerID())
	multiaddr, err := ma.NewMultiaddr(realAddr)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	addrInfo, err := peer.AddrInfoFromP2pAddr(multiaddr)
	if err != nil {
		return peer.AddrInfo{}, err
	}
	return *addrInfo, nil
}

type P2PWrapper struct {
	*P2P
	t              *testing.T
	realAddr       peer.AddrInfo
	realListenPort int
	PID            peer.ID
	sk             ic.PrivKey
	opts           []Option
}

func (w *P2PWrapper) Reset() {
	opts := []Option{
		WithLocalAddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", w.realListenPort)),
		WithPrivateKey(w.sk),
		WithProtocolID(protocolID),
		WithConnMgr(true, 10, 100, 20*time.Second),
	}
	opts = append(opts, w.opts...)
	p2p, err := New(
		context.Background(),
		opts...,
	)
	assert.Nil(w.t, err)

	err = p2p.Start()
	assert.Nil(w.t, err)
	w.P2P = p2p
}

func (w *P2PWrapper) ConnectP2P(t *P2PWrapper) {
	err := w.Connect(t.realAddr)
	assert.Nil(w.t, err)
}

func generateNetwork(t *testing.T, opts []Option) *P2PWrapper {
	sk, pk, err := ic.GenerateECDSAKeyPair(rand.Reader)
	assert.Nil(t, err)
	pid, err := peer.IDFromPublicKey(pk)
	assert.Nil(t, err)

	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 0)

	defaultOpts := []Option{
		WithLocalAddr(addr),
		WithPrivateKey(sk),
		WithProtocolID(protocolID),
		WithConnMgr(true, 10, 100, 20*time.Second),
	}

	p2p, err := New(
		context.Background(),
		append(defaultOpts, opts...)...,
	)
	assert.Nil(t, err)
	err = p2p.Start()
	assert.Nil(t, err)

	realAddr, err := getAddr(p2p)
	assert.Nil(t, err)
	realPort, err := p2p.listenPort()
	assert.Nil(t, err)

	return &P2PWrapper{
		P2P:            p2p,
		t:              t,
		realAddr:       realAddr,
		realListenPort: realPort,
		PID:            pid,
		sk:             sk,
		opts:           opts,
	}
}

func generateNetworks(t *testing.T, cnt int, autoConnect bool, commonOpts []Option, customOptsSetter func(index int) []Option) []*P2PWrapper {
	if commonOpts == nil {
		commonOpts = []Option{}
	}
	if customOptsSetter == nil {
		customOptsSetter = func(index int) []Option {
			return []Option{}
		}
	}

	p2ps := make([]*P2PWrapper, cnt)
	for i := 0; i < cnt; i++ {
		p2ps[i] = generateNetwork(t, append(commonOpts, customOptsSetter(i)...))
	}

	if autoConnect {
		for i := 0; i < cnt; i++ {
			for j := 0; j < cnt; j++ {
				if i != j {
					p2ps[i].ConnectP2P(p2ps[j])
				}
			}
		}
	}

	t.Cleanup(func() {
		for i := 0; i < cnt; i++ {
			_ = p2ps[i].Stop()
		}
	})
	time.Sleep(100 * time.Millisecond)
	return p2ps
}

func TestP2P_Disconnect(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	ch := make(chan string)
	p1.SetDisconnectCallback(func(s string) {
		ch <- s
	})

	go func() {
		err := p2.Disconnect(p1.PeerID())
		assert.Nil(t, err)
	}()

	var disconnectPeerID string
	select {
	case disconnectPeerID = <-ch:
	case <-time.After(1 * time.Second):
	}
	assert.Equal(t, p2.PeerID(), disconnectPeerID)
}

func TestP2P_MultiStreamSend(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello world")
	ack := []byte("ack")
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		err := s.AsyncSend(ack)
		assert.Nil(t, err)
	})

	testStreamNum := 100
	var wg sync.WaitGroup

	send := func(wg *sync.WaitGroup) {
		defer wg.Done()
		resp, err := p1.Send(p2.PeerID(), msg)
		assert.Nil(t, err)
		assert.Equal(t, resp, ack)
	}

	for i := 0; i < testStreamNum; i++ {
		wg.Add(1)
		go send(&wg)
	}

	wg.Wait()
}

func TestP2P_MultiStreamAsyncSend(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello world")
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
	})

	testStreamNum := 100
	var wg sync.WaitGroup

	send := func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := p1.AsyncSend(p2.PeerID(), msg)
		assert.Nil(t, err)
	}

	for i := 0; i < testStreamNum; i++ {
		wg.Add(1)
		go send(&wg)
	}

	wg.Wait()
}

func TestP2P_MultiStreamSendWithStream(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello world")
	ack := []byte("ack")
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		err := s.AsyncSend(ack)
		assert.Nil(t, err)
	})

	testStreamNum := 100
	var wg sync.WaitGroup

	send := func(wg *sync.WaitGroup) {
		defer wg.Done()
		resp, err := p1.Send(p2.PeerID(), msg)
		assert.Nil(t, err)
		assert.Equal(t, resp, ack)
	}

	for i := 0; i < testStreamNum; i++ {
		wg.Add(1)
		go send(&wg)
	}

	wg.Wait()
}

func TestP2P_MultiStreamSendWithAsyncStream(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello world")
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
	})

	testStreamNum := 100
	var wg sync.WaitGroup

	send := func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := p1.AsyncSend(p2.PeerID(), msg)
		assert.Nil(t, err)
	}

	for i := 0; i < testStreamNum; i++ {
		wg.Add(1)
		go send(&wg)
	}

	wg.Wait()
}

func TestP2P_AsyncSend(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello")
	ch := make(chan struct{})

	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		assert.Equal(t, msg, data)
		close(ch)
	})

	err := p1.AsyncSend(p2.PeerID(), msg)
	assert.Nil(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	select {
	case <-ch:
		return
	case <-ctx1.Done():
		assert.Error(t, fmt.Errorf("timeout"))
		return
	}
}

func TestP2P_AsyncSendWithNetworkBusy(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello")

	wg := &sync.WaitGroup{}
	wg.Add(2)
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		wg.Done()
	})

	err := p1.AsyncSend(p2.PeerID(), msg)
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)
	// simulate network disconnect
	assert.Nil(t, p2.Stop())
	time.Sleep(1 * time.Second)
	assert.False(t, p1.IsConnected(p2.PeerID()))

	err = p1.AsyncSend(p2.PeerID(), msg)
	assert.NotNil(t, err)
	time.Sleep(5 * time.Second)

	// recover p2
	p2.Reset()
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		wg.Done()
	})

	// send message to p2
	err = p1.AsyncSend(p2.PeerID(), msg)
	assert.Nil(t, err)

	wg.Wait()
}

func TestP2P_SendWithNetworkBusy(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	msg := []byte("hello")

	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		err := s.AsyncSend(data)
		assert.Nil(t, err)
	})

	rdata, err := p1.Send(p2.PeerID(), msg)
	assert.Nil(t, err)
	assert.Equal(t, msg, rdata)

	time.Sleep(1 * time.Second)
	// simulate network disconnect
	assert.Nil(t, p2.Stop())
	time.Sleep(1 * time.Second)
	assert.False(t, p1.IsConnected(p2.PeerID()))

	_, err = p1.Send(p2.PeerID(), msg)
	assert.NotNil(t, err)

	time.Sleep(5 * time.Second)
	// recover p2
	p2.Reset()
	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		err := s.AsyncSend(data)
		assert.Nil(t, err)
	})

	// send message to p2
	recvData, err := p1.Send(p2.PeerID(), msg)
	assert.Nil(t, err)
	assert.Equal(t, msg, recvData)
}

func TestP2p_MultiSend(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	N := 50
	msg := []byte("hello")
	count := 0
	ch := make(chan struct{})

	p2.SetMessageHandler(func(s Stream, data []byte) {
		defer p2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		count++
		if count == N {
			close(ch)
			return
		}
	})

	go func() {
		for i := 0; i < N; i++ {
			time.Sleep(200 * time.Microsecond)
			err := p1.AsyncSend(p2.PeerID(), msg)
			assert.Nil(t, err)
		}

	}()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	select {
	case <-ch:
		return
	case <-ctx1.Done():
		assert.Error(t, fmt.Errorf("timeout"))
	}
}

func TestP2P_FindPeer(t *testing.T) {
	p2ps := generateNetworks(t, 4, false, nil, nil)
	bootstrap1 := p2ps[0]
	bootstrap2 := p2ps[1]
	bootstrap3 := p2ps[2]
	bootstrap4 := p2ps[3]
	bsAddr4 := bootstrap4.realAddr

	bootstrap2.ConnectP2P(bootstrap1)
	bootstrap3.ConnectP2P(bootstrap1)

	addrs1, err := peer.AddrInfoToP2pAddrs(&bootstrap1.realAddr)
	require.Nil(t, err)
	addrs2, err := peer.AddrInfoToP2pAddrs(&bootstrap2.realAddr)
	require.Nil(t, err)
	addrs3, err := peer.AddrInfoToP2pAddrs(&bootstrap3.realAddr)
	require.Nil(t, err)

	var bs1 = []string{addrs1[0].String()}
	var bs2 = []string{addrs2[0].String()}
	var bs3 = []string{addrs3[0].String()}

	p2ps2 := generateNetworks(t, 3, false, nil, func(index int) []Option {
		switch index {
		case 0:
			return []Option{WithBootstrap(bs1)}
		case 1:
			return []Option{WithBootstrap(bs2)}
		case 2:
			return []Option{WithBootstrap(bs3)}
		}
		return []Option{}
	})
	dht1 := p2ps2[0]
	s1 := dht1.realAddr
	id1 := dht1.PeerID()
	dht2 := p2ps2[1]
	s2 := dht2.realAddr
	id2 := dht2.PeerID()
	dht3 := p2ps2[2]
	id3 := dht3.PeerID()

	time.Sleep(1 * time.Second)

	err = dht3.Provider(bsAddr4.ID.String(), true)
	require.Nil(t, err)

	findPeer1, err := dht1.FindPeer(id2)
	require.Nil(t, err)
	require.True(t, strings.Contains(findPeer1.String(), s2.String()))

	findPeer2, err := dht2.FindPeer(id1)
	require.Nil(t, err)
	require.True(t, strings.Contains(findPeer2.String(), s1.String()))

	_, err = dht1.FindPeer(id3)
	require.Nil(t, err)

	_, err = dht3.FindPeer(id1)
	require.Nil(t, err)

	findPeerC, err := dht1.FindProvidersAsync(bsAddr4.ID.String(), 1)
	require.Nil(t, err)
	<-findPeerC
}

func TestP2P_ConnMgr(t *testing.T) {
	p2ps := generateNetworks(t, 3, true, nil, nil)
	bootstrap1 := p2ps[0]
	bsAddr1 := bootstrap1.realAddr
	bootstrap2 := p2ps[1]
	bsAddr2 := bootstrap2.realAddr

	addrs1, err := peer.AddrInfoToP2pAddrs(&bsAddr1)
	require.Nil(t, err)
	addrs2, err := peer.AddrInfoToP2pAddrs(&bsAddr2)
	require.Nil(t, err)
	msg := []byte("hello")

	ch := make(chan struct{})

	var bs1 = []string{addrs1[0].String()}
	var bs2 = []string{addrs2[0].String()}
	p2ps2 := generateNetworks(t, 2, false, nil, func(index int) []Option {
		switch index {
		case 0:
			return []Option{WithBootstrap(bs1)}
		case 1:
			return []Option{WithBootstrap(bs2)}
		}
		return []Option{}
	})
	dht1 := p2ps2[0]
	dht2 := p2ps2[1]
	id2 := dht2.PeerID()

	dht2.SetMessageHandler(func(s Stream, data []byte) {
		defer dht2.ReleaseStream(s)
		assert.Equal(t, msg, data)
		close(ch)
	})

	time.Sleep(1 * time.Second)

	err = dht1.AsyncSend(id2, msg)
	assert.Nil(t, err)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()

	select {
	case <-ch:
		return
	case <-ctx1.Done():
		assert.Error(t, fmt.Errorf("timeout"))
		return
	}
}

func TestP2P_GetSwarmAllPeers(t *testing.T) {
	ns := "QmTZPmuE4AP4AacBDdst7TNK5iBir3uwHvMX8tD9doZ4c7"

	p2ps := generateNetworks(t, 3, true, nil, nil)
	bootstrap1 := p2ps[0]
	bsAddr1 := bootstrap1.realAddr
	bootstrap2 := p2ps[1]
	bsAddr2 := bootstrap2.realAddr
	bootstrap3 := p2ps[2]
	bsAddr3 := bootstrap3.realAddr

	// 1 ---> 2
	err := bootstrap1.Provider(ns, true)
	require.Nil(t, err)

	// 2 ---> 3
	err = bootstrap2.Provider(ns, true)
	require.Nil(t, err)

	// 3 ---> 4
	err = bootstrap3.Provider(ns, true)
	require.Nil(t, err)

	addrs1, err := peer.AddrInfoToP2pAddrs(&bsAddr1)
	require.Nil(t, err)
	addrs2, err := peer.AddrInfoToP2pAddrs(&bsAddr2)
	require.Nil(t, err)
	addrs3, err := peer.AddrInfoToP2pAddrs(&bsAddr3)
	require.Nil(t, err)

	var bs1 = []string{addrs1[0].String()}
	var bs2 = []string{addrs2[0].String()}
	var bs3 = []string{addrs3[0].String()}
	p2ps2 := generateNetworks(t, 3, false, nil, func(index int) []Option {
		switch index {
		case 0:
			return []Option{WithBootstrap(bs1)}
		case 1:
			return []Option{WithBootstrap(bs2)}
		case 2:
			return []Option{WithBootstrap(bs3)}
		}
		return []Option{}
	})
	dht1 := p2ps2[0]
	dht2 := p2ps2[1]
	dht3 := p2ps2[2]

	dht2.ConnectP2P(dht1)
	dht3.ConnectP2P(dht1)

	// dht3 provide cid `ns`
	err = dht1.Provider(ns, true)
	require.Nil(t, err)
	time.Sleep(2 * time.Second)

	// dht2 provide cid `ns`
	err = dht2.Provider(ns, true)
	require.Nil(t, err)
	time.Sleep(2 * time.Second)

	// dht3 provide cid `ns`
	err = dht3.Provider(ns, true)
	require.Nil(t, err)
	time.Sleep(2 * time.Second)

	findPeerC, err := dht1.FindProvidersAsync(ns, 0)
	require.Nil(t, err)

	done := make(chan struct{})

	go allPeers(findPeerC, done)

	<-done
}

func allPeers(peers <-chan peer.AddrInfo, done chan struct{}) {
	for p := range peers {
		log.Printf("peer #%s addrs #%s", p.ID, p.Addrs)
	}
	done <- struct{}{}
}

func TestP2P_IsConnected(t *testing.T) {
	p2ps := generateNetworks(t, 2, true, nil, nil)
	p1 := p2ps[0]
	p2 := p2ps[1]

	isConnected := p2.IsConnected(p1.PeerID())
	assert.Equal(t, true, isConnected)
}
