package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-log/v2"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type pipeResult struct {
	msgs            map[string][]bool
	receivedTracker map[string]int
}

func benchmarkPipeBroadcast(b *testing.B, typ PipeBroadcastType, compressionAlgo CompressionAlgo, tps int, totalMsgs int, extraBigData []byte) {
	l := logrus.New()
	l.Level = logrus.ErrorLevel
	err := log.SetLogLevelRegex("pubsub", "error")
	require.Nil(b, err)
	p2ps := generateNetworks(b, 4, true, []Option{
		WithPipe(PipeConfig{
			BroadcastType:       typ,
			ReceiveMsgCacheSize: 2048,
			SimpleBroadcast: PipeSimpleConfig{
				WorkerCacheSize:        1024,
				WorkerConcurrencyLimit: 20,
			},
			Gossipsub: PipeGossipsubConfig{
				SubBufferSize:          2048,
				PeerOutboundBufferSize: 2048,
				ValidateBufferSize:     2048,
				SeenMessagesTTL:        120 * time.Second,
				EventTracer:            &testEventTracer{},
			},
			UnicastReadTimeout:       5 * time.Second,
			UnicastSendRetryNumber:   5,
			UnicastSendRetryBaseTime: 100 * time.Millisecond,
			FindPeerTimeout:          10 * time.Second,
			ConnectTimeout:           1 * time.Second,
		}),
		WithLogger(l),
		WithCompressionOption(compressionAlgo),
	}, nil)

	ctx := context.Background()
	var pipes []Pipe
	var p2pIDs []string
	pipeID := "benchmark_pipe"
	for _, p2p := range p2ps {
		pipe, err := p2p.CreatePipe(ctx, pipeID)
		require.Nil(b, err)
		pipes = append(pipes, pipe)
		p2pIDs = append(p2pIDs, p2p.PeerID())
	}

	fmt.Println("\nwait for pubsub startup")
	// wait pubsub startup
	time.Sleep(1000 * time.Millisecond)

	results := make(map[string]*pipeResult)
	for i := range p2ps {
		r := &pipeResult{
			msgs:            make(map[string][]bool),
			receivedTracker: make(map[string]int),
		}
		results[p2pIDs[i]] = r
		for j := range p2ps {
			if i != j {
				r.msgs[p2pIDs[j]] = make([]bool, totalMsgs)
			}
		}
		go func(idx int) {
			for {
				msg := pipes[idx].Receive(context.Background())
				msgIdx := binary.BigEndian.Uint64(msg.Data[1:])
				r.msgs[msg.From][msgIdx] = true
				r.receivedTracker[msg.From]++
				if r.receivedTracker[msg.From]%(totalMsgs/20) == 0 {
					fmt.Printf("%s receive msg total count %d from %s\n", p2pIDs[idx], r.receivedTracker[msg.From], msg.From)
				}
			}
		}(i)
	}

	fmt.Println("\nstart broadcast")
	wg := new(sync.WaitGroup)
	wg.Add(len(p2ps))
	for i := range p2ps {
		go func(idx int) {
			defer wg.Done()
			limiter := rate.NewLimiter(rate.Limit(tps), 1)
			ctx := context.Background()
			for j := 0; j < totalMsgs; j++ {
				err := limiter.Wait(ctx)
				require.Nil(b, err)
				data := make([]byte, 9)
				data[0] = byte(idx)
				binary.BigEndian.PutUint64(data[1:], uint64(j))
				err = pipes[idx].Broadcast(context.Background(), p2pIDs, data)
				require.Nil(b, err)
			}
			fmt.Printf("\nfinished broadcast, pip index: %d, time: %s\n", idx, time.Now().Format("2006-01-02 15:04:05.999999999 -0700 MST"))
		}(i)
	}

	wg.Wait()

	fmt.Println("\nwaiting for receiving message")
	time.Sleep(5 * time.Second)
	fmt.Println("\nstart check for lost messages")
	// check for lost messages
	for receiver, r := range results {
		fmt.Println()
		for sender, msgList := range r.msgs {
			totalLost := 0
			for _, msgReceived := range msgList {
				if !msgReceived {
					totalLost++
					//fmt.Printf("%s lost msg id %d from %s\n", receiver, index, sender)
				}
				// require.True(b, msgReceived, fmt.Sprintf("%s lost msg[%d] from %s", receiver, msgIdx, sender))
			}
			fmt.Printf("%s lost msg total %d from %s\n", receiver, totalLost, sender)
		}
	}
}

func benchmarkMockPipeBroadcast(b *testing.B, tps int, totalMsgs int) {
	l := logrus.New()
	l.Level = logrus.ErrorLevel
	err := log.SetLogLevelRegex("pubsub", "error")
	require.Nil(b, err)
	peers := []string{"0", "1", "2", "3"}
	pm := GenMockHostManager(peers)
	p2ps := make([]*MockP2P, 0)
	lo.ForEach(peers, func(p string, i int) {
		p2p, err := NewMockP2P(p, pm, nil)
		require.Nil(b, err)
		err = p2p.Start()
		require.Nil(b, err)
		p2ps = append(p2ps, p2p)
	})

	ctx := context.Background()
	var pipes []Pipe
	var p2pIDs []string
	pipeID := "benchmark_pipe"
	for _, p2p := range p2ps {
		pipe, err := p2p.CreatePipe(ctx, pipeID)
		require.Nil(b, err)
		pipes = append(pipes, pipe)
		p2pIDs = append(p2pIDs, p2p.PeerID())
	}

	// wait pubsub startup
	time.Sleep(1000 * time.Millisecond)

	results := make(map[string]*pipeResult)
	for i := range p2ps {
		r := &pipeResult{
			msgs:            make(map[string][]bool),
			receivedTracker: make(map[string]int),
		}
		results[p2pIDs[i]] = r
		for j := range p2ps {
			if i != j {
				r.msgs[p2pIDs[j]] = make([]bool, totalMsgs)
			}
		}
		go func(idx int) {
			for {
				msg := pipes[idx].Receive(context.Background())
				msgIdx := binary.BigEndian.Uint64(msg.Data[1:])
				if r.msgs[msg.From][msgIdx] {
					panic(fmt.Sprintf("%s receive duplicated msg %d from %s", p2pIDs[idx], msgIdx, msg.From))
				}
				r.msgs[msg.From][msgIdx] = true
				r.receivedTracker[msg.From]++
				if r.receivedTracker[msg.From]%(totalMsgs/20) == 0 {
					fmt.Printf("%s receive msg total count %d from %s\n", p2pIDs[idx], r.receivedTracker[msg.From], msg.From)
				}
			}
		}(i)
	}

	fmt.Println("\nstart broadcast")
	wg := new(sync.WaitGroup)
	wg.Add(len(p2ps))
	for i := range p2ps {
		go func(idx int) {
			defer wg.Done()
			limiter := rate.NewLimiter(rate.Limit(tps), 1)
			ctx := context.Background()
			for j := 0; j < totalMsgs; j++ {
				err := limiter.Wait(ctx)
				require.Nil(b, err)
				data := make([]byte, 9)
				data[0] = byte(idx)
				binary.BigEndian.PutUint64(data[1:], uint64(j))
				remotePeers := lo.Filter(p2pIDs, func(p string, _ int) bool {
					return p != strconv.Itoa(idx)
				})
				err = pipes[idx].Broadcast(context.Background(), remotePeers, data)
				require.Nil(b, err)
				// time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	time.Sleep(5 * time.Second)
	fmt.Println("\nstart check for lost messages")
	// check for lost messages
	for receiver, r := range results {
		fmt.Println()
		for sender, msgList := range r.msgs {
			totalLost := 0
			for _, msgReceived := range msgList {
				if !msgReceived {
					totalLost++
				}
				// require.True(b, msgReceived, fmt.Sprintf("%s lost msg[%d] from %s", receiver, msgIdx, sender))
			}
			fmt.Printf("%s lost msg total %d from %s\n", receiver, totalLost, sender)
		}
	}
}

type testEventTracer struct {
}

func (t *testEventTracer) Trace(evt *pb.TraceEvent) {
	// switch *evt.Type {
	// case pb.TraceEvent_DUPLICATE_MESSAGE:
	//	fmt.Println("TraceEvent_DUPLICATE_MESSAGE")
	// case pb.TraceEvent_REJECT_MESSAGE:
	//	fmt.Println("TraceEvent_REJECT_MESSAGE")
	// }
}

func BenchmarkPipe_simple(b *testing.B) {
	tps := 2000
	benchmarkPipeBroadcast(b, PipeBroadcastSimple, NoCompression, tps, tps*20, nil)
	benchmarkPipeBroadcast(b, PipeBroadcastSimple, SnappyCompression, tps, tps*20, nil)
	benchmarkPipeBroadcast(b, PipeBroadcastSimple, ZstdCompression, tps, tps*20, nil)
}

func BenchmarkPipe_gossip(b *testing.B) {
	tps := 2000
	benchmarkPipeBroadcast(b, PipeBroadcastGossip, NoCompression, tps, tps*20, nil)
	benchmarkPipeBroadcast(b, PipeBroadcastGossip, SnappyCompression, tps, tps*20, nil)
	benchmarkPipeBroadcast(b, PipeBroadcastGossip, ZstdCompression, tps, tps*20, nil)
}

func BenchmarkNamePipe_unicast(b *testing.B) {
	benchmarkPipeUnicast(b, NoCompression)
	benchmarkPipeUnicast(b, SnappyCompression)
	benchmarkPipeUnicast(b, ZstdCompression)
}

func benchmarkPipeUnicast(b *testing.B, compressionAlgo CompressionAlgo) {
	tps := 2000
	totalMsgs := 20 * tps

	l := logrus.New()
	l.Level = logrus.ErrorLevel
	err := log.SetLogLevelRegex("pubsub", "error")
	require.Nil(b, err)
	p2ps := generateNetworks(b, 2, true, []Option{
		WithPipe(PipeConfig{
			BroadcastType:       PipeBroadcastGossip,
			ReceiveMsgCacheSize: 1024,
			SimpleBroadcast: PipeSimpleConfig{
				WorkerCacheSize:        1024,
				WorkerConcurrencyLimit: 20,
			},
			Gossipsub: PipeGossipsubConfig{
				SubBufferSize:          1024,
				PeerOutboundBufferSize: 1024,
				ValidateBufferSize:     1024,
				SeenMessagesTTL:        120 * time.Second,
				EventTracer:            &testEventTracer{},
			},
			UnicastReadTimeout:       5 * time.Second,
			UnicastSendRetryNumber:   5,
			UnicastSendRetryBaseTime: 100 * time.Millisecond,
			FindPeerTimeout:          10 * time.Second,
			ConnectTimeout:           1 * time.Second,
		}),
		WithLogger(l),
		WithCompressionOption(compressionAlgo),
	}, nil)

	ctx := context.Background()
	receiverID := p2ps[1].PeerID()
	pipeID := "benchmark_pipe"
	senderPipe, err := p2ps[0].CreatePipe(ctx, pipeID)
	require.Nil(b, err)
	receiverPipe, err := p2ps[1].CreatePipe(ctx, pipeID)
	require.Nil(b, err)

	go func() {
		limiter := rate.NewLimiter(rate.Limit(tps), 1)
		ctx := context.Background()
		for j := 0; j < totalMsgs; j++ {
			err := limiter.Wait(ctx)
			require.Nil(b, err)
			data := make([]byte, 9)
			data[0] = byte(j)
			binary.BigEndian.PutUint64(data[1:], uint64(j))
			err = senderPipe.Send(context.Background(), receiverID, data)
			require.Nil(b, err)
		}
	}()
	msgs := make([]bool, totalMsgs)
	receivedCnt := 0
	for {
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
		msg := receiverPipe.Receive(timeoutCtx)
		timeoutCancel()
		require.NotNil(b, msg)
		msgIdx := binary.BigEndian.Uint64(msg.Data[1:])
		msgs[msgIdx] = true
		receivedCnt++
		if receivedCnt == totalMsgs {
			break
		}
		if receivedCnt%(totalMsgs/20) == 0 {
			fmt.Printf("receive msg total count %d\n", receivedCnt)
		}
	}
	for _, received := range msgs {
		require.True(b, received)
	}
}

func BenchmarkMockPipe(b *testing.B) {
	tps := 2000
	totalMsgs := 20 * tps
	benchmarkMockPipeBroadcast(b, tps, totalMsgs)
}
