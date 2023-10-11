package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-log/v2"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type pipeResult struct {
	msgs            map[string][]bool
	receivedTracker map[string]int
}

func benchmarkPipe(b *testing.B, typ PipeBroadcastType, tps int, totalMsgs int) {
	l := logrus.New()
	l.Level = logrus.ErrorLevel
	err := log.SetLogLevelRegex("pubsub", "error")
	require.Nil(b, err)
	p2ps := generateNetworks(b, 4, true, []Option{
		WithPipe(PipeConfig{
			BroadcastType:       typ,
			ReceiveMsgCacheSize: 1024,
			SimpleBroadcast: PipeSimpleConfig{
				WorkerCacheSize:        1024,
				WorkerConcurrencyLimit: 20,
				RetryNumber:            5,
				RetryBaseTime:          100 * time.Millisecond,
			},
			Gossipsub: PipeGossipsubConfig{
				SubBufferSize:          1024,
				PeerOutboundBufferSize: 1024,
				ValidateBufferSize:     1024,
				SeenMessagesTTL:        120 * time.Second,
				EventTracer:            &EventTracer{},
			},
		}),
		WithLogger(l),
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
				time.Sleep(70 * time.Millisecond)
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

type EventTracer struct {
}

func (t *EventTracer) Trace(evt *pb.TraceEvent) {
	// switch *evt.Type {
	// case pb.TraceEvent_DUPLICATE_MESSAGE:
	//	fmt.Println("TraceEvent_DUPLICATE_MESSAGE")
	// case pb.TraceEvent_REJECT_MESSAGE:
	//	fmt.Println("TraceEvent_REJECT_MESSAGE")
	// }
}

func BenchmarkNamePipe_simple(b *testing.B) {
	tps := 2000
	benchmarkPipe(b, PipeBroadcastSimple, tps, tps*20)
}

func BenchmarkNamePipe_gossip(b *testing.B) {
	tps := 2000
	benchmarkPipe(b, PipeBroadcastGossip, tps, tps*20)
}
