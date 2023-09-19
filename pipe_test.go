package network

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testPipe(t *testing.T, typ PipeBroadcastType) {
	l := logrus.New()
	l.Level = logrus.ErrorLevel
	p2ps := generateNetworks(t, 3, true, []Option{
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
			},
		}),
		WithLogger(l),
	}, nil)

	ctx := context.Background()
	var pipes []Pipe
	var p2pIDs []string
	pipeID := "test_pipe"
	for _, p2p := range p2ps {
		pipe, err := p2p.CreatePipe(ctx, pipeID)
		require.Nil(t, err)
		pipes = append(pipes, pipe)
		p2pIDs = append(p2pIDs, p2p.PeerID())
	}

	var pipes2 []Pipe
	pipeID2 := "test_pipe2"
	for _, p2p := range p2ps {
		pipe, err := p2p.CreatePipe(ctx, pipeID2)
		require.Nil(t, err)
		pipes2 = append(pipes2, pipe)
	}

	// wait pubsub startup
	time.Sleep(1000 * time.Millisecond)

	for i, sender := range pipes {
		sender := sender
		msg := []byte(fmt.Sprintf("msg_from_sender_%d", i))
		err := sender.Broadcast(ctx, p2pIDs, msg)
		require.Nil(t, err)
		for j, pipe := range pipes {
			pipe := pipe
			if j == i {
				timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				pipeRecveMsg := pipe.Receive(timeoutCtx)
				require.Nil(t, pipeRecveMsg, p2pIDs[j])
				timeoutCancel()
			} else {
				timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				pipeRecveMsg := pipe.Receive(timeoutCtx)
				require.NotNil(t, pipeRecveMsg)
				require.Equal(t, p2pIDs[i], pipeRecveMsg.From)
				require.Equal(t, msg, pipeRecveMsg.Data)
				timeoutCancel()
			}
		}
	}

	// check no msgs
	for i, pipe := range pipes {
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer timeoutCancel()
		pipeRecveMsg := pipe.Receive(timeoutCtx)
		require.Nil(t, pipeRecveMsg, p2pIDs[i])
	}

	// check unicast
	for i, sender := range pipes {
		for j, receiver := range pipes {
			if i == j {
				continue
			}
			msg := []byte(fmt.Sprintf("msg_from_sender_%d_to_receiver_%d", i, j))
			err := sender.Send(ctx, p2pIDs[j], msg)
			require.Nil(t, err)
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			require.NotNil(t, pipeRecveMsg)
			require.Equal(t, p2pIDs[i], pipeRecveMsg.From)
			require.Equal(t, msg, pipeRecveMsg.Data)
		}
	}

	// check pipe isolation
	for i, sender := range pipes {
		for j, receiver := range pipes2 {
			if i == j {
				continue
			}

			msg := []byte(fmt.Sprintf("msg_from_sender_%d_to_receiver_%d", i, j))
			err := sender.Send(ctx, p2pIDs[j], msg)
			require.Nil(t, err)
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			require.Nil(t, pipeRecveMsg)
		}
	}
	for i, sender := range pipes {
		msg := []byte(fmt.Sprintf("msg_from_sender_%d", i))
		err := sender.Broadcast(ctx, p2pIDs, msg)
		require.Nil(t, err)
		for j, receiver := range pipes2 {
			if i == j {
				continue
			}
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			require.Nil(t, pipeRecveMsg)
		}
	}
}

func TestPipe_simple(t *testing.T) {
	testPipe(t, PipeBroadcastSimple)
}

func TestPipe_gossip(t *testing.T) {
	testPipe(t, PipeBroadcastGossip)
}
