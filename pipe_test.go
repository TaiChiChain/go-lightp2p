package network

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testPipe(t *testing.T, typ PipeBroadcastType, compressionAlgo CompressionAlgo) {
	l := logrus.New()
	l.Level = logrus.ErrorLevel
	p2ps := generateNetworks(t, 3, true, []Option{
		WithPipe(PipeConfig{
			BroadcastType:       typ,
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

	// check broadcast
	t.Run("check broadcast", func(t *testing.T) {
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
	})

	// check no msgs
	t.Run("check no msgs", func(t *testing.T) {
		for i, pipe := range pipes {
			func() {
				timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer timeoutCancel()
				pipeRecveMsg := pipe.Receive(timeoutCtx)
				require.Nil(t, pipeRecveMsg, p2pIDs[i])
			}()
		}
	})

	// check unicast
	t.Run("check unicast", func(t *testing.T) {
		for i, sender := range pipes {
			for j, receiver := range pipes {
				if i == j {
					continue
				}
				func() {
					msg := []byte(fmt.Sprintf("msg_from_sender_%d_to_receiver_%d", i, j))
					err := sender.Send(ctx, p2pIDs[j], msg)
					require.Nil(t, err)
					timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer timeoutCancel()
					pipeRecveMsg := receiver.Receive(timeoutCtx)
					require.NotNil(t, pipeRecveMsg)
					require.Equal(t, p2pIDs[i], pipeRecveMsg.From)
					require.Equal(t, msg, pipeRecveMsg.Data)
				}()
			}
		}
	})

	//	check unicast too large msg
	t.Run("check unicast too large msg", func(t *testing.T) {
		sender, receiver := pipes[0], pipes[1]

		func() {
			tooLargeMsg := make([]byte, maxMessageSize+100)
			err := sender.Send(ctx, p2pIDs[1], tooLargeMsg)
			fmt.Println(err)
			require.Error(t, err)
		}()

		// after is normal
		func() {
			msg := []byte("msg_from_sender_0_to_receiver_1")
			err := sender.Send(ctx, p2pIDs[1], msg)
			require.Nil(t, err)
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			require.NotNil(t, pipeRecveMsg)
			require.Equal(t, p2pIDs[0], pipeRecveMsg.From)
			require.Equal(t, msg, pipeRecveMsg.Data)
		}()
	})

	// check pipe isolation
	t.Run("check pipe isolation", func(t *testing.T) {
		for i, sender := range pipes {
			for j, receiver := range pipes2 {
				if i == j {
					continue
				}

				func() {
					msg := []byte(fmt.Sprintf("msg_from_sender_%d_to_receiver_%d", i, j))
					err := sender.Send(ctx, p2pIDs[j], msg)
					require.Nil(t, err)
					timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer timeoutCancel()
					pipeRecveMsg := receiver.Receive(timeoutCtx)
					require.Nil(t, pipeRecveMsg)
				}()
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
				func() {
					timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer timeoutCancel()
					pipeRecveMsg := receiver.Receive(timeoutCtx)
					require.Nil(t, pipeRecveMsg)
				}()
			}
		}
	})
}

func TestPipe_simple(t *testing.T) {
	testPipe(t, PipeBroadcastSimple, NoCompression)
	testPipe(t, PipeBroadcastSimple, SnappyCompression)
	testPipe(t, PipeBroadcastSimple, ZstdCompression)
}

func TestPipe_gossip(t *testing.T) {
	testPipe(t, PipeBroadcastGossip, NoCompression)
	testPipe(t, PipeBroadcastGossip, SnappyCompression)
	testPipe(t, PipeBroadcastGossip, ZstdCompression)
}
