package network

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testPipe(t *testing.T, typ PipeBroadcastType) {
	p2ps := generateNetworks(t, 3, true, []Option{
		WithPipeBroadcastType(typ),
	}, nil)

	ctx := context.Background()
	var pipes []Pipe
	var p2pIDs []string
	pipeID := "test_pipe"
	for _, p2p := range p2ps {
		pipe, err := p2p.CreatePipe(ctx, pipeID)
		assert.Nil(t, err)
		pipes = append(pipes, pipe)
		p2pIDs = append(p2pIDs, p2p.PeerID())
	}

	var pipes2 []Pipe
	pipeID2 := "test_pipe2"
	for _, p2p := range p2ps {
		pipe, err := p2p.CreatePipe(ctx, pipeID2)
		assert.Nil(t, err)
		pipes2 = append(pipes2, pipe)
	}

	// wait pubsub startup
	time.Sleep(1000 * time.Millisecond)

	for i, sender := range pipes {
		msg := []byte(fmt.Sprintf("msg_from_sender_%d", i))
		err := sender.Broadcast(ctx, p2pIDs, msg)
		assert.Nil(t, err)
		for j, pipe := range pipes {
			if j == i {
				timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer timeoutCancel()
				pipeRecveMsg := pipe.Receive(timeoutCtx)
				assert.Nil(t, pipeRecveMsg, p2pIDs[j])
			} else {
				timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer timeoutCancel()
				pipeRecveMsg := pipe.Receive(timeoutCtx)
				assert.NotNil(t, pipeRecveMsg)
				assert.Equal(t, p2pIDs[i], pipeRecveMsg.From)
				assert.Equal(t, msg, pipeRecveMsg.Data)
			}
		}
	}

	// check no msgs
	for i, pipe := range pipes {
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer timeoutCancel()
		pipeRecveMsg := pipe.Receive(timeoutCtx)
		assert.Nil(t, pipeRecveMsg, p2pIDs[i])
	}

	// check unicast
	for i, sender := range pipes {
		for j, receiver := range pipes {
			if i == j {
				continue
			}
			msg := []byte(fmt.Sprintf("msg_from_sender_%d_to_receiver_%d", i, j))
			err := sender.Send(ctx, p2pIDs[j], msg)
			assert.Nil(t, err)
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			assert.NotNil(t, pipeRecveMsg)
			assert.Equal(t, p2pIDs[i], pipeRecveMsg.From)
			assert.Equal(t, msg, pipeRecveMsg.Data)
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
			assert.Nil(t, err)
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			assert.Nil(t, pipeRecveMsg)
		}
	}
	for i, sender := range pipes {
		msg := []byte(fmt.Sprintf("msg_from_sender_%d", i))
		err := sender.Broadcast(ctx, p2pIDs, msg)
		assert.Nil(t, err)
		for j, receiver := range pipes2 {
			if i == j {
				continue
			}
			timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer timeoutCancel()
			pipeRecveMsg := receiver.Receive(timeoutCtx)
			assert.Nil(t, pipeRecveMsg)
		}
	}
}

func TestPipe_simple(t *testing.T) {
	testPipe(t, PipeBroadcastSimple)
}

func TestPipe_gossip(t *testing.T) {
	testPipe(t, PipeBroadcastGossip)
}

func TestPipe_flood(t *testing.T) {
	testPipe(t, PipeBroadcastFloodSub)
}
