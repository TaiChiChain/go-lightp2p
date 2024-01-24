package network

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestCreatePipe(t *testing.T) {
	peerIDs := []string{"1", "2"}
	mockHostManager := GenMockHostManager(peerIDs)
	peer1, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	peer2, err := NewMockP2P(peerIDs[1], mockHostManager, nil)
	assert.Nil(t, err)
	err = peer1.Start()
	assert.Nil(t, err)
	err = peer2.Start()
	assert.Nil(t, err)

	pipeId := "test"
	pipe1, err := peer1.CreatePipe(context.Background(), pipeId)
	assert.Nil(t, err)
	assert.Equal(t, pipeId, pipe1.String())

	pipe2, err := peer2.CreatePipe(context.Background(), pipeId)
	assert.Nil(t, err)
	assert.Equal(t, pipeId, pipe2.String())

	taskDoneCh := make(chan string, 1)
	go func() {
		for {
			msg := pipe2.Receive(context.Background())
			assert.Equal(t, "1", msg.From)
			assert.Equal(t, []byte("send pipe msg from 1 to 2"), msg.Data)
			taskDoneCh <- pipe2.String()
		}
	}()

	err = pipe1.Send(context.Background(), "2", []byte("send pipe msg from 1 to 2"))
	assert.Nil(t, err)
	v := <-taskDoneCh
	assert.Equal(t, pipe2.String(), v)

	// test duplicate pipe
	_, err = peer1.CreatePipe(context.Background(), pipeId)
	assert.NotNil(t, err)

	pipeId = "testPipe_v2"
	pip1, err := peer1.CreatePipe(context.Background(), pipeId)
	assert.Nil(t, err)

	// test pipe not exist
	err = pip1.Send(context.Background(), "2", []byte("send pipe_v2 msg from 1 to 2"))
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), ErrPipeNotExist.Error())

	pip2, err := peer2.CreatePipe(context.Background(), pipeId)
	assert.Nil(t, err)

	go func() {
		for {
			msg := pip2.Receive(context.Background())
			assert.Equal(t, "1", msg.From)
			assert.Equal(t, []byte("send pipe_v2 msg from 1 to 2"), msg.Data)
			taskDoneCh <- pip2.String()
		}
	}()

	err = pip1.Send(context.Background(), "2", []byte("send pipe_v2 msg from 1 to 2"))
	assert.Nil(t, err)
	v = <-taskDoneCh
	assert.Equal(t, pip2.String(), v)
}

func TestMockPipe_Broadcast(t *testing.T) {
	peerIDs := []string{"1", "2", "3", "4"}
	mockHostManager := GenMockHostManager(peerIDs)
	p2pM := make(map[string]*MockP2P)
	pipeM := make(map[string]Pipe)
	msgCh := make(map[string]chan *PipeMsg)
	lo.ForEach(peerIDs, func(id string, _ int) {
		p2p, err := NewMockP2P(id, mockHostManager, nil)
		assert.Nil(t, err)
		err = p2p.Start()
		assert.Nil(t, err)
		pipe, err := p2p.CreatePipe(context.Background(), "test")
		assert.Nil(t, err)

		msgCh[id] = make(chan *PipeMsg, queueSize)

		go func() {
			for {
				msg := pipe.Receive(context.Background())
				msgCh[id] <- msg
			}
		}()
		pipeM[id] = pipe
		p2pM[id] = p2p
	})

	err := pipeM["1"].Broadcast(context.Background(), []string{"2", "3", "4"}, []byte("test broadcast from 1"))
	assert.Nil(t, err)

	lo.ForEach(peerIDs, func(id string, _ int) {
		if id == "1" {
			return
		}
		msg := <-msgCh[id]
		assert.Equal(t, "1", msg.From)
		assert.Equal(t, []byte("test broadcast from 1"), msg.Data)
	})
}

func TestMockP2P_Send(t *testing.T) {
	peerIDs := []string{"1", "2"}
	mockHostManager := GenMockHostManager(peerIDs)
	peer1, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	peer2, err := NewMockP2P(peerIDs[1], mockHostManager, nil)
	assert.Nil(t, err)
	err = peer1.Start()
	assert.Nil(t, err)
	err = peer2.Start()
	assert.Nil(t, err)

	peer1.SetMessageHandler(func(s Stream, msg []byte) {
		err = s.AsyncSend(msg)
		assert.Nil(t, err)
	})

	msg := []byte("test")
	res, err := peer2.Send(peer1.PeerID(), msg)
	assert.Nil(t, err)
	assert.Equal(t, msg, res)
}

func TestMockP2P_AsyncSend(t *testing.T) {
	peerIDs := []string{"1", "2"}
	mockHostManager := GenMockHostManager(peerIDs)
	peer1, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	peer2, err := NewMockP2P(peerIDs[1], mockHostManager, nil)
	assert.Nil(t, err)
	err = peer1.Start()
	assert.Nil(t, err)
	err = peer2.Start()
	assert.Nil(t, err)

	data := []byte("test")
	peer1.SetMessageHandler(func(s Stream, msg []byte) {
		assert.Equal(t, string(data), string(msg))
	})
	peer2.SetMessageHandler(func(s Stream, msg []byte) {
		assert.Equal(t, string(data), string(msg))
	})

	err = peer2.AsyncSend(peer1.PeerID(), data)
	assert.Nil(t, err)

	err = peer1.AsyncSend(peer2.PeerID(), data)
	assert.Nil(t, err)
}

func TestMockP2P_AsyncSendWithStream(t *testing.T) {
	peerIDs := []string{"1", "2"}
	mockHostManager := GenMockHostManager(peerIDs)
	peer1, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	peer2, err := NewMockP2P(peerIDs[1], mockHostManager, nil)
	assert.Nil(t, err)
	err = peer1.Start()
	assert.Nil(t, err)
	err = peer2.Start()
	assert.Nil(t, err)

	data := genTestMessages(100)

	peer1.SetMessageHandler(func(s Stream, msg []byte) {
		err = s.AsyncSend(msg)
		assert.Nil(t, err)
		for _, v := range data {
			err = s.AsyncSend(v)
			assert.Nil(t, err)
		}
	})

	stream, err := peer2.GetStream(peer1.PeerID())
	assert.Nil(t, err)

	reqMsg := []byte("test")
	res, err := stream.Send(reqMsg)
	assert.Nil(t, err)
	assert.Equal(t, reqMsg, res)

	for _, v := range data {
		d, err := stream.Read(5 * time.Second)
		assert.Nil(t, err)
		assert.Equal(t, string(v), string(d))
	}

	peer2.ReleaseStream(stream)
}

func TestMockP2P_Broadcast(t *testing.T) {
	receiveNum := 500

	sendID := "0"
	peerIDs := []string{sendID}
	for i := 1; i <= receiveNum; i++ {
		peerIDs = append(peerIDs, strconv.Itoa(i))
	}

	mockHostManager := GenMockHostManager(peerIDs)
	sender, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	err = sender.Start()
	assert.Nil(t, err)

	data := []byte("test")

	for i := 1; i <= receiveNum; i++ {
		peer, err := NewMockP2P(peerIDs[i], mockHostManager, nil)
		assert.Nil(t, err)
		err = peer.Start()
		assert.Nil(t, err)

		peer.SetMessageHandler(func(s Stream, msg []byte) {
			assert.Equal(t, string(data), string(msg))
		})
	}

	err = sender.Broadcast(peerIDs[1:], data)
	assert.Nil(t, err)

	err = sender.Broadcast(peerIDs, data)
	assert.Nil(t, err)
}

func TestMockP2P_ParallelSend(t *testing.T) {
	receiveNum := 500

	sendID := "0"
	peerIDs := []string{sendID}
	for i := 1; i <= receiveNum; i++ {
		peerIDs = append(peerIDs, strconv.Itoa(i))
	}

	mockHostManager := GenMockHostManager(peerIDs)
	sender, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	err = sender.Start()
	assert.Nil(t, err)

	data := genTestMessages(1000)

	var receivers []*MockP2P
	for i := 1; i <= receiveNum; i++ {
		peer, err := NewMockP2P(peerIDs[i], mockHostManager, nil)
		assert.Nil(t, err)
		err = peer.Start()
		assert.Nil(t, err)
		receivers = append(receivers, peer)

		peer.SetMessageHandler(func(s Stream, msg []byte) {
			err = s.AsyncSend(msg)
			assert.Nil(t, err)
			for _, v := range data {
				err = s.AsyncSend(v)
				assert.Nil(t, err)
			}
		})
	}

	wg := sync.WaitGroup{}
	wg.Add(receiveNum)
	send := func(p *MockP2P) {
		stream, err := sender.GetStream(p.PeerID())
		assert.Nil(t, err)

		reqMsg := []byte("test")
		res, err := stream.Send(reqMsg)
		assert.Nil(t, err)
		assert.Equal(t, reqMsg, res)

		for _, v := range data {
			d, err := stream.Read(5 * time.Second)
			assert.Nil(t, err)
			assert.Equal(t, string(v), string(d))
		}
		wg.Done()
	}
	for _, v := range receivers {
		go send(v)
	}
	wg.Wait()
}

func TestMockP2P_ParallelReceive(t *testing.T) {
	senderNum := 500

	receiverID := "0"
	peerIDs := []string{receiverID}
	for i := 1; i <= senderNum; i++ {
		peerIDs = append(peerIDs, strconv.Itoa(i))
	}

	mockHostManager := GenMockHostManager(peerIDs)
	receiver, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	err = receiver.Start()
	assert.Nil(t, err)

	var senders []*MockP2P
	for i := 1; i <= senderNum; i++ {
		peer, err := NewMockP2P(peerIDs[i], mockHostManager, nil)
		assert.Nil(t, err)
		err = peer.Start()
		assert.Nil(t, err)
		senders = append(senders, peer)
	}

	data := genTestMessages(1000)

	receiver.SetMessageHandler(func(s Stream, msg []byte) {
		err = s.AsyncSend(msg)
		assert.Nil(t, err)
		for _, v := range data {
			err = s.AsyncSend(v)
			assert.Nil(t, err)
		}
	})

	wg := sync.WaitGroup{}
	wg.Add(senderNum)
	send := func(p *MockP2P) {
		stream, err := p.GetStream(receiver.PeerID())
		assert.Nil(t, err)

		reqMsg := []byte("test")
		res, err := stream.Send(reqMsg)
		assert.Nil(t, err)
		assert.Equal(t, reqMsg, res)

		for _, v := range data {
			d, err := stream.Read(5 * time.Second)
			assert.Nil(t, err)
			assert.Equal(t, string(v), string(d))
		}
		wg.Done()
	}
	for _, v := range senders {
		go send(v)
	}
	wg.Wait()
}

func TestMockP2P_ReleaseStream(t *testing.T) {
	peerIDs := []string{"1", "2"}
	mockHostManager := GenMockHostManager(peerIDs)
	peer1, err := NewMockP2P(peerIDs[0], mockHostManager, nil)
	assert.Nil(t, err)
	peer2, err := NewMockP2P(peerIDs[1], mockHostManager, nil)
	assert.Nil(t, err)
	err = peer1.Start()
	assert.Nil(t, err)
	err = peer2.Start()
	assert.Nil(t, err)

	stream, err := peer2.GetStream(peer1.PeerID())
	assert.Nil(t, err)

	peer1.SetMessageHandler(func(s Stream, msg []byte) {
		defer peer1.ReleaseStream(s)

		err = s.AsyncSend(msg)
		assert.Nil(t, err)
	})

	msg := []byte("test")
	res, err := stream.Send(msg)
	assert.Nil(t, err)
	assert.Equal(t, msg, res)

	peer2.ReleaseStream(stream)
}

func genTestMessages(size int) [][]byte {
	msgList := make([][]byte, size)
	for i := 1; i <= size; i++ {
		msgList = append(msgList, []byte(strconv.Itoa(i)))
	}
	return msgList
}
