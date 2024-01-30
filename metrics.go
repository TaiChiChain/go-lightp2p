package network

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	duplicateMsgNum = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "gossipsub",
		Name:      "duplicate_msg_counter",
		Help:      "the total number of duplicate msgs",
	})
	rejectedMsgNum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "gossipsub",
		Name:      "rejected_msg_counter",
		Help:      "the total number of rejected msgs",
	}, []string{"reason"})
	undeliverableMsgNum = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "gossipsub",
		Name:      "undeliverable_msg_counter",
		Help:      "the total number of undeliverable msgs",
	})
	deliveredMsgNum = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "gossipsub",
		Name:      "delivered_msg_counter",
		Help:      "the total number of delivered msgs",
	})
	sendDataSize = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "p2p",
		Name:      "send_data_size",
		Help:      "The size of sent data",
	})
	recvDataSize = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "p2p",
		Name:      "recv_data_size",
		Help:      "The size of receive data",
	})
	compressionReduceDataSize = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "p2p",
		Name:      "compression_reduce_data_size",
		Help:      "The reduce data size because of compression",
	})
	compressionIncreaseDataSize = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "p2p",
		Name:      "compression_increase_data_size",
		Help:      "The increase data size because of compression",
	})
)

func init() {
	prometheus.MustRegister(duplicateMsgNum)
	prometheus.MustRegister(rejectedMsgNum)
	prometheus.MustRegister(undeliverableMsgNum)
	prometheus.MustRegister(deliveredMsgNum)
	prometheus.MustRegister(sendDataSize)
	prometheus.MustRegister(recvDataSize)
	prometheus.MustRegister(compressionReduceDataSize)
	prometheus.MustRegister(compressionIncreaseDataSize)

	duplicateMsgNum.Set(0)
	undeliverableMsgNum.Set(0)
	deliveredMsgNum.Set(0)
}

type MetricsTracer struct {
}

func (t *MetricsTracer) AddPeer(p peer.ID, proto protocol.ID) {}

func (t *MetricsTracer) RemovePeer(p peer.ID) {}

func (t *MetricsTracer) Join(topic string) {}

func (t *MetricsTracer) Leave(topic string) {}

func (t *MetricsTracer) Graft(p peer.ID, topic string) {}

func (t *MetricsTracer) Prune(p peer.ID, topic string) {}

func (t *MetricsTracer) ValidateMessage(msg *pubsub.Message) {}

func (t *MetricsTracer) DeliverMessage(msg *pubsub.Message) {
	deliveredMsgNum.Inc()
}

func (t *MetricsTracer) RejectMessage(msg *pubsub.Message, reason string) {
	rejectedMsgNum.With(prometheus.Labels{"reason": reason}).Inc()
	rejectedMsgNum.With(prometheus.Labels{"reason": "all"}).Inc()
}

func (t *MetricsTracer) DuplicateMessage(msg *pubsub.Message) {
	duplicateMsgNum.Inc()
}

func (t *MetricsTracer) ThrottlePeer(p peer.ID) {}

func (t *MetricsTracer) RecvRPC(rpc *pubsub.RPC) {}

func (t *MetricsTracer) SendRPC(rpc *pubsub.RPC, p peer.ID) {}

func (t *MetricsTracer) DropRPC(rpc *pubsub.RPC, p peer.ID) {}

func (t *MetricsTracer) UndeliverableMessage(msg *pubsub.Message) {
	undeliverableMsgNum.Inc()
}
