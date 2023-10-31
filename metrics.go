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
	rejectedMsgNum = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "gossipsub",
		Name:      "rejected_msg_counter",
		Help:      "the total number of rejected msgs",
	})
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
)

func init() {
	prometheus.MustRegister(duplicateMsgNum)
	prometheus.MustRegister(rejectedMsgNum)
	prometheus.MustRegister(undeliverableMsgNum)
	prometheus.MustRegister(deliveredMsgNum)

	duplicateMsgNum.Set(0)
	rejectedMsgNum.Set(0)
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
	rejectedMsgNum.Inc()
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
