package core

import (
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"go.uber.org/zap"
	"testing"
)

type FakeNodeReceiver struct {
	ctx *ClientContext
	out []*protocol.InternodeReply
}

func (f *FakeNodeReceiver) SetCallback(ck NodeReceiverCallback) {
}

func (f *FakeNodeReceiver) OnConnect(ctx *ClientContext) error {
	return nil
}

func (f *FakeNodeReceiver) OnInit() {
}

func (f *FakeNodeReceiver) GetContext() *ClientContext {
	return f.ctx
}

func (f *FakeNodeReceiver) SendReply(reply *protocol.InternodeReply) error {
	f.out = append(f.out, reply)
	return nil
}

func TestStubAgent(t *testing.T) {
	node := &FakeNodeReceiver{
		ctx: &ClientContext{
			Version:  "0.1",
			Account:  System,
			DeviceId: "deviceId",
			RemoteIP: "127.0.0.1",
			Id:       1,
		},
	}
	proxyManager := &FakeProxyManager{}
	ip1 := "127.0.0.1"
	n := &NodeStatInfo{
		NodeId:    "node1",
		IP:        "192.168.1.1",
		PrivateIP: ip1,
	}
	feedManager := CreateFeedManager(NewFeedInfoDb(InMemory, "us-west-2", "feed_info"), proxyManager, n, zap.L())
	feedManager.Init()
	handler := InternodeCommandHandler{
		feedManager: feedManager,
	}
	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	stub1 := NewStubClient(node, &handler)
	stub1.OnInit()
	stub1.OnSendCommand(&protocol.InternodeCommand{
		Method: protocol.InternodeCommand_SUBSCRIBE,
		Feed:   feed,
	})
}
