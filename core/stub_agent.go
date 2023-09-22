package core

import (
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"go.uber.org/zap"
	"net"
	"runtime/debug"
	"strconv"
	"time"
)

// Handle ProxyAgent's request
type StubAgent interface {
	OnInit()
	GetClientAgent() ClientAgent
	OnSendCommand(req *protocol.InternodeCommand) error
	OnClose() error
	OnReceiveReply(reply *protocol.InternodeReply) error
}

type NodeStubAgent struct {
	node       NodeReceiver
	queueName  string
	cmdHandler *InternodeCommandHandler
	out        chan *ClientMessage
	done       chan bool
	logger     *zap.Logger
}

func (n *NodeStubAgent) OnSend(command *protocol.InternodeCommand) error {
	return n.OnSendCommand(command)
}

func (n *NodeStubAgent) OnError(err error) (abort bool) {
	nerr, ok := err.(net.Error)
	if ok {
		n.logger.Error("network error", zap.Error(nerr))
		n.Close()
		return true
	} else if nerr, ok := err.(*Error); ok && !nerr.IsTemporary() {
		n.logger.Error("receive error", zap.Error(nerr))
		n.Close()
		return true
	}

	// handle error
	n.logger.Error("read error", zap.Error(err))
	return false
}

func NewStubClient(node NodeReceiver, cmdHandler *InternodeCommandHandler) StubAgent {
	id := strconv.FormatUint(uint64(node.GetContext().Id), 10)
	client := &NodeStubAgent{
		queueName:  "stub-" + id,
		node:       node,
		cmdHandler: cmdHandler,
		out:        make(chan *ClientMessage, 1024),
		logger:     zap.L(),
	}
	node.SetCallback(client)
	return client
}

func (n *NodeStubAgent) OnInit() {
	n.node.OnInit()
	go n.Process()
}

func (n *NodeStubAgent) GetClientAgent() ClientAgent {
	return n
}

func (n *NodeStubAgent) GetId() ClientId {
	return n.node.GetContext().Id
}

func (n *NodeStubAgent) WriteMessage(message *ClientMessage) error {
	TrackQueueSize(n.queueName, float64(len(n.out)))
	select {
	case n.out <- message:
	case <-time.After(200 * time.Millisecond):
		return Timeout
	}
	return nil
}

func (n *NodeStubAgent) BroadcastData(message *ClientMessage) error {
	select {
	case n.out <- message:
	case <-time.After(200 * time.Millisecond):
	}
	return nil
}

func (n *NodeStubAgent) Process() {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			n.logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			go n.Process()
		}
	}()
	for {
		select {
		case msg := <-n.out:
			switch msg.Type {
			case Reply:
				n.OnReceiveReply(&protocol.InternodeReply{
					Id:          msg.Reply.Id,
					Error:       msg.Reply.Error,
					Subscribe:   msg.Reply.Subscribe,
					Unsubscribe: msg.Reply.Unsubscribe,
				})
			case Push:
				n.OnReceiveReply(&protocol.InternodeReply{
					Push: msg.Push,
				})
			}
		case _ = <-n.done:
			DeleteTrackQueueSize(n.queueName)
			break
		}
	}
}

func (n *NodeStubAgent) Close() {
	// network issue, disconnect now.
	n.cmdHandler.Disconnect(n)
	n.OnClose()
}

func (n *NodeStubAgent) getContext() *ClientContext {
	return n.node.GetContext()
}

func (n *NodeStubAgent) OnSendCommand(req *protocol.InternodeCommand) error {
	return n.cmdHandler.HandleCommand(n, req)
}

func (n *NodeStubAgent) OnClose() error {
	n.done <- true
	return nil
}

func (n *NodeStubAgent) OnReceiveReply(reply *protocol.InternodeReply) error {
	return n.node.SendReply(reply)
}
