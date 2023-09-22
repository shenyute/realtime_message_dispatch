package core

import (
	"github.com/gorilla/websocket"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"runtime/debug"
)

type NodeReceiverCallback interface {
	OnSend(*protocol.InternodeCommand) error
	OnError(error) (abort bool)
}

type NodeReceiver interface {
	SetCallback(callback NodeReceiverCallback)
	OnConnect(ctx *ClientContext) error
	OnInit()
	GetContext() *ClientContext
	SendReply(*protocol.InternodeReply) error
}

type NodeReceiverImpl struct {
	conn     *websocket.Conn
	callback NodeReceiverCallback
	context  *ClientContext
}

func CreateNodeReceiver(ws *websocket.Conn) NodeReceiver {
	node := NodeReceiverImpl{
		conn: ws,
	}
	return &node
}

func (n *NodeReceiverImpl) OnConnect(ctx *ClientContext) error {
	n.context = ctx
	return nil
}

func (n *NodeReceiverImpl) SetCallback(callback NodeReceiverCallback) {
	n.callback = callback
}

func (n *NodeReceiverImpl) OnInit() {
	go n.process()
}

func (n *NodeReceiverImpl) GetContext() *ClientContext {
	return n.context
}

func (n *NodeReceiverImpl) process() {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			n.context.Logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			// fork another goroutine to keep handling
			go n.process()
		}
	}()
	for {
		msgType, payload, err := n.conn.ReadMessage()
		if err != nil {
			abort := n.callback.OnError(NewIOError(err))
			if abort {
				break
			}
			continue
		}
		if msgType == websocket.BinaryMessage {
			cmd := protocol.InternodeCommand{}
			err = proto.Unmarshal(payload, &cmd)
			if err != nil {
				n.callback.OnError(err)
			} else {
				n.callback.OnSend(&cmd)
			}
		}
	}
}

func (n *NodeReceiverImpl) SendReply(reply *protocol.InternodeReply) error {
	if payload, err := proto.Marshal(reply); err != nil {
		return err
	} else {
		return n.conn.WriteMessage(websocket.BinaryMessage, payload)
	}
}
