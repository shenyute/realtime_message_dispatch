package core

import (
	"github.com/gorilla/websocket"
	"github.com/shenyute/realtime_message_dispatch/base"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"google.golang.org/protobuf/proto"
	"sync/atomic"
	"time"
)

// Not thread safe
type NodeConnector interface {
	Init(url, nodeId string) error
	Connect() error
	Close() error
	Send(cmd *protocol.InternodeCommand) error
	Receive() (*protocol.InternodeReply, error)
}

type NodeConnectorImpl struct {
	connected bool
	nodeId    string
	id        uint32
	url       string
	keySet    *base.KeySet
	conn      *websocket.Conn
}

const (
	System  = "System"
	Version = "1.0"
)

const InternodeWriteTimeout = time.Second

func (n *NodeConnectorImpl) Init(url, nodeId string) error {
	n.url = url
	n.nodeId = nodeId
	return n.Connect()
}

func (n *NodeConnectorImpl) Close() error {
	n.connected = false
	if n.conn == nil {
		return nil
	}
	return n.conn.Close()
}

func (n *NodeConnectorImpl) Connect() error {
	conn, _, err := websocket.DefaultDialer.Dial(n.url, nil)
	if err != nil {
		return err
	}
	n.conn = conn
	token, err := createJWT(System, Version, n.nodeId, false, n.keySet.HMACSecret[:])
	c := atomic.AddUint32(&n.id, 1)
	req := &protocol.Command{
		Method: protocol.Command_CONNECT,
		Id:     c,
		Connect: &protocol.ConnectRequest{
			Token: token,
		},
	}
	payload, err := proto.Marshal(req)
	err = n.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return NewIOError(err)
	}
	_, p, _ := n.conn.ReadMessage()
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err
	}
	if reply.Error != nil {
		return ProxyConnectFailError
	}
	if reply.Id != c {
		return ReplyIdIncorrect
	}
	n.connected = true
	return nil
}

func (n *NodeConnectorImpl) Send(cmd *protocol.InternodeCommand) error {
	if payload, err := proto.Marshal(cmd); err != nil {
		return err
	} else {
		n.conn.SetWriteDeadline(time.Now().Add(InternodeWriteTimeout))
		return n.conn.WriteMessage(websocket.BinaryMessage, payload)
	}
}

func (n *NodeConnectorImpl) Receive() (*protocol.InternodeReply, error) {
	// TODO: timeout to detect disconnect
	if n.conn == nil {
		return nil, ProxyConnectFailError
	}
	msgType, p, err := n.conn.ReadMessage()
	if err != nil {
		return nil, NewIOError(err)
	}
	// ignore other type
	if msgType != websocket.BinaryMessage {
		return nil, MessageTypeUnsupportedError
	}
	r := protocol.InternodeReply{}
	err = proto.Unmarshal(p, &r)
	if err != nil {
		return nil, err
	}
	return &r, nil
}
