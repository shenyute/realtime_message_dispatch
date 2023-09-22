package core

import (
	"github.com/gorilla/websocket"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"google.golang.org/protobuf/proto"
)

type ClientCommandHandler struct {
	feedManager FeedManager
}

func (c *ClientCommandHandler) Parse(msgType int, payload []byte) (*protocol.Command, error) {
	if msgType != websocket.BinaryMessage {
		return nil, MessageTypeUnsupportedError
	}
	var cmd protocol.Command
	if err := proto.Unmarshal(payload, &cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}

func (c *ClientCommandHandler) HandleCommand(client ClientAgent, cmd *protocol.Command) error {
	return c.feedManager.HandleCommand(client, cmd)
}

func NewClientCommandHandler(mgr FeedManager) *ClientCommandHandler {
	return &ClientCommandHandler{
		feedManager: mgr,
	}
}
