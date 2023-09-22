package core

import "github.com/shenyute/realtime_message_dispatch/protocol"

type InternodeCommandHandler struct {
	feedManager FeedManager
}

func NewInternodeCommandHandler(mgr FeedManager) *InternodeCommandHandler {
	handler := InternodeCommandHandler{
		feedManager: mgr,
	}
	return &handler
}

func (c *InternodeCommandHandler) Connect(client StubAgent, id uint32) error {
	return c.feedManager.HandleCommand(client.GetClientAgent(), &protocol.Command{
		Method: protocol.Command_CONNECT,
		Id:     id,
	})
}

func (c *InternodeCommandHandler) Disconnect(client StubAgent) error {
	return c.feedManager.HandleCommand(client.GetClientAgent(), &protocol.Command{
		Method: protocol.Command_DISCONNECT,
	})
}

func (c *InternodeCommandHandler) HandleCommand(client StubAgent, cmd *protocol.InternodeCommand) error {
	switch cmd.Method {
	case protocol.InternodeCommand_SUBSCRIBE:
		return c.feedManager.HandleCommand(client.GetClientAgent(), &protocol.Command{
			Id:        cmd.Id,
			Method:    protocol.Command_SUBSCRIBE,
			Feed:      cmd.Feed,
			Subscribe: cmd.Subscribe,
		})
	case protocol.InternodeCommand_UNSUBSCRIBE:
		return c.feedManager.HandleCommand(client.GetClientAgent(), &protocol.Command{
			Id:          cmd.Id,
			Method:      protocol.Command_UNSUBSCRIBE,
			Feed:        cmd.Feed,
			Unsubscribe: cmd.Unsubscribe,
		})
	default:
		return client.OnReceiveReply(&protocol.InternodeReply{
			Id: cmd.Id,
			Error: &protocol.Error{
				Code: protocol.Error_INVALID_COMMAND,
			},
		})
	}
}
