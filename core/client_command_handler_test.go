package core

import (
	"github.com/gorilla/websocket"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestClientCommandParser(t *testing.T) {
	handler := &ClientCommandHandler{}
	request1 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed: &protocol.Feed{
			Account: "account",
			Key:     []byte("key"),
			Kind:    "kind",
		},
	}
	rawBody, err := proto.Marshal(request1)
	if err != nil {
		t.Errorf("Can't serialize cmd err:%v", err)
	}
	cmd, err := handler.Parse(websocket.BinaryMessage, []byte(rawBody))
	if err != nil {
		t.Errorf("Can't parse cmd err:%v", err)
	}
	if cmd.Method != protocol.Command_SUBSCRIBE {
		t.Errorf("cmd type error type:%v", cmd.Method)
	}
	if cmd.Feed.Account != request1.Feed.Account {
		t.Errorf("cmd Feed was wrong feed:%v", cmd.Feed)
	}
}
