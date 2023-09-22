package core

import (
	"github.com/gorilla/websocket"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"sync"
	"sync/atomic"
	"time"
)

type ClientMessageType int

const (
	Reply = ClientMessageType(0)
	Push  = ClientMessageType(1)
)
const CheckInterval = time.Duration(30) * time.Second
const WriteTimeout = time.Duration(60) * time.Second
const ReadTimeout = time.Duration(60) * time.Second
const TimeoutLimit = 3
const ClientQueueSize = 1024

type ClientMessage struct {
	Type     ClientMessageType
	Duration time.Duration
	Req      *protocol.Command
	Reply    *protocol.Reply
	Push     *protocol.Push
}

type ClientAgent interface {
	GetId() ClientId
	WriteMessage(message *ClientMessage) error
	BroadcastData(message *ClientMessage) error
	Process()
	Close()
	getContext() *ClientContext
}

type ClientAgentImpl struct {
	context          *ClientContext
	cmdHandler       *ClientCommandHandler
	conn             *websocket.Conn
	out              chan *ClientMessage
	doneOnce         sync.Once
	done             bool
	timeoutCount     uint32
	timeoutQueueSize int
}

func NewClient(ctx *ClientContext, conn *websocket.Conn, cmdHandler *ClientCommandHandler) ClientAgent {
	out := make(chan *ClientMessage, ClientQueueSize)
	return &ClientAgentImpl{
		context:    ctx,
		cmdHandler: cmdHandler,
		conn:       conn,
		out:        out,
	}
}

func (client *ClientAgentImpl) getContext() *ClientContext {
	return client.context
}
func (client *ClientAgentImpl) GetId() ClientId {
	return client.context.Id
}

func (client *ClientAgentImpl) BroadcastData(message *ClientMessage) error {
	if client.done {
		return ClientClosedError
	}
	l := len(client.out)
	if client.timeoutCount > TimeoutLimit && l >= client.timeoutQueueSize {
		return ClientRateLimit
	}
	client.timeoutQueueSize = l
	select {
	case client.out <- message:
		atomic.SwapUint32(&client.timeoutCount, 0)
	case <-time.After(200 * time.Millisecond):
		atomic.AddUint32(&client.timeoutCount, 1)
		return Timeout
	}
	return nil
}

func (client *ClientAgentImpl) WriteMessage(message *ClientMessage) error {
	if client.done {
		return ClientClosedError
	}
	l := len(client.out)
	if client.timeoutCount > TimeoutLimit && l >= client.timeoutQueueSize {
		return ClientRateLimit
	}
	client.timeoutQueueSize = l
	select {
	case client.out <- message:
		atomic.SwapUint32(&client.timeoutCount, 0)
	case <-time.After(200 * time.Millisecond):
		atomic.AddUint32(&client.timeoutCount, 1)
		return Timeout
	}
	return nil
}

func (client *ClientAgentImpl) Close() {
	client.context.Logger.Debug("client closed")
	client.doneOnce.Do(func() {
		client.done = true
		// TODO: reduce unnecessary call if feed manager already know it's disconnect for feedManagerImpl::handleDisconnect
		for {
			err := client.cmdHandler.HandleCommand(client, &protocol.Command{
				Method: protocol.Command_DISCONNECT,
			})
			if err == nil {
				break
			}
		}
		client.conn.Close()
	})
}
func (client *ClientAgentImpl) onDisconnect() {
	client.done = true
	client.conn.Close()
}

func (client *ClientAgentImpl) Process() {
	go client.ProcessRequest()

	checkTimer := time.NewTimer(CheckInterval)
	lastReceiveTime := time.Now()
	for !client.done {
		var err error
		select {
		case msg := <-client.out:
			lastReceiveTime = time.Now()
			checkTimer.Reset(CheckInterval)
			var data []byte
			switch msg.Type {
			case Reply:
				data, err = proto.Marshal(msg.Reply)
				if msg.Reply.Error != nil {
					client.context.Logger.Info("reply", zap.String("type", "reply"),
						zap.Uint32("id", msg.Reply.Id),
						zap.Any("method", msg.Req.Method),
						zap.Int64("duration", msg.Duration.Milliseconds()),
						zap.String("code", msg.Reply.Error.Code.String()))
				} else if msg.Req.Method != protocol.Command_PUBLISH && msg.Req.Method != protocol.Command_PING {
					client.context.Logger.Debug("reply", zap.String("type", "reply"),
						zap.Any("method", msg.Req.Method),
						zap.Uint32("id", msg.Reply.Id), zap.Int64("duration", msg.Duration.Milliseconds()))
				}

				TrackRequestDuration("client", msg.Duration.Seconds())
				TrackSendData(float64(len(data)))
				err = client.conn.WriteMessage(websocket.BinaryMessage, data)
				if msg.Req.Method == protocol.Command_DISCONNECT && msg.Reply.Error == nil {
					client.onDisconnect()
					break
				}
			case Push:
				data, err = proto.Marshal(&protocol.Reply{
					Push: msg.Push,
				})
				if msg.Duration.Milliseconds() > 100 {
					client.context.Logger.Info("push", zap.String("type", msg.Push.Type.String()),
						zap.Int64("duration", msg.Duration.Milliseconds()))
				}
				TrackSendData(float64(len(data)))
				err = client.conn.WriteMessage(websocket.BinaryMessage, data)
			}
		case <-checkTimer.C:
			now := time.Now()
			if now.Sub(lastReceiveTime) > WriteTimeout {
				client.Close()
			}
			checkTimer.Reset(CheckInterval)
		}
		if err != nil {
			client.context.Logger.Error("Process error", zap.Error(err))
		}
	}
	client.context.Logger.Debug("Process done")
}

func clientAllowCommand(method protocol.Command_MethodType) bool {
	return method != protocol.Command_DELETE_FEED
}

func (client *ClientAgentImpl) ProcessRequest() {
	logger := client.context.Logger.With(zap.String("account", client.context.Account), zap.Uint32("clientId", uint32(client.GetId())))
	for {
		msgType, payload, err := client.conn.ReadMessage()
		if client.done {
			return
		}
		if err != nil {
			logger.Debug("Read fail", zap.Error(err))
			client.Close()
			return
		}
		TrackReceiveData(float64(len(payload)))
		switch msgType {
		case websocket.PingMessage:
			client.conn.WriteMessage(websocket.PongMessage, []byte(""))
			continue
		case websocket.CloseMessage:
			client.Close()
		case websocket.BinaryMessage:
			if cmd, err := client.cmdHandler.Parse(msgType, payload); err != nil {
				logger.Error("fail to parse request", zap.Error(err))
			} else {
				//logger.Debug("Execute", zap.Uint32("Id", cmd.Id), zap.Any("method", cmd.Method), zap.Int("size", len(payload)))
				if !clientAllowCommand(cmd.Method) {
					reply := &protocol.Reply{
						Id:    cmd.Id,
						Error: UnAuthorized,
					}
					client.WriteMessage(&ClientMessage{
						Type:  Reply,
						Req:   cmd,
						Reply: reply,
					})
				}
				if cmd.Method == protocol.Command_PING {
					reply := &protocol.Reply{
						Id:   cmd.Id,
						Ping: &protocol.PingResult{},
					}
					client.WriteMessage(&ClientMessage{
						Type:  Reply,
						Req:   cmd,
						Reply: reply,
					})
				} else {
					client.cmdHandler.HandleCommand(client, cmd)
				}
			}
		default:
		}
	}
	logger.Debug("ProcessRequest done")
}
