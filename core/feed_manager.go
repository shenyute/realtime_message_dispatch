package core

import (
	"encoding/base64"
	"errors"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"go.uber.org/zap"
	_ "google.golang.org/protobuf/encoding/protojson"
	_ "google.golang.org/protobuf/proto"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

type FeedManager interface {
	Init()
	HandleCommand(client ClientAgent, cmd *protocol.Command) error
	GetConnectedUserCount() int
	GetRoomCount(feed *protocol.Feed) int
	GetFeedInfo(feed *protocol.Feed, feedKey string) (*FeedInfoItem, error)
}

type FeedRoomInfo struct {
	members                  map[ClientId]ClientAgent
	subscriber               map[ClientId]*connectedUser
	channel                  chan *feedReq
	done                     chan bool
	feed                     *protocol.Feed
	AvatarBackgroundImageUrl string
	AvatarForegroundImageUrl string
	Metadata                 []byte
	Joiners                  map[uint32]*protocol.Joiner
}

type feedReq struct {
	cmd       *protocol.Command
	ctx       ClientAgent
	user      *connectedUser
	startTime time.Time
	feedKey   string // cached feedKey for |cmd.Feed| to reduce too much serialization
}

type subscribeFeed struct {
	feed         *protocol.Feed
	feedInfoItem *FeedInfoItem
	isLocal      bool
}
type connectedUser struct {
	joinFeeds map[string]*protocol.Feed
	subFeeds  map[string]*subscribeFeed
	ctx       ClientAgent
}

const RoomQueueSize = 1024
const FeedQueueSize = 1024
const DefaultFeedInfoTtl = 8 * time.Hour
const DefaultFeedRoomTimeout = time.Minute
const LongWaitTime = 200 // ms
const WaitRoomDone = 10 * time.Second

type FeedManagerImpl struct {
	que            chan feedReq
	rooms          map[string]*FeedRoomInfo
	connectedUsers map[ClientId]*connectedUser
	feedInfoDb     FeedInfoDb
	proxyManager   ProxyManager
	NodeId         string
	localIP        string
	port           string
	publicIP       string
	logger         *zap.Logger
	feedInfoCache  map[string]*FeedInfoItem
	lock           sync.Mutex
}

const FeedKeySeparator = "|"

func getFeedFromFeedKey(feedKey string) *protocol.Feed {
	tokens := strings.Split(feedKey, FeedKeySeparator)
	if len(tokens) != 3 {
		return nil
	}
	key, err := base64.StdEncoding.DecodeString(tokens[2])
	if err != nil {
		return nil
	}
	return &protocol.Feed{
		Account: tokens[0],
		Kind:    tokens[1],
		Key:     key,
	}
}

// TODO: Cache feedKey to reduce serialize/deserialize
func GetFeedKey(feed *protocol.Feed) string {
	var sb strings.Builder
	sb.WriteString(feed.Account)
	sb.WriteString(FeedKeySeparator)
	sb.WriteString(feed.Kind)
	sb.WriteString(FeedKeySeparator)
	sb.WriteString(base64.StdEncoding.EncodeToString(feed.Key))
	return sb.String()
}

func (room *FeedRoomInfo) handleIfRoomClosed(manager *FeedManagerImpl) {
	if len(room.members) == 0 {
		push := &protocol.Push{
			Feed: room.feed,
			Type: protocol.Push_FEED_ERROR,
		}
		msg := ClientMessage{
			Type: Push,
			Push: push,
		}
		feedKey := GetFeedKey(room.feed)
		manager.logger.Debug("Notify feed not exist", zap.Int("size", len(room.subscriber)), zap.String("feed", feedKey))
		for clientId, client := range room.subscriber {
			// notify feed no long exist
			if err := client.ctx.BroadcastData(&msg); err != nil {
				manager.logger.Error("fail to send broadcast feed error",
					zap.Uint32("clientId", uint32(clientId)),
					zap.String("feed", feedKey),
					zap.String("account", client.ctx.getContext().Account))
			}
		}
		manager.SendControlCommand(&protocol.Command{
			Feed:   room.feed,
			Method: protocol.Command_DELETE_FEED,
		})
	}
}

func (room *FeedRoomInfo) sendBroadcast(manager *FeedManagerImpl, req *feedReq) {
	// TODO: check client message type
	pub := &protocol.Publication{
		Timestamp:     req.cmd.Publish.Timestamp,
		UnityData:     req.cmd.Publish.UnityData,
		AudioData:     req.cmd.Publish.AudioData,
		AvatarCommand: req.cmd.Publish.AvatarCommand,
		Info: &protocol.ClientInfo{
			Account:  req.ctx.getContext().Account,
			DeviceId: req.ctx.getContext().DeviceId,
			ClientId: uint32(req.ctx.GetId()),
			IsBot:    req.ctx.getContext().IsBot,
		},
	}
	push := &protocol.Push{
		Feed: req.cmd.Feed,
		Pub:  pub,
		Type: protocol.Push_PUBLICATION,
	}
	msg := ClientMessage{
		Type: Push,
		Push: push,
	}
	for clientId, client := range room.subscriber {
		if client.ctx == req.ctx || client.ctx.getContext().DeviceId == req.ctx.getContext().DeviceId {
			continue
		}
		err := client.ctx.BroadcastData(&msg)
		if err != nil {
			manager.logger.Error("fail to send broadcast", zap.Uint32("clientId", uint32(clientId)),
				zap.String("account", client.ctx.getContext().Account), zap.Error(err))
		}
	}
}

func (room *FeedRoomInfo) broadcastPush(manager *FeedManagerImpl, push *protocol.Push, self ClientId) {
	for clientId, user := range room.subscriber {
		if self != clientId {
			msg := &ClientMessage{
				Type: Push,
				Push: push,
			}
			user.ctx.BroadcastData(msg)
		}
	}
}

func (room *FeedRoomInfo) handlePresence(manager *FeedManagerImpl, req *feedReq, reply *protocol.Reply) {
	account := req.ctx.getContext().Account
	changed := false
	clientId := req.ctx.GetId()
	rawClientId := uint32(clientId)
	presence := &protocol.Presence{
		AvatarUrl: req.cmd.Presence.GetAvatarUrl(),
		Info: &protocol.ClientInfo{
			Account:  account,
			DeviceId: req.ctx.getContext().DeviceId,
			ClientId: rawClientId,
			IsBot:    req.ctx.getContext().IsBot,
		},
		IsCalling: req.cmd.Presence.GetIsCalling(),
		Assets:    req.cmd.Presence.GetAssets(),
	}
	push := protocol.Push{
		Type:     protocol.Push_PRESENCE,
		Feed:     room.feed,
		Presence: presence,
	}
	reply.Presence = &protocol.PresenceResult{}
	joiner := room.Joiners[rawClientId]
	if joiner == nil {
		reply.Error = NotJoinFeedError
		return
	}
	if req.cmd.Presence.IsCalling != joiner.IsCalling {
		changed = true
		joiner.IsCalling = req.cmd.Presence.GetIsCalling()
	}
	if joiner.AvatarUrl != req.cmd.Presence.GetAvatarUrl() {
		joiner.AvatarUrl = req.cmd.Presence.GetAvatarUrl()
		changed = true
	}
	// TODO: check permission
	if joiner.AvatarForegroundImageUrl != req.cmd.Presence.GetAvatarForegroundImageUrl() {
		joiner.AvatarForegroundImageUrl = req.cmd.Presence.GetAvatarForegroundImageUrl()
		changed = true
	}
	if joiner.AvatarBackgroundImageUrl != req.cmd.Presence.GetAvatarBackgroundImageUrl() && req.cmd.Presence.GetAvatarBackgroundImageUrl() != "" {
		joiner.AvatarBackgroundImageUrl = req.cmd.Presence.GetAvatarBackgroundImageUrl()
		changed = true
	}
	if len(req.cmd.Presence.Metadata) > 0 {
		joiner.Metadata = req.cmd.Presence.Metadata
		changed = true
	}
	l1 := len(req.cmd.Presence.GetMoodKeys())
	l2 := len(joiner.GetMoodKeys())
	if l1 != l2 || l1 > 0 {
		joiner.MoodKeys = req.cmd.Presence.GetMoodKeys()
		changed = true
	}
	if joiner.IdleAnimationKey != req.cmd.Presence.IdleAnimationKey && req.cmd.Presence.IdleAnimationKey != "" {
		joiner.IdleAnimationKey = req.cmd.Presence.IdleAnimationKey
		changed = true
	}
	if req.cmd.Presence.GetAssets() != nil {
		joiner.Assets = req.cmd.Presence.GetAssets()
		changed = true
	}
	if changed {
		presence.AvatarUrl = joiner.AvatarUrl
		presence.AvatarForegroundImageUrl = joiner.AvatarForegroundImageUrl
		presence.AvatarBackgroundImageUrl = joiner.AvatarBackgroundImageUrl
		presence.Metadata = joiner.Metadata
		presence.MoodKeys = joiner.MoodKeys
		presence.IdleAnimationKey = joiner.IdleAnimationKey
		presence.IsCalling = joiner.IsCalling
		presence.Assets = joiner.Assets
		room.broadcastPush(manager, &push, clientId)
	}
}

func (room *FeedRoomInfo) handleKick(manager *FeedManagerImpl, req *feedReq, reply *protocol.Reply) {
	targetClientId := req.cmd.Kick.GetClientId()
	joiner := room.Joiners[targetClientId]
	if joiner != nil {
		presence := &protocol.Presence{
			Info: &protocol.ClientInfo{
				Account:  joiner.Info.Account,
				DeviceId: joiner.Info.DeviceId,
				ClientId: targetClientId,
				IsBot:    joiner.Info.IsBot,
			},
			State: protocol.JoinerState_KICK,
		}
		push := protocol.Push{
			Type:     protocol.Push_PRESENCE,
			Feed:     room.feed,
			Presence: presence,
		}
		room.broadcastPush(manager, &push, req.ctx.GetId())
	} else {
		reply.Error = &protocol.Error{
			Code: protocol.Error_NOT_JOIN_FEED,
		}
	}
	reply.Kick = &protocol.KickResult{}
}

func (room *FeedRoomInfo) handleModerator(manager *FeedManagerImpl, req *feedReq, reply *protocol.Reply) {
	targetClientId := req.cmd.Moderator.GetClientId()
	joiner := room.Joiners[targetClientId]
	if joiner != nil && joiner.State != req.cmd.Moderator.State {
		joiner.State = req.cmd.Moderator.State
		presence := &protocol.Presence{
			Info: &protocol.ClientInfo{
				Account:  joiner.Info.Account,
				DeviceId: joiner.Info.DeviceId,
				ClientId: targetClientId,
				IsBot:    joiner.Info.IsBot,
			},
			State: req.cmd.Moderator.State,
		}
		push := protocol.Push{
			Type:     protocol.Push_PRESENCE,
			Feed:     room.feed,
			Presence: presence,
		}
		room.broadcastPush(manager, &push, req.ctx.GetId())
	}
	reply.Moderator = &protocol.ModeratorResult{}
}

func (room *FeedRoomInfo) handleCommand(manager *FeedManagerImpl) {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			manager.logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			// fork another goroutine to keep handling
			go room.handleCommand(manager)
		}
	}()
	room.doHandleCommand(manager)
}

func (room *FeedRoomInfo) doHandleCommand(manager *FeedManagerImpl) {
	closed := false
	feedKey := GetFeedKey(room.feed)
	manager.logger.Debug("Room start handle command", zap.String("feed", feedKey))
	for !closed {
		select {
		case req := <-room.channel:
			clientId := req.ctx.GetId()
			reply := &protocol.Reply{
				Id: req.cmd.Id,
			}
			switch req.cmd.Method {
			case protocol.Command_JOIN:
				room.members[clientId] = req.ctx
				rawClientId := uint32(clientId)
				presence := &protocol.Presence{
					AvatarUrl:                req.cmd.Join.GetAvatarUrl(),
					AvatarBackgroundImageUrl: req.cmd.Join.GetAvatarBackgroundImageUrl(),
					AvatarForegroundImageUrl: req.cmd.Join.GetAvatarForegroundImageUrl(),
					Metadata:                 req.cmd.Join.GetMetadata(),
					MoodKeys:                 req.cmd.Join.GetMoodKeys(),
					IdleAnimationKey:         req.cmd.Join.GetIdleAnimationKey(),
				}
				clientInfo := &protocol.ClientInfo{
					Account:  req.ctx.getContext().Account,
					DeviceId: req.ctx.getContext().DeviceId,
					ClientId: uint32(req.ctx.GetId()),
					IsBot:    req.ctx.getContext().IsBot,
				}
				room.Joiners[rawClientId] = &protocol.Joiner{
					AvatarUrl:                req.cmd.Join.GetAvatarUrl(),
					Info:                     clientInfo,
					AvatarBackgroundImageUrl: req.cmd.Join.GetAvatarBackgroundImageUrl(),
					AvatarForegroundImageUrl: req.cmd.Join.GetAvatarForegroundImageUrl(),
					Metadata:                 req.cmd.Join.GetMetadata(),
					MoodKeys:                 req.cmd.Join.GetMoodKeys(),
					IdleAnimationKey:         req.cmd.Join.GetIdleAnimationKey(),
				}
				push := protocol.Push{
					Type: protocol.Push_JOIN,
					Feed: room.feed,
					Join: &protocol.Join{
						Info:     clientInfo,
						Presence: presence,
					},
				}
				room.broadcastPush(manager, &push, clientId)
				feedInfo := protocol.FeedInfo{
					Feed:    room.feed,
					Joiners: room.Joiners,
				}
				reply.Join = &protocol.JoinResult{
					Feed:     room.feed,
					Info:     clientInfo,
					FeedInfo: &feedInfo,
				}
			case protocol.Command_SUBSCRIBE:
				room.subscriber[clientId] = req.user
				info := protocol.FeedInfo{
					Feed:    room.feed,
					Joiners: room.Joiners,
				}
				ctx := req.user.ctx.getContext()
				reply.Subscribe = &protocol.SubscribeResult{
					Info: &info,
					Client: &protocol.ClientInfo{
						Account:  ctx.Account,
						DeviceId: ctx.DeviceId,
						ClientId: uint32(ctx.Id),
						IsBot:    ctx.IsBot,
					},
				}
			case protocol.Command_UNSUBSCRIBE:
				delete(room.subscriber, clientId)
				room.handleIfRoomClosed(manager)
				ctx := req.ctx.getContext()
				reply.Unsubscribe = &protocol.UnsubscribeResult{
					Feed: room.feed,
					Info: &protocol.ClientInfo{
						Account:  ctx.Account,
						DeviceId: ctx.DeviceId,
						ClientId: uint32(ctx.Id),
						IsBot:    ctx.IsBot,
					},
				}
			case protocol.Command_PUBLISH:
				if room.members[clientId] != nil {
					room.sendBroadcast(manager, req)
				} else {
					reply.Error = NotJoinFeedError
				}
				reply.Publish = &protocol.PublishResult{}
			case protocol.Command_LEAVE:
				// NOTE: check proxy agent
				delete(room.Joiners, uint32(clientId))
				clientInfo := &protocol.ClientInfo{
					Account:  req.ctx.getContext().Account,
					DeviceId: req.ctx.getContext().DeviceId,
					ClientId: uint32(req.ctx.GetId()),
					IsBot:    req.ctx.getContext().IsBot,
				}
				if room.members[clientId] != nil {
					push := protocol.Push{
						Type: protocol.Push_LEAVE,
						Feed: room.feed,
						Leave: &protocol.Leave{
							Info: clientInfo,
						},
					}
					room.broadcastPush(manager, &push, clientId)
					delete(room.members, clientId)
				}
				room.handleIfRoomClosed(manager)
				reply.Leave = &protocol.LeaveResult{Feed: room.feed}
			case protocol.Command_PRESENCE:
				room.handlePresence(manager, req, reply)
			case protocol.Command_FEED_INFO:
				info := protocol.FeedInfo{
					Feed:    room.feed,
					Joiners: room.Joiners,
				}
				reply.FeedInfo = &protocol.FeedInfoResult{
					Info: &info,
				}
			case protocol.Command_MODERATOR:
				room.handleModerator(manager, req, reply)
			case protocol.Command_KICK:
				room.handleKick(manager, req, reply)
			default:
				manager.logger.Error("Unsupported command for room", zap.String("method", req.cmd.Method.String()))
			}
			req.ctx.WriteMessage(&ClientMessage{
				Type:     Reply,
				Req:      req.cmd,
				Duration: time.Now().Sub(req.startTime),
				Reply:    reply,
			})
		case _ = <-room.done:
			closed = true
		case <-time.After(DefaultFeedRoomTimeout):
			manager.logger.Debug("Room timeout and clean up", zap.String("feed", feedKey),
				zap.Int("member", len(room.members)), zap.Int("subscribers", len(room.subscriber)))
			room.handleIfRoomClosed(manager)
		}
	}
	manager.logger.Debug("Room start handle command done", zap.String("feed", feedKey))
}

func newFeedRoom(manager *FeedManagerImpl, feed *protocol.Feed) *FeedRoomInfo {
	room := FeedRoomInfo{
		members:    make(map[ClientId]ClientAgent),
		subscriber: make(map[ClientId]*connectedUser),
		channel:    make(chan *feedReq, RoomQueueSize),
		done:       make(chan bool, 10),
		feed:       feed,
	}
	go room.handleCommand(manager)
	return &room
}

func (manager *FeedManagerImpl) getFeedInfoInCache(feedKey string) *FeedInfoItem {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	return manager.feedInfoCache[feedKey]
}

func (manager *FeedManagerImpl) updateFeedInfoInCache(feedKey string, item *FeedInfoItem) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	if item == nil {
		delete(manager.feedInfoCache, feedKey)
	} else {
		manager.feedInfoCache[feedKey] = item
	}
}

func (manager *FeedManagerImpl) GetFeedInfo(feed *protocol.Feed, feedKey string) (*FeedInfoItem, error) {
	if cached := manager.getFeedInfoInCache(feedKey); cached != nil {
		return cached, nil
	}
	return manager.feedInfoDb.GetFeedInfo(feed)
}

func (manager *FeedManagerImpl) HandleCommand(client ClientAgent, cmd *protocol.Command) error {
	var feedKey string
	if cmd.Feed != nil {
		feedKey = GetFeedKey(cmd.Feed)
	}
	TrackQueueSize("feed_manager", float64(len(manager.que)))
	req := feedReq{
		cmd:       cmd,
		ctx:       client,
		startTime: time.Now(),
		feedKey:   feedKey,
	}
	select {
	case manager.que <- req:
	case <-time.After(time.Second):
		// did not send success
		go manager.replyError(&req, protocol.Error_REQUEST_TIMEOUT, Timeout)
		return Timeout
	}
	return nil
}

func (manager *FeedManagerImpl) SendControlCommand(cmd *protocol.Command) error {
	if cmd == nil || cmd.Method != protocol.Command_DELETE_FEED {
		return InvalidCommandError
	}
	manager.que <- feedReq{
		feedKey:   GetFeedKey(cmd.Feed),
		cmd:       cmd,
		startTime: time.Now(),
	}
	return nil
}

func (manager *FeedManagerImpl) doProcessCommand() {
	for {
		req := <-manager.que
		var clientId ClientId
		var account string
		if req.ctx != nil {
			clientId = req.ctx.GetId()
			account = req.ctx.getContext().Account
		}
		waitTime := time.Now().Sub(req.startTime).Milliseconds()
		if waitTime > LongWaitTime {
			manager.logger.Warn("new cmd", zap.Any("method", req.cmd.Method),
				zap.Uint32("clientId", uint32(clientId)), zap.String("feed", req.feedKey), zap.String("account", account),
				zap.Int64("wait", waitTime))
		} else if req.cmd.Method != protocol.Command_PUBLISH {
			manager.logger.Debug("new cmd", zap.Any("method", req.cmd.Method),
				zap.Uint32("clientId", uint32(clientId)), zap.String("feed", req.feedKey), zap.String("account", account),
				zap.Int64("wait", waitTime))
		}
		manager.handleCommand(&req)
	}
}

func (manager *FeedManagerImpl) processCommand() {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			manager.logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			// fork another goroutine to keep handling
			go manager.processCommand()
		}
	}()
	manager.doProcessCommand()
}

func (manager *FeedManagerImpl) sendToRoom(room *FeedRoomInfo, req *feedReq, needReply bool) error {
	TrackQueueSize("feed_room", float64(len(room.channel)))
	select {
	case room.channel <- req:
	case <-time.After(200 * time.Millisecond):
		if needReply {
			// to avoid block main process goroutine
			go manager.replyError(req, protocol.Error_REQUEST_TIMEOUT, Timeout)
			return Timeout
		}
	}
	return nil
}

func (manager *FeedManagerImpl) handleDisconnect(req *feedReq) error {
	clientId := req.ctx.GetId()
	user := manager.connectedUsers[clientId]
	if user == nil {
		return nil
	}
	TrackFeedJoiner(float64(len(user.joinFeeds)), false)
	for _, feed := range user.joinFeeds {
		feedKey := GetFeedKey(feed)
		room := manager.rooms[feedKey]
		if room != nil {
			leaveFeedCmd := protocol.Command{
				Method: protocol.Command_LEAVE,
				Feed:   room.feed,
				Leave:  &protocol.LeaveRequest{},
			}

			req := &feedReq{
				feedKey:   feedKey,
				cmd:       &leaveFeedCmd,
				ctx:       req.ctx,
				startTime: time.Now(),
			}
			manager.sendToRoom(room, req, false)
		}
	}
	TrackFeedSubscriber(float64(len(user.subFeeds)), false)
	for _, f := range user.subFeeds {
		feed := f.feed
		feedInfo := f.feedInfoItem
		if feedInfo != nil {
			if feedInfo.NodeId != manager.NodeId {
				proxyAgent := manager.proxyManager.GetAgent(feedInfo.NodeId)
				if proxyAgent != nil {
					if err := proxyAgent.SendFeedRequest(req); err != nil {
						manager.logger.Warn("Fail to proxy disconnect", zap.Uint32("clientId", uint32(clientId)), zap.String("account", req.ctx.getContext().Account))
					}
				}
			} else {
				// handle local
				feedKey := GetFeedKey(feed)
				room := manager.rooms[feedKey]
				if room != nil {
					unsubscribeFeedCmd := protocol.Command{
						Method:      protocol.Command_UNSUBSCRIBE,
						Feed:        room.feed,
						Unsubscribe: &protocol.UnsubscribeRequest{},
					}
					req := &feedReq{
						feedKey:   feedKey,
						cmd:       &unsubscribeFeedCmd,
						ctx:       req.ctx,
						startTime: time.Now(),
					}
					manager.sendToRoom(room, req, false)
				}
			}
		}
	}
	delete(manager.connectedUsers, clientId)
	reply := &protocol.Reply{
		Id: req.cmd.Id,
	}
	req.ctx.WriteMessage(&ClientMessage{
		Type:     Reply,
		Req:      req.cmd,
		Duration: time.Now().Sub(req.startTime),
		Reply:    reply,
	})
	return nil
}

func (manager *FeedManagerImpl) replyError(req *feedReq, code protocol.Error_ErrorCode, err error) error {
	var msg string
	if err != nil {
		msg = err.Error() + " code=" + code.String()
	} else {
		msg = "code=" + code.String()
	}
	reply := protocol.Reply{
		Id: req.cmd.Id,
		Error: &protocol.Error{
			Code:    code,
			Message: msg,
		},
	}
	// Fill result if not exist
	switch req.cmd.Method {
	case protocol.Command_SUBSCRIBE:
		reply.Subscribe = &protocol.SubscribeResult{Feed: req.cmd.Feed}
	case protocol.Command_UNSUBSCRIBE:
		reply.Unsubscribe = &protocol.UnsubscribeResult{}
	case protocol.Command_PUBLISH:
		reply.Publish = &protocol.PublishResult{}
	case protocol.Command_JOIN:
		reply.Join = &protocol.JoinResult{}
	case protocol.Command_LEAVE:
		reply.Leave = &protocol.LeaveResult{}
	case protocol.Command_PRESENCE:
		reply.Presence = &protocol.PresenceResult{}
	case protocol.Command_FEED_INFO:
		reply.FeedInfo = &protocol.FeedInfoResult{}
	case protocol.Command_KICK:
		reply.Kick = &protocol.KickResult{}
	}
	return req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &reply, Duration: time.Now().Sub(req.startTime)})
}

func (manager *FeedManagerImpl) handleKick(req *feedReq) (success bool, code protocol.Error_ErrorCode) {
	clientId := req.ctx.GetId()
	feed := req.cmd.Feed
	feedKey := req.feedKey
	kicker := manager.connectedUsers[clientId]
	success = false
	if kicker == nil {
		code = protocol.Error_MISSING_STATE
		return
	}
	if !kicker.ctx.getContext().IsModerator || feed.Account != req.ctx.getContext().Account {
		code = protocol.Error_UNAUTHORIZED
		return
	}
	if kicker.joinFeeds[feedKey] == nil {
		code = protocol.Error_MISSING_STATE
		return
	}
	userId := ClientId(req.cmd.Kick.ClientId)
	user := manager.connectedUsers[userId]
	if user == nil {
		code = protocol.Error_MISSING_STATE
		return
	}
	if user.joinFeeds[feedKey] == nil {
		code = protocol.Error_MISSING_STATE
		return
	}
	room := manager.rooms[feedKey]
	if room == nil {
		code = protocol.Error_MISSING_STATE
		return
	} else {
		manager.sendToRoom(room, req, true)
	}
	go func() {
		time.Sleep(5 * time.Second)
		user.ctx.Close()
	}()
	success = true
	return
}

func (manager *FeedManagerImpl) handleCommand(req *feedReq) error {
	// TODO: move manager.UpdateFeedInfo out main goroutine to reduce latency?
	if req.cmd.Method == protocol.Command_PING {
		reply := &protocol.Reply{
			Id:   req.cmd.Id,
			Ping: &protocol.PingResult{},
		}
		req.ctx.WriteMessage(&ClientMessage{
			Type:     Reply,
			Req:      req.cmd,
			Duration: time.Now().Sub(req.startTime),
			Reply:    reply,
		})
		return nil
	} else if req.cmd.Method == protocol.Command_DELETE_FEED {
		return manager.deleteFeed(req.cmd.Feed, req.feedKey)
	} else if req.cmd.Method == protocol.Command_DISCONNECT {
		// handle disconnect & clean up all data
		return manager.handleDisconnect(req)
	} else if req.cmd.Method == protocol.Command_CONNECT {
		clientId := req.ctx.GetId()
		if manager.connectedUsers[clientId] == nil {
			manager.connectedUsers[clientId] = &connectedUser{
				ctx:       req.ctx,
				joinFeeds: make(map[string]*protocol.Feed),
				subFeeds:  make(map[string]*subscribeFeed),
			}
		}
		reply := &protocol.Reply{
			Id:      req.cmd.Id,
			Connect: &protocol.ConnectResult{},
		}
		req.ctx.WriteMessage(&ClientMessage{
			Type:     Reply,
			Req:      req.cmd,
			Duration: time.Now().Sub(req.startTime),
			Reply:    reply,
		})
		return nil
	}
	clientId := req.ctx.GetId()
	feed := req.cmd.Feed
	if feed == nil {
		return manager.replyError(req, protocol.Error_MISSING_FIELDS, nil)
	}
	feedKey := req.feedKey
	var feedInfo *FeedInfoItem
	var err error
	if req.cmd.Method == protocol.Command_JOIN || req.cmd.Method == protocol.Command_SUBSCRIBE {
		feedInfo, err = manager.feedInfoDb.GetFeedInfo(req.cmd.Feed)
		if err != nil {
			return manager.replyError(req, protocol.Error_FEED_INFO_DB_ERROR, err)
		}
		if feedInfo == nil {
			if req.cmd.Feed.Kind != "test" {
				manager.logger.Error("FeedInfoItem not in db")
				return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
			} else {
				// Make a fake one to pass the check below.
				feedInfo = &FeedInfoItem{
					NodeId: manager.NodeId,
				}
			}
		}
	}
	switch req.cmd.Method {
	case protocol.Command_JOIN:
		if feedInfo == nil || feedInfo.NodeId != manager.NodeId {
			var err error
			if feedInfo == nil {
				err = errors.New("feedInfoItem does not exist")
			} else {
				err = errors.New("nodeId mismatched " + feedInfo.NodeId + " != " + manager.NodeId)
			}
			return manager.replyError(req, protocol.Error_WRONG_FEED_INSTANCE, err)
		}
		room := manager.rooms[feedKey]
		user := manager.connectedUsers[clientId]
		if user == nil {
			return manager.replyError(req, protocol.Error_MISSING_STATE, nil)
		}
		// Can allow to join multiple feed in the future
		if len(user.joinFeeds) > 0 {
			return manager.replyError(req, protocol.Error_ALREADY_IN_OTHER_FEED, nil)
		}
		if room == nil {
			room = newFeedRoom(manager, feed)
			manager.rooms[feedKey] = room
			TrackFeedRoom(float64(len(manager.rooms)))
			ttl := time.Now().Add(DefaultFeedInfoTtl).Unix()
			room.Joiners = make(map[uint32]*protocol.Joiner)
			room.AvatarBackgroundImageUrl = req.cmd.Join.GetAvatarBackgroundImageUrl()
			room.AvatarForegroundImageUrl = req.cmd.Join.GetAvatarForegroundImageUrl()
			feedInfo := &FeedInfoItem{
				NodeId:                   manager.NodeId,
				Host:                     feed.Account,
				HostPrivateIP:            manager.localIP,
				HostPublicIP:             manager.publicIP,
				Ttl:                      ttl,
				AvatarBackgroundImageUrl: req.cmd.Join.GetAvatarBackgroundImageUrl(),
				AvatarForegroundImageUrl: req.cmd.Join.GetAvatarForegroundImageUrl(),
				// NOTE: dont save joiner info.
				// Joiners:             joiners,
			}

			if err := manager.feedInfoDb.UpdateFeedInfo(feed, feedInfo); err != nil {
				delete(manager.rooms, feedKey)
				return manager.replyError(req, protocol.Error_FEED_INFO_DB_ERROR, err)
			}
			manager.updateFeedInfoInCache(feedKey, feedInfo)
		}
		TrackFeedJoiner(1, true)
		user.joinFeeds[feedKey] = req.cmd.Feed
		manager.sendToRoom(room, req, true)
	case protocol.Command_LEAVE:
		//delete(feedInfoItem.Joiners, req.ctx.getContext().Account)
		//manager.feedInfoDb.UpdateFeedInfo(feed, feedInfoItem)
		room := manager.rooms[feedKey]
		if room == nil {
			return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
		}
		user := manager.connectedUsers[clientId]
		if user.joinFeeds[feedKey] != nil {
			TrackFeedJoiner(1, false)
			delete(user.joinFeeds, feedKey)
		}
		manager.sendToRoom(room, req, true)
	case protocol.Command_SUBSCRIBE:
		return manager.handleSubscribe(feedKey, feed, feedInfo, req)
	case protocol.Command_UNSUBSCRIBE:
		return manager.handleUnsubscribe(feedKey, feed, req)
	case protocol.Command_PUBLISH:
		room := manager.rooms[feedKey]
		if room == nil {
			return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
		}
		manager.sendToRoom(room, req, true)
	case protocol.Command_PRESENCE:
		room := manager.rooms[feedKey]
		if room == nil {
			return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
		}
		manager.sendToRoom(room, req, true)
	case protocol.Command_FEED_INFO:
		// Just check in memory cache to reduce overhead
		room := manager.rooms[feedKey]
		if room == nil {
			return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
		}
		manager.sendToRoom(room, req, true)
	case protocol.Command_KICK:
		success, code := manager.handleKick(req)
		if !success {
			return manager.replyError(req, code, nil)
		}
	case protocol.Command_MODERATOR:
		moderator := manager.connectedUsers[clientId]
		if moderator == nil {
			return manager.replyError(req, protocol.Error_MISSING_STATE, nil)
		}
		if !moderator.ctx.getContext().IsModerator || feed.Account != req.ctx.getContext().Account {
			return manager.replyError(req, protocol.Error_UNAUTHORIZED, nil)
		}
		if moderator.joinFeeds[feedKey] == nil {
			return manager.replyError(req, protocol.Error_MISSING_STATE, nil)
		}
		room := manager.rooms[feedKey]
		if room == nil {
			return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
		}
		manager.sendToRoom(room, req, true)
	}
	return nil
}

func (manager *FeedManagerImpl) deleteFeed(feed *protocol.Feed, feedKey string) error {
	// TODO: check members & subscriber
	room := manager.rooms[feedKey]
	TrackFeedRoom(float64(len(manager.rooms)))
	manager.updateFeedInfoInCache(feedKey, nil)
	if room == nil {
		manager.logger.Warn("Room not exist for deletion", zap.String("feed", feedKey))
		return nil
	}
	// Let TTL to delete it to reduce changing node too frequently
	// manager.feedInfoDb.DeleteFeedInfo(feed)
	delete(manager.rooms, feedKey)
	go func() {
		done := false
		for i := 1; i <= 3 && !done; i++ {
			select {
			case room.done <- true:
				done = true
			case <-time.After(WaitRoomDone):
				manager.logger.Error("Send delete timeout", zap.String("feed", feedKey), zap.Int("count", i))
			}
		}
	}()
	return nil
}

func (manager *FeedManagerImpl) GetConnectedUserCount() int {
	return len(manager.connectedUsers)
}

func (manager *FeedManagerImpl) GetRoomCount(feed *protocol.Feed) int {
	feedKey := GetFeedKey(feed)
	room := manager.rooms[feedKey]
	if room == nil {
		return 0
	}
	return len(room.members)
}

func (manager *FeedManagerImpl) handleUnsubscribe(feedKey string, feed *protocol.Feed, req *feedReq) error {
	TrackFeedSubscriber(1, false)
	clientId := req.ctx.GetId()
	user := manager.connectedUsers[clientId]

	subFeed := user.subFeeds[feedKey]
	if subFeed == nil || subFeed.feedInfoItem == nil {
		return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
	}
	feedInfo := subFeed.feedInfoItem
	if !subFeed.isLocal {
		proxyAgent := manager.proxyManager.GetAgent(feedInfo.GetTarget())
		if proxyAgent == nil {
			return manager.replyError(req, protocol.Error_PROXY_ERROR, nil)
		}
		// Unsubscribe
		delete(user.subFeeds, feedKey)
		if err := proxyAgent.SendFeedRequest(req); err != nil {
			return manager.replyError(req, protocol.Error_PROXY_ERROR, err)
		}
		return nil
	}
	delete(user.subFeeds, feedKey)
	room := manager.rooms[feedKey]
	if room == nil {
		manager.logger.Error("fail to unsub", zap.String("account", req.ctx.getContext().Account),
			zap.Uint32("clientId", uint32(clientId)), zap.String("feed", feedKey))
		return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
	}
	req.user = user
	return manager.sendToRoom(room, req, true)
}

func (manager *FeedManagerImpl) handleSubscribe(feedKey string, feed *protocol.Feed, feedInfo *FeedInfoItem, req *feedReq) error {
	if feedInfo == nil {
		return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
	}
	TrackFeedSubscriber(1, true)
	clientId := req.ctx.GetId()
	user := manager.connectedUsers[clientId]
	if feedInfo.NodeId != manager.NodeId {
		proxyAgent := manager.proxyManager.GetAgent(feedInfo.GetTarget())
		if proxyAgent == nil {
			return manager.replyError(req, protocol.Error_PROXY_ERROR, nil)
		}
		user.subFeeds[feedKey] = &subscribeFeed{
			feed:         feed,
			feedInfoItem: feedInfo,
			isLocal:      false,
		}
		if err := proxyAgent.SendFeedRequest(req); err != nil {
			return manager.replyError(req, protocol.Error_PROXY_ERROR, err)
		}
		return nil
	}
	user.subFeeds[feedKey] = &subscribeFeed{
		feed:         feed,
		feedInfoItem: feedInfo,
		isLocal:      true,
	}
	room := manager.rooms[feedKey]
	if room == nil {
		manager.logger.Error("fail to sub", zap.String("feed", feedKey))
		return manager.replyError(req, protocol.Error_FEED_NOT_EXIST, nil)
	}
	req.user = user
	return manager.sendToRoom(room, req, true)
}

func (manager *FeedManagerImpl) Init() {
	go manager.processCommand()
}

func CreateFeedManager(infoDb FeedInfoDb, proxyManager ProxyManager, nodeInfo *NodeStatInfo, logger *zap.Logger) FeedManager {
	l := logger.With(zap.String("ip", nodeInfo.IP))
	manager := &FeedManagerImpl{
		que:            make(chan feedReq, FeedQueueSize),
		rooms:          make(map[string]*FeedRoomInfo),
		connectedUsers: make(map[ClientId]*connectedUser),
		feedInfoDb:     infoDb,
		NodeId:         nodeInfo.NodeId,
		localIP:        nodeInfo.PrivateIP,
		publicIP:       nodeInfo.IP,
		proxyManager:   proxyManager,
		logger:         l,
		feedInfoCache:  make(map[string]*FeedInfoItem),
	}
	return manager
}
