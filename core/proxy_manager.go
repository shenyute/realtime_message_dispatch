package core

import (
	"github.com/shenyute/realtime_message_dispatch/base"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"github.com/shirou/gopsutil/v3/cpu"
	"go.uber.org/zap"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type void struct{}

var member void

type ProxyManager interface {
	Init(factory NodeConnectorFactory)
	GetAgent(target string) ProxyAgent
	ListNodeStats() []*NodeStatInfo
	GetNodeStats(target string) *NodeStatInfo
}

type ProxyAgent interface {
	SendFeedRequest(req *feedReq) error
	Close() error
	ReceiveData()
}

type ProxyManagerImpl struct {
	factory        NodeConnectorFactory
	statDb         NodeStatDb
	cachedStats    map[string]*NodeStatInfo
	agents         map[string]ProxyAgent
	NodeId         string
	IP             string
	keySet         *base.KeySet
	PrivateIP      string
	Port           string
	IPv6           string
	Cluster        string
	localStat      NodeStatInfo
	logger         *zap.Logger
	lastUpdateTime time.Time
}

func (p *ProxyManagerImpl) GetNodeStats(target string) *NodeStatInfo {
	return p.cachedStats[target]
}

func (p *ProxyManagerImpl) ListNodeStats() []*NodeStatInfo {
	c := p.cachedStats
	v := make([]*NodeStatInfo, 0, len(c))
	for _, value := range c {
		if value.Status == Running {
			v = append(v, value)
		}
	}
	return v
}

const RetryForever = -1
const UpdateInterval = time.Duration(5) * time.Second
const ProxyQueueLimit = 1024
const ReconnectLimit = 10
const InternodeReplyTimeout = 3 * time.Second
const WaitForClose = 30 * time.Second

func NewProxyManager(node *NodeStatInfo, factory NodeConnectorFactory, statDb NodeStatDb, logger *zap.Logger) ProxyManager {
	m := &ProxyManagerImpl{
		factory:   factory,
		statDb:    statDb,
		NodeId:    node.NodeId,
		IP:        node.IP,
		PrivateIP: node.PrivateIP,
		Port:      node.Port,
		Cluster:   node.Cluster,
		logger:    logger,
	}
	m.Init(factory)
	return m
}

func (p *ProxyManagerImpl) trackCpu() {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		p.logger.Error("Fail to get cpu percent", zap.Error(err))
		return
	}
	now := time.Now()
	if now.Sub(p.lastUpdateTime) > time.Minute {
		p.logger.Debug("UpdateStat", zap.String("ip", p.localStat.IP), zap.Float64("cpu", percents[0]))
		p.lastUpdateTime = now
	}
	p.localStat.Cpu = percents[0]
	err = p.statDb.UpdateNodeInfo(p.localStat.NodeId, &p.localStat)
	if err != nil {
		p.logger.Error("Fail to update cpu percent", zap.Error(err))
	}
}

func (p *ProxyManagerImpl) newProxyAgent(info *NodeStatInfo) ProxyAgent {
	agent := NodeProxyAgent{
		proxyIP:                    info.PrivateIP,
		queueName:                  "proxy-" + p.PrivateIP + "-" + info.PrivateIP,
		pendingQueueName:           "proxy-pending-" + p.PrivateIP + "-" + info.PrivateIP,
		replyQueueName:             "proxy-reply-" + p.PrivateIP + "-" + info.PrivateIP,
		ch:                         make(chan *proxyReq, ProxyQueueLimit),
		replyCh:                    make(chan *proxyReply, ProxyQueueLimit),
		disconnectCh:               make(chan bool, ReconnectLimit),
		subscribeFeeds:             make(map[ClientId]map[string]void),
		feedSubscribers:            make(map[string]map[ClientId]ClientAgent),
		pendingSubscribeChannels:   make(map[string]chan *protocol.InternodeReply),
		pendingUnsubscribeChannels: make(map[string]chan *protocol.InternodeReply),
		pendingRequests:            make(map[uint32]*feedReq),
		cacheFeedInfo:              make(map[string]*protocol.FeedInfo),
		nodeConnector:              p.factory.CreateConnector(info),
		logger:                     p.logger.With(zap.String("ip", info.IP), zap.String("privateIp", info.PrivateIP), zap.String("port", info.Port)),
		retryPolicy:                RetryForever,
	}
	agent.init()
	return &agent
}

func (p *ProxyManagerImpl) update() {
	p.trackCpu()
	stats, err := p.statDb.ListNodeInfos()
	if err != nil {
		p.logger.Error("Update stats fail", zap.Error(err))
		return
	}
	s := make(map[string]*NodeStatInfo)
	newAgents := make(map[string]ProxyAgent)
	for _, stat := range stats {
		s[stat.NodeId] = stat
		// add if new
		agent := p.agents[stat.NodeId]
		if stat.Status == Running && p.Cluster == stat.Cluster {
			if agent == nil {
				if stat.NodeId != p.NodeId {
					agent = p.newProxyAgent(stat)
				} else {
					// no need to create
					continue
				}
			}
			newAgents[stat.NodeId] = agent
		}
	}
	// close out of date newAgents
	for k, agent := range p.agents {
		if newAgents[k] == nil && agent != nil {
			go agent.Close()
		}
	}
	// replace old directly to avoid race condition
	p.cachedStats = s
	p.agents = newAgents
}

func (p *ProxyManagerImpl) updater() {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			p.logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			// fork another goroutine to keep handling
			go p.update()
		}
	}()
	time.Sleep(3 * time.Second)
	for {
		p.update()
		time.Sleep(UpdateInterval)
	}
}

func (p *ProxyManagerImpl) Init(factory NodeConnectorFactory) {
	p.factory = factory
	p.localStat.IP = p.IP
	p.localStat.NodeId = p.NodeId
	p.localStat.PrivateIP = p.PrivateIP
	p.localStat.IPv6 = p.IPv6
	p.localStat.Cluster = p.Cluster
	p.localStat.Port = p.Port
	p.localStat.Status = Running
	go p.updater()
}

// target example "127.0.0.1:8080"
func (p *ProxyManagerImpl) GetAgent(target string) ProxyAgent {
	if p.cachedStats[target] == nil {
		return nil
	}
	agent := p.agents[target]
	return agent
}

type proxyReqType int

const (
	FeedRequest = iota
	InternalFeedRequest
	FeedReply
)

type proxyReq struct {
	id      uint32
	reqType proxyReqType
	feedReq *feedReq
}

type proxyReply struct {
	reply *protocol.InternodeReply
}

const ProxyRetryLimit = 3
const QuickProxyRetryInterval = 3 * time.Second
const ProxyRetryInterval = 30 * time.Second

// track all subscriber for |node|
type NodeProxyAgent struct {
	id                         uint32
	queueName                  string
	pendingQueueName           string
	replyQueueName             string
	proxyIP                    string
	ch                         chan *proxyReq
	replyCh                    chan *proxyReply
	disconnectCh               chan bool
	nodeConnector              NodeConnector
	subscribeFeeds             map[ClientId](map[string]void)
	feedSubscribers            map[string](map[ClientId]ClientAgent)
	cacheFeedInfo              map[string]*protocol.FeedInfo
	pendingSubscribeChannels   map[string]chan *protocol.InternodeReply
	pendingUnsubscribeChannels map[string]chan *protocol.InternodeReply
	pendingRequests            map[uint32]*feedReq
	lock                       sync.Mutex
	done                       chan bool
	closed                     bool
	logger                     *zap.Logger
	retryCount                 int
	retryPolicy                int
}

func (n *NodeProxyAgent) needRetry() bool {
	return n.retryPolicy == RetryForever || n.retryCount < ProxyRetryLimit
}

func (n *NodeProxyAgent) onNotifyDisconnect() {
	n.clearCacheFeedInfo()
	// notify all subscriber to resubscribe again
	for feedKey, subscribers := range n.feedSubscribers {
		message := &ClientMessage{
			Type: Push,
			Push: &protocol.Push{
				Type: protocol.Push_FEED_ERROR,
				Feed: getFeedFromFeedKey(feedKey),
			},
		}
		for clientId, client := range subscribers {
			if err := client.BroadcastData(message); err != nil && shouldLogError(err) {
				n.logger.Error("Fail to broadcast push", zap.Uint32("clientId", uint32(clientId)),
					zap.String("type", protocol.Push_FEED_ERROR.String()), zap.Error(err))
			}
		}
	}
}

func (n *NodeProxyAgent) retryFlow() error {
	// reconnect again
	var err error
	go n.notifyDisconnected()
	for n.needRetry() {
		if err = n.nodeConnector.Connect(); err == nil {
			n.retryCount = 0
			n.logger.Debug("Connect proxy agent success")
			break
		} else if n.needRetry() {
			if n.retryCount < 3 {
				time.Sleep(QuickProxyRetryInterval)
			} else {
				time.Sleep(ProxyRetryInterval)
			}
			n.retryCount++
		} else {
			// retry too many times.
			// TODO: ask connected user to connect to other node & mark this node error??
			n.logger.Fatal("Hit max retry limit to connect for proxy agent", zap.Error(err))
		}
		n.logger.Error("Fail to connect proxy agent", zap.Error(err))
	}
	// TODO: clean up pending requests
	// Maybe not necessary since pending request will be timed out
	return err
}

func (n *NodeProxyAgent) sendInternalFeedRequest(r *feedReq) error {
	c := atomic.AddUint32(&n.id, 1)
	if r.feedKey == "" {
		n.logger.Error("Missing feed key")
	}
	req := &proxyReq{
		id:      c,
		reqType: InternalFeedRequest,
		feedReq: r,
	}
	TrackQueueSize(n.queueName, float64(len(n.ch)))
	select {
	case n.ch <- req:
	case <-time.After(200 * time.Millisecond):
		return Timeout
	}
	return nil
}

func (n *NodeProxyAgent) SendFeedRequest(r *feedReq) error {
	if r.feedKey == "" {
		n.logger.Error("Missing feed key", zap.String("method", r.cmd.Method.String()))
	}
	c := atomic.AddUint32(&n.id, 1)
	req := &proxyReq{
		id:      c,
		reqType: FeedRequest,
		feedReq: r,
	}
	TrackQueueSize(n.queueName, float64(len(n.ch)))
	select {
	case n.ch <- req:
	case <-time.After(200 * time.Millisecond):
		return Timeout
	}
	return nil
}

func (n *NodeProxyAgent) Close() error {
	n.closed = true
	select {
	case n.done <- true:
	case <-time.After(WaitForClose):
	}
	return n.nodeConnector.Close()
}

func (n *NodeProxyAgent) ResubscribeFeed(feedKey string, now time.Time) {
	feed := getFeedFromFeedKey(feedKey)
	n.logger.Debug("Resubscribe feed", zap.String("feed", feedKey))
	n.sendInternalFeedRequest(&feedReq{
		cmd: &protocol.Command{
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
		feedKey:   feedKey,
		startTime: now,
	})
}

func (n *NodeProxyAgent) ResubscribeFeeds() {
	now := time.Now()
	for feedKey, _ := range n.feedSubscribers {
		n.ResubscribeFeed(feedKey, now)
	}
}

func (n *NodeProxyAgent) notifyDisconnected() {
	n.logger.Debug("notify disconnected")
	select {
	case n.disconnectCh <- true:
	case <-time.After(WaitForClose):
		n.logger.Error("notify disconnected timeout")
	}
}

func (n *NodeProxyAgent) notifySubscribeError(feed *protocol.Feed) {
	push := protocol.Push{
		Feed: feed,
		Type: protocol.Push_SUBSCRIBE_ERROR,
	}
	n.handleReceivePush(&push)
	feedKey := GetFeedKey(feed)
	subscribers := n.feedSubscribers[feedKey]
	if subscribers != nil {
		for clientId, _ := range subscribers {
			subscribeFeeds := n.subscribeFeeds[clientId]
			if subscribeFeeds != nil {
				delete(subscribeFeeds, feedKey)
			}
		}
	}
	delete(n.feedSubscribers, feedKey)
}

func (n *NodeProxyAgent) receiveData(reply *proxyReply) {
	TrackQueueSize(n.replyQueueName, float64(len(n.replyCh)))
	n.replyCh <- reply
}

func (n *NodeProxyAgent) ReceiveData() {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			n.logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			// fork another goroutine to keep handling
			go n.ReceiveData()
		}
	}()
	for {
		reply, err := n.nodeConnector.Receive()
		if n.closed {
			n.logger.Info("Node closed")
			break
		}
		if _, ok := err.(net.Error); ok || err == ProxyConnectFailError {
			// TODO: retry if needed
			n.logger.Error("Fail to receive data for proxy agent", zap.Error(err))
			if !n.closed {
				n.retryFlow()
				continue
			}
		} else if nerr, ok := err.(*Error); ok && !nerr.IsTemporary() {
			// TODO: retry if needed
			n.logger.Error("Fail to receive data for proxy agent", zap.Error(err))
			if !n.closed {
				n.retryFlow()
				continue
			}
		}
		if err != nil {
			continue
		}
		n.receiveData(&proxyReply{reply: reply})
	}
}

func (n *NodeProxyAgent) init() {
	go n.ReceiveData()
	go n.process()
}

func (n *NodeProxyAgent) handleUnsubscribe(requestId uint32, req *feedReq, noReply bool) {
	feedKey := req.feedKey
	clientId := req.ctx.GetId()

	// TODO: add unittest
	n.lock.Lock()
	defer n.lock.Unlock()

	subscribers := n.feedSubscribers[feedKey]
	subscribeFeeds := n.subscribeFeeds[clientId]
	if subscribeFeeds != nil {
		delete(subscribeFeeds, feedKey)
	}
	if subscribers == nil {
		if !noReply {
			reply := protocol.Reply{
				Id:    req.cmd.Id,
				Error: NotSubscribeFeedError,
			}
			go req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &reply, Duration: time.Now().Sub(req.startTime)})
		}
		return
	}
	delete(subscribers, clientId)
	n.logger.Debug("handle unsubscribe", zap.Uint32("clientId", uint32(clientId)),
		zap.Uint32("id", requestId),
		zap.String("feed", feedKey), zap.Int("size", len(subscribers)))
	// return reply
	if len(subscribers) == 0 {
		cmd := &protocol.InternodeCommand{
			Id:     requestId,
			Method: protocol.InternodeCommand_UNSUBSCRIBE,
			Feed:   req.cmd.Feed,
		}
		delete(n.feedSubscribers, feedKey)
		ch := make(chan *protocol.InternodeReply, 1)
		n.pendingRequests[requestId] = req
		n.pendingUnsubscribeChannels[feedKey] = ch
		TrackQueueSize(n.pendingQueueName, float64(len(n.pendingRequests)))
		go n.waitPendingRequest(requestId, req, feedKey, ch, false)
		go func() {
			if err := n.nodeConnector.Send(cmd); err != nil {
				// reply proxy fail
				n.receiveData(&proxyReply{
					reply: &protocol.InternodeReply{
						Id:          requestId,
						Error:       InternodeProxyError,
						Unsubscribe: &protocol.UnsubscribeResult{},
					},
				})
				n.notifyDisconnected()
			}
		}()
	} else if !noReply {
		clientInfo := &protocol.ClientInfo{
			Account:  req.ctx.getContext().Account,
			DeviceId: req.ctx.getContext().DeviceId,
			ClientId: uint32(req.ctx.GetId()),
			IsBot:    req.ctx.getContext().IsBot,
		}
		reply := protocol.Reply{
			Id: req.cmd.Id,
			Unsubscribe: &protocol.UnsubscribeResult{
				Feed: req.cmd.Feed,
				Info: clientInfo,
			},
		}
		go req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &reply, Duration: time.Now().Sub(req.startTime)})
	}
}

func (n *NodeProxyAgent) handleInternalFeedRequest(requestId uint32, req *feedReq) {
	feed := req.cmd.Feed
	switch req.cmd.Method {
	case protocol.Command_SUBSCRIBE:
		cmd := &protocol.InternodeCommand{
			Id:     requestId,
			Method: protocol.InternodeCommand_SUBSCRIBE,
			Feed:   feed,
		}
		n.pendingRequests[requestId] = req
		TrackQueueSize(n.pendingQueueName, float64(len(n.pendingRequests)))
		go func() {
			if err := n.nodeConnector.Send(cmd); err != nil {
				// reply proxy fail
				n.receiveData(&proxyReply{
					reply: &protocol.InternodeReply{
						Id:        requestId,
						Error:     InternodeProxyError,
						Subscribe: &protocol.SubscribeResult{},
					},
				})
				n.notifyDisconnected()
			}
		}()
	default:
		n.logger.Error("Unhandled command type for internal",
			zap.String("method", req.cmd.Method.String()))
	}
}

func (n *NodeProxyAgent) handleFeedRequest(requestId uint32, req *feedReq) {
	feed := req.cmd.Feed
	n.logger.Debug("handle proxy feed request", zap.Any("method", req.cmd.Method), zap.Uint32("id", requestId))
	now := time.Now()
	switch req.cmd.Method {
	case protocol.Command_DISCONNECT:
		clientId := req.ctx.GetId()
		subscribeFeeds := n.subscribeFeeds[clientId]
		if subscribeFeeds != nil {
			// unsubscribe all feeds for this client
			for feedKey := range subscribeFeeds {
				feed := getFeedFromFeedKey(feedKey)
				if feed == nil {
					n.logger.Debug("can't unsubscribe feed", zap.String("feed", feedKey))
					continue
				}
				c := atomic.AddUint32(&n.id, 1)
				cmd := &protocol.Command{
					Method: protocol.Command_UNSUBSCRIBE,
					Feed:   feed,
					Id:     c,
				}
				n.handleUnsubscribe(c, &feedReq{
					feedKey:   feedKey,
					ctx:       req.ctx,
					cmd:       cmd,
					startTime: now,
				}, true)
			}
		} else {
			n.logger.Debug("did not subscribe any feed", zap.Uint32("clientId", uint32(clientId)))
		}
		req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &protocol.Reply{Id: req.cmd.Id}, Duration: now.Sub(req.startTime)})
	case protocol.Command_SUBSCRIBE:
		feedKey := req.feedKey
		clientId := req.ctx.GetId()
		alreadySub := true
		feedInfo := n.getCacheFeedInfo(feedKey)
		if feedInfo == nil && n.pendingSubscribeChannels[feedKey] == nil {
			cmd := &protocol.InternodeCommand{
				Id:     requestId,
				Method: protocol.InternodeCommand_SUBSCRIBE,
				Feed:   feed,
			}
			ch := make(chan *protocol.InternodeReply, 1)
			n.pendingRequests[requestId] = req
			n.pendingSubscribeChannels[feedKey] = ch
			TrackQueueSize(n.pendingQueueName, float64(len(n.pendingRequests)))
			go n.waitPendingRequest(requestId, req, feedKey, ch, false)
			go func() {
				if err := n.nodeConnector.Send(cmd); err != nil {
					// reply proxy fail
					n.receiveData(&proxyReply{
						reply: &protocol.InternodeReply{
							Id:        requestId,
							Error:     InternodeProxyError,
							Subscribe: &protocol.SubscribeResult{},
						},
					})
					n.notifyDisconnected()
				}
			}()
			alreadySub = false
		} else if feedInfo == nil {
			// still pending
			go n.waitPendingRequest(requestId, req, feedKey, n.pendingSubscribeChannels[feedKey], true)
			alreadySub = false
		}
		if alreadySub {
			n.handleAddSubscriber(feedKey, clientId, req)
			// return reply directly
			clientInfo := &protocol.ClientInfo{
				Account:  req.ctx.getContext().Account,
				DeviceId: req.ctx.getContext().DeviceId,
				ClientId: uint32(req.ctx.GetId()),
				IsBot:    req.ctx.getContext().IsBot,
			}
			reply := protocol.Reply{
				Id: req.cmd.Id,
				Subscribe: &protocol.SubscribeResult{
					Info:   feedInfo,
					Client: clientInfo,
				},
			}
			req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &reply, Duration: now.Sub(req.startTime)})
		}
	case protocol.Command_UNSUBSCRIBE:
		n.handleUnsubscribe(requestId, req, false)
	default:
		n.logger.Error("Unhandled command type", zap.String("method", req.cmd.Method.String()))
	}
}

func (n *NodeProxyAgent) updateFeedCacheIfNeed(push *protocol.Push) {
	n.lock.Lock()
	defer n.lock.Unlock()

	feedKey := GetFeedKey(push.Feed)
	info := n.cacheFeedInfo[feedKey]
	if info == nil {
		return
	}
	switch push.Type {
	case protocol.Push_JOIN:
		if push.Join == nil || push.Join.Presence == nil {
			return
		}
		joinerId := push.Join.Info.ClientId
		joiner := &protocol.Joiner{
			AvatarUrl: push.Join.Presence.AvatarUrl,
			Info:      push.Join.Info,
		}
		info.Joiners[joinerId] = joiner
	case protocol.Push_LEAVE:
		if push.Leave == nil {
			return
		}
		account := push.Leave.Info.ClientId
		delete(info.Joiners, account)
	case protocol.Push_PRESENCE:
		if push.Presence == nil {
			return
		}
		joinerId := push.Presence.Info.ClientId
		joiner := info.Joiners[joinerId]
		if joiner != nil {
			joiner.IsCalling = push.Presence.IsCalling
			if push.Presence.AvatarUrl != "" {
				joiner.AvatarUrl = push.Presence.AvatarUrl
			}
			joiner.AvatarForegroundImageUrl = push.Presence.AvatarForegroundImageUrl
			if push.Presence.AvatarBackgroundImageUrl != "" {
				joiner.AvatarBackgroundImageUrl = push.Presence.AvatarBackgroundImageUrl
			}
			if push.Presence.IdleAnimationKey != "" {
				joiner.IdleAnimationKey = push.Presence.IdleAnimationKey
			}
			if len(push.Presence.Metadata) > 0 {
				joiner.Metadata = push.Presence.Metadata
			}
			if len(push.Presence.MoodKeys) > 0 {
				joiner.MoodKeys = push.Presence.MoodKeys
			}
			if push.Presence.Assets != nil {
				joiner.Assets = push.Presence.GetAssets()
			}
		}
	case protocol.Push_FEED_ERROR:
		delete(n.cacheFeedInfo, feedKey)
	default:
		// ignore
	}
}

func shouldLogError(err error) bool {
	return err != ClientClosedError && err != ClientRateLimit
}

func (n *NodeProxyAgent) handleReceivePush(push *protocol.Push) {
	feed := push.Feed
	feedKey := GetFeedKey(feed)
	//n.logger.Debug("Receive push", zap.String("feed", feedKey), zap.String("type", push.Type.String()))
	subscribers := n.feedSubscribers[feedKey]
	if subscribers == nil {
		n.logger.Debug("No subscribers, skip it", zap.String("feed", feedKey), zap.String("type", push.Type.String()))
		return
	}
	n.updateFeedCacheIfNeed(push)
	message := &ClientMessage{
		Type: Push,
		Push: push,
	}
	var deviceId string
	switch push.Type {
	case protocol.Push_JOIN:
		deviceId = push.Join.Info.DeviceId
	case protocol.Push_LEAVE:
		deviceId = push.Leave.Info.DeviceId
	case protocol.Push_PRESENCE:
		deviceId = push.Presence.Info.DeviceId
	case protocol.Push_PUBLICATION:
		deviceId = push.Pub.Info.DeviceId
	case protocol.Push_FEED_ERROR:
	}
	for clientId, client := range subscribers {
		if client.getContext().DeviceId == deviceId {
			continue
		}
		if err := client.BroadcastData(message); err != nil && shouldLogError(err) {
			n.logger.Error("Fail to broadcast push", zap.Uint32("clientId", uint32(clientId)),
				zap.String("type", push.Type.String()), zap.Error(err))
		}
	}
}

func (n *NodeProxyAgent) getCacheFeedInfo(feedKey string) *protocol.FeedInfo {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.cacheFeedInfo[feedKey]
}

func (n *NodeProxyAgent) clearCacheFeedInfo() {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.cacheFeedInfo = make(map[string]*protocol.FeedInfo)
}

func (n *NodeProxyAgent) deleteCacheFeedInfo(feedKey string) {
	n.lock.Lock()
	defer n.lock.Unlock()
	delete(n.cacheFeedInfo, feedKey)
}

func (n *NodeProxyAgent) updateCacheFeedInfo(feedKey string, info *protocol.FeedInfo) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if info.Joiners == nil {
		info.Joiners = make(map[uint32]*protocol.Joiner)
	}
	n.cacheFeedInfo[feedKey] = info
	n.logger.Debug("UpdateCacheFeedInfo", zap.String("feed", feedKey))
}

func (n *NodeProxyAgent) waitPendingRequest(requestId uint32, req *feedReq, feedKey string, replyCh <-chan *protocol.InternodeReply, justWaiting bool) {
	reply := protocol.Reply{
		Id: req.cmd.Id,
	}
	n.logger.Debug("waitingPendingRequest", zap.String("method", req.cmd.Method.String()),
		zap.Uint32("id", requestId), zap.String("feed", feedKey))
	select {
	case r := <-replyCh:
		switch req.cmd.Method {
		case protocol.Command_SUBSCRIBE:
			info := n.getCacheFeedInfo(feedKey)
			if info != nil {
				n.handleAddSubscriber(feedKey, req.ctx.GetId(), req)
				clientInfo := &protocol.ClientInfo{
					Account:  req.ctx.getContext().Account,
					DeviceId: req.ctx.getContext().DeviceId,
					ClientId: uint32(req.ctx.GetId()),
					IsBot:    req.ctx.getContext().IsBot,
				}
				reply.Subscribe = &protocol.SubscribeResult{
					Info:   info,
					Client: clientInfo,
				}
				req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &reply,
					Duration: time.Now().Sub(req.startTime)})
				return
			} else {
				reply.Subscribe = &protocol.SubscribeResult{Feed: req.cmd.Feed}
			}
			if r == nil {
				reply.Error = InternodeProxyError
			} else {
				reply.Error = r.Error
			}
		case protocol.Command_UNSUBSCRIBE:
			n.deleteCacheFeedInfo(feedKey)
			if r != nil {
				reply.Error = r.Error
				reply.Unsubscribe = r.Unsubscribe
			} else {
				reply.Error = InternodeProxyError
				reply.Unsubscribe = &protocol.UnsubscribeResult{Feed: req.cmd.Feed}
			}
		default:
			// TODO: unsupported yet
			reply.Error = UnSupportedMethod
			n.logger.Error("Unsupported method", zap.String("feed", feedKey), zap.Uint32("id", requestId))
		}
	case <-time.After(InternodeReplyTimeout):
		reply.Error = InternodeTimeoutError
	}
	if reply.Error != nil {
		n.logger.Error("fail to get feed info cache", zap.String("feed", feedKey),
			zap.String("method", req.cmd.Method.String()),
			zap.Uint32("id", requestId), zap.String("code", reply.Error.Code.String()),
			zap.Bool("waiting", justWaiting))
	} else {
		n.logger.Debug("Send proxy reply", zap.String("feed", feedKey), zap.Uint32("id", requestId),
			zap.Bool("waiting", justWaiting))
	}
	// Subscribe did not success
	req.ctx.WriteMessage(&ClientMessage{Type: Reply, Req: req.cmd, Reply: &reply, Duration: time.Now().Sub(req.startTime)})
}

func (n *NodeProxyAgent) handleAddSubscriber(feedKey string, clientId ClientId, req *feedReq) {
	// TODO: check if already subscribe?
	n.lock.Lock()
	defer n.lock.Unlock()
	// add feed for clientId
	subscribeFeeds := n.subscribeFeeds[clientId]
	if subscribeFeeds == nil {
		subscribeFeeds = make(map[string]void)
	}
	subscribeFeeds[feedKey] = member
	n.subscribeFeeds[clientId] = subscribeFeeds

	// add client for feedKey
	subscribers := n.feedSubscribers[feedKey]
	if subscribers == nil {
		subscribers = make(map[ClientId]ClientAgent)
	}
	subscribers[clientId] = req.ctx
	n.feedSubscribers[feedKey] = subscribers
	n.logger.Debug("add subscriber", zap.String("feed", feedKey), zap.Uint32("clientId", uint32(clientId)),
		zap.Int("size", len(subscribers)), zap.String("account", req.ctx.getContext().Account))
}

func (n *NodeProxyAgent) process() {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			n.logger.Error("panic", zap.NamedError("err", err),
				zap.String("stack", string(debug.Stack())))
			// fork another goroutine to keep handling
			go n.process()
		}
	}()
	n.doProcess()
}

func (n *NodeProxyAgent) doProcess() {
	for !n.closed {
		select {
		case req := <-n.ch:
			switch req.reqType {
			case FeedRequest:
				n.handleFeedRequest(req.id, req.feedReq)
			case InternalFeedRequest:
				n.handleInternalFeedRequest(req.id, req.feedReq)
			default:
				// not support yet
			}
		case _ = <-n.disconnectCh:
			// now reconnected, ask subscriber to try again.
			n.onNotifyDisconnect()
		case reply := <-n.replyCh:
			if reply.reply.Push != nil {
				// it's push data
				n.handleReceivePush(reply.reply.Push)
				continue
			}
			// it's reply
			requestId := reply.reply.Id
			n.logger.Debug("Proxy agent receive reply", zap.Uint32("id", requestId))
			if req := n.pendingRequests[requestId]; req != nil {
				feedKey := req.feedKey
				var doneCh chan *protocol.InternodeReply
				if req.cmd.Method == protocol.Command_SUBSCRIBE {
					if reply.reply.Error == nil {
						n.updateCacheFeedInfo(feedKey, reply.reply.Subscribe.Info)
					}
					doneCh = n.pendingSubscribeChannels[feedKey]
					delete(n.pendingSubscribeChannels, feedKey)
				} else if req.cmd.Method == protocol.Command_UNSUBSCRIBE {
					doneCh = n.pendingUnsubscribeChannels[feedKey]
					delete(n.pendingUnsubscribeChannels, feedKey)
				}
				n.logger.Debug("doneChannel", zap.String("method", req.cmd.Method.String()), zap.Bool("ch", doneCh == nil), zap.String("feed", feedKey))
				if doneCh != nil {
					doneCh <- reply.reply
					close(doneCh)
				}
				if req.ctx == nil {
					if reply.reply.Error != nil {
						// Can not subscribe the feed, notify all clients
						n.notifySubscribeError(req.cmd.Feed)
					}
					// Just internal feed request, ignore it.
				}
				TrackQueueSize(n.pendingQueueName, float64(len(n.pendingRequests)))
				delete(n.pendingRequests, requestId)
			}
		case _ = <-n.done:
			break
		}
	}
	n.logger.Debug("NodeProxyAgent stopped")
}
