package core

import (
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"github.com/shenyute/realtime_message_dispatch/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	DEFAULT_WAIT_TIME = 200 * time.Millisecond
)

type FakeNodeConnector struct {
	request      []*protocol.InternodeCommand
	nodeId       string
	cond         *sync.Cond
	lock         sync.Mutex
	disconnected bool
}

func (f *FakeNodeConnector) SetDisconnect(d bool) {
	f.disconnected = d
}

func (f *FakeNodeConnector) Init(url, nodeId string) error {
	f.cond = sync.NewCond(&f.lock)
	f.nodeId = nodeId
	return nil
}

func (f *FakeNodeConnector) Connect() error {
	return nil
}

func (f *FakeNodeConnector) Close() error {
	return nil
}

func (f *FakeNodeConnector) Send(cmd *protocol.InternodeCommand) error {
	if f.disconnected {
		return ProxyConnectFailError
	}
	f.request = append(f.request, cmd)
	return nil
}

func (f *FakeNodeConnector) Receive() (*protocol.InternodeReply, error) {
	f.lock.Lock()
	f.cond.Wait()
	f.lock.Unlock()
	panic("Not implement yet")
	return nil, nil
}

type FakeNodeConnectorFactory struct {
	nodes map[string]*FakeNodeConnector
}

func (f *FakeNodeConnectorFactory) CreateConnector(info *NodeStatInfo) NodeConnector {
	target := info.NodeId
	if f.nodes[target] == nil {
		f.nodes[target] = &FakeNodeConnector{}
		f.nodes[target].Init("", target)
	}
	return f.nodes[target]
}

func TestLocalProxy(t *testing.T) {
	//omlogger := utils.InitLogger("./log/avatar.log", zapcore.DebugLevel)
	//zap.ReplaceGlobals(omlogger.Logger)
	t.Parallel()
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	ip1 := "127.0.0.1"
	nodeStatDb.UpdateNodeInfo(ip1, &NodeStatInfo{
		NodeId:    ip1,
		PrivateIP: ip1,
		Status:    Running,
	})
	ip2 := "127.0.0.2"
	nodeStatDb.UpdateNodeInfo(ip2, &NodeStatInfo{
		NodeId:    ip2,
		PrivateIP: ip2,
		Status:    Running,
	})
	keySet, err := utils.ReadKeySet("../megaphone.sec", "../megaphone.pub", "../megaphone.hmac")
	if err != nil {
		t.Errorf("fail to load keyset err:%v", err)
	}
	factory := &FakeNodeConnectorFactory{
		nodes: make(map[string]*FakeNodeConnector),
	}
	proxyManager := &ProxyManagerImpl{
		statDb:    nodeStatDb,
		factory:   factory,
		PrivateIP: "127.0.0.1",
		keySet:    keySet,
		logger:    zap.L(),
	}
	// update node & create proxy agent
	proxyManager.update()
	a := proxyManager.GetAgent(ip2)
	agent1, _ := a.(*NodeProxyAgent)
	// subscribe feed
	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	ctx := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: "127.0.0.1",
		Id:       1,
	}
	client := &FakeClient{
		ctx: &ctx,
	}
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     1,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	// fake response
	t.Logf("First reply")
	joiner := make(map[uint32]*protocol.Joiner)
	joiner[0] = &protocol.Joiner{
		AvatarBackgroundImageUrl: "bgUrl",
		AvatarForegroundImageUrl: "frUrl",
	}
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 1,
			Subscribe: &protocol.SubscribeResult{
				Info: &protocol.FeedInfo{
					Joiners: joiner,
				},
				Client: &protocol.ClientInfo{
					ClientId: 1,
				},
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("did not receive  reply")
	}
	consumeOut(client)
	ctx2 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: "127.0.0.1",
		Id:       2,
	}
	client2 := &FakeClient{
		ctx: &ctx2,
	}
	// client2 subscribe the same feed
	agent1.SendFeedRequest(&feedReq{
		ctx:     client2,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     2,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)

	if r := client2.outQueue[0]; r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("client 2 did not receive  reply")
	}
	consumeOut(client2)
	t.Logf("2 reply")
	// receive broadcast data
	data := "hello"
	var datas [][]byte
	datas = append(datas, []byte(data))
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Push: &protocol.Push{
				Type: protocol.Push_PUBLICATION,
				Feed: feed,
				Pub: &protocol.Publication{
					UnityData: datas,
					Info:      &protocol.ClientInfo{},
				},
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)

	if r := client.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_PUBLICATION || string(r.Push.Pub.UnityData[0]) != data {
		t.Errorf("client did not receive  reply")
	}
	if r := client2.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_PUBLICATION || string(r.Push.Pub.UnityData[0]) != data {
		t.Errorf("client 2 did not receive  reply")
	}
}

func setupNodes(nodeStatDb NodeStatDb, n int) []string {
	var ips []string
	for i := 1; i <= n; i++ {
		ip1 := "127.0.0." + strconv.Itoa(i)
		ips = append(ips, ip1)
		nodeStatDb.UpdateNodeInfo(ip1, &NodeStatInfo{
			NodeId:    ip1,
			PrivateIP: ip1,
			Status:    Running,
		})
	}
	return ips
}
func TestLocalProxyPendingRequest(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log")
	//zap.ReplaceGlobals(omlogger.Logger)
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	ips := setupNodes(nodeStatDb, 2)
	keySet, err := utils.ReadKeySet("../megaphone.sec", "../megaphone.pub", "../megaphone.hmac")
	if err != nil {
		t.Errorf("fail to load keyset err:%v", err)
	}
	factory := &FakeNodeConnectorFactory{
		nodes: make(map[string]*FakeNodeConnector),
	}
	proxyManager := &ProxyManagerImpl{
		statDb:    nodeStatDb,
		factory:   factory,
		PrivateIP: ips[0],
		keySet:    keySet,
		logger:    zap.L(),
	}
	// update node & create proxy agent
	proxyManager.update()
	a := proxyManager.GetAgent(ips[1])
	agent1, _ := a.(*NodeProxyAgent)
	// subscribe feed
	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	ctx := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: ips[0],
		Id:       1,
	}
	client := &FakeClient{
		ctx: &ctx,
	}
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     1,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	ctx2 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: ips[0],
		Id:       2,
	}
	client2 := &FakeClient{
		ctx: &ctx2,
	}
	// client2 subscribe the same feed
	agent1.SendFeedRequest(&feedReq{
		ctx:     client2,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     2,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})

	time.Sleep(DEFAULT_WAIT_TIME)
	// Now client1 & client2 should still waiting proxy reply
	if len(client.outQueue) != 0 || len(client2.outQueue) != 0 {
		t.Errorf("should not receive reply")
	}

	// fake response
	t.Logf("First reply")
	joiner := make(map[uint32]*protocol.Joiner)
	joiner[0] = &protocol.Joiner{
		AvatarBackgroundImageUrl: "bgUrl",
		AvatarForegroundImageUrl: "frUrl",
	}
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 1,
			Subscribe: &protocol.SubscribeResult{
				Info: &protocol.FeedInfo{
					Feed:    feed,
					Joiners: joiner,
				},
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	zap.L().Debug("xxx")
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("did not receive  reply")
	}
	consumeOut(client)

	if r := client2.outQueue[0]; r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("client 2 did not receive  reply")
	}
	consumeOut(client2)

	// receive broadcast data
	data := "hello"
	var datas [][]byte
	datas = append(datas, []byte(data))
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Push: &protocol.Push{
				Type: protocol.Push_PUBLICATION,
				Feed: feed,
				Pub: &protocol.Publication{
					UnityData: datas,
					Info:      &protocol.ClientInfo{},
				},
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)

	if r := client.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_PUBLICATION || string(r.Push.Pub.UnityData[0]) != data {
		t.Errorf("client did not receive  reply")
	}
	if r := client2.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_PUBLICATION || string(r.Push.Pub.UnityData[0]) != data {
		t.Errorf("client 2 did not receive  reply")
	}
}

func TestLocalProxyUpdateFeedCache(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log")
	//zap.ReplaceGlobals(omlogger.Logger)
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	ips := setupNodes(nodeStatDb, 2)
	keySet, err := utils.ReadKeySet("../megaphone.sec", "../megaphone.pub", "../megaphone.hmac")
	if err != nil {
		t.Errorf("fail to load keyset err:%v", err)
	}
	factory := &FakeNodeConnectorFactory{
		nodes: make(map[string]*FakeNodeConnector),
	}
	proxyManager := &ProxyManagerImpl{
		statDb:    nodeStatDb,
		factory:   factory,
		PrivateIP: ips[0],
		keySet:    keySet,
		logger:    zap.L(),
	}
	// update node & create proxy agent
	proxyManager.update()
	a := proxyManager.GetAgent(ips[1])
	agent1, _ := a.(*NodeProxyAgent)
	// subscribe feed
	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	ctx := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: ips[0],
		Id:       1,
	}
	client := &FakeClient{
		ctx: &ctx,
	}
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     1,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})

	time.Sleep(DEFAULT_WAIT_TIME)
	// Now client1 should still waiting proxy reply
	require.Equal(t, 0, len(client.outQueue), "should not receive reply")

	// fake response
	t.Logf("First reply")
	joiner := make(map[uint32]*protocol.Joiner)
	joiner[1] = &protocol.Joiner{
		AvatarUrl:                "avaurl",
		AvatarBackgroundImageUrl: "bgUrl",
		AvatarForegroundImageUrl: "frUrl",
	}
	feedInfo := &protocol.FeedInfo{
		Feed:    feed,
		Joiners: joiner,
	}
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 1,
			Subscribe: &protocol.SubscribeResult{
				Info: feedInfo,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	zap.L().Debug("xxx")
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("did not receive reply")
	}
	consumeOut(client)

	ctx2 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: ips[0],
		Id:       2,
	}
	client2 := &FakeClient{
		ctx: &ctx2,
	}
	// client2 subscribe the same feed
	agent1.SendFeedRequest(&feedReq{
		ctx:     client2,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     3,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})

	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client2.outQueue[0]; r.Type != Reply || r.Reply.Error != nil ||
		r.Reply.Subscribe == nil || len(r.Reply.Subscribe.Info.Joiners) != 1 {
		t.Errorf("client 2 did not receive  reply")
	}
	consumeOut(client2)
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 4,
			Push: &protocol.Push{
				Type: protocol.Push_JOIN,
				Feed: feed,
				Join: &protocol.Join{
					Info: &protocol.ClientInfo{
						Account: "accout2",
					},
					Presence: &protocol.Presence{
						AvatarUrl: "avat2",
					},
				},
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_JOIN || r.Push.Join.Info.Account != "accout2" {
		t.Errorf("client did not receive  reply")
	}
	if r := client2.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_JOIN || r.Push.Join.Info.Account != "accout2" {
		t.Errorf("client 2 did not receive  reply")
	}
	consumeOut(client)
	consumeOut(client2)

	ctx3 := ClientContext{
		Version:  "0.1",
		Account:  "account3",
		DeviceId: "deviceId3",
		RemoteIP: ips[1],
		Id:       3,
	}
	client3 := &FakeClient{
		ctx: &ctx3,
	}
	// client3 subscribe the same feed
	agent1.SendFeedRequest(&feedReq{
		ctx:     client3,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     5,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	// client3 should get updated feedInfoItem
	if r := client3.outQueue[0]; r.Type != Reply || r.Reply.Error != nil ||
		r.Reply.Subscribe == nil || len(r.Reply.Subscribe.Info.Joiners) != 2 {
		t.Errorf("client 3 did not receive  reply")
	}
	consumeOut(client3)
	// someone leave
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 6,
			Push: &protocol.Push{
				Type: protocol.Push_LEAVE,
				Feed: feed,
				Leave: &protocol.Leave{
					Info: &protocol.ClientInfo{
						Account: "accout2",
					},
				},
			},
		},
	})
	time.Sleep(2 * DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_LEAVE || r.Push.Leave.Info.Account != "accout2" {
		t.Errorf("client did not receive  reply")
	}
	if r := client2.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_LEAVE || r.Push.Leave.Info.Account != "accout2" {
		t.Errorf("client 2 did not receive  reply")
	}
	if r := client3.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_LEAVE || r.Push.Leave.Info.Account != "accout2" {
		t.Errorf("client 3 did not receive  reply")
	}
	consumeOut(client)
	consumeOut(client2)
	consumeOut(client3)
	// subscribe again
	agent1.SendFeedRequest(&feedReq{
		ctx:     client3,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     7,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	// client3 should get updated feedInfoItem
	if r := client3.outQueue[0]; r.Type != Reply || r.Reply.Error != nil ||
		r.Reply.Subscribe == nil || len(r.Reply.Subscribe.Info.Joiners) != 1 {
		t.Errorf("client 3 did not receive  reply")
	}
	consumeOut(client3)
}
func TestLocalProxyCleanFeedCache(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log", zapcore.DebugLevel)
	//zap.ReplaceGlobals(omlogger.Logger)
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	ips := setupNodes(nodeStatDb, 2)
	keySet, err := utils.ReadKeySet("../megaphone.sec", "../megaphone.pub", "../megaphone.hmac")
	if err != nil {
		t.Errorf("fail to load keyset err:%v", err)
	}
	factory := &FakeNodeConnectorFactory{
		nodes: make(map[string]*FakeNodeConnector),
	}
	proxyManager := &ProxyManagerImpl{
		statDb:    nodeStatDb,
		factory:   factory,
		PrivateIP: ips[0],
		keySet:    keySet,
		logger:    zap.L(),
	}
	// update node & create proxy agent
	proxyManager.update()
	a := proxyManager.GetAgent(ips[1])
	agent1, _ := a.(*NodeProxyAgent)
	fakeNode := factory.nodes[ips[1]]
	// subscribe feed
	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	ctx := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: ips[0],
		Id:       1,
	}
	client := &FakeClient{
		ctx: &ctx,
	}
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     1,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	joiner := make(map[uint32]*protocol.Joiner)
	joiner[1] = &protocol.Joiner{
		AvatarUrl:                "avaurl",
		AvatarBackgroundImageUrl: "bgUrl",
		AvatarForegroundImageUrl: "frUrl",
	}
	feedInfo := &protocol.FeedInfo{
		Feed:    feed,
		Joiners: joiner,
	}
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 1,
			Subscribe: &protocol.SubscribeResult{
				Info: feedInfo,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Subscribe == nil || r.Reply.Error != nil {
		t.Errorf("did not receive reply")
	}
	l := len(fakeNode.request)
	require.Equal(t, protocol.InternodeCommand_SUBSCRIBE, fakeNode.request[l-1].Method)
	consumeOut(client)
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     2,
			Method: protocol.Command_UNSUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	l = len(fakeNode.request)
	require.Equal(t, protocol.InternodeCommand_UNSUBSCRIBE, fakeNode.request[l-1].Method)
	require.Equal(t, 0, len(client.outQueue))
	// receive unsubscribe
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 2,
			Unsubscribe: &protocol.UnsubscribeResult{
				Feed: feed,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Unsubscribe == nil || r.Reply.Error != nil {
		t.Errorf("did not receive reply")
	}
	consumeOut(client)
	// resubscribe again
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     3,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	// Now client1 should still waiting proxy reply
	require.Equal(t, 0, len(client.outQueue), "should not receive reply")
	l = len(fakeNode.request)
	require.Equal(t, protocol.InternodeCommand_SUBSCRIBE, fakeNode.request[l-1].Method)
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 3,
			Subscribe: &protocol.SubscribeResult{
				Info: feedInfo,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Subscribe == nil || r.Reply.Error != nil {
		t.Errorf("did not receive reply")
	}
	consumeOut(client)
}

func TestLocalProxyCleanFeedCacheIfFeedNotExist(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log", zapcore.DebugLevel)
	//zap.ReplaceGlobals(omlogger.Logger)
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	ips := setupNodes(nodeStatDb, 2)
	keySet, err := utils.ReadKeySet("../megaphone.sec", "../megaphone.pub", "../megaphone.hmac")
	if err != nil {
		t.Errorf("fail to load keyset err:%v", err)
	}
	factory := &FakeNodeConnectorFactory{
		nodes: make(map[string]*FakeNodeConnector),
	}
	proxyManager := &ProxyManagerImpl{
		statDb:    nodeStatDb,
		factory:   factory,
		PrivateIP: ips[0],
		keySet:    keySet,
		logger:    zap.L(),
	}
	// update node & create proxy agent
	proxyManager.update()
	a := proxyManager.GetAgent(ips[1])
	agent1, _ := a.(*NodeProxyAgent)
	fakeNode := factory.nodes[ips[1]]

	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	ctx := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: ips[0],
		Id:       1,
	}
	client := &FakeClient{
		ctx: &ctx,
	}
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     1,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	joiner := make(map[uint32]*protocol.Joiner)
	joiner[1] = &protocol.Joiner{
		AvatarUrl:                "avaurl",
		AvatarBackgroundImageUrl: "bgUrl",
		AvatarForegroundImageUrl: "frUrl",
	}
	feedInfo := &protocol.FeedInfo{
		Feed:    feed,
		Joiners: joiner,
	}
	time.Sleep(DEFAULT_WAIT_TIME)
	l := len(fakeNode.request)
	require.Equal(t, 1, l)
	require.Equal(t, protocol.InternodeCommand_SUBSCRIBE, fakeNode.request[l-1].Method)
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 1,
			Subscribe: &protocol.SubscribeResult{
				Info: feedInfo,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Subscribe == nil || r.Reply.Error != nil {
		t.Errorf("did not receive reply")
	}
	consumeOut(client)
	// receive feed error
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 2,
			Push: &protocol.Push{
				Feed: feed,
				Type: protocol.Push_FEED_ERROR,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_FEED_ERROR {
		t.Errorf("did not receive push ")
	}
	consumeOut(client)
	ctx2 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: ips[0],
		Id:       2,
	}
	client2 := &FakeClient{
		ctx: &ctx2,
	}
	// client2 subscribe the same feed
	agent1.SendFeedRequest(&feedReq{
		ctx:     client2,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     3,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	require.Equal(t, 0, len(client2.outQueue), "should not receive reply")
	l = len(fakeNode.request)
	require.Equal(t, protocol.InternodeCommand_SUBSCRIBE, fakeNode.request[l-1].Method)
	require.Equal(t, 2, l)
}

func TestLocalProxyDisconnect(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log", zapcore.DebugLevel)
	//zap.ReplaceGlobals(omlogger.Logger)
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	ips := setupNodes(nodeStatDb, 2)
	keySet, err := utils.ReadKeySet("../megaphone.sec", "../megaphone.pub", "../megaphone.hmac")
	if err != nil {
		t.Errorf("fail to load keyset err:%v", err)
	}
	factory := &FakeNodeConnectorFactory{
		nodes: make(map[string]*FakeNodeConnector),
	}
	proxyManager := &ProxyManagerImpl{
		statDb:    nodeStatDb,
		factory:   factory,
		PrivateIP: ips[0],
		keySet:    keySet,
		logger:    zap.L(),
	}
	// update node & create proxy agent
	proxyManager.update()
	a := proxyManager.GetAgent(ips[1])
	agent1, _ := a.(*NodeProxyAgent)
	fakeNode := factory.nodes[ips[1]]
	// subscribe feed
	feed := &protocol.Feed{
		Kind:    "k",
		Account: "account",
		Key:     []byte("key"),
	}
	ctx := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: ips[0],
		Id:       1,
	}
	client := &FakeClient{
		ctx: &ctx,
	}
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     1,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	joiner := make(map[uint32]*protocol.Joiner)
	joiner[1] = &protocol.Joiner{
		AvatarUrl:                "avaurl",
		AvatarBackgroundImageUrl: "bgUrl",
		AvatarForegroundImageUrl: "frUrl",
	}
	feedInfo := &protocol.FeedInfo{
		Feed:    feed,
		Joiners: joiner,
	}
	time.Sleep(DEFAULT_WAIT_TIME)
	// Now success subscribe
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 1,
			Subscribe: &protocol.SubscribeResult{
				Info: feedInfo,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	require.Equal(t, 1, len(client.outQueue))
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Error != nil || r.Reply.Subscribe == nil {
		t.Errorf("did not receive  reply")
	}
	consumeOut(client)
	// Now proxy disconnect
	fakeNode.SetDisconnect(true)
	// new subscribe to trigger retry
	ctx2 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: ips[0],
		Id:       2,
	}
	client2 := &FakeClient{
		ctx: &ctx2,
	}
	feed2 := &protocol.Feed{
		Kind:    "test",
		Account: "account2",
		Key:     []byte("key"),
	}
	// client2 subscribe the other feed
	agent1.SendFeedRequest(&feedReq{
		ctx:     client2,
		feedKey: GetFeedKey(feed2),
		cmd: &protocol.Command{
			Id:     2,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed2,
		},
	})
	// client should receive feed error
	time.Sleep(DEFAULT_WAIT_TIME)
	require.Equal(t, 1, len(client.outQueue))
	if r := client.outQueue[0]; r.Type != Push || r.Push.Type != protocol.Push_FEED_ERROR {
		t.Errorf("did not receive push")
	}
	consumeOut(client)
	require.Equal(t, 0, len(client.outQueue))
	require.Equal(t, 0, len(agent1.cacheFeedInfo))
	// client 1 resubscribe again
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     3,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	require.Equal(t, 2, len(client.outQueue))
	// One for feed error push & reply error
	if r := client.outQueue[0]; r.Type == Reply {
		if r.Reply.Error == nil || r.Reply.Error.Code != protocol.Error_PROXY_ERROR {
			t.Errorf("did not receive sub reply %v\n", r)
		}
		if r2 := client.outQueue[1]; r2.Type != Push || r2.Push == nil || r2.Push.Type != protocol.Push_FEED_ERROR {
			t.Errorf("did not receive disconnect push %v\n", r2)
		}
	} else {
		// receive push first
		if r.Push.Type != protocol.Push_FEED_ERROR {
			t.Errorf("did not receive disconnect push %v\n", r)
		}
		if r2 := client.outQueue[1]; r2.Reply.Error == nil || r2.Reply.Error.Code != protocol.Error_PROXY_ERROR {
			t.Errorf("did not receive sub reply %v\n", r2)
		}
	}
	consumeOut(client)
	consumeOut(client)

	// Now proxy reconnect
	fakeNode.SetDisconnect(false)
	fakeNode.request = fakeNode.request[:0]
	agent1.SendFeedRequest(&feedReq{
		ctx:     client,
		feedKey: GetFeedKey(feed),
		cmd: &protocol.Command{
			Id:     4,
			Method: protocol.Command_SUBSCRIBE,
			Feed:   feed,
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	require.Equal(t, 0, len(client.outQueue))
	// Make sure it send internode request
	l := len(fakeNode.request)
	require.Equal(t, 1, l)
	require.Equal(t, protocol.InternodeCommand_SUBSCRIBE, fakeNode.request[l-1].Method)
	agent1.receiveData(&proxyReply{
		reply: &protocol.InternodeReply{
			Id: 4,
			Subscribe: &protocol.SubscribeResult{
				Info: feedInfo,
			},
		},
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if r := client.outQueue[0]; r.Type != Reply || r.Reply.Error != nil || r.Reply.Subscribe == nil {
		t.Errorf("did not receive reply")
	}
	consumeOut(client)
}
