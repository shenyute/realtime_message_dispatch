package core

import (
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

type FakeClient struct {
	ctx      *ClientContext
	closed   bool
	outQueue []*ClientMessage
	logger   *zap.Logger
}

func (c *FakeClient) GetId() ClientId {
	return c.ctx.Id
}

func (c *FakeClient) BroadcastData(message *ClientMessage) error {
	return c.WriteMessage(message)
}

func (c *FakeClient) WriteMessage(message *ClientMessage) error {
	if c.closed {
		return ClientClosedError
	}
	if c.logger != nil {
		c.logger.Debug("write message", zap.Uint32("clientId", uint32(c.GetId())), zap.Any("message", message))
	}
	c.outQueue = append(c.outQueue, message)
	return nil
}

func (c *FakeClient) Process() {
}

func (c *FakeClient) Close() {
	c.closed = true
}

func (c *FakeClient) getContext() *ClientContext {
	return c.ctx
}

type FakeProxyManager struct {
	requests []*feedReq
}

func (f *FakeProxyManager) GetNodeStats(target string) *NodeStatInfo {
	//TODO implement me
	panic("implement me")
}

func (f *FakeProxyManager) ListNodeStats() []*NodeStatInfo {
	//TODO implement me
	panic("implement me")
}

func (f *FakeProxyManager) SendFeedRequest(req *feedReq) error {
	switch req.cmd.Method {
	case protocol.Command_SUBSCRIBE:
		reply := &protocol.Reply{
			Id: req.cmd.Id,
		}
		req.ctx.WriteMessage(&ClientMessage{
			Type:     Reply,
			Duration: time.Now().Sub(req.startTime),
			Reply:    reply,
		})
		f.requests = append(f.requests, req)
	case protocol.Command_UNSUBSCRIBE:
		reply := &protocol.Reply{
			Id: req.cmd.Id,
		}
		req.ctx.WriteMessage(&ClientMessage{
			Type:     Reply,
			Duration: time.Now().Sub(req.startTime),
			Reply:    reply,
		})
		f.requests = append(f.requests, req)
	default:
		panic("Unsupported")
	}
	return nil
}

func (f *FakeProxyManager) Close() error {
	return nil
}

func (f *FakeProxyManager) ReceiveData() {
}

func (f *FakeProxyManager) Init(factory NodeConnectorFactory) {
}

func (f *FakeProxyManager) GetAgent(target string) ProxyAgent {
	return f
}

func newLogin(client ClientAgent, impl FeedManager) {
	cmd1 := &protocol.Command{
		Method: protocol.Command_CONNECT,
	}
	impl.HandleCommand(client, cmd1)
	time.Sleep(DEFAULT_WAIT_TIME)
}
func consumeOut(client *FakeClient) {
	client.outQueue = client.outQueue[1:]
}

func TestFeedKeySerialization(t *testing.T) {
	feed := &protocol.Feed{
		Account: "account",
		Key:     []byte("some-key"),
		Kind:    "some-kind",
	}
	key := GetFeedKey(feed)
	actual := getFeedFromFeedKey(key)
	if !proto.Equal(feed, actual) {
		t.Errorf("doesn't match")
	}
}

func TestLocalFeedResubscribe(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log")
	//zap.ReplaceGlobals(omlogger.Logger)
	proxyManager := &FakeProxyManager{}
	ip1 := "127.0.0.1"
	n := &NodeStatInfo{
		NodeId:    "node1",
		IP:        "192.168.1.1",
		PrivateIP: ip1,
	}
	feedInfoDb := NewFeedInfoDb(InMemory, "us-west-2", "feed_info")
	feedManager := CreateFeedManager(feedInfoDb, proxyManager, n, zap.L())
	feedManager.Init()
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
	newLogin(client, feedManager)
	count := feedManager.GetConnectedUserCount()
	require.Equal(t, 1, count)
	require.Equal(t, 1, len(client.outQueue))
	consumeOut(client)

	feed := &protocol.Feed{
		Kind:    "k",
		Account: ctx.Account,
		Key:     []byte("key"),
	}
	feedInfoDb.UpdateFeedInfo(feed, &FeedInfoItem{
		FeedKey: GetFeedKey(feed),
		NodeId:  n.NodeId,
	})
	cmd := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client, cmd, "join")
	c := feedManager.GetRoomCount(feed)
	require.Equal(t, 1, c)

	// leave
	cmd = &protocol.Command{
		Method: protocol.Command_LEAVE,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client, cmd, "leave")
	c = feedManager.GetRoomCount(feed)
	require.Equal(t, 0, c)

	feedInfoDb.UpdateFeedInfo(feed, &FeedInfoItem{
		FeedKey: GetFeedKey(feed),
		NodeId:  n.NodeId,
	})
	// rejoin again
	cmd = &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client, cmd, "rejoin")
	c = feedManager.GetRoomCount(feed)
	require.Equal(t, 1, c)

	cmd = &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client, cmd, "subscribe")
	c = feedManager.GetRoomCount(feed)
	require.Equal(t, 1, c)
}

func TestLocalFeed(t *testing.T) {
	t.Parallel()

	proxyManager := &FakeProxyManager{}
	ip1 := "127.0.0.1"
	n := &NodeStatInfo{
		NodeId:    "node1",
		IP:        "192.168.1.1",
		PrivateIP: ip1,
	}
	feedInfoDb := NewFeedInfoDb(InMemory, "us-west-2", "feed_info")
	feedManager := CreateFeedManager(feedInfoDb, proxyManager, n, zap.L())
	feedManager.Init()
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
	newLogin(client, feedManager)
	count := feedManager.GetConnectedUserCount()
	require.Equal(t, 1, count)
	require.Equal(t, 1, len(client.outQueue))
	consumeOut(client)

	feed := &protocol.Feed{
		Kind:    "k",
		Account: ctx.Account,
		Key:     []byte("key"),
	}
	feedInfoDb.UpdateFeedInfo(feed, &FeedInfoItem{
		FeedKey: GetFeedKey(feed),
		NodeId:  n.NodeId,
	})
	cmd := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client, cmd, "join")
	c := feedManager.GetRoomCount(feed)
	require.Equal(t, 1, c)

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

	newLogin(client2, feedManager)
	consumeOut(client2)

	cmd3 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd3)
	time.Sleep(DEFAULT_WAIT_TIME)
	r := client2.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("Subscribe not success err:%v", r.Reply.Error)
	}
	if c := feedManager.GetConnectedUserCount(); c != 2 {
		t.Errorf("connect user wrong")
	}

	// stub subscribe
	node := &FakeNodeReceiver{
		ctx: &ClientContext{
			Version:  "0.1",
			Account:  System,
			DeviceId: "other",
			RemoteIP: "127.0.0.1",
			Id:       3,
		},
	}
	handler := InternodeCommandHandler{
		feedManager: feedManager,
	}
	stub1 := NewStubClient(node, &handler)
	stub1.OnInit()
	newLogin(stub1.GetClientAgent(), feedManager)
	if node.out[0].Error != nil {
		t.Errorf("Fail to join for stub")
	}
	stub1.OnSendCommand(&protocol.InternodeCommand{
		Id:     2,
		Method: protocol.InternodeCommand_SUBSCRIBE,
		Feed:   feed,
	})
	time.Sleep(DEFAULT_WAIT_TIME)
	if node.out[1].Id != 2 || node.out[1].Error != nil {
		t.Errorf("Fail to subscribe for stub")
	}

	var data [][]byte
	data = append(data, []byte("hello1"))
	cmd4 := &protocol.Command{
		Method: protocol.Command_PUBLISH,
		Feed:   feed,
		Publish: &protocol.PublishRequest{
			UnityData: data,
		},
	}
	feedManager.HandleCommand(client, cmd4)
	time.Sleep(DEFAULT_WAIT_TIME)
	if Push != client2.outQueue[1].Type {
		t.Errorf("Not receive broadcast type:%d", client2.outQueue[1].Type)
	}

	if node.out[2].Push == nil {
		t.Errorf("Fail to receive broadcast for stub")
	}

	cmd5 := &protocol.Command{
		Method: protocol.Command_UNSUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd5)
	time.Sleep(DEFAULT_WAIT_TIME)

	if Reply != client2.outQueue[2].Type || client2.outQueue[2].Reply.Error != nil {
		t.Errorf("Not receive broadcast type:%d", client2.outQueue[2].Type)
	}

	// publish another
	cmd6 := &protocol.Command{
		Method: protocol.Command_PUBLISH,
		Feed:   feed,
		Publish: &protocol.PublishRequest{
			UnityData: data,
		},
	}

	feedManager.HandleCommand(client, cmd6)
	time.Sleep(DEFAULT_WAIT_TIME)
	if len(client2.outQueue) > 3 {
		t.Errorf("Should not receive broadcast type:%d", client2.outQueue[3].Type)
	}

	if node.out[3].Push == nil {
		t.Errorf("Fail to receive broadcast for stub")
	}
	cmd7 := &protocol.Command{
		Method: protocol.Command_DISCONNECT,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd7)
	time.Sleep(DEFAULT_WAIT_TIME)

	c = feedManager.GetRoomCount(feed)
	if c != 1 {
		t.Errorf("room count not correct %d", c)
	}
	c = feedManager.GetConnectedUserCount()
	if c != 2 {
		t.Errorf("connected user count not correct %d", c)
	}
}

func TestRemoteFeed(t *testing.T) {
	t.Parallel()

	proxyManager := &FakeProxyManager{}
	feedInfoDb := NewFeedInfoDb(InMemory, "us-west-2", "feed_info")
	ip1 := "127.0.0.1"
	node := NodeStatInfo{
		NodeId:    "node1",
		IP:        "192.168.1.1",
		PrivateIP: ip1,
	}
	feedManager := CreateFeedManager(feedInfoDb, proxyManager, &node, zap.L())
	feedManager.Init()
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
	newLogin(client, feedManager)
	count := feedManager.GetConnectedUserCount()
	if count != 1 {
		t.Errorf("connected user count error %d", count)
	}
	if len(client.outQueue) != 1 {
		t.Errorf("Did not receive join success")
	} else {
		consumeOut(client)
	}
	feed := &protocol.Feed{
		Kind:    "k",
		Account: ctx.Account,
		Key:     []byte("key"),
	}
	cmd2 := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	feedInfoDb.UpdateFeedInfo(feed, &FeedInfoItem{
		FeedKey: GetFeedKey(feed),
		NodeId:  node.NodeId,
	})
	feedManager.HandleCommand(client, cmd2)
	time.Sleep(DEFAULT_WAIT_TIME)
	if len(client.outQueue) != 1 {
		t.Errorf("Did not receive join success")
	}
	r := client.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("Join not success err:%v", r.Reply.Error)
	}
	consumeOut(client)
	c := feedManager.GetRoomCount(feed)
	if c != 1 {
		t.Errorf("room count not correct %d", c)
	}

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

	newLogin(client2, feedManager)
	consumeOut(client2)

	cmd3 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd3)
	time.Sleep(DEFAULT_WAIT_TIME)
	r = client2.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("Subscribe not success err:%v", r.Reply.Error)
	}
	if c := feedManager.GetConnectedUserCount(); c != 2 {
		t.Errorf("connect user wrong")
	}

	var data [][]byte
	data = append(data, []byte("hello1"))
	cmd4 := &protocol.Command{
		Method: protocol.Command_PUBLISH,
		Feed:   feed,
		Publish: &protocol.PublishRequest{
			UnityData: data,
		},
	}

	feedManager.HandleCommand(client, cmd4)
	time.Sleep(DEFAULT_WAIT_TIME)
	if Push != client2.outQueue[1].Type {
		t.Errorf("Not receive broadcast type:%d", client2.outQueue[1].Type)
	}
	ctx3 := ClientContext{
		Version:  "0.1",
		Account:  "account3",
		DeviceId: "deviceId",
		RemoteIP: "127.0.0.1",
		Id:       3,
	}
	client3 := &FakeClient{
		ctx: &ctx3,
	}
	newLogin(client3, feedManager)
	consumeOut(client3)
	feed2 := &protocol.Feed{
		Kind:    "k2",
		Account: ctx3.Account,
		Key:     []byte("key"),
	}
	// client3 subscribe non-exist feed
	cmd6 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed2,
	}
	feedManager.HandleCommand(client3, cmd6)
	time.Sleep(DEFAULT_WAIT_TIME)
	if client3.outQueue[0].Type != Reply || client3.outQueue[0].Reply.Error.Code != protocol.Error_FEED_NOT_EXIST {
		t.Errorf("Should return error for non-exist feed")
	}
	consumeOut(client3)
	// insert new feed to db
	ip2 := "127.0.0.2"
	feedInfoDb.UpdateFeedInfo(feed2, &FeedInfoItem{
		Host: ip2,
	})

	feedManager.HandleCommand(client3, cmd6)
	time.Sleep(DEFAULT_WAIT_TIME)
	if len(proxyManager.requests) != 1 || proxyManager.requests[0].ctx != client3 {
		t.Errorf("Did not send subscribe request")
	}
	if client3.outQueue[0].Type != Reply || client3.outQueue[0].Reply.Error != nil {
		t.Errorf("Should subscribe success")
	}
}

func handleAndVerifyCommand(t *testing.T, feedManager FeedManager, client *FakeClient, cmd *protocol.Command, errorMessage string) {
	feedManager.HandleCommand(client, cmd)
	time.Sleep(DEFAULT_WAIT_TIME)
	require.Equal(t, 1, len(client.outQueue), errorMessage)
	r := client.outQueue[0]
	require.Equal(t, Reply, r.Type, errorMessage)
	if r.Reply.Error != nil {
		t.Errorf("%s err:%v", errorMessage, r.Reply.Error)
	}
	consumeOut(client)
}

// test scenario
// user A (tcp1-> join) ==> not leave
// user A (tcp2 -> sub) ==> not unsubscribe
// user B (tcp3-> join)
// user B (tcp4 -> sub) ==> 此時 subscribe reply feed info 包含 user A 與 user B
// user A (tcp5-> join)
// user A (tcp6 -> sub)
// =====
// User B should receive push
func TestMultiSubLocal(t *testing.T) {
	//omlogger := utils.InitLogger("./log/avatar.log", zapcore.DebugLevel)
	//zap.ReplaceGlobals(omlogger.Logger)
	t.Parallel()

	proxyManager := &FakeProxyManager{}
	ip1 := "127.0.0.1"
	n := &NodeStatInfo{
		NodeId:    "node1",
		IP:        "192.168.1.1",
		PrivateIP: ip1,
	}
	feedInfoDb := NewFeedInfoDb(InMemory, "us-west-2", "feed_info")
	feedManager := CreateFeedManager(feedInfoDb, proxyManager, n, zap.L())
	feedManager.Init()
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
	newLogin(client, feedManager)
	count := feedManager.GetConnectedUserCount()
	require.Equal(t, 1, count)
	require.Equal(t, 1, len(client.outQueue))
	consumeOut(client)

	feed := &protocol.Feed{
		Kind:    "k",
		Account: ctx.Account,
		Key:     []byte("key"),
	}
	feedInfoDb.UpdateFeedInfo(feed, &FeedInfoItem{
		FeedKey: GetFeedKey(feed),
		NodeId:  n.NodeId,
	})
	// account 1 join
	cmd := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client, cmd, "join")
	c := feedManager.GetRoomCount(feed)
	require.Equal(t, 1, c)

	ctx12 := ClientContext{
		Version:  "0.1",
		Account:  "account",
		DeviceId: "deviceId",
		RemoteIP: "127.0.0.1",
		Id:       2,
	}
	client12 := &FakeClient{
		ctx: &ctx12,
	}

	newLogin(client12, feedManager)
	consumeOut(client12)
	require.Equal(t, 2, feedManager.GetConnectedUserCount())

	// account 1 subscribe
	cmd3 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client12, cmd3)
	time.Sleep(DEFAULT_WAIT_TIME)
	r := client12.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("Subscribe not success err:%v", r.Reply.Error)
	}

	// account 2 join
	ctx4 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: "127.0.0.1",
		Id:       3,
	}
	client4 := &FakeClient{
		ctx: &ctx4,
	}
	newLogin(client4, feedManager)
	consumeOut(client4)
	require.Equal(t, 3, feedManager.GetConnectedUserCount())
	cmd5 := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	handleAndVerifyCommand(t, feedManager, client4, cmd5, "join")

	// account 2 subscribe
	ctx2 := ClientContext{
		Version:  "0.1",
		Account:  "account2",
		DeviceId: "deviceId2",
		RemoteIP: "127.0.0.1",
		Id:       4,
	}
	client2 := &FakeClient{
		ctx: &ctx2,
	}

	newLogin(client2, feedManager)
	consumeOut(client2)
	require.Equal(t, 4, feedManager.GetConnectedUserCount())

	cmd4 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd4)
	time.Sleep(DEFAULT_WAIT_TIME)
	r = client2.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("Subscribe not success err:%v", r.Reply.Error)
	}
	consumeOut(client2)
	require.Equal(t, 0, len(client2.outQueue))

	// account1 join again with same device id
	ctx3 := ClientContext{
		Version:  "0.1",
		Account:  "account1",
		DeviceId: "deviceId",
		RemoteIP: "127.0.0.1",
		Id:       5,
	}
	client13 := &FakeClient{
		ctx: &ctx3,
	}
	newLogin(client13, feedManager)
	consumeOut(client13)
	cmd6 := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
		Join: &protocol.JoinRequest{
			AvatarUrl: "avatar://ooo",
		},
	}
	handleAndVerifyCommand(t, feedManager, client13, cmd6, "join")

	require.Equal(t, 1, len(client2.outQueue))
	// check account2 push
	r = client2.outQueue[0]
	if r.Type != Push || r.Push == nil || r.Push.Join == nil || r.Push.Join.Presence == nil || r.Push.Join.Presence.AvatarUrl != "avatar://ooo" {
		t.Errorf("Did not receive new join push err:%v", r)
	}
}

func TestRemoteFeedResub(t *testing.T) {
	t.Parallel()
	//omlogger := utils.InitLogger("./log/avatar.log", zapcore.DebugLevel)
	//zap.ReplaceGlobals(omlogger.Logger)

	proxyManager := &FakeProxyManager{}
	feedInfoDb := NewFeedInfoDb(InMemory, "us-west-2", "feed_info")
	ip1 := "127.0.0.1"
	node := NodeStatInfo{
		NodeId:    "node1",
		IP:        "192.168.1.1",
		PrivateIP: ip1,
	}
	feedManager := CreateFeedManager(feedInfoDb, proxyManager, &node, zap.L())
	feedManager.Init()
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
	newLogin(client, feedManager)
	if len(client.outQueue) != 1 {
		t.Errorf("Did not receive join success")
	} else {
		consumeOut(client)
	}
	feed := &protocol.Feed{
		Kind:    "test",
		Account: ctx.Account,
		Key:     []byte("key"),
	}
	cmd2 := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	feedManager.HandleCommand(client, cmd2)
	time.Sleep(DEFAULT_WAIT_TIME)
	if len(client.outQueue) != 1 {
		t.Errorf("Did not receive join success")
	}
	r := client.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil {
		t.Errorf("Join not success err:%v", r.Reply.Error)
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

	newLogin(client2, feedManager)
	consumeOut(client2)

	cmd3 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd3)
	time.Sleep(DEFAULT_WAIT_TIME)
	r = client2.outQueue[0]
	if r.Type != Reply || r.Reply.Error != nil || r.Reply.Subscribe == nil {
		t.Errorf("Subscribe not success err:%v", r.Reply.Error)
	}
	consumeOut(client2)
	if c := feedManager.GetConnectedUserCount(); c != 2 {
		t.Errorf("connect user wrong")
	}

	cmd4 := &protocol.Command{
		Method: protocol.Command_LEAVE,
		Feed:   feed,
		Leave:  &protocol.LeaveRequest{},
	}

	feedManager.HandleCommand(client, cmd4)
	time.Sleep(DEFAULT_WAIT_TIME)
	if Push != client2.outQueue[0].Type || client2.outQueue[0].Push.Type != protocol.Push_LEAVE {
		t.Errorf("Not receive brodcast type:%d", client2.outQueue[1].Type)
	}
	consumeOut(client2)
	if Push != client2.outQueue[0].Type || client2.outQueue[0].Push.Type != protocol.Push_FEED_ERROR {
		t.Errorf("Not receive broadcast type:%d", client2.outQueue[1].Type)
	}
	consumeOut(client2)
	cmd5 := &protocol.Command{
		Method: protocol.Command_UNSUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd5)
	time.Sleep(DEFAULT_WAIT_TIME)
	if client2.outQueue[0].Type != Reply || client2.outQueue[0].Reply.Error.Code != protocol.Error_FEED_NOT_EXIST {
		t.Errorf("Did not receive unsubcribe result %v", client2.outQueue[0].Reply)
	}
	consumeOut(client2)
	cmd6 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd6)
	time.Sleep(DEFAULT_WAIT_TIME)
	if client2.outQueue[0].Type != Reply || client2.outQueue[0].Reply.Error == nil {
		t.Errorf("Should subscribe fail")
	}
	consumeOut(client2)
	// join again
	cmd7 := &protocol.Command{
		Method: protocol.Command_JOIN,
		Feed:   feed,
	}
	feedManager.HandleCommand(client, cmd7)
	time.Sleep(DEFAULT_WAIT_TIME)
	if client.outQueue[0].Type != Reply || client.outQueue[0].Reply.Error != nil {
		t.Errorf("Join fail")
	}
	consumeOut(client)

	cmd8 := &protocol.Command{
		Method: protocol.Command_SUBSCRIBE,
		Feed:   feed,
	}
	feedManager.HandleCommand(client2, cmd8)
	time.Sleep(DEFAULT_WAIT_TIME)
	if client2.outQueue[0].Type != Reply || client2.outQueue[0].Reply.Error != nil {
		t.Errorf("subscribe fail %v", client2.outQueue[0].Reply)
	}
	consumeOut(client2)
}
