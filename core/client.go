package core

import (
	"errors"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/websocket"
	"github.com/shenyute/realtime_message_dispatch/base"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"google.golang.org/protobuf/proto"
)

/*
A client can only join or subscribe one feed.
An account can have multiple devices and each device represents as a client.
*/
type Client struct {
	id       uint32
	login    bool
	feed     *protocol.Feed
	Account  string
	Version  string
	DeviceId string
	KeySet   *base.KeySet
	conn     *websocket.Conn
}

func NewWebSocketClient(account, version, deviceId string, keySet *base.KeySet) *Client {
	return &Client{
		Account:  account,
		KeySet:   keySet,
		Version:  version,
		DeviceId: deviceId,
	}
}

func createJWT(account, version, deviceId string, isMod bool, hmacSecret []byte) (string, error) {
	claims := jwt.MapClaims{
		"sub":     account,
		"version": version,
		"device":  deviceId,
	}
	if isMod {
		claims["mod"] = 1 // All that matters is that the claim exists
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(hmacSecret)
}

func (c *Client) Login(url string) error {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return err
	}
	c.conn = conn
	token, err := createJWT(c.Account, c.Version, c.DeviceId, false, c.KeySet.HMACSecret[:])
	id := c.id
	req := &protocol.Command{
		Method: protocol.Command_CONNECT,
		Id:     id,
		Connect: &protocol.ConnectRequest{
			Token: token,
		},
	}
	c.id += 1
	payload, err := proto.Marshal(req)
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return err
	}
	_, p, _ := c.conn.ReadMessage()
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err
	}
	if reply.Error != nil {
		return errors.New(reply.Error.Message)
	}
	if reply.Id != id {
		return errors.New("Out of order id")
	}
	c.login = true
	return nil
}

func (c *Client) Leave() error {
	if !c.login {
		return errors.New("UNAUTHORIZED")
	}

	id := c.id
	req := &protocol.Command{
		Method: protocol.Command_LEAVE,
		Id:     id,
		Feed:   c.feed,
		Join:   &protocol.JoinRequest{},
	}
	c.id += 1
	payload, err := proto.Marshal(req)
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return err
	}

	_, p, _ := c.conn.ReadMessage()
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err
	}
	if reply.Error != nil {
		return errors.New(reply.Error.Message)
	}
	if reply.Id != id {
		return errors.New(fmt.Sprintf("Out of order id %d %d", reply.Id, id))
	}
	c.feed = nil
	return nil
}

func (c *Client) Join(feed *protocol.Feed) error {
	if !c.login {
		return errors.New("UNAUTHORIZED")
	}

	id := c.id
	req := &protocol.Command{
		Method: protocol.Command_JOIN,
		Id:     id,
		Feed:   feed,
		Join:   &protocol.JoinRequest{},
	}
	c.id += 1
	payload, err := proto.Marshal(req)
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return err
	}

	_, p, _ := c.conn.ReadMessage()
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err
	}
	if reply.Error != nil {
		return errors.New(reply.Error.Message)
	}
	if reply.Id != id {
		return errors.New(fmt.Sprintf("Out of order id %d %d", reply.Id, id))
	}
	c.feed = feed
	return nil
}

func (c *Client) Publish(data []byte) (error, bool) {
	if c.feed == nil {
		return errors.New("Not join feed yet"), true
	}

	id := c.id
	var datas [][]byte
	datas = append(datas, data)
	req := &protocol.Command{
		Method: protocol.Command_PUBLISH,
		Id:     id,
		Feed:   c.feed,
		Publish: &protocol.PublishRequest{
			UnityData: datas,
		},
	}
	c.id += 1
	payload, err := proto.Marshal(req)
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return err, true
	}

	_, p, err := c.conn.ReadMessage()
	if err != nil {
		return err, true
	}
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err, false
	}
	if reply.Error != nil {
		return errors.New(reply.Error.Message), false
	}
	if reply.Id != id {
		return errors.New("Out of order id"), false
	}
	return nil, false
}

func (c *Client) Subscribe(feed *protocol.Feed) (error, bool) {
	if !c.login {
		return errors.New("UNAUTHORIZED"), true
	}

	id := c.id
	req := &protocol.Command{
		Method:    protocol.Command_SUBSCRIBE,
		Id:        id,
		Feed:      feed,
		Subscribe: &protocol.SubscribeRequest{},
	}
	c.id += 1
	payload, err := proto.Marshal(req)
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return err, true
	}

	_, p, _ := c.conn.ReadMessage()
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err, true
	}
	if reply.Error != nil {
		return errors.New(reply.Error.Code.String()), false
	}
	if reply.Id != id {
		return errors.New("Out of order id"), false
	}
	if reply.Subscribe == nil || reply.Subscribe.Info == nil {
		return errors.New("Do not have feedInfoItem"), false
	}
	if reply.Subscribe.Info.Joiners == nil || len(reply.Subscribe.Info.Joiners) < 1 {
		return errors.New("Do not have joiner"), false
	}
	c.feed = feed
	return nil, false
}

func (c *Client) Unsubscribe(skipRead bool) error {
	if !c.login {
		return errors.New("UNAUTHORIZED")
	}
	if c.feed == nil {
		return errors.New("No subscribed feed")
	}

	id := c.id
	req := &protocol.Command{
		Method: protocol.Command_UNSUBSCRIBE,
		Id:     id,
		Feed:   c.feed,
	}
	c.id += 1
	payload, err := proto.Marshal(req)
	err = c.conn.WriteMessage(websocket.BinaryMessage, payload)
	if err != nil {
		return err
	}

	c.feed = nil

	if skipRead {
		return nil
	}

	_, p, _ := c.conn.ReadMessage()
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return err
	}
	if reply.Error != nil {
		return errors.New(reply.Error.Message)
	}
	if reply.Id != id {
		return errors.New("Out of order id")
	}
	return nil
}

func (c *Client) ReadMessage() (*protocol.Reply, error) {
	_, p, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func (c *Client) ReceivePush() (*protocol.Push, error) {
	_, p, err := c.conn.ReadMessage()
	if err != nil {
		return nil, NewIOError(err)
	}

	reply := protocol.Reply{}
	err = proto.Unmarshal(p, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Push, nil
}

func (c *Client) GetJoinedFeed() protocol.Feed {
	if c.feed == nil {
		return protocol.Feed{}
	}
	return *c.feed
}
