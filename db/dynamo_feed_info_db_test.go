package db

import (
	"github.com/shenyute/realtime_message_dispatch/core"
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"testing"
	"time"
)

// To run tests in this file, use production AWS keys and set region = ap-northeast-1 in ~/.aws/config
const testRegion = "ap-northeast-1"
const feedInfoTableName = "avatar_feed_info"

func TestFeedDb(t *testing.T) {
	if testing.Short() {
		// This test requires a real DynamoDB.
		t.Skip("skipping testing in short mode")
	}
	joiners := make(map[string]*protocol.Joiner)
	joiners["account"] = &protocol.Joiner{
		AvatarUrl: "avatarurl",
	}
	feed := &protocol.Feed{
		Account: "account",
		Key:     []byte("livestream-123"),
		Kind:    "kind",
	}
	feedInfo := &core.FeedInfoItem{
		FeedKey:                  core.GetFeedKey(feed),
		TypeId:                   "virtualStream",
		NodeId:                   "node1",
		Host:                     "account",
		HostPrivateIP:            "127.0.0.1",
		HostPublicIP:             "1.1.1.1",
		Ttl:                      time.Now().Add(core.DefaultFeedInfoTtl).Unix(),
		AvatarBackgroundImageUrl: "avatarBackgroundUrl",
		AvatarForegroundImageUrl: "avatarFloorUrl",
		Joiners:                  joiners,
	}
	db, err := CreateDynamoFeedInfoDb(testRegion, feedInfoTableName)
	if err != nil {
		t.Errorf("Fail to connect to db %v", err)
	}
	var info *core.FeedInfoItem
	info, err = db.GetFeedInfo(feed)
	if err != nil {
		t.Errorf("Fail to get from db %v", err)
	}
	if info != nil {
		t.Errorf("Expect no value yet %v", err)
	}
	err = db.UpdateFeedInfo(feed, feedInfo)
	if err != nil {
		t.Errorf("Fail to update db %v", err)
	}

	info, err = db.GetFeedInfo(feed)
	if err != nil {
		t.Errorf("Fail to get from db %v", err)
	}
	if info.AvatarForegroundImageUrl != feedInfo.AvatarForegroundImageUrl ||
		info.AvatarBackgroundImageUrl != feedInfo.AvatarBackgroundImageUrl ||
		len(info.Joiners) != 1 {
		t.Errorf("Fail to get from db %v", err)
	}
	if info.Joiners["account"].AvatarUrl != feedInfo.Joiners["account"].AvatarUrl {
		t.Errorf("Fail to get from db %v", err)
	}

	err = db.DeleteFeedInfo(feed)
	if err != nil {
		t.Errorf("Fail to delete from db %v", err)
	}
	info, err = db.GetFeedInfo(feed)
	if err != nil {
		t.Errorf("Fail to get from db %v", err)
	}
	if info != nil {
		t.Errorf("Expect no value after the deletion %v", err)
	}
}
