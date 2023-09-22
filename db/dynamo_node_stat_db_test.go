package db

import (
	"github.com/shenyute/realtime_message_dispatch/core"
	"testing"
	"time"
)

const nodeStatTableName = "avatar_node_stat"

func TestDb(t *testing.T) {
	if testing.Short() {
		// This test requires a real DynamoDB.
		t.Skip("skipping testing in short mode")
	}

	stat := &core.NodeStatInfo{
		NodeId:    "node1",
		Status:    core.Running,
		IP:        "1.1.1.1",
		PrivateIP: "127.0.0.1",
	}
	var stat1 *core.NodeStatInfo
	db, err := CreateDynamoNodeStatDB(testRegion, nodeStatTableName, stat.IP, stat.PrivateIP, stat.IPv6, stat.NodeId, time.Minute, ":8080")
	if err != nil {
		t.Errorf("Fail to connect to db %v", err)
	}
	stat1, err = db.GetNodeInfo(stat.NodeId)
	if stat1 != nil || err != nil {
		t.Errorf("Expect no value %v", err)
	}
	db.UpdateNodeInfo(stat.NodeId, stat)
	stat1, err = db.GetNodeInfo(stat.NodeId)
	if stat1 == nil || err != nil {
		t.Errorf("Fail to get to db %v", err)
	}
	if stat1.IP != stat.IP {
		t.Errorf("Get wrong node stat")
	}
	if stats, err := db.ListNodeInfos(); err != nil {
		t.Errorf("Failed to list nodes %v", err)

	} else {
		if len(stats) != 1 {
			t.Errorf("Wrong number %d", len(stats))
		}
	}
	db.DeleteNodeInfo(stat.NodeId)
	stat1, err = db.GetNodeInfo(stat.NodeId)
	if stat1 != nil || err != nil {
		t.Errorf("Expect no value after deletion %v", err)
	}
}
