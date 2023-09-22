package core

import (
	"github.com/shenyute/realtime_message_dispatch/protocol"
	"sync"
)

type FeedInfoDbMode int

const (
	InMemory = iota
	Dynamo
)

type FeedInfoDb interface {
	GetFeedInfo(f *protocol.Feed) (*FeedInfoItem, error)
	UpdateFeedInfo(f *protocol.Feed, info *FeedInfoItem) error
	DeleteFeedInfo(f *protocol.Feed) error
}

type FeedInfoItem struct {
	FeedKey                  string `json:"fk,omitempty"`
	TypeId                   string `json:"ti,omitempty"`
	NodeId                   string `json:"ni,omitempty"`
	Host                     string `json:"h,omitempty"`
	HostPublicIP             string `json:"hpi,omitempty"`
	HostPrivateIP            string `json:"hpp,omitempty"`
	Ttl                      int64  `json:"tl,omitempty"`
	AvatarBackgroundImageUrl string `json:"ab,omitempty"`
	AvatarForegroundImageUrl string `json:"af,omitempty"`
	// Not used now.
	Joiners map[string]*protocol.Joiner `json:"js,omitempty"`
}

func (f *FeedInfoItem) GetTarget() string {
	return f.NodeId
}

type InMemoryFeedInfoDb struct {
	records map[string]*FeedInfoItem
	mutex   sync.RWMutex
}

func (db *InMemoryFeedInfoDb) GetFeedInfo(f *protocol.Feed) (*FeedInfoItem, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	return db.records[GetFeedKey(f)], nil
}

func (db *InMemoryFeedInfoDb) UpdateFeedInfo(f *protocol.Feed, info *FeedInfoItem) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.records[GetFeedKey(f)] = info
	return nil
}

func (db *InMemoryFeedInfoDb) DeleteFeedInfo(f *protocol.Feed) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	delete(db.records, GetFeedKey(f))
	return nil
}

func NewFeedInfoDb(mode FeedInfoDbMode, region, tableName string) FeedInfoDb {
	if InMemory == mode {
		return &InMemoryFeedInfoDb{
			records: make(map[string]*FeedInfoItem),
		}
	} else if Dynamo == mode {
		//if db, err := CreateDynamoFeedInfoDb(region, tableName); err != nil {
		//	return nil
		//} else {
		//	return db
		//}
	}
	panic("Not supported yet")
}
