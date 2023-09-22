package core

import "sync"

type NodeStatus string

const (
	Running     = "running"
	WaitingDown = "waiting_down"
	Down        = "down"
)

type NodeStatInfo struct {
	NodeId    string     `json:"ni,omitempty"`
	Status    NodeStatus `json:"s,omitempty"`
	IP        string     `json:"i,omitempty"`
	PrivateIP string     `json:"pi,omitempty"`
	IPv6      string     `json:"pip,omitempty"`
	Port      string     `json:"p,omitempty"`
	Cluster   string     `json:"c,omitempty"`
	Ttl       int64      `json:"tl,omitempty"`
	Cpu       float64    `json:"c,omitempty"`
}

func (n *NodeStatInfo) getProxyUrl() string {
	return "ws://" + n.PrivateIP + n.Port + "/proxy"
}

type NodeStatDb interface {
	Init()
	GetNodeInfo(target string) (*NodeStatInfo, error)
	ListNodeInfos() ([]*NodeStatInfo, error)
	UpdateNodeInfo(nodeId string, info *NodeStatInfo) error
	DeleteNodeInfo(nodeId string) error
}

type InMemoryNodeStatDb struct {
	nodes map[string]*NodeStatInfo
	mutex sync.RWMutex
}

func (f *InMemoryNodeStatDb) Init() {
}

func (f *InMemoryNodeStatDb) GetNodeInfo(target string) (*NodeStatInfo, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	return f.nodes[target], nil
}

func (f *InMemoryNodeStatDb) ListNodeInfos() ([]*NodeStatInfo, error) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	var result []*NodeStatInfo
	for _, info := range f.nodes {
		result = append(result, info)
	}
	return result, nil
}

func (f *InMemoryNodeStatDb) UpdateNodeInfo(target string, info *NodeStatInfo) error {
	f.mutex.Lock()
	f.nodes[target] = info
	defer f.mutex.Unlock()
	return nil
}

func (f *InMemoryNodeStatDb) DeleteNodeInfo(target string) error {
	f.mutex.Lock()
	delete(f.nodes, target)
	defer f.mutex.Unlock()
	return nil
}

func NewFakeNodeStatDb() NodeStatDb {
	nodeStatDb := &InMemoryNodeStatDb{
		nodes: make(map[string]*NodeStatInfo),
	}
	return nodeStatDb
}
