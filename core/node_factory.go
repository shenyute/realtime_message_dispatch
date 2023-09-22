package core

import (
	"github.com/shenyute/realtime_message_dispatch/base"
	"go.uber.org/zap"
)

var nodeKeySet *base.KeySet

func SetNodeKeySet(k *base.KeySet) {
	nodeKeySet = k
}

type NodeConnectorFactory interface {
	CreateConnector(info *NodeStatInfo) NodeConnector
}

type NodeConnectorFactoryImpl struct {
	publicIP string
	localIP  string
	logger   *zap.Logger
}

func (f *NodeConnectorFactoryImpl) CreateConnector(info *NodeStatInfo) NodeConnector {
	if nodeKeySet == nil {
		panic("Not setup node keyset")
	}
	node := &NodeConnectorImpl{
		keySet: nodeKeySet,
	}
	url := info.getProxyUrl()
	err := node.Init(url, info.NodeId)
	if err != nil {
		f.logger.Error("fail to connect", zap.Error(err),
			zap.String("ip", f.publicIP),
			zap.String("privateIp", f.localIP),
			zap.String("url", url))
	}
	return node
}

func CreateNodeConnectorFactory(publicIP, localIP string) NodeConnectorFactory {
	return &NodeConnectorFactoryImpl{
		publicIP: publicIP,
		localIP:  localIP,
		logger:   zap.L(),
	}
}
