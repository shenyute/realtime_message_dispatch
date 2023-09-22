package core

import "github.com/shenyute/realtime_message_dispatch/protocol"

var InternodeTimeoutError = &protocol.Error{
	Code:      protocol.Error_INTERNODE_TIMEOUT,
	Temporary: true,
}

var InternodeProxyError = &protocol.Error{
	Code:      protocol.Error_PROXY_ERROR,
	Temporary: true,
}

var NotJoinFeedError = &protocol.Error{
	Code:      protocol.Error_NOT_JOIN_FEED,
	Temporary: true,
}

var NotSubscribeFeedError = &protocol.Error{
	Code: protocol.Error_NOT_SUBSCRIBED_FEED,
}

var UnSupportedMethod = &protocol.Error{
	Code: protocol.Error_NOT_SUPPORTED_YET,
}

var UnAuthorized = &protocol.Error{
	Code: protocol.Error_UNAUTHORIZED,
}
