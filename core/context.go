package core

import (
	"go.uber.org/zap"
)

type ClientId uint32

type ClientContext struct {
	Version     string
	Account     string
	DeviceId    string
	RemoteIP    string
	ReadOnly    bool
	Feed        string
	Logger      *zap.Logger
	Id          ClientId
	IsModerator bool
	IsBot       bool
}
