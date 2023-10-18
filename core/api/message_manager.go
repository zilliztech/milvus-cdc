package api

import "github.com/milvus-io/milvus/pkg/log"

type MessageManager interface {
	ReplicateMessage(message *ReplicateMessage)
	Close(channelName string)
}

type ReplicateMessage struct {
	Param       *ReplicateMessageParam
	SuccessFunc func(param *ReplicateMessageParam)
	FailFunc    func(param *ReplicateMessageParam, err error)
}

type DefaultMessageManager struct{}

var _ MessageManager = (*DefaultMessageManager)(nil)

func (d *DefaultMessageManager) ReplicateMessage(message *ReplicateMessage) {
	log.Warn("ReplicateMessage is not implemented, please check it")
}

func (d *DefaultMessageManager) Close(channelName string) {
	log.Warn("Close is not implemented, please check it")
}
