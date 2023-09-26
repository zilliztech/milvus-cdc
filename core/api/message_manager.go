package api

type MessageManager interface {
	ReplicateMessage(message *ReplicateMessage)
	Close(channelName string)
}

type ReplicateMessage struct {
	Param       *ReplicateMessageParam
	SuccessFunc func(param *ReplicateMessageParam)
	FailFunc    func(param *ReplicateMessageParam, err error)
}
