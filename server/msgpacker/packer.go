package msgpacker

import (
	"time"

	"github.com/zilliztech/milvus-cdc/core/api"
)

var (
	DefaultTimerInterval = 5000
	DefaultMaxCount      = 10
	DefaultMaxMsgSize    = 512 * 1024      // 1MB
	DefaultMemoryLimit   = 4 * 1024 * 1024 // 4GB
)

type PackerConfig struct {
	TimerInterval int // unit: ms
	MaxCount      int
	MaxMsgSize    int
	MemoryLimit   int // new field for memory limit
}

type Packer struct {
	msgs               []*api.ReplicateMsg
	checkers           []PackerChecker
	memoryProtector    *MemoryProtector
	maxMsgSize         int
	currentMsgPackSize int
}

func NewPacker(packerConfig PackerConfig) *Packer {
	if packerConfig.TimerInterval <= 0 {
		packerConfig.TimerInterval = DefaultTimerInterval
	}
	if packerConfig.MaxCount <= 0 {
		packerConfig.MaxCount = DefaultMaxCount
	}
	if packerConfig.MaxMsgSize <= 0 {
		packerConfig.MaxMsgSize = DefaultMaxMsgSize
	}
	if packerConfig.MemoryLimit <= 0 {
		packerConfig.MemoryLimit = DefaultMemoryLimit
	}
	checkers := []PackerChecker{
		NewTimerChecker(time.Duration(packerConfig.TimerInterval) * time.Millisecond),
		NewMsgCountChecker(packerConfig.MaxCount),
	}
	memoryProtector := GetMemoryChecker(packerConfig.MemoryLimit * 1024)
	return &Packer{
		checkers:        checkers,
		memoryProtector: memoryProtector,
		msgs:            make([]*api.ReplicateMsg, 0),
		maxMsgSize:      packerConfig.MaxMsgSize * 1024,
	}
}

func (p *Packer) Receive(msg *api.ReplicateMsg, handler func([]*api.ReplicateMsg) error) error {
	msgSize := 0
	for _, msg := range msg.MsgPack.Msgs {
		msgSize += msg.Size() // unit: bytes
	}
	p.msgs = append(p.msgs, msg)
	p.currentMsgPackSize += msgSize
	hasHandlerMsg := false

	defer func() {
		if hasHandlerMsg {
			for _, checker := range p.checkers {
				checker.Reset()
			}
			p.memoryProtector.Remove(p.currentMsgPackSize)
			p.msgs = make([]*api.ReplicateMsg, 0)
			p.currentMsgPackSize = 0
		}
	}()

	if ok := p.memoryProtector.Add(msgSize); ok {
		err := handler(p.msgs)
		hasHandlerMsg = true
		return err
	}

	if msgSize > p.maxMsgSize {
		err := handler(p.msgs)
		hasHandlerMsg = true
		return err
	}

	for _, checker := range p.checkers {
		if checker.Check(msg) {
			err := handler(p.msgs)
			hasHandlerMsg = true
			return err
		}
	}

	return nil
}

func (p *Packer) ClearMsgs(handler func([]*api.ReplicateMsg) error) error {
	err := handler(p.msgs)
	for _, checker := range p.checkers {
		checker.Reset()
	}
	p.memoryProtector.Remove(p.currentMsgPackSize)
	p.msgs = make([]*api.ReplicateMsg, 0)
	p.currentMsgPackSize = 0
	return err
}
