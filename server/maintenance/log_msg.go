package maintenance

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	pkglog "github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"

	"github.com/zilliztech/milvus-cdc/core/log"
	"github.com/zilliztech/milvus-cdc/server/model/request"
)

var logger *MsgLog

type MsgLog struct {
	log         *pkglog.MLogger
	lock        sync.RWMutex
	current     int
	target      int
	startTime   time.Time
	duration    time.Duration
	active      bool
	forceActive bool
}

func (m *MsgLog) SetActive(active bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.active = active
}

func InitMsgLog() {
	logger = &MsgLog{
		log:       log.With(zap.String("module", "maintenance")),
		target:    5,
		startTime: time.Now(),
		duration:  5 * time.Minute,
	}

	logger.SetActive(true)
}

func checkLogger() bool {
	if logger == nil {
		return false
	}
	logger.lock.RLock()
	defer logger.lock.RUnlock()
	if logger.forceActive {
		return true
	}
	if !logger.active {
		return false
	}
	if logger.current >= logger.target {
		return false
	}
	if time.Since(logger.startTime) > logger.duration {
		return false
	}
	return true
}

func LogInsertMsg(insertMsg *msgstream.InsertMsg) {
	if !checkLogger() {
		if logger != nil {
			logger.SetActive(false)
		}
		return
	}
	logger.lock.Lock()
	logger.current++
	logger.lock.Unlock()
	fieldInfos := make(map[string]interface{})
	for _, fieldData := range insertMsg.GetFieldsData() {
		if fieldData.GetScalars() == nil {
			continue
		}
		if data := fieldData.GetScalars().GetLongData(); data != nil {
			fieldInfos[fieldData.GetFieldName()] = GetPartElements(data.GetData())
		}
		if data := fieldData.GetScalars().GetStringData(); data != nil {
			fieldInfos[fieldData.GetFieldName()] = GetPartElements(data.GetData())
		}
	}
	logger.log.Info("log insert msg",
		zap.String("database", insertMsg.GetDbName()),
		zap.String("collection", insertMsg.GetCollectionName()),
		zap.String("partition", insertMsg.GetPartitionName()),
		zap.String("shard", insertMsg.GetShardName()),
		zap.Uint64("ts", insertMsg.GetBase().Timestamp),
		zap.Uint64("numRows", insertMsg.GetNumRows()),
		zap.Any("partFields", fieldInfos),
	)
}

func GetPartElements[T any](arr []T) []T {
	if len(arr) < 10 {
		return arr
	}
	return arr[:10]
}

func GetIDs(ids *schemapb.IDs) any {
	if a := ids.GetIntId(); a != nil {
		return GetPartElements(a.GetData())
	} else if b := ids.GetStrId(); b != nil {
		return GetPartElements(b.GetData())
	}
	return nil
}

func LogDeleteMsg(deleteMsg *msgstream.DeleteMsg) {
	if !checkLogger() {
		if logger != nil {
			logger.SetActive(false)
		}
		return
	}
	logger.lock.Lock()
	logger.current++
	logger.lock.Unlock()
	logger.log.Info("log delete msg",
		zap.String("database", deleteMsg.GetDbName()),
		zap.String("collection", deleteMsg.GetCollectionName()),
		zap.String("partition", deleteMsg.GetPartitionName()),
		zap.String("shard", deleteMsg.GetShardName()),
		zap.Uint64("ts", deleteMsg.GetBase().Timestamp),
		zap.Int64("numRows", deleteMsg.GetNumRows()),
		zap.Any("primaryKeys", GetIDs(deleteMsg.GetPrimaryKeys())),
	)
}

func ForceLogMsg(req *request.MaintenanceRequest) (*request.MaintenanceResponse, error) {
	forceParam, ok := req.Params["force"]
	if !ok {
		return &request.MaintenanceResponse{
			State: "force is required",
		}, nil
	}
	force, err := strconv.ParseBool(fmt.Sprint(forceParam))
	if err != nil {
		return &request.MaintenanceResponse{
			State: fmt.Sprintf("parse force failed: %s", err.Error()),
		}, nil
	}
	if logger == nil {
		return &request.MaintenanceResponse{
			State: "logger is nil",
		}, nil
	}
	logger.lock.Lock()
	logger.forceActive = force
	logger.lock.Unlock()
	return &request.MaintenanceResponse{
		State: fmt.Sprintf("set force log msg to %t", force),
	}, nil
}

func ResetLogMsg(req *request.MaintenanceRequest) (*request.MaintenanceResponse, error) {
	cntParam, ok := req.Params["count"]
	if !ok {
		return &request.MaintenanceResponse{
			State: "count is required",
		}, nil
	}
	cnt, err := strconv.Atoi(fmt.Sprint(cntParam))
	if err != nil {
		return &request.MaintenanceResponse{
			State: fmt.Sprintf("parse count failed: %s", err.Error()),
		}, nil
	}
	durationParam, ok := req.Params["duration"]
	if !ok {
		return &request.MaintenanceResponse{
			State: "duration is required",
		}, nil
	}
	duration, err := strconv.Atoi(fmt.Sprint(durationParam))
	if err != nil {
		return &request.MaintenanceResponse{
			State: fmt.Sprintf("parse duration failed: %s", err.Error()),
		}, nil
	}
	if logger == nil {
		return &request.MaintenanceResponse{
			State: "logger is nil",
		}, nil
	}
	logger.lock.Lock()
	defer logger.lock.Unlock()
	logger.current = 0
	logger.duration = time.Duration(duration) * time.Minute
	logger.startTime = time.Now()
	logger.target = cnt

	return &request.MaintenanceResponse{
		State: fmt.Sprintf("reset log msg count to %d, duration to %d", cnt, duration),
	}, nil
}
