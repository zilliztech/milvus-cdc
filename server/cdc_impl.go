// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/samber/lo"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/pb"
	cdcreader "github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	cdcwriter "github.com/zilliztech/milvus-cdc/core/writer"
	servererror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"github.com/zilliztech/milvus-cdc/server/model/request"
	"github.com/zilliztech/milvus-cdc/server/store"
	"go.uber.org/zap"
)

type MetaCDC struct {
	BaseCDC
	metaStoreFactory store.MetaStoreFactory
	rootPath         string
	config           *CDCServerConfig

	// collectionNames are used to make sure no duplicate task for a collection.
	// key -> milvus ip:port, value -> collection names
	collectionNames struct {
		sync.RWMutex
		data        map[string][]string
		excludeData map[string][]string
	}
	cdcTasks struct {
		sync.RWMutex
		data map[string]*CDCTask
	}
	factoryCreator FactoryCreator
}

func NewMetaCDC(serverConfig *CDCServerConfig) *MetaCDC {
	if serverConfig.MaxNameLength == 0 {
		serverConfig.MaxNameLength = 256
	}

	rootPath := serverConfig.MetaStoreConfig.RootPath
	var factory store.MetaStoreFactory
	var err error
	switch serverConfig.MetaStoreConfig.StoreType {
	case "mysql":
		factory, err = store.NewMySQLMetaStore(context.Background(), serverConfig.MetaStoreConfig.MysqlSourceUrl, rootPath)
		if err != nil {
			log.Panic("fail to new mysql meta store", zap.Error(err))
		}
	case "etcd":
		factory, err = store.NewEtcdMetaStore(context.Background(), serverConfig.MetaStoreConfig.EtcdEndpoints, rootPath)
		if err != nil {
			log.Panic("fail to new etcd meta store", zap.Error(err))
		}
	default:
		log.Panic("not support the meta store type, valid type: [mysql, etcd]", zap.String("type", serverConfig.MetaStoreConfig.StoreType))
	}

	_, err = util.GetEtcdClient(serverConfig.SourceConfig.EtcdAddress)
	if err != nil {
		log.Panic("fail to get etcd client for connect the source etcd data", zap.Error(err))
	}
	// TODO check mq status

	cdc := &MetaCDC{
		metaStoreFactory: factory,
		config:           serverConfig,
	}
	cdc.collectionNames.data = make(map[string][]string)
	cdc.collectionNames.excludeData = make(map[string][]string)
	cdc.cdcTasks.data = make(map[string]*CDCTask)
	cdc.factoryCreator = NewCDCFactory
	return cdc
}

func (e *MetaCDC) ReloadTask() {
	reverse := e.config.EnableReverse
	reverseConfig := e.config.ReverseMilvus
	currentConfig := e.config.CurrentMilvus
	if reverse && (reverseConfig.Host == "" ||
		reverseConfig.Port <= 0 ||
		currentConfig.Host == "" ||
		currentConfig.Port <= 0) {
		log.Panic("the reverse milvus config is invalid, the host or port of reverse and current param should be set", zap.Any("config", reverseConfig))
	}

	ctx := context.Background()
	taskInfos, err := e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Get(ctx, &meta.TaskInfo{}, nil)
	if err != nil {
		log.Panic("fail to get all task info", zap.Error(err))
	}

	if reverse {
		var err error
		reverseTxn, commitFunc, err := e.metaStoreFactory.Txn(ctx)
		if err != nil {
			log.Panic("fail to new the reverse txn", zap.Error(err))
		}
		for _, taskInfo := range taskInfos {
			if taskInfo.MilvusConnectParam.Host == currentConfig.Host && taskInfo.MilvusConnectParam.Port == currentConfig.Port {
				taskInfo.MilvusConnectParam.Host = reverseConfig.Host
				taskInfo.MilvusConnectParam.Port = reverseConfig.Port
				taskInfo.MilvusConnectParam.Username = reverseConfig.Username
				taskInfo.MilvusConnectParam.Password = reverseConfig.Password
				taskInfo.MilvusConnectParam.EnableTls = reverseConfig.EnableTls
				if err = e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Put(ctx, taskInfo, reverseTxn); err != nil {
					log.Panic("fail to put the task info to metastore when reversing", zap.Error(err))
				}
				if err = e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Delete(ctx, &meta.TaskCollectionPosition{TaskID: taskInfo.TaskID}, reverseTxn); err != nil {
					log.Panic("fail to delete the task collection position to metastore when reversing", zap.Error(err))
				}
			}
		}
		if err = commitFunc(err); err != nil {
			log.Panic("fail to commit the reverse txn", zap.Error(err))
		}
	}

	for _, taskInfo := range taskInfos {
		milvusAddress := fmt.Sprintf("%s:%d", taskInfo.MilvusConnectParam.Host, taskInfo.MilvusConnectParam.Port)
		newCollectionNames := lo.Map(taskInfo.CollectionInfos, func(t model.CollectionInfo, _ int) string {
			return t.Name
		})
		e.collectionNames.data[milvusAddress] = append(e.collectionNames.data[milvusAddress], newCollectionNames...)
		e.collectionNames.excludeData[milvusAddress] = append(e.collectionNames.excludeData[milvusAddress], taskInfo.ExcludeCollections...)
		task, err := e.newCdcTask(taskInfo)
		if err != nil {
			log.Warn("fail to new cdc task", zap.Any("task_info", taskInfo), zap.Error(err))
			continue
		}
		if taskInfo.State == meta.TaskStateRunning {
			if err = <-task.Resume(nil); err != nil {
				log.Warn("fail to start cdc task", zap.Any("task_info", taskInfo), zap.Error(err))
			}
		}
	}
}

func (e *MetaCDC) Create(req *request.CreateRequest) (resp *request.CreateResponse, err error) {
	defer func() {
		log.Info("create request done")
		if err != nil {
			log.Warn("fail to create cdc task", zap.Any("req", req), zap.Error(err))
		}
	}()
	if err = e.validCreateRequest(req); err != nil {
		return nil, err
	}
	milvusAddress := fmt.Sprintf("%s:%d", req.MilvusConnectParam.Host, req.MilvusConnectParam.Port)
	newCollectionNames := lo.Map(req.CollectionInfos, func(t model.CollectionInfo, _ int) string {
		return t.Name
	})
	e.collectionNames.Lock()
	if names, ok := e.collectionNames.data[milvusAddress]; ok {
		existAll := lo.Contains(names, cdcreader.AllCollection)
		duplicateCollections := lo.Filter(req.CollectionInfos, func(info model.CollectionInfo, _ int) bool {
			return (!existAll && lo.Contains(names, info.Name)) || (existAll && info.Name == cdcreader.AllCollection)
		})
		if len(duplicateCollections) > 0 {
			e.collectionNames.Unlock()
			return nil, servererror.NewClientError(fmt.Sprintf("some collections are duplicate with existing tasks, %v", lo.Map(duplicateCollections, func(t model.CollectionInfo, i int) string {
				return t.Name
			})))
		}
		if existAll {
			excludeCollectionNames := lo.Filter(e.collectionNames.excludeData[milvusAddress], func(s string, _ int) bool {
				return !lo.Contains(names, s)
			})
			duplicateCollections = lo.Filter(req.CollectionInfos, func(info model.CollectionInfo, _ int) bool {
				return !lo.Contains(excludeCollectionNames, info.Name)
			})
			if len(duplicateCollections) > 0 {
				e.collectionNames.Unlock()
				return nil, servererror.NewClientError(fmt.Sprintf("some collections are duplicate with existing tasks, check the `*` collection task and other tasks, %v", lo.Map(duplicateCollections, func(t model.CollectionInfo, i int) string {
					return t.Name
				})))
			}
		}
	}
	// release lock early to accept other requests
	var excludeCollectionNames []string
	if newCollectionNames[0] == cdcreader.AllCollection {
		existCollectionNames := e.collectionNames.data[milvusAddress]
		excludeCollectionNames = make([]string, len(existCollectionNames))
		copy(excludeCollectionNames, existCollectionNames)
		if !lo.Contains(excludeCollectionNames, util.RpcRequestCollectionName) {
			excludeCollectionNames = append(excludeCollectionNames, util.RpcRequestCollectionName)
		}
		e.collectionNames.excludeData[milvusAddress] = excludeCollectionNames
	}
	e.collectionNames.data[milvusAddress] = append(e.collectionNames.data[milvusAddress], newCollectionNames...)
	e.collectionNames.Unlock()

	revertCollectionNames := func() {
		e.collectionNames.Lock()
		defer e.collectionNames.Unlock()
		if newCollectionNames[0] == cdcreader.AllCollection {
			e.collectionNames.excludeData[milvusAddress] = []string{}
		}
		e.collectionNames.data[milvusAddress] = lo.Without(e.collectionNames.data[milvusAddress], newCollectionNames...)
	}

	ctx := context.Background()
	getResp, err := e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Get(ctx, &meta.TaskInfo{}, nil)
	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to get task list to check num"))
	}
	if len(getResp) >= e.config.MaxTaskNum {
		return nil, servererror.NewServerError(errors.Newf("the task num has reach the limit, %d", e.config.MaxTaskNum))
	}

	info := &meta.TaskInfo{
		TaskID:                e.getUuid(),
		MilvusConnectParam:    req.MilvusConnectParam,
		CollectionInfos:       req.CollectionInfos,
		RpcRequestChannelInfo: req.RpcChannelInfo,
		ExcludeCollections:    excludeCollectionNames,
		WriterCacheConfig:     req.BufferConfig,
		State:                 meta.TaskStateInitial,
	}
	err = e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Put(ctx, info, nil)
	if err != nil {
		revertCollectionNames()
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task info to etcd"))
	}

	info.State = meta.TaskStateRunning
	task, err := e.newCdcTask(info)
	if err != nil {
		log.Warn("fail to new cdc task", zap.Error(err))
		return nil, servererror.NewServerError(err)
	}
	if err = <-task.Resume(func() error {
		err = store.UpdateTaskState(
			e.metaStoreFactory.GetTaskInfoMetaStore(ctx),
			info.TaskID,
			meta.TaskStateRunning,
			[]meta.TaskState{meta.TaskStateInitial})
		if err != nil {
			log.Warn("fail to update the task meta", zap.Error(err))
			return servererror.NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+info.TaskID))
		}
		return nil
	}); err != nil {
		log.Warn("fail to start cdc task", zap.Error(err))
		return nil, servererror.NewServerError(err)
	}

	return &request.CreateResponse{TaskID: info.TaskID}, nil
}

func (e *MetaCDC) validCreateRequest(req *request.CreateRequest) error {
	connectParam := req.MilvusConnectParam
	if connectParam.Host == "" {
		return servererror.NewClientError("the milvus host is empty")
	}
	if connectParam.Port <= 0 {
		return servererror.NewClientError("the milvus port is less or equal zero")
	}
	if (connectParam.Username != "" && connectParam.Password == "") ||
		(connectParam.Username == "" && connectParam.Password != "") {
		return servererror.NewClientError("cannot set only one of the milvus username and password")
	}
	if connectParam.ConnectTimeout < 0 {
		return servererror.NewClientError("the milvus connect timeout is less zero")
	}
	cacheParam := req.BufferConfig
	if cacheParam.Period < 0 {
		return servererror.NewClientError("the cache period is less zero")
	}
	if cacheParam.Size < 0 {
		return servererror.NewClientError("the cache size is less zero")
	}

	if req.RpcChannelInfo.Name == "" {
		if err := e.checkCollectionInfos(req.CollectionInfos); err != nil {
			return err
		}
	} else {
		if len(req.CollectionInfos) > 0 {
			return servererror.NewClientError("the collection info should be empty when the rpc channel is not empty")
		}
		req.CollectionInfos = []model.CollectionInfo{{Name: util.RpcRequestCollectionName}}
	}

	_, err := cdcwriter.NewMilvusDataHandler(
		cdcwriter.AddressOption(fmt.Sprintf("%s:%d", connectParam.Host, connectParam.Port)),
		cdcwriter.UserOption(connectParam.Username, connectParam.Password),
		cdcwriter.TlsOption(connectParam.EnableTls),
		cdcwriter.IgnorePartitionOption(connectParam.IgnorePartition),
		cdcwriter.ConnectTimeoutOption(connectParam.ConnectTimeout))
	if err != nil {
		log.Warn("fail to connect the milvus", zap.Any("connect_param", connectParam), zap.Error(err))
		return errors.WithMessage(err, "fail to connect the milvus")
	}
	return nil
}

func (e *MetaCDC) checkCollectionInfos(infos []model.CollectionInfo) error {
	if len(infos) == 0 {
		return servererror.NewClientError("empty collection info")
	}

	var (
		longNames []string
		emptyName bool
	)
	for _, info := range infos {
		if info.Name == "" {
			emptyName = true
		}
		if info.Name == cdcreader.AllCollection && len(infos) > 1 {
			return servererror.NewClientError(fmt.Sprintf("make sure the only one collection if you want to use the '*' collection param, current param: %v",
				lo.Map(infos, func(t model.CollectionInfo, _ int) string {
					return t.Name
				})))
		}
		if len(info.Name) > e.config.MaxNameLength {
			longNames = append(longNames, info.Name)
		}
	}
	if !emptyName && len(longNames) == 0 {
		return nil
	}
	var errMsg string
	if emptyName {
		errMsg += "there is a collection name that is empty. "
	}
	if len(longNames) > 0 {
		errMsg += fmt.Sprintf("there are some collection names whose length exceeds 256 characters, %v", longNames)
	}
	return servererror.NewClientError(errMsg)
}

func (e *MetaCDC) getUuid() string {
	uid := uuid.Must(uuid.NewRandom())
	return strings.ReplaceAll(uid.String(), "-", "")
}

func (e *MetaCDC) newCdcTask(info *meta.TaskInfo) (*CDCTask, error) {
	metrics.StreamingCollectionCountVec.WithLabelValues(info.TaskID, metrics.TotalStatusLabel).Add(float64(len(info.CollectionInfos)))

	e.cdcTasks.Lock()
	e.cdcTasks.data[info.TaskID] = EmptyCdcTask
	metrics.TaskNumVec.AddInitial()
	e.cdcTasks.Unlock()

	newReaderFunc := NewReaderFunc(func() (cdcreader.CDCReader, error) {
		var err error
		taskLog := log.With(zap.String("task_id", info.TaskID), zap.Error(err))
		ctx := context.Background()
		positions, err := e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Get(ctx, &meta.TaskCollectionPosition{TaskID: info.TaskID}, nil)
		if err != nil {
			taskLog.Warn("fail to get the task collection position", zap.Error(err))
			return nil, errors.WithMessage(err, "fail to get the task meta, task_id: "+info.TaskID)
		}
		sourceConfig := e.config.SourceConfig
		if info.RpcRequestChannelInfo.Name != "" {
			channelName := info.RpcRequestChannelInfo.Name
			channelPosition := ""
			if len(positions) != 0 {
				position := positions[0]
				if position.CollectionName != util.RpcRequestCollectionName || position.CollectionID != util.RpcRequestCollectionID {
					log.Panic("the collection name or id is not match the rpc request channel info", zap.Any("position", position))
				}
				kp, ok := position.Positions[channelName]
				if !ok {
					log.Panic("the channel name is not match the rpc request channel info", zap.Any("position", position))
				}
				positionBytes, err := proto.Marshal(kp)
				if err != nil {
					log.Warn("fail to marshal the key data pair", zap.Error(err))
					return nil, err
				}
				channelPosition = base64.StdEncoding.EncodeToString(positionBytes)
			} else if info.RpcRequestChannelInfo.Position != "" {
				channelPosition = info.RpcRequestChannelInfo.Position
			}
			reader, err := cdcreader.NewChannelReader(
				cdcreader.MqChannelOption(sourceConfig.Pulsar, sourceConfig.Kafka),
				cdcreader.ChannelNameOption(channelName),
				cdcreader.SubscriptionPositionChannelOption(mqwrapper.SubscriptionPositionLatest),
				cdcreader.SeekPositionChannelOption(channelPosition),
				cdcreader.DataChanChannelOption(sourceConfig.ReadChanLen),
			)
			if err != nil {
				return nil, errors.WithMessage(err, "fail to new the channel reader, task_id: "+info.TaskID)
			}
			return reader, nil
		}

		taskPosition := make(map[string]map[string]*commonpb.KeyDataPair)
		for _, position := range positions {
			taskPosition[position.CollectionName] = position.Positions
		}

		var options []config.Option[*cdcreader.MilvusCollectionReader]
		for _, collectionInfo := range info.CollectionInfos {
			options = append(options, cdcreader.CollectionInfoOption(collectionInfo.Name, taskPosition[collectionInfo.Name]))
		}
		monitor := NewReaderMonitor(info.TaskID)
		etcdConfig := config.NewMilvusEtcdConfig(config.MilvusEtcdEndpointsOption(sourceConfig.EtcdAddress),
			config.MilvusEtcdRootPathOption(sourceConfig.EtcdRootPath),
			config.MilvusEtcdMetaSubPathOption(sourceConfig.EtcdMetaSubPath))
		reader, err := cdcreader.NewMilvusCollectionReader(append(options,
			cdcreader.EtcdOption(etcdConfig),
			cdcreader.MqOption(sourceConfig.Pulsar, sourceConfig.Kafka),
			cdcreader.MonitorOption(monitor),
			cdcreader.ShouldReadFuncOption(GetShouldReadFunc(info)),
			cdcreader.ChanLenOption(sourceConfig.ReadChanLen))...)
		if err != nil {
			return nil, errors.WithMessage(err, "fail to new the reader, task_id: "+info.TaskID)
		}
		return reader, nil
	})

	writeCallback := NewWriteCallback(e.metaStoreFactory, e.rootPath, info.TaskID)
	newWriterFunc := NewWriterFunc(func() (cdcwriter.CDCWriter, error) {
		var err error
		taskLog := log.With(zap.String("task_id", info.TaskID), zap.Error(err))
		targetConfig := info.MilvusConnectParam
		dataHandler, err := cdcwriter.NewMilvusDataHandler(
			cdcwriter.AddressOption(fmt.Sprintf("%s:%d", targetConfig.Host, targetConfig.Port)),
			cdcwriter.UserOption(targetConfig.Username, targetConfig.Password),
			cdcwriter.TlsOption(targetConfig.EnableTls),
			cdcwriter.IgnorePartitionOption(targetConfig.IgnorePartition),
			cdcwriter.ConnectTimeoutOption(targetConfig.ConnectTimeout))
		if err != nil {
			taskLog.Warn("fail to new the data handler")
			return nil, errors.WithMessage(err, "fail to new the data handler, task_id: "+info.TaskID)
		}

		cacheConfig := info.WriterCacheConfig
		writer := cdcwriter.NewCDCWriterTemplate(
			cdcwriter.HandlerOption(NewDataHandlerWrapper(info.TaskID, dataHandler)),
			cdcwriter.BufferOption(time.Duration(cacheConfig.Period)*time.Second,
				int64(cacheConfig.Size), writeCallback.UpdateTaskCollectionPosition))
		return writer, nil
	})

	e.cdcTasks.Lock()
	defer e.cdcTasks.Unlock()
	task := NewCdcTask(info.TaskID, e.factoryCreator(newReaderFunc, newWriterFunc), writeCallback, func() error {
		// update the meta task state
		err := store.UpdateTaskState(
			e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()),
			info.TaskID,
			meta.TaskStatePaused,
			[]meta.TaskState{meta.TaskStateRunning})
		if err != nil {
			log.Warn("fail to update the task meta state", zap.String("task_id", info.TaskID), zap.Error(err))
		}
		return err
	})
	e.cdcTasks.data[info.TaskID] = task
	return task, nil
}

func (e *MetaCDC) Delete(req *request.DeleteRequest) (*request.DeleteResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}

	var err error

	err = <-cdcTask.Terminate(func() error {
		var info *meta.TaskInfo
		info, err = store.DeleteTask(e.metaStoreFactory, e.rootPath, req.TaskID)
		if err != nil {
			return servererror.NewServerError(errors.WithMessage(err, "fail to delete the task meta, task_id: "+req.TaskID))
		}
		milvusAddress := fmt.Sprintf("%s:%d", info.MilvusConnectParam.Host, info.MilvusConnectParam.Port)
		collectionNames := info.CollectionNames()
		e.collectionNames.Lock()
		if collectionNames[0] == cdcreader.AllCollection {
			e.collectionNames.excludeData[milvusAddress] = []string{}
		}
		e.collectionNames.data[milvusAddress] = lo.Without(e.collectionNames.data[milvusAddress], collectionNames...)
		e.collectionNames.Unlock()

		e.cdcTasks.Lock()
		delete(e.cdcTasks.data, req.TaskID)
		e.cdcTasks.Unlock()
		return err
	})

	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to terminate the task, task_id: "+req.TaskID))
	}

	return &request.DeleteResponse{}, err
}

func (e *MetaCDC) Pause(req *request.PauseRequest) (*request.PauseResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}

	var err error

	err = <-cdcTask.Pause(func() error {
		err = store.UpdateTaskState(
			e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()),
			req.TaskID,
			meta.TaskStatePaused,
			[]meta.TaskState{meta.TaskStateRunning})
		if err != nil {
			return servererror.NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+req.TaskID))
		}
		return nil
	})
	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to pause the task state, task_id: "+req.TaskID))
	}

	return &request.PauseResponse{}, err
}

func (e *MetaCDC) Resume(req *request.ResumeRequest) (*request.ResumeResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}

	var err error

	err = <-cdcTask.Resume(func() error {
		err = store.UpdateTaskState(
			e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()),
			req.TaskID,
			meta.TaskStateRunning,
			[]meta.TaskState{meta.TaskStatePaused})
		if err != nil {
			return servererror.NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+req.TaskID))
		}
		return nil
	})
	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to resume the task state, task_id: "+req.TaskID))
	}

	return &request.ResumeResponse{}, err
}

func (e *MetaCDC) Get(req *request.GetRequest) (*request.GetResponse, error) {
	taskInfo, err := store.GetTaskInfo(e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()), req.TaskID)
	if err != nil {
		if errors.Is(err, NotFoundErr) {
			return nil, servererror.NewClientError(err.Error())
		}
		return nil, servererror.NewServerError(err)
	}
	return &request.GetResponse{
		Task: request.GetTask(taskInfo),
	}, nil
}

func (e *MetaCDC) List(req *request.ListRequest) (*request.ListResponse, error) {
	taskInfos, err := store.GetAllTaskInfo(e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()))
	if err != nil && !errors.Is(err, NotFoundErr) {
		return nil, servererror.NewServerError(err)
	}
	return &request.ListResponse{
		Tasks: lo.Map(taskInfos, func(t *meta.TaskInfo, _ int) request.Task {
			return request.GetTask(t)
		}),
	}, nil
}

func GetShouldReadFunc(taskInfo *meta.TaskInfo) cdcreader.ShouldReadFunc {
	isAll := taskInfo.CollectionInfos[0].Name == cdcreader.AllCollection
	return func(collectionInfo *pb.CollectionInfo) bool {
		currentCollectionName := collectionInfo.Schema.Name
		notStarContains := !isAll && lo.ContainsBy(taskInfo.CollectionInfos, func(taskCollectionInfo model.CollectionInfo) bool {
			return taskCollectionInfo.Name == currentCollectionName
		})
		starContains := isAll && !lo.ContainsBy(taskInfo.ExcludeCollections, func(s string) bool {
			return s == currentCollectionName
		})

		return notStarContains || starContains
	}
}
