/* Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * //
 *     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	coremodel "github.com/zilliztech/milvus-cdc/core/model"
	"github.com/zilliztech/milvus-cdc/core/pb"
	cdcreader "github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	cdcwriter "github.com/zilliztech/milvus-cdc/core/writer"
	serverapi "github.com/zilliztech/milvus-cdc/server/api"
	servererror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/maintenance"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"github.com/zilliztech/milvus-cdc/server/model/request"
	"github.com/zilliztech/milvus-cdc/server/store"
)

type ReplicateEntity struct {
	channelManager api.ChannelManager
	targetClient   api.TargetAPI
	metaOp         api.MetaOp
	writerObj      api.Writer
	mqDispatcher   msgdispatcher.Client
	mqTTDispatcher msgdispatcher.Client
	entityQuitFunc func()
	taskQuitFuncs  *typeutil.ConcurrentMap[string, func()]
	refCnt         atomic.Int32
}

func (r *ReplicateEntity) UpdateMapping(mapping map[string]string) {
	w, ok := r.writerObj.(*cdcwriter.ChannelWriter)
	if !ok {
		return
	}
	w.UpdateNameMappings(mapping)
	targetClient, ok := r.targetClient.(*cdcreader.TargetClient)
	if !ok {
		return
	}
	targetClient.UpdateNameMappings(mapping)
}

type MetaCDC struct {
	BaseCDC
	metaStoreFactory serverapi.MetaStoreFactory
	mqFactoryCreator cdcreader.FactoryCreator
	rootPath         string
	config           *CDCServerConfig

	// collectionNames are used to make sure no duplicate task for a collection.
	// key -> milvus ip:port, value -> collection names
	collectionNames struct {
		sync.RWMutex
		data        map[string][]string
		excludeData map[string][]string
		extraInfos  map[string]model.ExtraInfo
		nameMapping map[string]map[string]string
	}
	cdcTasks struct {
		sync.RWMutex
		data map[string]*meta.TaskInfo
	}
	// factoryCreator FactoryCreator
	replicateEntityMap struct {
		sync.RWMutex
		data map[string]*ReplicateEntity
	}
}

func NewMetaCDC(serverConfig *CDCServerConfig) *MetaCDC {
	if serverConfig.MaxNameLength == 0 {
		serverConfig.MaxNameLength = 256
	}

	rootPath := serverConfig.MetaStoreConfig.RootPath
	var factory serverapi.MetaStoreFactory
	var err error
	switch serverConfig.MetaStoreConfig.StoreType {
	case "mysql":
		factory, err = store.NewMySQLMetaStore(context.Background(), serverConfig.MetaStoreConfig.MysqlSourceURL, rootPath)
		if err != nil {
			log.Panic("fail to new mysql meta store", zap.Error(err))
		}
	case "etcd":
		etcdServerConfig := GetEtcdServerConfigFromMetaConfig(serverConfig.MetaStoreConfig)
		factory, err = store.NewEtcdMetaStore(context.Background(), etcdServerConfig, rootPath)
		if err != nil {
			log.Panic("fail to new etcd meta store", zap.Error(err))
		}
	default:
		log.Panic("not support the meta store type, valid type: [mysql, etcd]", zap.String("type", serverConfig.MetaStoreConfig.StoreType))
	}

	if serverConfig.SourceConfig.ReplicateChan == "" {
		log.Panic("the replicate channel in the source config is empty")
	}

	_, err = util.GetEtcdClient(GetEtcdServerConfigFromSourceConfig(serverConfig.SourceConfig))
	if err != nil {
		log.Panic("fail to get etcd client for connect the source etcd data", zap.Error(err))
	}

	cdc := &MetaCDC{
		metaStoreFactory: factory,
		config:           serverConfig,
		mqFactoryCreator: cdcreader.NewDefaultFactoryCreator(),
	}

	err = cdc.checkMQConnection()
	if err != nil {
		log.Panic("fail to check the mq connection", zap.Error(err))
	}

	cdc.collectionNames.data = make(map[string][]string)
	cdc.collectionNames.excludeData = make(map[string][]string)
	cdc.collectionNames.extraInfos = make(map[string]model.ExtraInfo)
	cdc.collectionNames.nameMapping = make(map[string]map[string]string)
	cdc.cdcTasks.data = make(map[string]*meta.TaskInfo)
	cdc.replicateEntityMap.data = make(map[string]*ReplicateEntity)
	return cdc
}

func (e *MetaCDC) checkMQConnection() error {
	mqConfig := config.MQConfig{
		Pulsar: e.config.SourceConfig.Pulsar,
		Kafka:  e.config.SourceConfig.Kafka,
	}
	f, err := cdcreader.GetStreamFactory(e.mqFactoryCreator, mqConfig, false)
	if err != nil {
		return err
	}
	d := cdcreader.NewDisptachClientStreamCreator(f, nil)
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return d.CheckConnection(timeoutCtx, util.GetVChannel(e.config.SourceConfig.ReplicateChan, "000000"), nil)
}

func (e *MetaCDC) ReloadTask() {
	ctx := context.Background()
	taskInfos, err := e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Get(ctx, &meta.TaskInfo{}, nil)
	if err != nil {
		log.Panic("fail to get all task info", zap.Error(err))
	}

	for _, taskInfo := range taskInfos {
		uKey := getTaskUniqueIDFromInfo(taskInfo)
		newCollectionNames := GetCollectionNamesFromTaskInfo(taskInfo)
		e.collectionNames.data[uKey] = append(e.collectionNames.data[uKey], newCollectionNames...)
		e.collectionNames.excludeData[uKey] = append(e.collectionNames.excludeData[uKey], taskInfo.ExcludeCollections...)
		e.collectionNames.excludeData[uKey] = lo.Uniq(e.collectionNames.excludeData[uKey])
		e.collectionNames.extraInfos[uKey] = taskInfo.ExtraInfo
		e.cdcTasks.Lock()
		e.cdcTasks.data[taskInfo.TaskID] = taskInfo
		e.cdcTasks.Unlock()

		metrics.TaskNumVec.Add(taskInfo.TaskID, taskInfo.State)
		metrics.TaskStateVec.WithLabelValues(taskInfo.TaskID).Set(float64(taskInfo.State))
		if taskInfo.DisableAutoStart {
			if taskInfo.State != meta.TaskStatePaused {
				_ = e.pauseTaskWithReason(taskInfo.TaskID, "the task is disabled auto start", []meta.TaskState{})
			}
			continue
		}
		if err := e.startInternal(taskInfo, taskInfo.State == meta.TaskStateRunning); err != nil {
			log.Warn("fail to start the task", zap.Any("task_info", taskInfo), zap.Error(err))
			_ = e.pauseTaskWithReason(taskInfo.TaskID, "fail to start task, err: "+err.Error(), []meta.TaskState{})
		}
	}
}

func GetMilvusURI(milvusConnectParam model.MilvusConnectParam) string {
	if milvusConnectParam.URI != "" {
		return milvusConnectParam.URI
	}
	return util.GetURI(milvusConnectParam.Host, milvusConnectParam.Port, milvusConnectParam.EnableTLS)
}

func GetMilvusToken(milvusConnectParam model.MilvusConnectParam) string {
	if milvusConnectParam.Token != "" {
		return milvusConnectParam.Token
	}
	return util.GetToken(milvusConnectParam.Username, milvusConnectParam.Password)
}

func GetKafkaAddress(kafkaConnectParam model.KafkaConnectParam) string {
	return kafkaConnectParam.Address
}

func getTaskUniqueIDFromInfo(info *meta.TaskInfo) string {
	milvusURI := GetMilvusURI(info.MilvusConnectParam)
	if milvusURI != "" {
		return milvusURI
	}
	kafkaAddress := GetKafkaAddress(info.KafkaConnectParam)
	if kafkaAddress != "" {
		return kafkaAddress
	}
	panic("fail to get the task unique id")
}

func getTaskUniqueIDFromReq(req *request.CreateRequest) string {
	milvusURI := GetMilvusURI(req.MilvusConnectParam)
	if milvusURI != "" {
		return milvusURI
	}
	kafkaAddress := GetKafkaAddress(req.KafkaConnectParam)
	if kafkaAddress != "" {
		return kafkaAddress
	}
	panic("fail to get the task unique id")
}

func GetCollectionNamesFromTaskInfo(info *meta.TaskInfo) []string {
	var newCollectionNames []string
	if len(info.CollectionInfos) > 0 {
		newCollectionNames = lo.Map(info.CollectionInfos, func(t model.CollectionInfo, _ int) string {
			return util.GetFullCollectionName(t.Name, cdcreader.DefaultDatabase)
		})
	}
	if len(info.DBCollections) > 0 {
		for db, infos := range info.DBCollections {
			for _, t := range infos {
				newCollectionNames = append(newCollectionNames, util.GetFullCollectionName(t.Name, db))
			}
		}
	}
	return newCollectionNames
}

func GetCollectionNamesFromReq(req *request.CreateRequest) []string {
	var newCollectionNames []string
	if len(req.CollectionInfos) > 0 {
		newCollectionNames = lo.Map(req.CollectionInfos, func(t model.CollectionInfo, _ int) string {
			return util.GetFullCollectionName(t.Name, cdcreader.DefaultDatabase)
		})
	}
	if len(req.DBCollections) > 0 {
		for db, infos := range req.DBCollections {
			for _, t := range infos {
				newCollectionNames = append(newCollectionNames, util.GetFullCollectionName(t.Name, db))
			}
		}
	}
	return newCollectionNames
}

func GetCollectionMappingFromReq(req *request.CreateRequest) map[string]string {
	mapCollectionNames := make(map[string]string)
	for _, mapping := range req.NameMapping {
		for s, t := range mapping.CollectionMapping {
			mapCollectionNames[util.GetFullCollectionName(s, mapping.SourceDB)] = util.GetFullCollectionName(t, mapping.TargetDB)
		}
		if len(mapping.CollectionMapping) == 0 {
			mapCollectionNames[util.GetFullCollectionName(cdcreader.AllCollection, mapping.SourceDB)] = util.GetFullCollectionName(cdcreader.AllCollection, mapping.TargetDB)
		}
	}
	return mapCollectionNames
}

func GetCollectionMappingFromTaskInfo(info *meta.TaskInfo) map[string]string {
	mapCollectionNames := make(map[string]string)
	for _, mapping := range info.NameMapping {
		for s, t := range mapping.CollectionMapping {
			mapCollectionNames[util.GetFullCollectionName(s, mapping.SourceDB)] = util.GetFullCollectionName(t, mapping.TargetDB)
		}
		if len(mapping.CollectionMapping) == 0 {
			mapCollectionNames[util.GetFullCollectionName(cdcreader.AllCollection, mapping.SourceDB)] = util.GetFullCollectionName(cdcreader.AllCollection, mapping.TargetDB)
		}
	}
	return mapCollectionNames
}

func matchCollectionName(sampleCollection, targetCollection string) (bool, bool) {
	db1, collection1 := util.GetCollectionNameFromFull(sampleCollection)
	db2, collection2 := util.GetCollectionNameFromFull(targetCollection)
	return (db1 == db2 || db1 == cdcreader.AllDatabase) &&
			(collection1 == collection2 || collection1 == cdcreader.AllCollection),
		db1 == cdcreader.AllDatabase || collection1 == cdcreader.AllCollection
}

func (e *MetaCDC) checkDuplicateCollection(uKey string,
	newCollectionNames []string,
	extraInfo model.ExtraInfo,
	mapCollectionNames map[string]string,
) ([]string, error) {
	e.collectionNames.Lock()
	defer e.collectionNames.Unlock()
	existExtraInfo := e.collectionNames.extraInfos[uKey]
	if existExtraInfo.EnableUserRole && extraInfo.EnableUserRole {
		return nil, servererror.NewClientError("the enable user role param is duplicate")
	}
	if names, ok := e.collectionNames.data[uKey]; ok {
		var duplicateCollections []string
		for _, newCollectionName := range newCollectionNames {
			if lo.Contains(names, newCollectionName) {
				duplicateCollections = append(duplicateCollections, newCollectionName)
				continue
			}
			nd, nc := util.GetCollectionNameFromFull(newCollectionName)
			if nd == cdcreader.AllDatabase && nc == cdcreader.AllCollection {
				continue
			}
			for _, name := range names {
				match, containAny := matchCollectionName(name, newCollectionName)
				if match && containAny && !lo.Contains(e.collectionNames.excludeData[uKey], newCollectionName) {
					duplicateCollections = append(duplicateCollections, newCollectionName)
					break
				}
			}
		}
		if len(duplicateCollections) > 0 {
			log.Info("duplicate collections",
				zap.Strings("request_collections", newCollectionNames),
				zap.Strings("exist_collections", names),
				zap.Strings("exclude_collections", e.collectionNames.excludeData[uKey]),
				zap.Strings("duplicate_collections", duplicateCollections))
			return nil, servererror.NewClientError(fmt.Sprintf("the collection name is duplicate with existing task, %v", duplicateCollections))
		}
	}
	if len(mapCollectionNames) > 0 {
		for name := range mapCollectionNames {
			var match bool
			for _, newName := range newCollectionNames {
				match, _ = matchCollectionName(newName, name)
				if match {
					break
				}
			}
			if !match {
				log.Warn("the collection name in the name mapping is not in the collection info",
					zap.Strings("collection_names", newCollectionNames),
					zap.String("mapping_name", name))
				return nil, servererror.NewClientError("the collection name in the name mapping is not in the collection info, checkout it")
			}
		}
	}
	var excludeCollectionNames []string
	for _, newCollectionName := range newCollectionNames {
		for _, existCollectionName := range e.collectionNames.data[uKey] {
			if match, _ := matchCollectionName(newCollectionName, existCollectionName); match {
				excludeCollectionNames = append(excludeCollectionNames, existCollectionName)
			}
		}
	}
	e.collectionNames.excludeData[uKey] = append(e.collectionNames.excludeData[uKey], excludeCollectionNames...)
	e.collectionNames.data[uKey] = append(e.collectionNames.data[uKey], newCollectionNames...)
	e.collectionNames.extraInfos[uKey] = model.ExtraInfo{
		EnableUserRole: existExtraInfo.EnableUserRole || extraInfo.EnableUserRole,
	}
	nameMappings := e.collectionNames.nameMapping[uKey]
	if nameMappings == nil {
		nameMappings = make(map[string]string)
		e.collectionNames.nameMapping[uKey] = nameMappings
	}
	for s, t := range mapCollectionNames {
		nameMappings[s] = t
	}
	return excludeCollectionNames, nil
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
	uKey := getTaskUniqueIDFromReq(req)
	newCollectionNames := GetCollectionNamesFromReq(req)
	mapCollectionNames := GetCollectionMappingFromReq(req)

	excludeCollectionNames, err := e.checkDuplicateCollection(uKey, newCollectionNames, req.ExtraInfo, mapCollectionNames)
	if err != nil {
		return nil, err
	}

	revertCollectionNames := func() {
		e.collectionNames.Lock()
		defer e.collectionNames.Unlock()
		e.collectionNames.excludeData[uKey] = lo.Without(e.collectionNames.excludeData[uKey], excludeCollectionNames...)
		e.collectionNames.data[uKey] = lo.Without(e.collectionNames.data[uKey], newCollectionNames...)
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
		TaskID:                util.GetUUID(),
		MilvusConnectParam:    req.MilvusConnectParam,
		KafkaConnectParam:     req.KafkaConnectParam,
		CollectionInfos:       req.CollectionInfos,
		DBCollections:         req.DBCollections,
		NameMapping:           req.NameMapping,
		RPCRequestChannelInfo: req.RPCChannelInfo,
		ExtraInfo:             req.ExtraInfo,
		ExcludeCollections:    excludeCollectionNames,
		WriterCacheConfig:     req.BufferConfig,
		State:                 meta.TaskStateInitial,
		DisableAutoStart:      req.DisableAutoStart,
	}
	for _, collectionInfo := range req.CollectionInfos {
		positions := make(map[string]*meta.PositionInfo, len(collectionInfo.Positions))
		collectionID := int64(-1)
		for vchannel, collectionPosition := range collectionInfo.Positions {
			channelInfo, err := util.ParseVChannel(vchannel)
			if err != nil {
				revertCollectionNames()
				return nil, servererror.NewClientError(fmt.Sprintf("the vchannel is invalid, %s, err: %s", vchannel, err.Error()))
			}
			decodePosition, err := util.Base64DecodeMsgPosition(collectionPosition)
			if err != nil {
				return nil, servererror.NewServerError(errors.WithMessage(err, "fail to decode the position data"))
			}
			p := &meta.PositionInfo{
				DataPair: &commonpb.KeyDataPair{
					Key:  channelInfo.PChannelName,
					Data: decodePosition.MsgID,
				},
			}
			positions[channelInfo.PChannelName] = p
			if collectionID == -1 {
				collectionID = channelInfo.CollectionID
			}
			if collectionID != channelInfo.CollectionID {
				revertCollectionNames()
				return nil, servererror.NewClientError("the channel position info should be in the same collection")
			}
		}
		collectionName := collectionInfo.Name
		metaPosition := &meta.TaskCollectionPosition{
			TaskID:         info.TaskID,
			CollectionID:   collectionID,
			CollectionName: collectionName,
			Positions:      positions,
		}
		err = e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Put(ctx, metaPosition, nil)
		if err != nil {
			return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task collection position to etcd"))
		}

		collectionInfo.Positions = make(map[string]string)
	}

	if req.RPCChannelInfo.Position != "" {
		decodePosition, err := util.Base64DecodeMsgPosition(req.RPCChannelInfo.Position)
		if err != nil {
			return nil, servererror.NewServerError(errors.WithMessage(err, "fail to decode the rpc position data"))
		}
		rpcChannel := e.getRPCChannelName(req.RPCChannelInfo)

		metaPosition := &meta.TaskCollectionPosition{
			TaskID:         info.TaskID,
			CollectionID:   model.ReplicateCollectionID,
			CollectionName: model.ReplicateCollectionName,
			Positions: map[string]*meta.PositionInfo{
				rpcChannel: {
					DataPair: &commonpb.KeyDataPair{
						Key:  rpcChannel,
						Data: decodePosition.MsgID,
					},
				},
			},
		}
		err = e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Put(ctx, metaPosition, nil)
		if err != nil {
			return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task rpc position to etcd"))
		}
		req.RPCChannelInfo.Position = ""
	}

	err = e.metaStoreFactory.GetTaskInfoMetaStore(ctx).Put(ctx, info, nil)
	if err != nil {
		revertCollectionNames()
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to put the task info to etcd"))
	}
	metrics.TaskNumVec.Add(info.TaskID, info.State)
	metrics.TaskStateVec.WithLabelValues(info.TaskID).Set(float64(info.State))
	e.cdcTasks.Lock()
	e.cdcTasks.data[info.TaskID] = info
	e.cdcTasks.Unlock()
	err = e.startInternal(info, false)
	if err != nil {
		deleteErr := e.delete(info.TaskID)
		if deleteErr != nil {
			log.Warn("fail to delete the task", zap.String("task_id", info.TaskID), zap.Error(deleteErr))
			return nil, servererror.NewServerError(deleteErr)
		}
		return nil, err
	}

	return &request.CreateResponse{TaskID: info.TaskID}, nil
}

func (e *MetaCDC) getRPCChannelName(channelInfo model.ChannelInfo) string {
	if channelInfo.Name != "" {
		return channelInfo.Name
	}
	return e.config.SourceConfig.ReplicateChan
}

func (e *MetaCDC) validCreateRequest(req *request.CreateRequest) error {
	milvusConnectParam := req.MilvusConnectParam
	kafkaConnectParam := req.KafkaConnectParam
	isMilvusEmpty := milvusConnectParam.URI == "" && milvusConnectParam.Host == "" && milvusConnectParam.Port <= 0
	if isMilvusEmpty && kafkaConnectParam.Address == "" {
		return servererror.NewClientError("the downstream address is empty")
	} else if !isMilvusEmpty && kafkaConnectParam.Address != "" {
		return servererror.NewClientError("don't support milvus and kafka at the same time now")
	}

	if !isMilvusEmpty {
		if milvusConnectParam.URI == "" {
			if milvusConnectParam.Host == "" {
				return servererror.NewClientError("the milvus host is empty")
			}
			if milvusConnectParam.Port <= 0 {
				return servererror.NewClientError("the milvus port is less or equal zero")
			}
		}

		if (milvusConnectParam.Username != "" && milvusConnectParam.Password == "") ||
			(milvusConnectParam.Username == "" && milvusConnectParam.Password != "") {
			return servererror.NewClientError("cannot set only one of the milvus username and password")
		}
		if milvusConnectParam.ConnectTimeout < 0 {
			return servererror.NewClientError("the milvus connect timeout is less zero")
		}
	}

	if kafkaConnectParam.Address != "" {
		if kafkaConnectParam.Topic == "" {
			return servererror.NewClientError("the kafka topic is empty")
		}
	}
	cacheParam := req.BufferConfig
	if cacheParam.Period < 0 {
		return servererror.NewClientError("the cache period is less zero")
	}
	if cacheParam.Size < 0 {
		return servererror.NewClientError("the cache size is less zero")
	}

	if len(req.CollectionInfos) == 0 && len(req.DBCollections) == 0 {
		return servererror.NewClientError("the collection info is empty")
	}
	if len(req.CollectionInfos) > 1 || len(req.DBCollections) > 1 {
		return servererror.NewClientError("the collection info should be only one")
	}
	if len(req.CollectionInfos) == 1 && len(req.DBCollections) == 1 {
		return servererror.NewClientError("the collection info and db collection info should be only one")
	}
	var err error
	if len(req.CollectionInfos) == 1 {
		err = e.checkCollectionInfos(req.CollectionInfos)
	} else if len(req.DBCollections) == 1 {
		for db, infos := range req.DBCollections {
			if len(db) > e.config.MaxNameLength {
				return servererror.NewClientError(fmt.Sprintf("the db name length exceeds %d characters, %s", e.config.MaxNameLength, db))
			}
			err = e.checkCollectionInfos(infos)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	if req.RPCChannelInfo.Name != "" && req.RPCChannelInfo.Name != e.config.SourceConfig.ReplicateChan {
		return servererror.NewClientError("the rpc channel is invalid, the channel name should be the same as the source config")
	}

	if !isMilvusEmpty {
		milvusConnectParam.Token = GetMilvusToken(milvusConnectParam)
		milvusConnectParam.URI = GetMilvusURI(milvusConnectParam)

		_, err := cdcwriter.NewMilvusDataHandler(
			cdcwriter.URIOption(milvusConnectParam.URI),
			cdcwriter.TokenOption(milvusConnectParam.Token),
			cdcwriter.IgnorePartitionOption(milvusConnectParam.IgnorePartition),
			cdcwriter.ConnectTimeoutOption(milvusConnectParam.ConnectTimeout),
			cdcwriter.DialConfigOption(milvusConnectParam.DialConfig),
		)
		if err != nil {
			log.Warn("fail to connect the milvus", zap.Any("connect_param", milvusConnectParam), zap.Error(err))
			return errors.WithMessage(err, "fail to connect the milvus")
		}
	} else if kafkaConnectParam.Address != "" {
		_, err := cdcwriter.NewKafkaDataHandler(
			cdcwriter.KafkaAddressOption(kafkaConnectParam.Address),
			cdcwriter.KafkaTopicOption(kafkaConnectParam.Topic),
		)
		if err != nil {
			log.Warn("fail to connect the kafka", zap.Any("connect_param", kafkaConnectParam), zap.Error(err))
			return errors.WithMessage(err, "fail to connect the kafka")
		}
	}
	return nil
}

func (e *MetaCDC) checkCollectionInfos(infos []model.CollectionInfo) error {
	if len(infos) == 0 {
		return servererror.NewClientError("empty collection info")
	}

	if len(infos) != 1 {
		return servererror.NewClientError("the collection info should be only one.")
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
		if info.Name == cdcreader.AllCollection && len(info.Positions) > 0 {
			// because the position info can't include the collection name when the collection name is `*`
			return servererror.NewClientError("the collection name is `*`, the positions should be empty")
		}
		if len(info.Name) > e.config.MaxNameLength {
			longNames = append(longNames, info.Name)
		}
		for positionChannel := range info.Positions {
			if !cdcreader.IsVirtualChannel(positionChannel) {
				return servererror.NewClientError(fmt.Sprintf("the position channel name is not virtual channel, %s", positionChannel))
			}
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
		errMsg += fmt.Sprintf("there are some collection names whose length exceeds %d characters, %v", e.config.MaxNameLength, longNames)
	}
	return servererror.NewClientError(errMsg)
}

func (e *MetaCDC) startInternal(info *meta.TaskInfo, ignoreUpdateState bool) error {
	taskLog := log.With(zap.String("task_id", info.TaskID))
	uKey := getTaskUniqueIDFromInfo(info)

	e.replicateEntityMap.RLock()
	replicateEntity, ok := e.replicateEntityMap.data[uKey]
	e.replicateEntityMap.RUnlock()

	if !ok {
		var err error
		replicateEntity, err = e.newReplicateEntity(info)
		if err != nil {
			return err
		}
	}

	ctx := context.Background()
	taskPositions, err := e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Get(ctx, &meta.TaskCollectionPosition{TaskID: info.TaskID}, nil)
	if err != nil {
		taskLog.Warn("fail to get the task collection position", zap.Error(err))
		return servererror.NewServerError(errors.WithMessage(err, "fail to get the task collection position"))
	}

	channelSeekPosition := make(map[int64]map[string]*msgpb.MsgPosition)
	for _, taskPosition := range taskPositions {
		collectionSeekPosition := make(map[string]*msgpb.MsgPosition)
		// the positionChannel is pchannel name
		for positionChannel, positionInfo := range taskPosition.Positions {
			positionTs := uint64(0)
			if positionInfo.Time > 0 {
				positionTs = tsoutil.ComposeTS(positionInfo.Time+1, 0)
			}
			collectionSeekPosition[positionChannel] = &msgpb.MsgPosition{
				ChannelName: positionChannel,
				MsgID:       positionInfo.DataPair.Data,
				Timestamp:   positionTs,
			}
		}
		channelSeekPosition[taskPosition.CollectionID] = collectionSeekPosition
	}

	collectionReader, err := cdcreader.NewCollectionReader(info.TaskID,
		replicateEntity.channelManager, replicateEntity.metaOp,
		channelSeekPosition, GetShouldReadFunc(info),
		config.ReaderConfig{
			Retry: e.config.Retry,
		})
	if err != nil {
		taskLog.Warn("fail to new the collection reader", zap.Error(err))
		return servererror.NewServerError(errors.WithMessage(err, "fail to new the collection reader"))
	}
	go func() {
		err := <-collectionReader.ErrorChan()
		if err == nil {
			return
		}
		log.Warn("fail to read the message", zap.Error(err))
		_ = e.pauseTaskWithReason(info.TaskID, "fail to read the message, err:"+err.Error(), []meta.TaskState{})
	}()
	rpcRequestChannelName := e.getRPCChannelName(info.RPCRequestChannelInfo)
	rpcRequestPosition := info.RPCRequestChannelInfo.Position
	if rpcRequestPosition == "" && channelSeekPosition[model.ReplicateCollectionID] != nil {
		replicateSeekPosition := channelSeekPosition[model.ReplicateCollectionID][rpcRequestChannelName]
		if replicateSeekPosition != nil {
			rpcRequestPosition = base64.StdEncoding.EncodeToString(replicateSeekPosition.MsgID)
		}
	}
	channelReader, err := e.getChannelReader(info, replicateEntity, rpcRequestChannelName, rpcRequestPosition)
	if err != nil {
		return err
	}
	readCtx, cancelReadFunc := context.WithCancel(log.WithTraceID(context.Background(), info.TaskID))
	replicateEntity.taskQuitFuncs.Insert(info.TaskID, func() {
		collectionReader.QuitRead(readCtx)
		channelReader.QuitRead(readCtx)
		cancelReadFunc()
	})
	replicateEntity.refCnt.Inc()
	replicateEntity.UpdateMapping(GetCollectionMappingFromTaskInfo(info))

	if !ignoreUpdateState {
		err = store.UpdateTaskState(e.metaStoreFactory.GetTaskInfoMetaStore(ctx), info.TaskID, meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial, meta.TaskStatePaused}, "")
		if err != nil {
			taskLog.Warn("fail to update the task meta", zap.Error(err))
			return servererror.NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+info.TaskID))
		}
	}
	e.cdcTasks.Lock()
	info.State = meta.TaskStateRunning
	info.Reason = ""
	e.cdcTasks.Unlock()
	collectionReader.StartRead(readCtx)
	channelReader.StartRead(readCtx)
	return nil
}

func (e *MetaCDC) newReplicateEntity(info *meta.TaskInfo) (*ReplicateEntity, error) {
	taskLog := log.With(zap.String("task_id", info.TaskID))
	milvusConnectParam := info.MilvusConnectParam
	kafkaAddress := GetKafkaAddress(info.KafkaConnectParam)
	uKey := getTaskUniqueIDFromInfo(info)

	var milvusClient api.TargetAPI
	var err error
	ctx := context.TODO()
	milvusConnectParam.Token = GetMilvusToken(milvusConnectParam)
	milvusConnectParam.URI = GetMilvusURI(milvusConnectParam)
	milvusAddress := milvusConnectParam.URI
	if milvusAddress != "" {
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, time.Duration(milvusConnectParam.ConnectTimeout)*time.Second)
		milvusClient, err = cdcreader.NewTarget(timeoutCtx, cdcreader.TargetConfig{
			URI:        milvusAddress,
			Token:      milvusConnectParam.Token,
			DialConfig: milvusConnectParam.DialConfig,
		})
		cancelFunc()
		if err != nil {
			taskLog.Warn("fail to new target", zap.String("address", milvusAddress), zap.Error(err))
			return nil, servererror.NewClientError("fail to connect target milvus server")
		}
	}
	sourceConfig := e.config.SourceConfig
	etcdServerConfig := GetEtcdServerConfigFromSourceConfig(sourceConfig)
	metaOp, err := cdcreader.NewEtcdOp(etcdServerConfig, sourceConfig.DefaultPartitionName, config.EtcdRetryConfig{
		Retry: e.config.Retry,
	}, milvusClient)
	if err != nil {
		taskLog.Warn("fail to new the meta op", zap.Error(err))
		return nil, servererror.NewClientError("fail to new the meta op")
	}

	mqConfig := config.MQConfig{
		Pulsar: e.config.SourceConfig.Pulsar,
		Kafka:  e.config.SourceConfig.Kafka,
	}
	msgDispatcherClient, err := cdcreader.GetMsgDispatcherClient(e.mqFactoryCreator, mqConfig, false)
	if err != nil {
		taskLog.Warn("fail to get the msg dispatcher client", zap.Error(err))
		return nil, servererror.NewClientError("fail to get the msg dispatcher client")
	}

	msgTTDispatcherClient, _ := cdcreader.GetMsgDispatcherClient(e.mqFactoryCreator, mqConfig, true)
	streamFactory, _ := cdcreader.GetStreamFactory(e.mqFactoryCreator, mqConfig, false)

	var downstream string
	if milvusAddress != "" {
		downstream = "milvus"
	} else if kafkaAddress != "" {
		downstream = "kafka"
	}
	// default value: 10
	bufferSize := e.config.SourceConfig.ReadChanLen
	ttInterval := e.config.SourceConfig.TimeTickInterval
	channelManager, err := cdcreader.NewReplicateChannelManager(
		msgTTDispatcherClient,
		streamFactory,
		milvusClient,
		config.ReaderConfig{
			MessageBufferSize: bufferSize,
			TTInterval:        ttInterval,
			Retry:             e.config.Retry,
			SourceChannelNum:  e.config.SourceConfig.ChannelNum,
			TargetChannelNum:  info.MilvusConnectParam.ChannelNum,
			ReplicateID:       uKey,
		}, metaOp, func(s string, pack *msgstream.MsgPack) {
			replicateMetric(info.TaskID, s, pack, metrics.OPTypeRead)
		}, downstream)
	if err != nil {
		taskLog.Warn("fail to create replicate channel manager", zap.Error(err))
		return nil, servererror.NewClientError("fail to create replicate channel manager")
	}

	var dataHandler api.DataHandler
	if kafkaAddress != "" {
		dataHandler, err = cdcwriter.NewKafkaDataHandler(
			cdcwriter.KafkaAddressOption(info.KafkaConnectParam.Address),
			cdcwriter.KafkaTopicOption(info.KafkaConnectParam.Topic),
		)
	} else if milvusConnectParam.URI != "" {
		targetConfig := milvusConnectParam
		dataHandler, err = cdcwriter.NewMilvusDataHandler(
			cdcwriter.URIOption(targetConfig.URI),
			cdcwriter.TokenOption(targetConfig.Token),
			cdcwriter.IgnorePartitionOption(targetConfig.IgnorePartition),
			cdcwriter.ConnectTimeoutOption(targetConfig.ConnectTimeout),
			cdcwriter.DialConfigOption(targetConfig.DialConfig),
		)
	}
	if err != nil {
		taskLog.Warn("fail to new the data handler", zap.Error(err))
		return nil, servererror.NewClientError("fail to new the data handler, task_id: " + info.TaskID)
	}
	writerObj := cdcwriter.NewChannelWriter(dataHandler, config.WriterConfig{
		MessageBufferSize: bufferSize,
		Retry:             e.config.Retry,
		ReplicateID:       e.config.ReplicateID,
	}, metaOp.GetAllDroppedObj(), downstream)
	e.replicateEntityMap.Lock()
	defer e.replicateEntityMap.Unlock()
	// TODO fubang should be fix
	entity, ok := e.replicateEntityMap.data[uKey]
	if !ok {
		replicateCtx, cancelReplicateFunc := context.WithCancel(ctx)
		channelManager.SetCtx(replicateCtx)
		entity = &ReplicateEntity{
			targetClient:   milvusClient,
			channelManager: channelManager,
			metaOp:         metaOp,
			writerObj:      writerObj,
			entityQuitFunc: cancelReplicateFunc,
			mqDispatcher:   msgDispatcherClient,
			mqTTDispatcher: msgTTDispatcherClient,
			taskQuitFuncs:  typeutil.NewConcurrentMap[string, func()](),
		}
		e.replicateEntityMap.data[uKey] = entity
		e.startReplicateAPIEvent(replicateCtx, entity)
		e.startReplicateDMLChannel(replicateCtx, entity)
	}
	return entity, nil
}

func (e *MetaCDC) startReplicateAPIEvent(replicateCtx context.Context, entity *ReplicateEntity) {
	go func() {
		for {
			select {
			case <-replicateCtx.Done():
				log.Warn("event chan, the replicate context has closed")
				return
			case replicateAPIEvent, ok := <-entity.channelManager.GetEventChan():
				taskID := replicateAPIEvent.TaskID
				if !ok {
					log.Warn("the replicate api event channel has closed", zap.String("task_id", taskID))
					return
				}
				if replicateAPIEvent.EventType == api.ReplicateError {
					log.Warn("receive the error event", zap.Any("event", replicateAPIEvent), zap.String("task_id", taskID))
					_ = e.pauseTaskWithReason(taskID, "fail to read the replicate event", []meta.TaskState{})
					return
				}
				if !e.isRunningTask(taskID) {
					log.Warn("not running task", zap.Any("event", replicateAPIEvent), zap.String("task_id", taskID))
					return
				}
				if replicateAPIEvent.EventType == api.ReplicateCreateCollection {
					writeCallback := NewWriteCallback(e.metaStoreFactory, e.rootPath, taskID)
					collectionID := replicateAPIEvent.CollectionInfo.ID
					collectionName := replicateAPIEvent.CollectionInfo.Schema.Name
					msgTime, _ := tsoutil.ParseHybridTs(replicateAPIEvent.CollectionInfo.CreateTime)
					for _, startPosition := range replicateAPIEvent.CollectionInfo.StartPositions {
						metaPosition := &meta.PositionInfo{
							Time: msgTime,
							DataPair: &commonpb.KeyDataPair{
								Key:  startPosition.Key,
								Data: startPosition.Data,
							},
						}
						err := writeCallback.UpdateTaskCollectionPosition(collectionID, collectionName, funcutil.ToPhysicalChannel(startPosition.Key),
							metaPosition, metaPosition, nil)
						if err != nil {
							log.Warn("fail to update the collection start position",
								zap.String("name", collectionName),
								zap.Int64("id", collectionID),
								zap.String("task_id", taskID),
								zap.Error(err))
							_ = e.pauseTaskWithReason(taskID, "fail to update start task position, err:"+err.Error(), []meta.TaskState{})
							return
						}
					}
				}
				err := entity.writerObj.HandleReplicateAPIEvent(replicateCtx, replicateAPIEvent)
				if err != nil {
					log.Warn("fail to handle replicate event", zap.Any("event", replicateAPIEvent),
						zap.String("task_id", taskID),
						zap.Error(err))
					_ = e.pauseTaskWithReason(taskID, "fail to handle the replicate event, err: "+err.Error(), []meta.TaskState{})
					return
				}
				metrics.APIExecuteCountVec.WithLabelValues(taskID, replicateAPIEvent.EventType.String()).Inc()
			}
		}
	}()
}

func (e *MetaCDC) startReplicateDMLChannel(replicateCtx context.Context, entity *ReplicateEntity) {
	go func() {
		for {
			select {
			case <-replicateCtx.Done():
				log.Warn("channel chan, the replicate context has closed")
				return
			case channelName, ok := <-entity.channelManager.GetChannelChan():
				log.Info("start to replicate channel", zap.String("channel", channelName))
				if !ok {
					log.Warn("the channel name channel has closed")
					return
				}
				e.startReplicateDMLMsg(replicateCtx, entity, channelName)
			}
		}
	}()
}

func (e *MetaCDC) startReplicateDMLMsg(replicateCtx context.Context, entity *ReplicateEntity, channelName string) {
	go func() {
		msgChan := entity.channelManager.GetMsgChan(channelName)
		if msgChan == nil {
			log.Warn("not found the message channel", zap.String("channel", channelName))
			return
		}
		for {
			select {
			case <-replicateCtx.Done():
				log.Warn("msg chan, the replicate context has closed")
				return
			case replicateMsg, ok := <-msgChan:
				taskID := replicateMsg.TaskID
				if !ok {
					log.Warn("the data channel has closed", zap.String("task_id", taskID))
					return
				}
				if !e.isRunningTask(taskID) {
					log.Warn("not running task", zap.Any("pack", replicateMsg), zap.String("task_id", taskID))
					return
				}
				msgPack := replicateMsg.MsgPack
				if msgPack == nil {
					log.Warn("the message pack is nil, the task may be stopping", zap.String("task_id", taskID))
					return
				}
				if replicateMsg.CollectionName == "" || replicateMsg.CollectionID == 0 {
					log.Warn("fail to handle the replicate message",
						zap.String("collection_name", replicateMsg.CollectionName),
						zap.Int64("collection_id", replicateMsg.CollectionID),
						zap.String("task_id", taskID),
					)
					_ = e.pauseTaskWithReason(taskID, "fail to handle replicate message, invalid collection name or id", []meta.TaskState{})
					return
				}
				streamChannelName := replicateMsg.PChannelName
				targetPChannel := msgPack.EndPositions[0].GetChannelName()
				if cdcreader.IsVirtualChannel(targetPChannel) {
					targetPChannel = funcutil.ToPhysicalChannel(targetPChannel)
				}
				position, targetPosition, err := entity.writerObj.HandleReplicateMessage(replicateCtx, targetPChannel, msgPack)
				if err != nil {
					log.Warn("fail to handle the replicate message",
						zap.Any("pack", msgPack),
						zap.String("task_id", taskID),
						zap.Error(err),
					)
					_ = e.pauseTaskWithReason(taskID, "fail to handle replicate message, err:"+err.Error(), []meta.TaskState{})
					return
				}
				msgTime, _ := tsoutil.ParseHybridTs(msgPack.EndTs)
				replicateMetric(taskID, streamChannelName, msgPack, metrics.OPTypeWrite)

				metaPosition := &meta.PositionInfo{
					Time: msgTime,
					DataPair: &commonpb.KeyDataPair{
						Key:  streamChannelName,
						Data: position,
					},
				}
				var metaOpPosition *meta.PositionInfo
				if len(msgPack.Msgs) > 0 && msgPack.Msgs[0].Type() != commonpb.MsgType_TimeTick {
					metaOpPosition = metaPosition
					metrics.APIExecuteCountVec.WithLabelValues(taskID, "ReplicateMessage").Inc()
				}
				metaTargetPosition := &meta.PositionInfo{
					Time: msgTime,
					DataPair: &commonpb.KeyDataPair{
						Key:  targetPChannel,
						Data: targetPosition,
					},
				}
				if position != nil {
					msgCollectionName := replicateMsg.CollectionName
					msgCollectionID := replicateMsg.CollectionID
					writeCallback := NewWriteCallback(e.metaStoreFactory, e.rootPath, taskID)
					err = writeCallback.UpdateTaskCollectionPosition(msgCollectionID, msgCollectionName, streamChannelName,
						metaPosition, metaOpPosition, metaTargetPosition)
					if err != nil {
						log.Warn("fail to update the collection position", zap.Any("pack", msgPack), zap.Error(err))
						_ = e.pauseTaskWithReason(taskID, "fail to update task position, err:"+err.Error(), []meta.TaskState{})
						return
					}
				}
			}
		}
	}()
}

func replicateMetric(taskID string, channelName string, msgPack *msgstream.MsgPack, op string) {
	msgTime, _ := tsoutil.ParseHybridTs(msgPack.EndTs)
	metrics.ReplicateTimeVec.
		WithLabelValues(taskID, channelName, op).
		Set(float64(msgTime))
	var packSize int
	for _, msg := range msgPack.Msgs {
		packSize += msg.Size()
		switch realMsg := msg.(type) {
		case *msgstream.InsertMsg:
			metrics.ReplicateDataCntVec.WithLabelValues(taskID,
				strconv.FormatInt(realMsg.GetCollectionID(), 10), realMsg.GetCollectionName(), op, "insert").Add(float64(realMsg.GetNumRows()))
		case *msgstream.DeleteMsg:
			metrics.ReplicateDataCntVec.WithLabelValues(taskID,
				strconv.FormatInt(realMsg.GetCollectionID(), 10), realMsg.GetCollectionName(), op, "delete").Add(float64(realMsg.GetNumRows()))
		}
	}
	metrics.ReplicateDataSizeVec.WithLabelValues(taskID, channelName, op).Add(float64(packSize))
}

func (e *MetaCDC) getChannelReader(info *meta.TaskInfo, replicateEntity *ReplicateEntity, channelName, channelPosition string) (api.Reader, error) {
	taskLog := log.With(zap.String("task_id", info.TaskID))
	// collectionName := info.CollectionNames()[0]
	// databaseName := getDatabaseName(info)
	// isAnyCollection := collectionName == cdcreader.AllCollection
	// isAnyDatabase := databaseName == cdcreader.AllDatabase
	// isTmpCollection := collectionName == model.TmpCollectionName

	dataHandleFunc := func(funcCtx context.Context, pack *msgstream.MsgPack) bool {
		if !e.isRunningTask(info.TaskID) {
			taskLog.Warn("not running task", zap.Any("pack", pack))
			return false
		}
		msgTime, _ := tsoutil.ParseHybridTs(pack.EndTs)

		metrics.ReplicateTimeVec.
			WithLabelValues(info.TaskID, channelName, metrics.OPTypeRead).
			Set(float64(msgTime))

		msgCollectionName := util.GetCollectionNameFromMsgPack(pack)
		msgDatabaseName := util.GetDatabaseNameFromMsgPack(pack)
		// TODO it should be changed if replicate the user and role info or multi collection
		// TODO how to handle it when there are "*" and "foo" collection names in the task list
		if msgCollectionName == "" && msgDatabaseName == "" {
			extraSkip := true
			if info.ExtraInfo.EnableUserRole && util.IsUserRoleMessage(pack) {
				extraSkip = false
			}
			if extraSkip {
				return true
			}
		} else {
			// skip the msg when db or collection name is not matched
			collectionInfos := GetCollectionInfos(info, msgDatabaseName, msgCollectionName)
			if collectionInfos == nil {
				return true
			}
			if msgCollectionName != "" && !MatchCollection(info, collectionInfos, msgDatabaseName, msgCollectionName) {
				return true
			}
		}

		positionBytes, err := replicateEntity.writerObj.HandleOpMessagePack(funcCtx, pack)
		if err != nil {
			taskLog.Warn("fail to handle the op message pack", zap.Any("pack", pack), zap.Error(err))
			_ = e.pauseTaskWithReason(info.TaskID, "fail to handle the op message pack, err:"+err.Error(), []meta.TaskState{})
			return false
		}

		metrics.ReplicateTimeVec.
			WithLabelValues(info.TaskID, channelName, metrics.OPTypeWrite).
			Set(float64(msgTime))
		metrics.APIExecuteCountVec.WithLabelValues(info.TaskID, pack.Msgs[0].Type().String()).Inc()

		rpcChannelName := e.getRPCChannelName(info.RPCRequestChannelInfo)
		metaPosition := &meta.PositionInfo{
			Time: msgTime,
			DataPair: &commonpb.KeyDataPair{
				Key:  rpcChannelName,
				Data: positionBytes,
			},
		}
		writeCallback := NewWriteCallback(e.metaStoreFactory, e.rootPath, info.TaskID)
		err = writeCallback.UpdateTaskCollectionPosition(model.ReplicateCollectionID, model.ReplicateCollectionName, channelName,
			metaPosition, metaPosition, nil)
		if err != nil {
			log.Warn("fail to update the collection position", zap.Any("pack", pack), zap.Error(err))
			_ = e.pauseTaskWithReason(info.TaskID, "fail to update task position, err:"+err.Error(), []meta.TaskState{})
			return false
		}
		return true
	}

	channelReader, err := cdcreader.NewChannelReader(channelName, channelPosition, replicateEntity.mqDispatcher, info.TaskID, dataHandleFunc)
	if err != nil {
		taskLog.Warn("fail to new the channel reader", zap.Error(err))
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to new the channel reader"))
	}
	return channelReader, nil
}

func (e *MetaCDC) isRunningTask(taskID string) bool {
	e.cdcTasks.RLock()
	defer e.cdcTasks.RUnlock()
	task, ok := e.cdcTasks.data[taskID]
	if !ok {
		return false
	}
	return task.State == meta.TaskStateRunning
}

func (e *MetaCDC) pauseTaskWithReason(taskID, reason string, currentStates []meta.TaskState) error {
	log.Info("pause task", zap.String("task_id", taskID), zap.String("reason", reason))
	err := store.UpdateTaskState(
		e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()),
		taskID,
		meta.TaskStatePaused,
		currentStates,
		reason)
	if err != nil {
		log.Warn("fail to update task reason", zap.String("task_id", taskID), zap.String("reason", reason))
	}
	e.cdcTasks.Lock()
	cdcTask := e.cdcTasks.data[taskID]
	if cdcTask == nil {
		e.cdcTasks.Unlock()
		return err
	}
	cdcTask.State = meta.TaskStatePaused
	cdcTask.Reason = reason
	metrics.TaskStateVec.WithLabelValues(cdcTask.TaskID).Set(float64(cdcTask.State))
	e.cdcTasks.Unlock()

	var uKey string
	milvusURI := GetMilvusURI(cdcTask.MilvusConnectParam)
	kafkaAddress := GetKafkaAddress(cdcTask.KafkaConnectParam)
	uKey = milvusURI + kafkaAddress
	e.replicateEntityMap.Lock()
	if replicateEntity, ok := e.replicateEntityMap.data[uKey]; ok {
		if quitFunc, ok := replicateEntity.taskQuitFuncs.GetAndRemove(taskID); ok {
			quitFunc()
			replicateEntity.refCnt.Dec()
		}
		if replicateEntity.refCnt.Load() == 0 {
			replicateEntity.entityQuitFunc()
			delete(e.replicateEntityMap.data, uKey)
		}
	}
	e.replicateEntityMap.Unlock()
	return err
}

func (e *MetaCDC) Delete(req *request.DeleteRequest) (*request.DeleteResponse, error) {
	e.cdcTasks.RLock()
	_, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		if req.IgnoreNotFound {
			return &request.DeleteResponse{}, nil
		}
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}

	err := e.delete(req.TaskID)
	if err != nil {
		return nil, servererror.NewServerError(err)
	}
	return &request.DeleteResponse{}, nil
}

func (e *MetaCDC) delete(taskID string) error {
	e.cdcTasks.RLock()
	_, ok := e.cdcTasks.data[taskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return errors.Errorf("not found the task, task_id: " + taskID)
	}

	var err error
	var info *meta.TaskInfo

	info, err = store.DeleteTask(e.metaStoreFactory, taskID)
	if err != nil {
		return errors.WithMessage(err, "fail to delete the task meta, task_id: "+taskID)
	}
	uKey := getTaskUniqueIDFromInfo(info)
	collectionNames := GetCollectionNamesFromTaskInfo(info)
	e.collectionNames.Lock()
	e.collectionNames.excludeData[uKey] = lo.Without(e.collectionNames.excludeData[uKey], info.ExcludeCollections...)
	e.collectionNames.data[uKey] = lo.Without(e.collectionNames.data[uKey], collectionNames...)
	e.collectionNames.Unlock()

	e.cdcTasks.Lock()
	delete(e.cdcTasks.data, taskID)
	e.cdcTasks.Unlock()

	e.replicateEntityMap.Lock()
	if replicateEntity, ok := e.replicateEntityMap.data[uKey]; ok {
		if quitFunc, ok := replicateEntity.taskQuitFuncs.GetAndRemove(taskID); ok {
			quitFunc()
			replicateEntity.refCnt.Dec()
		}
		if replicateEntity.refCnt.Load() == 0 {
			replicateEntity.entityQuitFunc()
			delete(e.replicateEntityMap.data, uKey)
		}
	}
	e.replicateEntityMap.Unlock()

	return err
}

func (e *MetaCDC) Pause(req *request.PauseRequest) (*request.PauseResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	if !ok {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}
	if cdcTask.State == meta.TaskStatePaused {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("the task has paused, task_id: " + req.TaskID)
	}
	e.cdcTasks.RUnlock()

	err := e.pauseTaskWithReason(req.TaskID, "manually pause through http interface", []meta.TaskState{meta.TaskStateRunning})
	if err != nil {
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to update the task state, task_id: "+req.TaskID))
	}

	return &request.PauseResponse{}, err
}

func (e *MetaCDC) Resume(req *request.ResumeRequest) (*request.ResumeResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	if !ok {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("not found the task, task_id: " + req.TaskID)
	}
	if cdcTask.State == meta.TaskStateRunning {
		e.cdcTasks.RUnlock()
		return nil, servererror.NewClientError("the task has running, task_id: " + req.TaskID)
	}
	e.cdcTasks.RUnlock()

	if err := e.startInternal(cdcTask, false); err != nil {
		log.Warn("fail to start the task", zap.Error(err))
		return nil, servererror.NewServerError(errors.WithMessage(err, "fail to start the task, task_id: "+req.TaskID))
	}

	return &request.ResumeResponse{}, nil
}

func (e *MetaCDC) Get(req *request.GetRequest) (*request.GetResponse, error) {
	if req.TaskID == "" {
		return nil, servererror.NewClientError("task_id is empty")
	}
	taskInfo, err := store.GetTaskInfo(e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()), req.TaskID)
	if err != nil {
		if errors.Is(err, servererror.NotFoundErr) {
			return nil, servererror.NewClientError(err.Error())
		}
		return nil, servererror.NewServerError(err)
	}
	return &request.GetResponse{
		Task: request.GetTask(taskInfo),
	}, nil
}

func (e *MetaCDC) GetPosition(req *request.GetPositionRequest) (*request.GetPositionResponse, error) {
	ctx := context.Background()
	positions, err := e.metaStoreFactory.GetTaskCollectionPositionMetaStore(ctx).Get(ctx, &meta.TaskCollectionPosition{TaskID: req.TaskID}, nil)
	if err != nil {
		return nil, servererror.NewServerError(err)
	}
	resp := &request.GetPositionResponse{}
	for _, position := range positions {
		for s, info := range position.Positions {
			msgID, err := EncodeMetaPosition(info)
			if err != nil {
				return nil, servererror.NewServerError(err)
			}
			resp.Positions = append(resp.Positions, request.Position{
				ChannelName: s,
				Time:        info.Time,
				TT:          tsoutil.ComposeTS(info.Time+1, 0),
				MsgID:       msgID,
			})
		}
		for s, info := range position.OpPositions {
			msgID, err := EncodeMetaPosition(info)
			if err != nil {
				return nil, servererror.NewServerError(err)
			}
			resp.OpPositions = append(resp.OpPositions, request.Position{
				ChannelName: s,
				Time:        info.Time,
				TT:          tsoutil.ComposeTS(info.Time+1, 0),
				MsgID:       msgID,
			})
		}
		for s, info := range position.TargetPositions {
			msgID, err := EncodeMetaPosition(info)
			if err != nil {
				return nil, servererror.NewServerError(err)
			}
			resp.TargetPositions = append(resp.TargetPositions, request.Position{
				ChannelName: s,
				Time:        info.Time,
				TT:          tsoutil.ComposeTS(info.Time+1, 0),
				MsgID:       msgID,
			})
		}
	}
	return resp, nil
}

func EncodeMetaPosition(position *meta.PositionInfo) (string, error) {
	msgPosition := &msgpb.MsgPosition{
		ChannelName: position.DataPair.Key,
		MsgID:       position.DataPair.Data,
	}
	positionBytes, err := proto.Marshal(msgPosition)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(positionBytes), nil
}

func (e *MetaCDC) List(req *request.ListRequest) (*request.ListResponse, error) {
	taskInfos, err := store.GetAllTaskInfo(e.metaStoreFactory.GetTaskInfoMetaStore(context.Background()))
	if err != nil && !errors.Is(err, servererror.NotFoundErr) {
		return nil, servererror.NewServerError(err)
	}
	return &request.ListResponse{
		Tasks: lo.Map(taskInfos, func(t *meta.TaskInfo, _ int) request.Task {
			return request.GetTask(t)
		}),
	}, nil
}

func (e *MetaCDC) Maintenance(req *request.MaintenanceRequest) (*request.MaintenanceResponse, error) {
	return maintenance.Handle(req)
}

func GetShouldReadFunc(taskInfo *meta.TaskInfo) cdcreader.ShouldReadFunc {
	return func(databaseInfo *coremodel.DatabaseInfo, collectionInfo *pb.CollectionInfo) bool {
		currentCollectionName := collectionInfo.Schema.Name
		if databaseInfo.Dropped {
			log.Info("database is dropped", zap.String("database", databaseInfo.Name), zap.String("collection", currentCollectionName))
			return false
		}
		taskCollectionInfos := GetCollectionInfos(taskInfo, databaseInfo.Name, currentCollectionName)
		if taskCollectionInfos == nil {
			return false
		}
		return MatchCollection(taskInfo, taskCollectionInfos, databaseInfo.Name, currentCollectionName)
	}
}

func GetCollectionInfos(taskInfo *meta.TaskInfo, dbName string, collectionName string) []model.CollectionInfo {
	var taskCollectionInfos []model.CollectionInfo
	if len(taskInfo.CollectionInfos) > 0 {
		if dbName != cdcreader.DefaultDatabase {
			return nil
		}
		taskCollectionInfos = taskInfo.CollectionInfos
	}
	if len(taskInfo.DBCollections) > 0 {
		taskCollectionInfos = taskInfo.DBCollections[dbName]
		if taskCollectionInfos == nil {
			isExclude := lo.ContainsBy(taskInfo.ExcludeCollections, func(s string) bool {
				db, collection := util.GetCollectionNameFromFull(s)
				return db == dbName && collection == collectionName
			})
			if isExclude {
				return nil
			}
			taskCollectionInfos = taskInfo.DBCollections[cdcreader.AllDatabase]
		}
	}
	return taskCollectionInfos
}

func MatchCollection(taskInfo *meta.TaskInfo, taskCollectionInfos []model.CollectionInfo, currentDatabaseName, currentCollectionName string) bool {
	isAllCollection := taskCollectionInfos[0].Name == cdcreader.AllCollection

	notStarContains := !isAllCollection && lo.ContainsBy(taskCollectionInfos, func(taskCollectionInfo model.CollectionInfo) bool {
		return taskCollectionInfo.Name == currentCollectionName
	})
	starContains := isAllCollection && !lo.ContainsBy(taskInfo.ExcludeCollections, func(s string) bool {
		match, _ := matchCollectionName(s, util.GetFullCollectionName(currentCollectionName, currentDatabaseName))
		return match
	})
	return notStarContains || starContains
}
