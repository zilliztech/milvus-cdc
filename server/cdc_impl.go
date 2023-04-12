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
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/samber/lo"
	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/pb"
	cdcreader "github.com/zilliztech/milvus-cdc/core/reader"
	"github.com/zilliztech/milvus-cdc/core/util"
	cdcwriter "github.com/zilliztech/milvus-cdc/core/writer"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/meta"
	"github.com/zilliztech/milvus-cdc/server/model/request"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type MetaCDC struct {
	BaseCDC
	etcdCli  util.KVApi
	rootPath string
	config   *CDCServerConfig

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
	cli, err := util.GetEtcdClient(serverConfig.EtcdConfig.Endpoints)
	if err != nil {
		log.Panic("fail to get etcd client for saving cdc meta data", zap.Error(err))
	}
	_, err = util.GetEtcdClient(serverConfig.SourceConfig.EtcdAddress)
	if err != nil {
		log.Panic("fail to get etcd client for connect the source etcd data", zap.Error(err))
	}
	// TODO check mq status

	cdc := &MetaCDC{
		etcdCli:  cli,
		rootPath: serverConfig.EtcdConfig.RootPath,
		config:   serverConfig,
	}
	cdc.collectionNames.data = make(map[string][]string)
	cdc.collectionNames.excludeData = make(map[string][]string)
	cdc.cdcTasks.data = make(map[string]*CDCTask)
	cdc.factoryCreator = NewCDCFactory
	return cdc
}

func (e *MetaCDC) ReloadTask() {
	taskPrefixKey := getTaskInfoPrefix(e.rootPath)
	taskResp, err := util.EtcdGet(e.etcdCli, taskPrefixKey, clientv3.WithPrefix())
	if err != nil {
		log.Panic("fail to get all task info", zap.String("key", taskPrefixKey), zap.Error(err))
	}
	taskInfos := make(map[string]*meta.TaskInfo)
	for _, kv := range taskResp.Kvs {
		info := &meta.TaskInfo{}
		err = json.Unmarshal(kv.Value, info)
		if err != nil {
			log.Panic("fail to unmarshal the task byte", zap.String("key", util.ToString(kv.Key)), zap.Error(err))
		}
		taskInfos[info.TaskID] = info
		milvusAddress := fmt.Sprintf("%s:%d", info.MilvusConnectParam.Host, info.MilvusConnectParam.Port)
		newCollectionNames := lo.Map(info.CollectionInfos, func(t model.CollectionInfo, _ int) string {
			return t.Name
		})
		e.collectionNames.data[milvusAddress] = append(e.collectionNames.data[milvusAddress], newCollectionNames...)
		e.collectionNames.excludeData[milvusAddress] = append(e.collectionNames.excludeData[milvusAddress], info.ExcludeCollections...)
	}
	for _, taskInfo := range taskInfos {
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
			return nil, NewClientError(fmt.Sprintf("some collections are duplicate with existing tasks, %v", lo.Map(duplicateCollections, func(t model.CollectionInfo, i int) string {
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
				return nil, NewClientError(fmt.Sprintf("some collections are duplicate with existing tasks, check the `*` collection task and other tasks, %v", lo.Map(duplicateCollections, func(t model.CollectionInfo, i int) string {
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

	getResp, err := util.EtcdGet(e.etcdCli, getTaskInfoPrefix(e.rootPath), clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return nil, NewServerError(errors.WithMessage(err, "fail to get task list to check num"))
	}
	if getResp.Count >= int64(e.config.MaxTaskNum) {
		return nil, NewServerError(errors.Newf("the task num has reach the limit, %d", e.config.MaxTaskNum))
	}

	info := &meta.TaskInfo{
		TaskID:             e.getUuid(),
		MilvusConnectParam: req.MilvusConnectParam,
		CollectionInfos:    req.CollectionInfos,
		ExcludeCollections: excludeCollectionNames,
		WriterCacheConfig:  req.BufferConfig,
		State:              meta.TaskStateInitial,
	}
	infoByte, err := json.Marshal(info)
	if err != nil {
		revertCollectionNames()
		return nil, NewServerError(errors.WithMessage(err, "fail to marshal the task info"))
	}
	err = util.EtcdPut(e.etcdCli, getTaskInfoKey(e.rootPath, info.TaskID), util.ToString(infoByte))
	if err != nil {
		revertCollectionNames()
		return nil, NewServerError(errors.WithMessage(err, "fail to put the task info to etcd"))
	}

	info.State = meta.TaskStateRunning
	task, err := e.newCdcTask(info)
	if err != nil {
		log.Warn("fail to new cdc task", zap.Error(err))
		return nil, NewServerError(err)
	}
	if err = <-task.Resume(func() error {
		err = updateTaskState(e.etcdCli, e.rootPath, info.TaskID,
			meta.TaskStateRunning, []meta.TaskState{meta.TaskStateInitial})
		if err != nil {
			return NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+info.TaskID))
		}
		return nil
	}); err != nil {
		log.Warn("fail to start cdc task", zap.Error(err))
		return nil, NewServerError(err)
	}

	return &request.CreateResponse{TaskID: info.TaskID}, nil
}

func (e *MetaCDC) validCreateRequest(req *request.CreateRequest) error {
	connectParam := req.MilvusConnectParam
	if connectParam.Host == "" {
		return NewClientError("the milvus host is empty")
	}
	if connectParam.Port <= 0 {
		return NewClientError("the milvus port is less or equal zero")
	}
	if (connectParam.Username != "" && connectParam.Password == "") ||
		(connectParam.Username == "" && connectParam.Password != "") {
		return NewClientError("cannot set only one of the milvus username and password")
	}
	if connectParam.ConnectTimeout < 0 {
		return NewClientError("the milvus connect timeout is less zero")
	}
	cacheParam := req.BufferConfig
	if cacheParam.Period < 0 {
		return NewClientError("the cache period is less zero")
	}
	if cacheParam.Size < 0 {
		return NewClientError("the cache size is less zero")
	}

	if err := e.checkCollectionInfos(req.CollectionInfos); err != nil {
		return err
	}
	_, err := cdcwriter.NewMilvusDataHandler(
		cdcwriter.AddressOption(fmt.Sprintf("%s:%d", connectParam.Host, connectParam.Port)),
		cdcwriter.UserOption(connectParam.Username, connectParam.Password),
		cdcwriter.TlsOption(connectParam.EnableTls),
		cdcwriter.IgnorePartitionOption(connectParam.IgnorePartition),
		cdcwriter.ConnectTimeoutOption(connectParam.ConnectTimeout))
	if err != nil {
		return errors.WithMessage(err, "fail to connect the milvus")
	}
	return nil
}

func (e *MetaCDC) checkCollectionInfos(infos []model.CollectionInfo) error {
	if len(infos) == 0 {
		return NewClientError("empty collection info")
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
			return NewClientError(fmt.Sprintf("make sure the only one collection if you want to use the '*' collection param, current param: %v",
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
	return NewClientError(errMsg)
}

func (e *MetaCDC) getUuid() string {
	uid := uuid.Must(uuid.NewRandom())
	return uid.String()
}

func (e *MetaCDC) newCdcTask(info *meta.TaskInfo) (*CDCTask, error) {
	streamingCollectionCountVec.WithLabelValues(info.TaskID, totalStatusLabel).Add(float64(len(info.CollectionInfos)))

	e.cdcTasks.Lock()
	e.cdcTasks.data[info.TaskID] = EmptyCdcTask
	taskNumVec.AddInitial()
	e.cdcTasks.Unlock()

	newReaderFunc := NewReaderFunc(func() (cdcreader.CDCReader, error) {
		var err error
		taskLog := log.With(zap.String("task_id", info.TaskID), zap.Error(err))
		positionPrefixKey := getTaskCollectionPositionPrefixWithTaskID(e.rootPath, info.TaskID)
		positionResp, err := util.EtcdGet(e.etcdCli, positionPrefixKey, clientv3.WithPrefix())
		if err != nil {
			taskLog.Warn("fail to get the task meta", zap.String("prefix_key", positionPrefixKey))
			return nil, errors.WithMessage(err, "fail to get the task meta, task_id: "+info.TaskID)
		}

		taskPosition := make(map[string]map[string]*commonpb.KeyDataPair)
		for _, kv := range positionResp.Kvs {
			positions := &meta.TaskCollectionPosition{}
			err = json.Unmarshal(kv.Value, positions)
			if err != nil {
				positionKey := util.ToString(kv.Key)
				taskLog.Warn("fail to unmarshal the task byte", zap.String("key", positionKey))
				return nil, errors.WithMessage(err, "fail to unmarshal the task byte, task_id: "+info.TaskID)
			}
			taskPosition[positions.CollectionName] = positions.Positions
		}

		var options []config.Option[*cdcreader.MilvusCollectionReader]
		for _, collectionInfo := range info.CollectionInfos {
			options = append(options, cdcreader.CollectionInfoOption(collectionInfo.Name, taskPosition[collectionInfo.Name]))
		}
		sourceConfig := e.config.SourceConfig
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

	writeCallback := NewWriteCallback(e.etcdCli, e.rootPath, info.TaskID)
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
		err := updateTaskState(e.etcdCli, e.rootPath, info.TaskID,
			meta.TaskStatePaused, []meta.TaskState{meta.TaskStateRunning})
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
		return nil, NewClientError("not found the task, task_id: " + req.TaskID)
	}

	var err error

	err = <-cdcTask.Terminate(func() error {
		var info *meta.TaskInfo
		info, err = deleteTask(e.etcdCli, e.rootPath, req.TaskID)
		if err != nil {
			return NewServerError(errors.WithMessage(err, "fail to delete the task meta, task_id: "+req.TaskID))
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
		return nil, NewServerError(errors.WithMessage(err, "fail to terminate the task, task_id: "+req.TaskID))
	}

	return &request.DeleteResponse{}, err
}

func (e *MetaCDC) Pause(req *request.PauseRequest) (*request.PauseResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return nil, NewClientError("not found the task, task_id: " + req.TaskID)
	}

	var err error

	err = <-cdcTask.Pause(func() error {
		err = updateTaskState(e.etcdCli, e.rootPath, req.TaskID,
			meta.TaskStatePaused, []meta.TaskState{meta.TaskStateRunning})
		if err != nil {
			return NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+req.TaskID))
		}
		return nil
	})
	if err != nil {
		return nil, NewServerError(errors.WithMessage(err, "fail to pause the task state, task_id: "+req.TaskID))
	}

	return &request.PauseResponse{}, err
}

func (e *MetaCDC) Resume(req *request.ResumeRequest) (*request.ResumeResponse, error) {
	e.cdcTasks.RLock()
	cdcTask, ok := e.cdcTasks.data[req.TaskID]
	e.cdcTasks.RUnlock()
	if !ok {
		return nil, NewClientError("not found the task, task_id: " + req.TaskID)
	}

	var err error

	err = <-cdcTask.Resume(func() error {
		err = updateTaskState(e.etcdCli, e.rootPath, req.TaskID,
			meta.TaskStateRunning, []meta.TaskState{meta.TaskStatePaused})
		if err != nil {
			return NewServerError(errors.WithMessage(err, "fail to update the task meta, task_id: "+req.TaskID))
		}
		return nil
	})
	if err != nil {
		return nil, NewServerError(errors.WithMessage(err, "fail to resume the task state, task_id: "+req.TaskID))
	}

	return &request.ResumeResponse{}, err
}

func (e *MetaCDC) Get(req *request.GetRequest) (*request.GetResponse, error) {
	taskInfo, err := getTaskInfo(e.etcdCli, e.rootPath, req.TaskID)
	if err != nil {
		if errors.Is(err, NotFoundErr) {
			return nil, NewClientError(err.Error())
		}
		return nil, NewServerError(err)
	}
	return &request.GetResponse{
		Task: request.GetTask(taskInfo),
	}, nil
}

func (e *MetaCDC) List(req *request.ListRequest) (*request.ListResponse, error) {
	taskInfos, err := getAllTaskInfo(e.etcdCli, e.rootPath)
	if err != nil && !errors.Is(err, NotFoundErr) {
		return nil, NewServerError(err)
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
