/*
 * Licensed to the LF AI & Data foundation under one
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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/goccy/go-json"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"go.uber.org/zap"

	config2 "github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/log"
	cdcerror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/maintenance"
	"github.com/zilliztech/milvus-cdc/server/metrics"
	modelrequest "github.com/zilliztech/milvus-cdc/server/model/request"
)

type CDCServer struct {
	api          CDCService
	serverConfig *CDCServerConfig
}

func (c *CDCServer) Run(config *CDCServerConfig) {
	metrics.RegisterMetric()
	config2.InitCommonConfig(func(c *config2.CommonConfig) {
		c.Retry = config.Retry
	})
	maintenance.InitMsgLog()

	c.serverConfig = config
	c.api = GetCDCAPI(c.serverConfig)
	c.api.ReloadTask()
	cdcHandler := c.getCDCHandler()
	http.Handle("/cdc", cdcHandler)
	log.Info("start server...")
	if config.DryRun {
		log.Info("dry run mode")
	}
	err := http.ListenAndServe(c.serverConfig.Address, nil)
	log.Panic("cdc server down", zap.Error(err))
}

func (c *CDCServer) getCDCHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startTime := time.Now()
		writer.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodPost {
			c.handleError(writer, "only support the POST method", http.StatusMethodNotAllowed,
				zap.String("method", request.Method))
			metrics.TaskRequestCountVec.WithLabelValues(metrics.UnknownTypeLabel, metrics.InvalidMethodStatusLabel).Inc()
			return
		}
		bodyBytes, err := ioutil.ReadAll(request.Body)
		if err != nil {
			c.handleError(writer, "fail to read the request body, error: "+err.Error(), http.StatusInternalServerError)
			metrics.TaskRequestCountVec.WithLabelValues(metrics.UnknownTypeLabel, metrics.ReadErrorStatusLabel).Inc()
			return
		}
		cdcRequest := &modelrequest.CDCRequest{}
		err = json.Unmarshal(bodyBytes, cdcRequest)
		if err != nil {
			c.handleError(writer, "fail to unmarshal the request, error: "+err.Error(), http.StatusInternalServerError)
			metrics.TaskRequestCountVec.WithLabelValues(metrics.UnknownTypeLabel, metrics.UnmarshalErrorStatusLabel).Inc()
			return
		}
		metrics.TaskRequestCountVec.WithLabelValues(cdcRequest.RequestType, metrics.TotalStatusLabel).Inc()

		response := c.handleRequest(cdcRequest, writer)

		if response != nil {
			var m map[string]interface{}
			err = mapstructure.Decode(response, &m)
			if err != nil {
				log.Warn("fail to decode the response", zap.Any("resp", response), zap.Error(err))
				c.handleError(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			realResp := &modelrequest.CDCResponse{
				Code: 200,
				Data: m,
			}
			_ = json.NewEncoder(writer).Encode(realResp)
			metrics.TaskRequestCountVec.WithLabelValues(cdcRequest.RequestType, metrics.SuccessStatusLabel).Inc()
			metrics.TaskRequestLatencyVec.WithLabelValues(cdcRequest.RequestType).Observe(float64(time.Since(startTime).Milliseconds()))
		}
	})
}

func (c *CDCServer) handleError(w http.ResponseWriter, error string, code int, fields ...zap.Field) {
	log.Warn(error, fields...)
	errResp := &modelrequest.CDCResponse{
		Code:    code,
		Message: error,
	}
	_ = json.NewEncoder(w).Encode(errResp)
}

func (c *CDCServer) handleRequest(cdcRequest *modelrequest.CDCRequest, writer http.ResponseWriter) any {
	requestType := cdcRequest.RequestType
	handler, ok := requestHandlers[requestType]
	if !ok {
		c.handleError(writer, fmt.Sprintf("invalid 'request_type' param, can be set %v", lo.Keys(requestHandlers)), http.StatusBadRequest,
			zap.String("type", requestType))
		return nil
	}
	requestModel := handler.generateModel()
	if err := mapstructure.Decode(cdcRequest.RequestData, requestModel); err != nil {
		c.handleError(writer, fmt.Sprintf("fail to decode the %s request, error: %s", requestType, err.Error()), http.StatusInternalServerError)
		return nil
	}
	log.Info("request receive", zap.String("type", requestType), zap.String("data", GetRequestInfo(requestModel)))
	response, err := handler.handle(c.api, requestModel)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, cdcerror.ClientErr) {
			code = http.StatusBadRequest
		}
		c.handleError(writer, fmt.Sprintf("fail to handle the %s request, error: %s", requestType, err.Error()), code, zap.Error(err))
		return nil
	}

	return response
}

func GetRequestInfo(request any) string {
	r := request
	if _, ok := request.(*modelrequest.CreateRequest); ok {
		// deep copy
		create := &modelrequest.CreateRequest{}
		buffer, _ := json.Marshal(request)
		_ = json.Unmarshal(buffer, create)
		create.MilvusConnectParam.Password = ""
		create.MilvusConnectParam.Token = ""
		r = create
	}
	requestBytes, _ := json.Marshal(r)
	return string(requestBytes)
}
