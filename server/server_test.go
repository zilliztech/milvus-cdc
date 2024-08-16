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
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
	cdcerror "github.com/zilliztech/milvus-cdc/server/error"
	"github.com/zilliztech/milvus-cdc/server/model"
	"github.com/zilliztech/milvus-cdc/server/model/request"
)

type MockResponseWriter struct {
	t          *testing.T
	exceptCode int
	resp       []byte
}

func (m *MockResponseWriter) Header() http.Header {
	return map[string][]string{}
}

func (m *MockResponseWriter) Write(bytes []byte) (int, error) {
	m.resp = append(m.resp, bytes...)
	return len(bytes), nil
}

func (m *MockResponseWriter) WriteHeader(statusCode int) {
	assert.Equal(m.t, m.exceptCode, statusCode)
}

type MockReaderCloser struct {
	err error
}

func (m *MockReaderCloser) Read(p []byte) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	return len(p), nil
}

func (m *MockReaderCloser) Close() error {
	return nil
}

type MockBaseCDC struct {
	BaseCDC
	resp *request.CreateResponse
	err  error
}

func (m *MockBaseCDC) Create(request *request.CreateRequest) (*request.CreateResponse, error) {
	return m.resp, m.err
}

func TestCDCHandler(t *testing.T) {
	server := &CDCServer{}
	handler := server.getCDCHandler()

	t.Run("get method", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusMethodNotAllowed,
		}
		handler.ServeHTTP(responseWriter, &http.Request{Method: http.MethodGet})
		assert.Contains(t, string(responseWriter.resp), "only support the POST method")
	})

	t.Run("read error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   &MockReaderCloser{err: errors.New("FOO")},
		})
		assert.Contains(t, string(responseWriter.resp), "fail to read the request body")
	})

	t.Run("unmarshal error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   io.NopCloser(bytes.NewReader([]byte("fooooo"))),
		})
		assert.Contains(t, string(responseWriter.resp), "fail to unmarshal the request")
	})

	t.Run("request type error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusBadRequest,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: "foo",
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   io.NopCloser(bytes.NewReader(requestBytes)),
		})
		assert.Contains(t, string(responseWriter.resp), "invalid 'request_type' param")
	})

	t.Run("request data format error", func(t *testing.T) {
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{
				"buffer_config": 10001,
			},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   io.NopCloser(bytes.NewReader(requestBytes)),
		})
		assert.Contains(t, string(responseWriter.resp), "fail to decode the create request")
	})

	t.Run("request data handle server error", func(t *testing.T) {
		server.api = &MockBaseCDC{
			err: errors.New("foo"),
		}
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusInternalServerError,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   io.NopCloser(bytes.NewReader(requestBytes)),
		})
		assert.Contains(t, string(responseWriter.resp), "fail to handle the create request")
	})

	t.Run("request data handle client error", func(t *testing.T) {
		server.api = &MockBaseCDC{
			err: cdcerror.NewClientError("foo"),
		}
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusBadRequest,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   io.NopCloser(bytes.NewReader(requestBytes)),
		})
		assert.Contains(t, string(responseWriter.resp), "fail to handle the create request")
	})

	t.Run("request success", func(t *testing.T) {
		taskID := "123456789"
		server.api = &MockBaseCDC{
			resp: &request.CreateResponse{
				TaskID: taskID,
			},
		}
		responseWriter := &MockResponseWriter{
			t:          t,
			exceptCode: http.StatusOK,
		}
		cdcRequest := &request.CDCRequest{
			RequestType: request.Create,
			RequestData: map[string]any{},
		}
		requestBytes, _ := json.Marshal(cdcRequest)
		handler.ServeHTTP(responseWriter, &http.Request{
			Method: http.MethodPost,
			Body:   io.NopCloser(bytes.NewReader(requestBytes)),
		})
		assert.Contains(t, string(responseWriter.resp), taskID)
	})
}

func TestDecodeStruct(t *testing.T) {
	t.Run("err", func(t *testing.T) {
		buf := bytes.NewBufferString("")
		errResp := &request.CDCResponse{
			Code:    500,
			Message: "error msg",
		}
		_ = json.NewEncoder(buf).Encode(errResp)
		log.Warn("err", zap.Any("resp", buf.String()))
	})

	t.Run("success", func(t *testing.T) {
		buf := bytes.NewBufferString("")
		var m map[string]interface{}
		response := &request.ListResponse{
			Tasks: []request.Task{
				{
					TaskID: "123",
					ConnectParam: model.ConnectParam{
						Milvus: model.MilvusConnectParam{
							Host: "localhost",
							Port: 19530,
						},
					},
					CollectionInfos: []model.CollectionInfo{
						{
							Name: "foo",
						},
					},
					State:           "Running",
					LastPauseReason: "receive the pause request",
				},
			},
		}

		_ = mapstructure.Decode(response, &m)
		realResp := &request.CDCResponse{
			Code: 200,
			Data: m,
		}
		_ = json.NewEncoder(buf).Encode(realResp)
		log.Warn("err", zap.Any("resp", buf.String()))
	})
}
