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
	"github.com/milvus-io/milvus/pkg/log"

	"github.com/zilliztech/milvus-cdc/server/model/request"
)

type CDCService interface {
	ReloadTask()
	Create(request *request.CreateRequest) (*request.CreateResponse, error)
	Delete(request *request.DeleteRequest) (*request.DeleteResponse, error)
	Pause(request *request.PauseRequest) (*request.PauseResponse, error)
	Resume(request *request.ResumeRequest) (*request.ResumeResponse, error)
	Get(request *request.GetRequest) (*request.GetResponse, error)
	GetPosition(req *request.GetPositionRequest) (*request.GetPositionResponse, error)
	List(request *request.ListRequest) (*request.ListResponse, error)
}

type BaseCDC struct{}

func NewBaseCDC() *BaseCDC {
	return &BaseCDC{}
}

func (b *BaseCDC) ReloadTask() {
	log.Warn("ReloadTask is not implemented, please check it")
}

func (b *BaseCDC) Create(request *request.CreateRequest) (*request.CreateResponse, error) {
	log.Warn("Create is not implemented, please check it")
	return nil, nil
}

func (b *BaseCDC) Delete(request *request.DeleteRequest) (*request.DeleteResponse, error) {
	log.Warn("Delete is not implemented, please check it")
	return nil, nil
}

func (b *BaseCDC) Pause(request *request.PauseRequest) (*request.PauseResponse, error) {
	log.Warn("Pause is not implemented, please check it")
	return nil, nil
}

func (b *BaseCDC) Resume(request *request.ResumeRequest) (*request.ResumeResponse, error) {
	log.Warn("Resume is not implemented, please check it")
	return nil, nil
}

func (b *BaseCDC) Get(request *request.GetRequest) (*request.GetResponse, error) {
	log.Warn("Get is not implemented, please check it")
	return nil, nil
}

func (b *BaseCDC) GetPosition(req *request.GetPositionRequest) (*request.GetPositionResponse, error) {
	log.Warn("GetPosition is not implemented, please check it")
	return nil, nil
}

func (b *BaseCDC) List(request *request.ListRequest) (*request.ListResponse, error) {
	log.Warn("List is not implemented, please check it")
	return nil, nil
}

func GetCDCAPI(config *CDCServerConfig) CDCService {
	return NewMetaCDC(config)
}
