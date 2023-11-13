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

package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	milvusNamespace = "milvus"
	systemName      = "cdc"

	UnknownTypeLabel = "unknown"

	// request status label
	TotalStatusLabel          = "total"
	SuccessStatusLabel        = "success"
	FailStatusLabel           = "fail"
	FinishStatusLabel         = "finish"
	InvalidMethodStatusLabel  = "invalid_method"
	ReadErrorStatusLabel      = "read_error"
	UnmarshalErrorStatusLabel = "unmarshal_error"

	// op type
	OPTypeRead  = "read"
	OPTypeWrite = "write"

	taskStateLabelName                 = "task_state"
	requestTypeLabelName               = "request_type"
	requestStatusLabelName             = "request_status"
	taskIDLabelName                    = "task_id"
	writeFailFuncLabelName             = "write_fail_func"
	collectionIDLabelName              = "collection_id"
	collectionNameLabelName            = "collection_name"
	vchannelLabelName                  = "vchannel_name"
	readFailFuncLabelName              = "read_fail_func"
	streamingCollectionStatusLabelName = "streaming_collection_status"
	messageTypeLabelName               = "msg_type"
	apiTypeLabelName                   = "api_type"
	apiStatusLabelName                 = "api_status"
	opTypeName                         = "op_type" // read or write
)

var (
	registry *prometheus.Registry

	TaskNumVec = &TaskNumMetric{
		metricDesc: prometheus.NewDesc(prometheus.BuildFQName(milvusNamespace, systemName, "task_num"),
			"cdc task number", []string{taskStateLabelName}, nil),
	}

	TaskStateVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "task_state",
			Help:      "cdc task state",
		}, []string{taskIDLabelName})

	TaskRequestLatencyVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "request_latency",
			Help:      "cdc request latency on the client side ",
			// 0.1 0.2 0.4 0.8 1.6 3.2 6.4 12.8
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 8),
		}, []string{requestTypeLabelName})

	TaskRequestCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "request_total",
			Help:      "cdc request count",
		}, []string{requestTypeLabelName, requestStatusLabelName})

	ReplicateTimeDifferenceVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "replicate_tt_lag",
			Help:      "the time difference between the current time and the current message timestamp, unit: ms",
			Buckets:   []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 200000, 400000, 800000},
		}, []string{taskIDLabelName, vchannelLabelName, opTypeName})

	ReplicateDataSizeVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "replicate_data_size",
			Help:      "the size of the message",
			// 16 64 256 1k 4k 16k 64k 256k 1M 4M 16M 64M 256M
			Buckets: prometheus.ExponentialBuckets(16, 4, 13),
		}, []string{taskIDLabelName, vchannelLabelName, opTypeName})

	APIExecuteCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "api_execute_total",
		Help:      "the counter of executing api",
	}, []string{taskIDLabelName, apiTypeLabelName})
)

func init() {
	registry = prometheus.NewRegistry()
	registry.MustRegister(TaskNumVec)
	registry.MustRegister(TaskStateVec)
	registry.MustRegister(TaskRequestLatencyVec)
	registry.MustRegister(TaskRequestCountVec)
	registry.MustRegister(ReplicateTimeDifferenceVec)
	registry.MustRegister(ReplicateDataSizeVec)
	registry.MustRegister(APIExecuteCountVec)
}

func RegisterMetric() {
	http.Handle("/cdc/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	http.Handle("/cdc/metrics_default", promhttp.Handler())
}
