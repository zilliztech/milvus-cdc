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

	// write fail func label
	WriteFailOnUpdatePosition = "on_update_position"
	WriteFailOnFail           = "on_fail"
	WriteFailOnDrop           = "on_drop"

	// read fail func label
	ReadFailUnknown           = "unknown"
	ReadFailGetCollectionInfo = "get_collection_info"
	ReadFailReadStream        = "read_stream"

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
)

var (
	registry *prometheus.Registry
	buckets  = prometheus.ExponentialBuckets(1, 2, 18)

	TaskNumVec = &TaskNumMetric{
		metricDesc: prometheus.NewDesc(prometheus.BuildFQName(milvusNamespace, systemName, "task_num"),
			"cdc task number", []string{taskStateLabelName}, nil),
	}
	TaskRequestLatencyVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "request_latency",
			Help:      "cdc request latency on the client side ",
			Buckets:   buckets,
		}, []string{requestTypeLabelName})

	TaskRequestCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "request_total",
			Help:      "cdc request count",
		}, []string{requestTypeLabelName, requestStatusLabelName})

	StreamingCollectionCountVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "streaming_collection_total",
			Help:      "the number of collections that are synchronizing data",
		}, []string{taskIDLabelName, streamingCollectionStatusLabelName})

	ReaderFailCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "reader_fail_total",
		Help:      "the fail count when the reader reads milvus data",
	}, []string{taskIDLabelName, readFailFuncLabelName})

	WriterFailCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "writer_callback_fail_total",
		Help:      "the fail count when the writer executes the callback function",
	}, []string{taskIDLabelName, writeFailFuncLabelName})

	WriterTimeDifferenceVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: systemName,
			Name:      "writer_tt_lag_ms",
			Help:      "the time difference between the current time and the current message timestamp",
		}, []string{taskIDLabelName, collectionIDLabelName, vchannelLabelName})
	ReadMsgRowCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "read_msg_row_total",
		Help:      "the counter of messages that the reader has read",
	}, []string{taskIDLabelName, collectionIDLabelName, messageTypeLabelName})
	WriteMsgRowCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "write_msg_row_total",
		Help:      "the counter of messages that the writer has written",
	}, []string{taskIDLabelName, collectionIDLabelName, messageTypeLabelName})
	ApiExecuteCountVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: systemName,
		Name:      "api_execute_total",
		Help:      "the counter of executing api",
	}, []string{taskIDLabelName, collectionNameLabelName, apiTypeLabelName, apiStatusLabelName})
)

func init() {
	registry = prometheus.NewRegistry()
	registry.MustRegister(TaskNumVec)
	registry.MustRegister(TaskRequestLatencyVec)
	registry.MustRegister(TaskRequestCountVec)
	registry.MustRegister(StreamingCollectionCountVec)
	registry.MustRegister(ReaderFailCountVec)
	registry.MustRegister(WriterFailCountVec)
	registry.MustRegister(WriterTimeDifferenceVec)
	registry.MustRegister(ReadMsgRowCountVec)
	registry.MustRegister(WriteMsgRowCountVec)
	registry.MustRegister(ApiExecuteCountVec)
}

func RegisterMetric() {
	http.Handle("/cdc/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	http.Handle("/cdc/metrics_default", promhttp.Handler())
}
