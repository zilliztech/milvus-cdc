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

package log

import (
	"context"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/log"
)

var (
	_l atomic.Value
	_p atomic.Value
)

type ctxLogKeyType struct{}

var CtxLogKey = ctxLogKeyType{}

func init() {
	conf := &log.Config{
		Level:  "info",
		Stdout: true,
		File: log.FileLogConfig{
			RootPath: "/tmp/cdc_log",
			Filename: "cdc.log",
		},
	}

	l, p, err := log.InitLogger(conf)
	if err != nil {
		panic(err)
	}
	_l.Store(l)
	_p.Store(p)
}

func L() *zap.Logger {
	return _l.Load().(*zap.Logger)
}

func Prop() *log.ZapProperties {
	return _p.Load().(*log.ZapProperties)
}

func SetLevel(level zapcore.Level) {
	_p.Load().(*log.ZapProperties).Level.SetLevel(level)
}

func Debug(msg string, fields ...zap.Field) {
	L().Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	L().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	L().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	L().Error(msg, fields...)
}

func Panic(msg string, fields ...zap.Field) {
	L().Panic(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	L().Fatal(msg, fields...)
}

func Ctx(ctx context.Context) *log.MLogger {
	if ctx == nil {
		return &log.MLogger{Logger: L()}
	}
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*log.MLogger); ok {
		return ctxLogger
	}
	return &log.MLogger{Logger: L()}
}

func WithTraceID(ctx context.Context, traceID string) context.Context {
	return WithFields(ctx, zap.String("traceID", traceID))
}

func WithFields(ctx context.Context, fields ...zap.Field) context.Context {
	var zlogger *zap.Logger
	if ctxLogger, ok := ctx.Value(CtxLogKey).(*log.MLogger); ok {
		zlogger = ctxLogger.Logger
	} else {
		zlogger = L()
	}
	mLogger := &log.MLogger{
		Logger: zlogger.With(fields...),
	}
	return context.WithValue(ctx, CtxLogKey, mLogger)
}

func With(fields ...zap.Field) *log.MLogger {
	return &log.MLogger{
		Logger: L().With(fields...).WithOptions(zap.AddCallerSkip(-1)),
	}
}
