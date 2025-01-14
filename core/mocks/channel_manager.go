// Code generated by mockery v2.46.0. DO NOT EDIT.

package mocks

import (
	context "context"

	api "github.com/zilliztech/milvus-cdc/core/api"

	mock "github.com/stretchr/testify/mock"

	model "github.com/zilliztech/milvus-cdc/core/model"

	msgpb "github.com/milvus-io/milvus-proto/go-api/v2/msgpb"

	pb "github.com/zilliztech/milvus-cdc/core/pb"
)

// ChannelManager is an autogenerated mock type for the ChannelManager type
type ChannelManager struct {
	mock.Mock
}

type ChannelManager_Expecter struct {
	mock *mock.Mock
}

func (_m *ChannelManager) EXPECT() *ChannelManager_Expecter {
	return &ChannelManager_Expecter{mock: &_m.Mock}
}

// AddDroppedCollection provides a mock function with given fields: ids
func (_m *ChannelManager) AddDroppedCollection(ids []int64) {
	_m.Called(ids)
}

// ChannelManager_AddDroppedCollection_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddDroppedCollection'
type ChannelManager_AddDroppedCollection_Call struct {
	*mock.Call
}

// AddDroppedCollection is a helper method to define mock.On call
//   - ids []int64
func (_e *ChannelManager_Expecter) AddDroppedCollection(ids interface{}) *ChannelManager_AddDroppedCollection_Call {
	return &ChannelManager_AddDroppedCollection_Call{Call: _e.mock.On("AddDroppedCollection", ids)}
}

func (_c *ChannelManager_AddDroppedCollection_Call) Run(run func(ids []int64)) *ChannelManager_AddDroppedCollection_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]int64))
	})
	return _c
}

func (_c *ChannelManager_AddDroppedCollection_Call) Return() *ChannelManager_AddDroppedCollection_Call {
	_c.Call.Return()
	return _c
}

func (_c *ChannelManager_AddDroppedCollection_Call) RunAndReturn(run func([]int64)) *ChannelManager_AddDroppedCollection_Call {
	_c.Call.Return(run)
	return _c
}

// AddDroppedPartition provides a mock function with given fields: ids
func (_m *ChannelManager) AddDroppedPartition(ids []int64) {
	_m.Called(ids)
}

// ChannelManager_AddDroppedPartition_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddDroppedPartition'
type ChannelManager_AddDroppedPartition_Call struct {
	*mock.Call
}

// AddDroppedPartition is a helper method to define mock.On call
//   - ids []int64
func (_e *ChannelManager_Expecter) AddDroppedPartition(ids interface{}) *ChannelManager_AddDroppedPartition_Call {
	return &ChannelManager_AddDroppedPartition_Call{Call: _e.mock.On("AddDroppedPartition", ids)}
}

func (_c *ChannelManager_AddDroppedPartition_Call) Run(run func(ids []int64)) *ChannelManager_AddDroppedPartition_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].([]int64))
	})
	return _c
}

func (_c *ChannelManager_AddDroppedPartition_Call) Return() *ChannelManager_AddDroppedPartition_Call {
	_c.Call.Return()
	return _c
}

func (_c *ChannelManager_AddDroppedPartition_Call) RunAndReturn(run func([]int64)) *ChannelManager_AddDroppedPartition_Call {
	_c.Call.Return(run)
	return _c
}

// AddPartition provides a mock function with given fields: ctx, dbInfo, collectionInfo, partitionInfo
func (_m *ChannelManager) AddPartition(ctx context.Context, dbInfo *model.DatabaseInfo, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo) error {
	ret := _m.Called(ctx, dbInfo, collectionInfo, partitionInfo)

	if len(ret) == 0 {
		panic("no return value specified for AddPartition")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *model.DatabaseInfo, *pb.CollectionInfo, *pb.PartitionInfo) error); ok {
		r0 = rf(ctx, dbInfo, collectionInfo, partitionInfo)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ChannelManager_AddPartition_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AddPartition'
type ChannelManager_AddPartition_Call struct {
	*mock.Call
}

// AddPartition is a helper method to define mock.On call
//   - ctx context.Context
//   - dbInfo *model.DatabaseInfo
//   - collectionInfo *pb.CollectionInfo
//   - partitionInfo *pb.PartitionInfo
func (_e *ChannelManager_Expecter) AddPartition(ctx interface{}, dbInfo interface{}, collectionInfo interface{}, partitionInfo interface{}) *ChannelManager_AddPartition_Call {
	return &ChannelManager_AddPartition_Call{Call: _e.mock.On("AddPartition", ctx, dbInfo, collectionInfo, partitionInfo)}
}

func (_c *ChannelManager_AddPartition_Call) Run(run func(ctx context.Context, dbInfo *model.DatabaseInfo, collectionInfo *pb.CollectionInfo, partitionInfo *pb.PartitionInfo)) *ChannelManager_AddPartition_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*model.DatabaseInfo), args[2].(*pb.CollectionInfo), args[3].(*pb.PartitionInfo))
	})
	return _c
}

func (_c *ChannelManager_AddPartition_Call) Return(_a0 error) *ChannelManager_AddPartition_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ChannelManager_AddPartition_Call) RunAndReturn(run func(context.Context, *model.DatabaseInfo, *pb.CollectionInfo, *pb.PartitionInfo) error) *ChannelManager_AddPartition_Call {
	_c.Call.Return(run)
	return _c
}

// GetChannelChan provides a mock function with given fields:
func (_m *ChannelManager) GetChannelChan() <-chan string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetChannelChan")
	}

	var r0 <-chan string
	if rf, ok := ret.Get(0).(func() <-chan string); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan string)
		}
	}

	return r0
}

// ChannelManager_GetChannelChan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetChannelChan'
type ChannelManager_GetChannelChan_Call struct {
	*mock.Call
}

// GetChannelChan is a helper method to define mock.On call
func (_e *ChannelManager_Expecter) GetChannelChan() *ChannelManager_GetChannelChan_Call {
	return &ChannelManager_GetChannelChan_Call{Call: _e.mock.On("GetChannelChan")}
}

func (_c *ChannelManager_GetChannelChan_Call) Run(run func()) *ChannelManager_GetChannelChan_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ChannelManager_GetChannelChan_Call) Return(_a0 <-chan string) *ChannelManager_GetChannelChan_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ChannelManager_GetChannelChan_Call) RunAndReturn(run func() <-chan string) *ChannelManager_GetChannelChan_Call {
	_c.Call.Return(run)
	return _c
}

// GetChannelLatestMsgID provides a mock function with given fields: ctx, channelName
func (_m *ChannelManager) GetChannelLatestMsgID(ctx context.Context, channelName string) ([]byte, error) {
	ret := _m.Called(ctx, channelName)

	if len(ret) == 0 {
		panic("no return value specified for GetChannelLatestMsgID")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) ([]byte, error)); ok {
		return rf(ctx, channelName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) []byte); ok {
		r0 = rf(ctx, channelName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, channelName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ChannelManager_GetChannelLatestMsgID_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetChannelLatestMsgID'
type ChannelManager_GetChannelLatestMsgID_Call struct {
	*mock.Call
}

// GetChannelLatestMsgID is a helper method to define mock.On call
//   - ctx context.Context
//   - channelName string
func (_e *ChannelManager_Expecter) GetChannelLatestMsgID(ctx interface{}, channelName interface{}) *ChannelManager_GetChannelLatestMsgID_Call {
	return &ChannelManager_GetChannelLatestMsgID_Call{Call: _e.mock.On("GetChannelLatestMsgID", ctx, channelName)}
}

func (_c *ChannelManager_GetChannelLatestMsgID_Call) Run(run func(ctx context.Context, channelName string)) *ChannelManager_GetChannelLatestMsgID_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *ChannelManager_GetChannelLatestMsgID_Call) Return(_a0 []byte, _a1 error) *ChannelManager_GetChannelLatestMsgID_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *ChannelManager_GetChannelLatestMsgID_Call) RunAndReturn(run func(context.Context, string) ([]byte, error)) *ChannelManager_GetChannelLatestMsgID_Call {
	_c.Call.Return(run)
	return _c
}

// GetEventChan provides a mock function with given fields:
func (_m *ChannelManager) GetEventChan() <-chan *api.ReplicateAPIEvent {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for GetEventChan")
	}

	var r0 <-chan *api.ReplicateAPIEvent
	if rf, ok := ret.Get(0).(func() <-chan *api.ReplicateAPIEvent); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *api.ReplicateAPIEvent)
		}
	}

	return r0
}

// ChannelManager_GetEventChan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetEventChan'
type ChannelManager_GetEventChan_Call struct {
	*mock.Call
}

// GetEventChan is a helper method to define mock.On call
func (_e *ChannelManager_Expecter) GetEventChan() *ChannelManager_GetEventChan_Call {
	return &ChannelManager_GetEventChan_Call{Call: _e.mock.On("GetEventChan")}
}

func (_c *ChannelManager_GetEventChan_Call) Run(run func()) *ChannelManager_GetEventChan_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *ChannelManager_GetEventChan_Call) Return(_a0 <-chan *api.ReplicateAPIEvent) *ChannelManager_GetEventChan_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ChannelManager_GetEventChan_Call) RunAndReturn(run func() <-chan *api.ReplicateAPIEvent) *ChannelManager_GetEventChan_Call {
	_c.Call.Return(run)
	return _c
}

// GetMsgChan provides a mock function with given fields: pChannel
func (_m *ChannelManager) GetMsgChan(pChannel string) <-chan *api.ReplicateMsg {
	ret := _m.Called(pChannel)

	if len(ret) == 0 {
		panic("no return value specified for GetMsgChan")
	}

	var r0 <-chan *api.ReplicateMsg
	if rf, ok := ret.Get(0).(func(string) <-chan *api.ReplicateMsg); ok {
		r0 = rf(pChannel)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *api.ReplicateMsg)
		}
	}

	return r0
}

// ChannelManager_GetMsgChan_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetMsgChan'
type ChannelManager_GetMsgChan_Call struct {
	*mock.Call
}

// GetMsgChan is a helper method to define mock.On call
//   - pChannel string
func (_e *ChannelManager_Expecter) GetMsgChan(pChannel interface{}) *ChannelManager_GetMsgChan_Call {
	return &ChannelManager_GetMsgChan_Call{Call: _e.mock.On("GetMsgChan", pChannel)}
}

func (_c *ChannelManager_GetMsgChan_Call) Run(run func(pChannel string)) *ChannelManager_GetMsgChan_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *ChannelManager_GetMsgChan_Call) Return(_a0 <-chan *api.ReplicateMsg) *ChannelManager_GetMsgChan_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ChannelManager_GetMsgChan_Call) RunAndReturn(run func(string) <-chan *api.ReplicateMsg) *ChannelManager_GetMsgChan_Call {
	_c.Call.Return(run)
	return _c
}

// SetCtx provides a mock function with given fields: ctx
func (_m *ChannelManager) SetCtx(ctx context.Context) {
	_m.Called(ctx)
}

// ChannelManager_SetCtx_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SetCtx'
type ChannelManager_SetCtx_Call struct {
	*mock.Call
}

// SetCtx is a helper method to define mock.On call
//   - ctx context.Context
func (_e *ChannelManager_Expecter) SetCtx(ctx interface{}) *ChannelManager_SetCtx_Call {
	return &ChannelManager_SetCtx_Call{Call: _e.mock.On("SetCtx", ctx)}
}

func (_c *ChannelManager_SetCtx_Call) Run(run func(ctx context.Context)) *ChannelManager_SetCtx_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *ChannelManager_SetCtx_Call) Return() *ChannelManager_SetCtx_Call {
	_c.Call.Return()
	return _c
}

func (_c *ChannelManager_SetCtx_Call) RunAndReturn(run func(context.Context)) *ChannelManager_SetCtx_Call {
	_c.Call.Return(run)
	return _c
}

// StartReadCollection provides a mock function with given fields: ctx, db, info, seekPositions
func (_m *ChannelManager) StartReadCollection(ctx context.Context, db *model.DatabaseInfo, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition) error {
	ret := _m.Called(ctx, db, info, seekPositions)

	if len(ret) == 0 {
		panic("no return value specified for StartReadCollection")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *model.DatabaseInfo, *pb.CollectionInfo, []*msgpb.MsgPosition) error); ok {
		r0 = rf(ctx, db, info, seekPositions)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ChannelManager_StartReadCollection_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StartReadCollection'
type ChannelManager_StartReadCollection_Call struct {
	*mock.Call
}

// StartReadCollection is a helper method to define mock.On call
//   - ctx context.Context
//   - db *model.DatabaseInfo
//   - info *pb.CollectionInfo
//   - seekPositions []*msgpb.MsgPosition
func (_e *ChannelManager_Expecter) StartReadCollection(ctx interface{}, db interface{}, info interface{}, seekPositions interface{}) *ChannelManager_StartReadCollection_Call {
	return &ChannelManager_StartReadCollection_Call{Call: _e.mock.On("StartReadCollection", ctx, db, info, seekPositions)}
}

func (_c *ChannelManager_StartReadCollection_Call) Run(run func(ctx context.Context, db *model.DatabaseInfo, info *pb.CollectionInfo, seekPositions []*msgpb.MsgPosition)) *ChannelManager_StartReadCollection_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*model.DatabaseInfo), args[2].(*pb.CollectionInfo), args[3].([]*msgpb.MsgPosition))
	})
	return _c
}

func (_c *ChannelManager_StartReadCollection_Call) Return(_a0 error) *ChannelManager_StartReadCollection_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ChannelManager_StartReadCollection_Call) RunAndReturn(run func(context.Context, *model.DatabaseInfo, *pb.CollectionInfo, []*msgpb.MsgPosition) error) *ChannelManager_StartReadCollection_Call {
	_c.Call.Return(run)
	return _c
}

// StopReadCollection provides a mock function with given fields: ctx, info
func (_m *ChannelManager) StopReadCollection(ctx context.Context, info *pb.CollectionInfo) error {
	ret := _m.Called(ctx, info)

	if len(ret) == 0 {
		panic("no return value specified for StopReadCollection")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *pb.CollectionInfo) error); ok {
		r0 = rf(ctx, info)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ChannelManager_StopReadCollection_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'StopReadCollection'
type ChannelManager_StopReadCollection_Call struct {
	*mock.Call
}

// StopReadCollection is a helper method to define mock.On call
//   - ctx context.Context
//   - info *pb.CollectionInfo
func (_e *ChannelManager_Expecter) StopReadCollection(ctx interface{}, info interface{}) *ChannelManager_StopReadCollection_Call {
	return &ChannelManager_StopReadCollection_Call{Call: _e.mock.On("StopReadCollection", ctx, info)}
}

func (_c *ChannelManager_StopReadCollection_Call) Run(run func(ctx context.Context, info *pb.CollectionInfo)) *ChannelManager_StopReadCollection_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*pb.CollectionInfo))
	})
	return _c
}

func (_c *ChannelManager_StopReadCollection_Call) Return(_a0 error) *ChannelManager_StopReadCollection_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *ChannelManager_StopReadCollection_Call) RunAndReturn(run func(context.Context, *pb.CollectionInfo) error) *ChannelManager_StopReadCollection_Call {
	_c.Call.Return(run)
	return _c
}

// NewChannelManager creates a new instance of ChannelManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewChannelManager(t interface {
	mock.TestingT
	Cleanup(func())
}) *ChannelManager {
	mock := &ChannelManager{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
