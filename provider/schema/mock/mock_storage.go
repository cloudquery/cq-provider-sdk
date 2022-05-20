// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/cloudquery/cq-provider-sdk/provider/execution (interfaces: Storage)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	io "io"
	reflect "reflect"
	time "time"

	schema "github.com/cloudquery/cq-provider-sdk/provider/schema"
	gomock "github.com/golang/mock/gomock"
	pgx "github.com/jackc/pgx/v4"
)

// MockStorage is a mock of Storage interface.
type MockStorage struct {
	ctrl     *gomock.Controller
	recorder *MockStorageMockRecorder
}

// MockStorageMockRecorder is the mock recorder for MockStorage.
type MockStorageMockRecorder struct {
	mock *MockStorage
}

// NewMockStorage creates a new mock instance.
func NewMockStorage(ctrl *gomock.Controller) *MockStorage {
	mock := &MockStorage{ctrl: ctrl}
	mock.recorder = &MockStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStorage) EXPECT() *MockStorageMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockStorage) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockStorageMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockStorage)(nil).Close))
}

// CopyFrom mocks base method.
func (m *MockStorage) CopyFrom(arg0 context.Context, arg1 schema.Resources, arg2 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CopyFrom", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// CopyFrom indicates an expected call of CopyFrom.
func (mr *MockStorageMockRecorder) CopyFrom(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CopyFrom", reflect.TypeOf((*MockStorage)(nil).CopyFrom), arg0, arg1, arg2)
}

// Delete mocks base method.
func (m *MockStorage) Delete(arg0 context.Context, arg1 *schema.Table, arg2 []interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockStorageMockRecorder) Delete(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockStorage)(nil).Delete), arg0, arg1, arg2)
}

// Dialect mocks base method.
func (m *MockStorage) Dialect() schema.Dialect {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Dialect")
	ret0, _ := ret[0].(schema.Dialect)
	return ret0
}

// Dialect indicates an expected call of Dialect.
func (mr *MockStorageMockRecorder) Dialect() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Dialect", reflect.TypeOf((*MockStorage)(nil).Dialect))
}

// Exec mocks base method.
func (m *MockStorage) Exec(arg0 context.Context, arg1 string, arg2 ...interface{}) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Exec", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Exec indicates an expected call of Exec.
func (mr *MockStorageMockRecorder) Exec(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Exec", reflect.TypeOf((*MockStorage)(nil).Exec), varargs...)
}

// Insert mocks base method.
func (m *MockStorage) Insert(arg0 context.Context, arg1 *schema.Table, arg2 schema.Resources) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Insert", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Insert indicates an expected call of Insert.
func (mr *MockStorageMockRecorder) Insert(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Insert", reflect.TypeOf((*MockStorage)(nil).Insert), arg0, arg1, arg2)
}

// Query mocks base method.
func (m *MockStorage) Query(arg0 context.Context, arg1 string, arg2 ...interface{}) (pgx.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Query", varargs...)
	ret0, _ := ret[0].(pgx.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Query indicates an expected call of Query.
func (mr *MockStorageMockRecorder) Query(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Query", reflect.TypeOf((*MockStorage)(nil).Query), varargs...)
}

// RawCopyFrom mocks base method.
func (m *MockStorage) RawCopyFrom(arg0 context.Context, arg1 io.Reader, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RawCopyFrom", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// RawCopyFrom indicates an expected call of RawCopyFrom.
func (mr *MockStorageMockRecorder) RawCopyFrom(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RawCopyFrom", reflect.TypeOf((*MockStorage)(nil).RawCopyFrom), arg0, arg1, arg2)
}

// RawCopyTo mocks base method.
func (m *MockStorage) RawCopyTo(arg0 context.Context, arg1 io.Writer, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RawCopyTo", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// RawCopyTo indicates an expected call of RawCopyTo.
func (mr *MockStorageMockRecorder) RawCopyTo(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RawCopyTo", reflect.TypeOf((*MockStorage)(nil).RawCopyTo), arg0, arg1, arg2)
}

// RemoveStaleData mocks base method.
func (m *MockStorage) RemoveStaleData(arg0 context.Context, arg1 *schema.Table, arg2 time.Time, arg3 []interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveStaleData", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveStaleData indicates an expected call of RemoveStaleData.
func (mr *MockStorageMockRecorder) RemoveStaleData(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveStaleData", reflect.TypeOf((*MockStorage)(nil).RemoveStaleData), arg0, arg1, arg2, arg3)
}
