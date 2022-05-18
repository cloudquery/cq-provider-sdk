package execution

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cloudquery/cq-provider-sdk/provider/schema"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/mock"
)

// DatabaseMock is an autogenerated mock type for the Storage type
type DatabaseMock struct {
	mock.Mock
}

var _ Storage = (*DatabaseMock)(nil)

// Close provides a mock function with given fields:
func (_m *DatabaseMock) Close() {
	_m.Called()
}

// CopyFrom provides a mock function with given fields: ctx, resources, shouldCascade, CascadeDeleteFilters
func (_m *DatabaseMock) CopyFrom(ctx context.Context, resources schema.Resources, shouldCascade bool, CascadeDeleteFilters map[string]interface{}) error {
	ret := _m.Called(ctx, resources, shouldCascade, CascadeDeleteFilters)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, schema.Resources, bool, map[string]interface{}) error); ok {
		r0 = rf(ctx, resources, shouldCascade, CascadeDeleteFilters)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: ctx, t, args
func (_m *DatabaseMock) Delete(ctx context.Context, t *schema.Table, args []interface{}) error {
	ret := _m.Called(ctx, t, args)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *schema.Table, []interface{}) error); ok {
		r0 = rf(ctx, t, args)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Exec provides a mock function with given fields: ctx, query, args
func (_m *DatabaseMock) Exec(ctx context.Context, query string, args ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, ctx, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) error); ok {
		r0 = rf(ctx, query, args...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Insert provides a mock function with given fields: ctx, t, instance
func (_m *DatabaseMock) Insert(ctx context.Context, t *schema.Table, instance schema.Resources) error {
	ret := _m.Called(ctx, t, instance)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *schema.Table, schema.Resources) error); ok {
		r0 = rf(ctx, t, instance)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Query provides a mock function with given fields: ctx, query, args
func (_m *DatabaseMock) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	var _ca []interface{}
	_ca = append(_ca, ctx, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	var r0 pgx.Rows
	if rf, ok := ret.Get(0).(func(context.Context, string, ...interface{}) pgx.Rows); ok {
		r0 = rf(ctx, query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pgx.Rows)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, string, ...interface{}) error); ok {
		r1 = rf(ctx, query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RemoveStaleData provides a mock function with given fields: ctx, t, executionStart, kvFilters
func (_m *DatabaseMock) RemoveStaleData(ctx context.Context, t *schema.Table, executionStart time.Time, kvFilters []interface{}) error {
	ret := _m.Called(ctx, t, executionStart, kvFilters)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *schema.Table, time.Time, []interface{}) error); ok {
		r0 = rf(ctx, t, executionStart, kvFilters)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Dialect mocks base method.
func (_m *DatabaseMock) Dialect() schema.Dialect {
	ret := _m.Called()

	var r0 schema.Dialect
	if rf, ok := ret.Get(0).(func() schema.Dialect); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(schema.Dialect)
		}
	}

	return r0
}

// RawCopyTo provides a mock function with given fields: ctx, w, sql
func (_m *DatabaseMock) RawCopyTo(ctx context.Context, w io.Writer, sql string) error {
	ret := _m.Called(ctx, w, sql)
	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, io.Writer, string) error); ok {
		r0 = rf(ctx, w, sql)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// RawCopyFrom provides a mock function with given fields: ctx, r, sql
func (_m *DatabaseMock) RawCopyFrom(ctx context.Context, r io.Reader, sql string) error {
	ret := _m.Called(ctx, r, sql)
	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, io.Reader, string) error); ok {
		r0 = rf(ctx, r, sql)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

func (_m *DatabaseMock) Begin(ctx context.Context) (TXQueryExecer, error) {
	return nil, fmt.Errorf("not implemented")
}
