package schema

import (
	"context"
	"testing"
	"time"

	"github.com/cloudquery/cq-provider-sdk/helpers"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

type innerStruct struct {
	Value string
}

type testStruct struct {
	NilInner   *innerStruct
	Inner      innerStruct
	Value      int
	unexported bool
}

type testDateStruct struct {
	Date string
}

type testNetStruct struct {
	IP  string
	MAC string
	Net string
	IPS []string
}

type testTransformersStruct struct {
	Int      int
	String   string
	Float    float64
	BadFloat string
}

type testUUIDStruct struct {
	UUID    string
	BadUUID string
}

var TransformersTestTable = &Table{
	Columns: []Column{
		{
			Name: "string_to_int",
			Type: TypeInt,
		},
		{
			Name: "float_to_int",
			Type: TypeInt,
		},
		{
			Name: "int_to_string",
			Type: TypeString,
		},
		{
			Name: "float_to_string",
			Type: TypeString,
		},
	},
}

var networkTestTable = &Table{
	Columns: []Column{
		{
			Name: "ip",
			Type: TypeInet,
		},
		{
			Name: "mac",
			Type: TypeMacAddr,
		},
		{
			Name: "net",
			Type: TypeCIDR,
		},
		{
			Name: "ips",
			Type: TypeInetArray,
		},
	},
}

var (
	pathTestTable = &Table{
		Columns: []Column{
			{
				Name: "test",
				Type: TypeString,
			},
			{
				Name: "int_value",
				Type: TypeInt,
			},
			{
				Name: "unexported",
				Type: TypeBool,
			},
		},
	}
	dateTestTable = &Table{
		Columns: []Column{
			{
				Name: "date",
				Type: TypeTimestamp,
			},
		},
	}
	netTests = []testNetStruct{
		{IP: "192.168.1.12", MAC: "2C:54:91:88:C9:E3", Net: "192.168.0.1/24", IPS: []string{"192.168.1.12"}},
		{IP: "2001:0db8:85a3:0000:0000:8a2e:0370:7334", MAC: "2C-54-91-88-C9-E3", Net: "2002::1234:abcd:ffff:c0a8:101/64", IPS: []string{"2001:0db8:85a3:0000:0000:8a2e:0370:7334", "192.168.1.12"}},
		{IP: "::1234:5678", MAC: "2C-54-91-88-C9-E3", Net: "::1234:5678/12", IPS: []string{"::1234:5678", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "192.168.1.12"}},
	}
	netTestsFails = []testNetStruct{
		{IP: "192.168.1/", MAC: "2C:54:91:88:C9", Net: "192.168.0.1-24", IPS: []string{"192.168.1.12", "192.168.1/"}},
		{IP: "::1234:5678:", MAC: "2C:54-91-88-C9-E3", Net: "2002::1234:abcd:ffff:c0a8:101-64", IPS: []string{"192.168.1.12", "::1234:5678:"}},
	}
)

var UUIDTestTable = &Table{
	Columns: []Column{
		{
			Name: "uuid",
			Type: TypeUUID,
		},
	},
}

func TestPathTableResolver(t *testing.T) {
	results := make(chan interface{})
	pathTableTests := []struct {
		path          string
		isNil         bool
		expectedValue interface{}
	}{
		{
			path:          "Inner",
			expectedValue: innerStruct{Value: "bla"},
		},
		{
			path:          "Inner.Value",
			expectedValue: "bla",
		},
		{
			path:          "NilInner.Value",
			expectedValue: "",
		}, {
			path:          "unexported",
			expectedValue: nil,
		},
	}

	resource := NewResourceData(PostgresDialect{}, pathTestTable, nil, testStruct{Inner: innerStruct{Value: "bla"}, Value: 5, unexported: false}, nil, time.Now())
	for _, test := range pathTableTests {
		r1 := PathTableResolver(test.path)
		go func() {
			err := r1(context.TODO(), nil, resource, results)
			assert.Nil(t, err)
		}()
		result := <-results
		assert.Equal(t, test.expectedValue, result, "path: %s", test.path)
	}
}

func TestPathResolver(t *testing.T) {
	pathTableTests := []struct {
		path       string
		getterPath string
		isNil      bool
		value      interface{}
	}{
		{
			path:       "Inner.Value",
			getterPath: "test",
			value:      "bla",
		},
		{
			path:       "Value",
			getterPath: "int_value",
			value:      5,
		},
		{
			path:       "unexported",
			getterPath: "unexported",
			isNil:      true,
			value:      nil,
		},
	}

	resource := NewResourceData(PostgresDialect{}, pathTestTable, nil, testStruct{Inner: innerStruct{Value: "bla"}, Value: 5, unexported: false}, nil, time.Now())
	for _, test := range pathTableTests {
		r1 := PathResolver(test.path)
		err := r1(context.TODO(), nil, resource, Column{Name: test.getterPath})
		assert.Nil(t, err)
		if test.isNil {
			assert.Nil(t, resource.Get(test.getterPath))
		} else {
			assert.Equal(t, test.value, resource.Get(test.getterPath))
		}
	}
}

func TestInterfaceSlice(t *testing.T) {
	var sType []interface{}
	var names []string
	names = append(names, "first", "second")
	assert.IsTypef(t, sType, helpers.InterfaceSlice(names), "")
	assert.IsTypef(t, sType, helpers.InterfaceSlice(&names), "")
	assert.IsTypef(t, sType, helpers.InterfaceSlice(1), "")
	assert.IsTypef(t, sType, helpers.InterfaceSlice(innerStruct{"asdsa"}), "")
	assert.IsTypef(t, sType, helpers.InterfaceSlice(&innerStruct{"asdsa"}), "")
	pSlice := []*innerStruct{{"asdsa"}, {"asdsa"}, {"asdsa"}}
	assert.IsTypef(t, sType, helpers.InterfaceSlice(pSlice), "")
	assert.IsTypef(t, sType, helpers.InterfaceSlice(&pSlice), "")
}

func TestDateTimeResolver(t *testing.T) {
	r1 := DateResolver("Date")
	resource := NewResourceData(PostgresDialect{}, dateTestTable, nil, testDateStruct{Date: "2011-10-05T14:48:00.000Z"}, nil, time.Now())
	err := r1(context.TODO(), nil, resource, Column{Name: "date"})

	assert.Nil(t, err)
	t1 := time.Date(2011, 10, 5, 14, 48, 0, 0, time.UTC)
	assert.Equal(t, resource.Get("date"), &t1)

	r2 := DateResolver("Date", time.RFC822)
	resource = NewResourceData(PostgresDialect{}, dateTestTable, nil, testDateStruct{Date: "2011-10-05T14:48:00.000Z"}, nil, time.Now())
	err = r2(context.TODO(), nil, resource, Column{Name: "date"})

	assert.Error(t, err)

	resource = NewResourceData(PostgresDialect{}, dateTestTable, nil, testDateStruct{Date: "03 Jan 06 15:04 EST"}, nil, time.Now())
	err = r2(context.TODO(), nil, resource, Column{Name: "date"})
	assert.Nil(t, err)

	t2 := time.Date(2006, 1, 3, 15, 4, 0, 0, time.UTC)
	assert.Equal(t, t2.Unix(), resource.Get("date").(*time.Time).UTC().Unix())

	r3 := DateResolver("Date", time.RFC822, "2006-01-02")
	resource = NewResourceData(PostgresDialect{}, dateTestTable, nil, testDateStruct{Date: "2011-10-05"}, nil, time.Now())
	err = r3(context.TODO(), nil, resource, Column{Name: "date"})
	assert.Nil(t, err)

	t3 := time.Date(2011, 10, 5, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, resource.Get("date"), &t3)
}

func TestNetResolvers(t *testing.T) {
	r1 := IPAddressResolver("IP")
	r2 := MACAddressResolver("MAC")
	r3 := IPNetResolver("Net")
	r4 := IPAddressesResolver("IPS")
	for _, r := range netTests {
		resource := NewResourceData(PostgresDialect{}, networkTestTable, nil, r, nil, time.Now())
		err := r1(context.TODO(), nil, resource, Column{Name: "ip"})
		assert.Nil(t, err)
		err = r2(context.TODO(), nil, resource, Column{Name: "mac"})
		assert.Nil(t, err)
		err = r3(context.TODO(), nil, resource, Column{Name: "net"})
		assert.Nil(t, err)
		err = r4(context.TODO(), nil, resource, Column{Name: "ips"})
		assert.Nil(t, err)
	}
	for _, r := range netTestsFails {
		resource := NewResourceData(PostgresDialect{}, networkTestTable, nil, r, nil, time.Now())
		err := r1(context.TODO(), nil, resource, Column{Name: "ip"})
		assert.Error(t, err)
		err = r2(context.TODO(), nil, resource, Column{Name: "mac"})
		assert.Error(t, err)
		err = r3(context.TODO(), nil, resource, Column{Name: "net"})
		assert.Error(t, err)
		err = r4(context.TODO(), nil, resource, Column{Name: "ips"})
		assert.Error(t, err)
	}
}

func TestTransformersResolvers(t *testing.T) {
	r1 := StringResolver("Int")
	r2 := StringResolver("Float")
	r3 := IntResolver("String")
	r4 := IntResolver("Float")
	r5 := IntResolver("BadFloat")
	resource := NewResourceData(PostgresDialect{}, TransformersTestTable, nil, testTransformersStruct{Int: 10, Float: 10.2, String: "123", BadFloat: "10,1"}, nil, time.Now())
	err := r1(context.TODO(), nil, resource, Column{Name: "int_to_string"})
	assert.Nil(t, err)
	assert.Equal(t, resource.Get("int_to_string"), "10")

	err = r2(context.TODO(), nil, resource, Column{Name: "float_to_string"})
	assert.Nil(t, err)
	assert.Equal(t, resource.Get("float_to_string"), "10.2")

	err = r3(context.TODO(), nil, resource, Column{Name: "string_to_int"})
	assert.Nil(t, err)
	assert.Equal(t, resource.Get("string_to_int"), 123)

	err = r4(context.TODO(), nil, resource, Column{Name: "float_to_int"})
	assert.Nil(t, err)
	assert.Equal(t, resource.Get("float_to_int"), 10)

	err = r5(context.TODO(), nil, resource, Column{Name: "float_to_int"})
	assert.Error(t, err)
}

func TestUUIDResolver(t *testing.T) {
	r1 := UUIDResolver("UUID")
	r2 := UUIDResolver("BadUUID")
	resource := NewResourceData(PostgresDialect{}, UUIDTestTable, nil, testUUIDStruct{UUID: "123e4567-e89b-12d3-a456-426614174000", BadUUID: "123e4567-e89b-12d3-a456-4266141740001"}, nil, time.Now())

	err := r1(context.TODO(), nil, resource, Column{Name: "uuid"})
	assert.Nil(t, err)
	u, err := uuid.FromString("123e4567-e89b-12d3-a456-426614174000")
	assert.Nil(t, err)
	assert.Equal(t, u, resource.Get("uuid"))

	err = r2(context.TODO(), nil, resource, Column{Name: "uuid"})
	assert.Error(t, err)
}
