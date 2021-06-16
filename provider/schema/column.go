package schema

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	gofrs "github.com/gofrs/uuid"
	"github.com/google/uuid"
	"github.com/modern-go/reflect2"
	"github.com/thoas/go-funk"
)

type ValueType int

const (
	TypeInvalid ValueType = iota
	TypeBool
	TypeSmallInt
	TypeInt
	TypeBigInt
	TypeFloat
	TypeUUID
	TypeString
	TypeByteArray
	TypeStringArray
	TypeIntArray
	TypeTimestamp
	TypeJSON
	TypeUUIDArray
	TypeInet
	TypeCIDR
	TypeMacAddr
)

func (v ValueType) String() string {
	switch v {
	case TypeBool:
		return "TypeBool"
	case TypeBigInt:
		return "TypeBigInt"
	case TypeSmallInt:
		return "TypeSmallInt"
	case TypeInt:
		return "TypeInt"
	case TypeFloat:
		return "TypeFloat"
	case TypeUUID:
		return "TypeUUID"
	case TypeString:
		return "TypeString"
	case TypeJSON:
		return "TypeJSON"
	case TypeIntArray:
		return "TypeIntArray"
	case TypeStringArray:
		return "TypeStringArray"
	case TypeTimestamp:
		return "TypeTimestamp"
	case TypeByteArray:
		return "TypeByteArray"
	case TypeUUIDArray:
		return "TypeUUIDArray"
	case TypeInet:
		return "TypeInet"
	case TypeMacAddr:
		return "TypeMacAddr"
	case TypeCIDR:
		return "TypeCIDR"
	case TypeInvalid:
		fallthrough
	default:
		return "TypeInvalid"
	}
}

func ValueTypeFromString(s string) ValueType {
	switch strings.ToLower(s) {
	case "bool", "TypeBool":
		return TypeBool
	case "int", "TypeInt":
		return TypeInt
	case "bigint", "TypeBigInt":
		return TypeBigInt
	case "smallint", "TypeSmallInt":
		return TypeSmallInt
	case "float", "TypeFloat":
		return TypeFloat
	case "uuid", "TypeUUID":
		return TypeUUID
	case "string", "TypeString":
		return TypeString
	case "json", "TypeJSON":
		return TypeJSON
	case "intarray", "TypeIntArray":
		return TypeIntArray
	case "stringarray", "TypeStringArray":
		return TypeStringArray
	case "bytearray":
		return TypeByteArray
	case "timestamp", "TypeTimestamp":
		return TypeTimestamp
	case "uuidarray", "TypeUUIDArray":
		return TypeUUIDArray
	case "typeinet", "TypeInet":
		return TypeInet
	case "typemacaddr", "TypeMacAddr":
		return TypeMacAddr
	case "typecidr", "TypeCIDR":
		return TypeCIDR
	case "invalid", "TypeInvalid":
		return TypeInvalid
	default:
		return TypeInvalid
	}
}

// ColumnResolver is called for each row received in TableResolver's data fetch.
// execution holds all relevant information regarding execution as well as the Column bieng called.
// resource holds the current row we are resolving the column for.
type ColumnResolver func(ctx context.Context, meta ClientMeta, resource *Resource, c Column) error

// ColumnCreationOptions allow to modify how column is defined when table is created
type ColumnCreationOptions struct {
	Nullable bool
	Unique   bool
}

// Column definition for Table
type Column struct {
	// Name of column
	Name string
	// Value Type of column i.e String, UUID etc'
	Type ValueType
	// Description about column, this description is added as a comment in the database
	Description string
	// Default value if the resolver/default getting gets a nil value
	Default interface{}
	// Column Resolver allows to set you own data based on resolving this can be an API call or setting multiple embedded values etc'
	Resolver ColumnResolver
	// Creation options allow to modify how column is defined when table is created
	CreationOptions ColumnCreationOptions
}

func (c Column) ValidateType(v interface{}) error {
	if !c.checkType(v) {
		return fmt.Errorf("column %s expected %s got %T", c.Name, c.Type.String(), v)
	}
	return nil
}

func (c Column) checkType(v interface{}) bool {
	if reflect2.IsNil(v) {
		return true
	}

	if reflect2.TypeOf(v).Kind() == reflect.Ptr {
		return c.checkType(funk.GetOrElse(v, nil))
	}

	// Maps are jsons
	if reflect2.TypeOf(v).Kind() == reflect.Map {
		return c.Type == TypeJSON
	}

	switch val := v.(type) {
	case int8, *int8, uint8, *uint8, int16, *int16:
		return c.Type == TypeSmallInt
	case uint16, int32, *int32:
		return c.Type == TypeInt
	case int, *int, uint32, *uint32, int64, *int64:
		return c.Type == TypeBigInt
	case []byte:
		if c.Type == TypeUUID {
			if _, err := uuid.FromBytes(val); err != nil {
				return false
			}
		}
		return c.Type == TypeByteArray || c.Type == TypeJSON
	case bool, *bool:
		return c.Type == TypeBool
	case string:
		if c.Type == TypeUUID {
			if _, err := uuid.Parse(val); err == nil {
				return true
			}
		}
		if c.Type == TypeJSON {
			return true
		}
		return c.Type == TypeString
	case *string:
		if c.Type == TypeJSON {
			return true
		}
		return c.Type == TypeString
	case *float32, float32, *float64, float64:
		return c.Type == TypeFloat
	case []string, []*string, *[]string:
		return c.Type == TypeStringArray
	case []int, []*int, *[]int:
		return c.Type == TypeIntArray
	case time.Time, *time.Time:
		return c.Type == TypeTimestamp
	case uuid.UUID, *uuid.UUID:
		return c.Type == TypeUUID
	case gofrs.UUID, *gofrs.UUID:
		return c.Type == TypeUUID
	case [16]byte:
		return c.Type == TypeUUID
	case net.HardwareAddr, *net.HardwareAddr:
		return c.Type == TypeMacAddr
	case net.IPAddr, *net.IPAddr, *net.IP, net.IP:
		return c.Type == TypeInet
	case net.IPNet, *net.IPNet:
		return c.Type == TypeCIDR
	case interface{}:
		kindName := reflect2.TypeOf(v).Kind()
		if kindName == reflect.String && c.Type == TypeString {
			return true
		}
		if kindName == reflect.Slice {
			if c.Type == TypeStringArray && reflect.String == reflect2.TypeOf(v).Type1().Elem().Kind() {
				return true
			}
			if c.Type == TypeIntArray && reflect.Int == reflect2.TypeOf(v).Type1().Elem().Kind() {
				return true
			}
			if c.Type == TypeUUIDArray && reflect2.TypeOf(v).String() == "uuid.UUID" || reflect2.TypeOf(v).String() == "*uuid.UUID" {
				return c.Type == TypeUUIDArray
			}
		}
		if c.Type == TypeSmallInt && (kindName == reflect.Int8 || kindName == reflect.Int16 || kindName == reflect.Uint8) {
			return true
		}

		if c.Type == TypeInt && (kindName == reflect.Uint16 || kindName == reflect.Int32) {
			return true
		}
		if c.Type == TypeBigInt && (kindName == reflect.Int || kindName == reflect.Int64 || kindName == reflect.Uint || kindName == reflect.Uint32 || kindName == reflect.Uint64) {
			return true
		}
	}

	return false
}
