package application

import (
	"encoding/binary"
	"errors"
	"fmt"
	domain "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
)

func EncodePerformative(p domain.Performative) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	fields := trimNulls(p.Fields)
	list, err := encodeList(fields)
	if err != nil {
		return nil, err
	}
	out := []byte{0x00, 0x53, p.Descriptor}
	out = append(out, list...)
	return out, nil
}
func trimNulls(v []any) []any {
	n := len(v)
	for n > 0 && v[n-1] == nil {
		n--
	}
	return v[:n]
}
func encodeList(fields []any) ([]byte, error) {
	if len(fields) == 0 {
		return []byte{0x45}, nil
	}
	payload := []byte{}
	for _, f := range fields {
		b, err := encodeValue(f)
		if err != nil {
			return nil, err
		}
		payload = append(payload, b...)
	}
	if len(payload)+1 <= 255 && len(fields) <= 255 {
		return append([]byte{0xc0, byte(len(payload) + 1), byte(len(fields))}, payload...), nil
	}
	out := make([]byte, 9)
	out[0] = 0xd0
	binary.BigEndian.PutUint32(out[1:5], uint32(len(payload)+4))
	binary.BigEndian.PutUint32(out[5:9], uint32(len(fields)))
	return append(out, payload...), nil
}
func encodeValue(v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return []byte{0x40}, nil
	case bool:
		if x {
			return []byte{0x41}, nil
		}
		return []byte{0x42}, nil
	case uint8:
		return []byte{0x50, x}, nil
	case uint16:
		b := []byte{0x60, 0, 0}
		binary.BigEndian.PutUint16(b[1:], x)
		return b, nil
	case uint32:
		if x == 0 {
			return []byte{0x43}, nil
		}
		if x < 256 {
			return []byte{0x52, byte(x)}, nil
		}
		b := make([]byte, 5)
		b[0] = 0x70
		binary.BigEndian.PutUint32(b[1:], x)
		return b, nil
	case uint64:
		if x == 0 {
			return []byte{0x44}, nil
		}
		if x < 256 {
			return []byte{0x53, byte(x)}, nil
		}
		b := make([]byte, 9)
		b[0] = 0x80
		binary.BigEndian.PutUint64(b[1:], x)
		return b, nil
	case string:
		return encodeBytes(0xa1, 0xb1, []byte(x)), nil
	case domain.Symbol:
		return encodeBytes(0xa3, 0xb3, []byte(x)), nil
	case []byte:
		return encodeBytes(0xa0, 0xb0, x), nil
	case []any:
		return encodeList(x)
	case map[string]any:
		desc, ok := x["descriptor"].(byte)
		if !ok {
			return nil, errors.New("described value lacks descriptor")
		}
		value, err := encodeValue(x["value"])
		if err != nil {
			return nil, err
		}
		return append([]byte{0x00, 0x53, desc}, value...), nil
	default:
		return nil, fmt.Errorf("cannot encode AMQP value %T", v)
	}
}
func encodeBytes(short, long byte, v []byte) []byte {
	if len(v) <= 255 {
		return append([]byte{short, byte(len(v))}, v...)
	}
	out := make([]byte, 5)
	out[0] = long
	binary.BigEndian.PutUint32(out[1:], uint32(len(v)))
	return append(out, v...)
}
