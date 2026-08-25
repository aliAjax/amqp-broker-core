package application

import (
	"encoding/binary"
	"errors"
	"fmt"
	domain "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
)

type decoder struct {
	data []byte
	pos  int
}

func DecodePerformative(data []byte) (domain.Performative, int, error) {
	d := decoder{data: data}
	if err := d.need(3); err != nil {
		return domain.Performative{}, 0, err
	}
	if d.byte() != 0x00 {
		return domain.Performative{}, 0, errors.New("performative must be described type")
	}
	descriptor, err := d.descriptor()
	if err != nil {
		return domain.Performative{}, 0, err
	}
	value, err := d.value()
	if err != nil {
		return domain.Performative{}, 0, fmt.Errorf("decode %s fields: %w", domain.Names[descriptor], err)
	}
	fields, ok := value.([]any)
	if !ok {
		return domain.Performative{}, 0, errors.New("performative value must be list")
	}
	p := domain.Performative{Descriptor: descriptor, Fields: fields}
	if err = p.Validate(); err != nil {
		return p, 0, err
	}
	return p, d.pos, nil
}
func (d *decoder) descriptor() (byte, error) {
	if err := d.need(1); err != nil {
		return 0, err
	}
	code := d.byte()
	switch code {
	case 0x53:
		if err := d.need(1); err != nil {
			return 0, err
		}
		return d.byte(), nil
	case 0x70:
		if err := d.need(4); err != nil {
			return 0, err
		}
		v := d.uint32()
		if v > 255 {
			return 0, errors.New("descriptor exceeds supported range")
		}
		return byte(v), nil
	default:
		return 0, fmt.Errorf("unsupported descriptor constructor 0x%x", code)
	}
}
func (d *decoder) value() (any, error) {
	if err := d.need(1); err != nil {
		return nil, err
	}
	code := d.byte()
	switch code {
	case 0x40:
		return nil, nil
	case 0x41:
		return true, nil
	case 0x42:
		return false, nil
	case 0x56:
		if err := d.need(1); err != nil {
			return nil, err
		}
		return d.byte() != 0, nil
	case 0x43:
		return uint32(0), nil
	case 0x44:
		return uint64(0), nil
	case 0x50:
		if err := d.need(1); err != nil {
			return nil, err
		}
		return uint8(d.byte()), nil
	case 0x52:
		if err := d.need(1); err != nil {
			return nil, err
		}
		return uint32(d.byte()), nil
	case 0x53:
		if err := d.need(1); err != nil {
			return nil, err
		}
		return uint64(d.byte()), nil
	case 0x60:
		if err := d.need(2); err != nil {
			return nil, err
		}
		return d.uint16(), nil
	case 0x70:
		if err := d.need(4); err != nil {
			return nil, err
		}
		return d.uint32(), nil
	case 0x80:
		if err := d.need(8); err != nil {
			return nil, err
		}
		return d.uint64(), nil
	case 0xa0:
		return d.binary8()
	case 0xb0:
		return d.binary32()
	case 0xa1:
		v, e := d.binary8()
		return string(v), e
	case 0xb1:
		v, e := d.binary32()
		return string(v), e
	case 0xa3:
		v, e := d.binary8()
		return domain.Symbol(v), e
	case 0xb3:
		v, e := d.binary32()
		return domain.Symbol(v), e
	case 0x45:
		return []any{}, nil
	case 0xc0:
		return d.list8()
	case 0xd0:
		return d.list32()
	case 0x00:
		desc, err := d.descriptor()
		if err != nil {
			return nil, err
		}
		val, err := d.value()
		if err != nil {
			return nil, err
		}
		return map[string]any{"descriptor": desc, "value": val}, nil
	default:
		return nil, fmt.Errorf("unsupported AMQP type 0x%x", code)
	}
}
func (d *decoder) list8() ([]any, error) {
	if err := d.need(2); err != nil {
		return nil, err
	}
	size := int(d.byte())
	count := int(d.byte())
	if size < 1 || size-1 > d.remaining() {
		return nil, errors.New("invalid list8 size")
	}
	end := d.pos + size - 1
	out := make([]any, 0, count)
	for i := 0; i < count; i++ {
		v, err := d.value()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if d.pos > end {
		return nil, errors.New("list8 fields exceed declared size")
	}
	d.pos = end
	return out, nil
}
func (d *decoder) list32() ([]any, error) {
	if err := d.need(8); err != nil {
		return nil, err
	}
	size := int(d.uint32())
	count := int(d.uint32())
	if size < 4 || size-4 > d.remaining() {
		return nil, errors.New("invalid list32 size")
	}
	end := d.pos + size - 4
	out := make([]any, 0, count)
	for i := 0; i < count; i++ {
		v, err := d.value()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if d.pos > end {
		return nil, errors.New("list32 fields exceed declared size")
	}
	d.pos = end
	return out, nil
}
func (d *decoder) binary8() ([]byte, error) {
	if err := d.need(1); err != nil {
		return nil, err
	}
	n := int(d.byte())
	if err := d.need(n); err != nil {
		return nil, err
	}
	v := append([]byte(nil), d.data[d.pos:d.pos+n]...)
	d.pos += n
	return v, nil
}
func (d *decoder) binary32() ([]byte, error) {
	if err := d.need(4); err != nil {
		return nil, err
	}
	n := int(d.uint32())
	if n < 0 || n > d.remaining() {
		return nil, errors.New("invalid binary32 length")
	}
	v := append([]byte(nil), d.data[d.pos:d.pos+n]...)
	d.pos += n
	return v, nil
}
func (d *decoder) need(n int) error {
	if n < 0 || d.remaining() < n {
		return errors.New("truncated AMQP value")
	}
	return nil
}
func (d *decoder) remaining() int { return len(d.data) - d.pos }
func (d *decoder) byte() byte     { v := d.data[d.pos]; d.pos++; return v }
func (d *decoder) uint16() uint16 { v := binary.BigEndian.Uint16(d.data[d.pos:]); d.pos += 2; return v }
func (d *decoder) uint32() uint32 { v := binary.BigEndian.Uint32(d.data[d.pos:]); d.pos += 4; return v }
func (d *decoder) uint64() uint64 { v := binary.BigEndian.Uint64(d.data[d.pos:]); d.pos += 8; return v }
