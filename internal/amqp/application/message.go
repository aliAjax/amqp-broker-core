package application

import (
	"errors"
	"fmt"
)

// DecodeDataSection recognizes the AMQP data section (descriptor 0x75) and returns its binary payload.
func DecodeDataSection(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty transfer payload")
	}
	if len(raw) >= 3 && raw[0] == 0x00 && raw[1] == 0x53 && raw[2] == 0x75 {
		d := decoder{data: raw[3:]}
		v, err := d.value()
		if err != nil {
			return nil, fmt.Errorf("decode data section: %w", err)
		}
		b, ok := v.([]byte)
		if !ok {
			return nil, errors.New("data section is not binary")
		}
		return b, nil
	}
	return raw, nil
}
func EncodeDataSection(body []byte) []byte {
	return append([]byte{0x00, 0x53, 0x75}, encodeBytes(0xa0, 0xb0, body)...)
}
