package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Frame struct {
	Size       uint32
	DataOffset byte
	Type       byte
	Channel    uint16
	Body       []byte
}

func ParseFrame(raw []byte, max uint32) (Frame, error) {
	if len(raw) < FrameHeaderSize {
		return Frame{}, errors.New("frame shorter than header")
	}
	size := binary.BigEndian.Uint32(raw[:4])
	if size < uint32(FrameHeaderSize) {
		return Frame{}, errors.New("frame size below minimum")
	}
	if max > 0 && size > max {
		return Frame{}, fmt.Errorf("frame size %d exceeds maximum %d", size, max)
	}
	if int(size) != len(raw) {
		return Frame{}, fmt.Errorf("declared frame size %d differs from bytes %d", size, len(raw))
	}
	offset := raw[4]
	if offset < 2 {
		return Frame{}, errors.New("data offset below two words")
	}
	headerBytes := int(offset) * 4
	if headerBytes > len(raw) {
		return Frame{}, errors.New("data offset exceeds frame")
	}
	typ := raw[5]
	if typ != FrameTypeAMQP && typ != FrameTypeSASL {
		return Frame{}, fmt.Errorf("unsupported frame type %d", typ)
	}
	return Frame{Size: size, DataOffset: offset, Type: typ, Channel: binary.BigEndian.Uint16(raw[6:8]), Body: raw[headerBytes:]}, nil
}
func (f Frame) Marshal(max uint32) ([]byte, error) {
	offset := f.DataOffset
	if offset == 0 {
		offset = 2
	}
	size := uint32(offset)*4 + uint32(len(f.Body))
	if size < 8 {
		return nil, errors.New("invalid frame size")
	}
	if max > 0 && size > max {
		return nil, errors.New("frame exceeds negotiated maximum")
	}
	var out []byte
	if cap(f.Body) >= int(size) {
		out = f.Body[:size]
	} else {
		out = make([]byte, size)
	}
	for i := range out {
		out[i] = 0
	}
	binary.BigEndian.PutUint32(out[:4], size)
	out[4] = offset
	out[5] = f.Type
	binary.BigEndian.PutUint16(out[6:8], f.Channel)
	copy(out[int(offset)*4:], f.Body)
	return out, nil
}
