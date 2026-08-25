package application

import (
	"encoding/binary"
	"fmt"
	domain "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
	"io"
)

func ReadFrame(r io.Reader, max uint32) (domain.Frame, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return domain.Frame{}, err
	}
	size := binary.BigEndian.Uint32(header[:4])
	if size < 8 {
		return domain.Frame{}, fmt.Errorf("invalid frame size %d", size)
	}
	if max > 0 && size > max {
		return domain.Frame{}, fmt.Errorf("frame size %d exceeds maximum %d", size, max)
	}
	raw := make([]byte, size)
	copy(raw, header)
	if _, err := io.ReadFull(r, raw[8:]); err != nil {
		return domain.Frame{}, fmt.Errorf("read frame body: %w", err)
	}
	return domain.ParseFrame(raw, max)
}
func WriteFrame(w io.Writer, f domain.Frame, max uint32) error {
	raw, err := f.Marshal(max)
	if err != nil {
		return err
	}
	for len(raw) > 0 {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		raw = raw[n:]
	}
	return nil
}
func FrameFor(channel uint16, p domain.Performative, payload []byte) (domain.Frame, error) {
	body, err := EncodePerformative(p)
	if err != nil {
		return domain.Frame{}, err
	}
	body = append(body, payload...)
	return domain.Frame{DataOffset: 2, Type: domain.FrameTypeAMQP, Channel: channel, Body: body}, nil
}
