package domain

import (
	"bytes"
	"errors"
	"fmt"
)

var AMQPHeader = []byte{'A', 'M', 'Q', 'P', 0, 1, 0, 0}
var SASLHeader = []byte{'A', 'M', 'Q', 'P', 3, 1, 0, 0}

func ValidateProtocolHeader(h []byte) (byte, error) {
	if len(h) != 8 {
		return 0, errors.New("protocol header must be eight bytes")
	}
	if !bytes.Equal(h[:4], []byte("AMQP")) {
		return 0, errors.New("invalid AMQP magic")
	}
	if h[5] != 1 || h[6] != 0 || h[7] != 0 {
		return 0, fmt.Errorf("unsupported AMQP version %d.%d.%d", h[5], h[6], h[7])
	}
	if h[4] != 0 && h[4] != 3 {
		return 0, fmt.Errorf("unsupported protocol id %d", h[4])
	}
	return h[4], nil
}
