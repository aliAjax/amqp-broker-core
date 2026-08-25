package main

import (
	"flag"
	"fmt"
	amqpapp "github.com/enterprise/amqp-broker-core/internal/amqp/application"
	amqp "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
	"io"
	"net"
	"os"
	"time"
)

type client struct {
	conn net.Conn
	max  uint32
}

func main() {
	addr := flag.String("addr", "127.0.0.1:5672", "AMQP listener")
	address := flag.String("address", "demo.queue", "broker address")
	body := flag.String("body", "hello-amqp", "message body")
	mode := flag.String("mode", "publish", "publish or consume")
	sessions := flag.Int("sessions", 1, "number of sessions for publish")
	flag.Parse()
	if err := run(*addr, *address, *body, *mode, *sessions); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(network, address, body, mode string, sessions int) error {
	nc, err := net.DialTimeout("tcp", network, 3*time.Second)
	if err != nil {
		return err
	}
	defer nc.Close()
	c := &client{conn: nc, max: 1 << 20}
	if _, err = nc.Write(amqp.AMQPHeader); err != nil {
		return err
	}
	header := make([]byte, 8)
	if _, err = io.ReadFull(nc, header); err != nil {
		return err
	}
	if _, err = amqp.ValidateProtocolHeader(header); err != nil {
		return err
	}
	if err = c.send(0, amqp.Performative{Descriptor: amqp.DescriptorOpen, Fields: []any{"amqp-sim", nil, uint32(1 << 20), uint16(65535)}}); err != nil {
		return err
	}
	if _, err = c.receive(); err != nil {
		return err
	}
	if mode == "consume" {
		return c.consume(address)
	}
	for i := 0; i < sessions; i++ {
		channel := uint16(i + 1)
		if err = c.publish(channel, address, fmt.Sprintf("%s-%d", body, i+1)); err != nil {
			return err
		}
	}
	if err = c.send(0, amqp.Performative{Descriptor: amqp.DescriptorClose, Fields: []any{}}); err != nil {
		return err
	}
	_, err = c.receive()
	return err
}
func (c *client) publish(channel uint16, address, body string) error {
	if err := c.begin(channel); err != nil {
		return err
	}
	handle := uint32(0)
	attach := amqp.Performative{Descriptor: amqp.DescriptorAttach, Fields: []any{fmt.Sprintf("sender-%d", channel), handle, false, nil, nil, nil, address}}
	if err := c.send(channel, attach); err != nil {
		return err
	}
	if _, err := c.receive(); err != nil {
		return err
	}
	transfer := amqp.Performative{Descriptor: amqp.DescriptorTransfer, Fields: []any{handle, uint32(channel), []byte(fmt.Sprintf("tag-%d", channel)), uint32(0), false}}
	if err := c.sendPayload(channel, transfer, amqpapp.EncodeDataSection([]byte(body))); err != nil {
		return err
	}
	reply, err := c.receive()
	if err != nil {
		return err
	}
	if reply.Descriptor != amqp.DescriptorDisposition {
		return fmt.Errorf("expected disposition, got %s", reply.Name())
	}
	fmt.Printf("published channel=%d address=%s body=%q disposition=%s\n", channel, address, body, reply.Name())
	if err = c.send(channel, amqp.Performative{Descriptor: amqp.DescriptorEnd, Fields: []any{}}); err != nil {
		return err
	}
	_, err = c.receive()
	return err
}
func (c *client) consume(address string) error {
	channel := uint16(1)
	if err := c.begin(channel); err != nil {
		return err
	}
	handle := uint32(0)
	attach := amqp.Performative{Descriptor: amqp.DescriptorAttach, Fields: []any{"receiver", handle, true, nil, nil, address, nil}}
	if err := c.send(channel, attach); err != nil {
		return err
	}
	if _, err := c.receive(); err != nil {
		return err
	}
	flow := amqp.Performative{Descriptor: amqp.DescriptorFlow, Fields: []any{uint32(0), uint32(2048), uint32(0), uint32(2048), handle, uint32(0), uint32(1)}}
	if err := c.send(channel, flow); err != nil {
		return err
	}
	frame, perf, used, err := c.receiveFrame()
	if err != nil {
		return err
	}
	if perf.Descriptor != amqp.DescriptorTransfer {
		return fmt.Errorf("expected transfer, got %s", perf.Name())
	}
	data, err := amqpapp.DecodeDataSection(frame.Body[used:])
	if err != nil {
		return err
	}
	id := perf.Uint(1)
	fmt.Printf("consumed address=%s delivery=%d body=%q\n", address, id, string(data))
	accepted := map[string]any{"descriptor": byte(0x24), "value": []any{}}
	disp := amqp.Performative{Descriptor: amqp.DescriptorDisposition, Fields: []any{false, id, nil, true, accepted}}
	if err = c.send(channel, disp); err != nil {
		return err
	}
	if err = c.send(0, amqp.Performative{Descriptor: amqp.DescriptorClose, Fields: []any{}}); err != nil {
		return err
	}
	_, err = c.receive()
	return err
}
func (c *client) begin(channel uint16) error {
	begin := amqp.Performative{Descriptor: amqp.DescriptorBegin, Fields: []any{nil, uint32(0), uint32(2048), uint32(2048)}}
	if err := c.send(channel, begin); err != nil {
		return err
	}
	reply, err := c.receive()
	if err != nil {
		return err
	}
	if reply.Descriptor != amqp.DescriptorBegin {
		return fmt.Errorf("expected begin, got %s", reply.Name())
	}
	return nil
}
func (c *client) send(channel uint16, p amqp.Performative) error {
	return c.sendPayload(channel, p, nil)
}
func (c *client) sendPayload(channel uint16, p amqp.Performative, payload []byte) error {
	f, err := amqpapp.FrameFor(channel, p, payload)
	if err != nil {
		return err
	}
	return amqpapp.WriteFrame(c.conn, f, c.max)
}
func (c *client) receive() (amqp.Performative, error) {
	_, p, _, err := c.receiveFrame()
	return p, err
}
func (c *client) receiveFrame() (amqp.Frame, amqp.Performative, int, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := amqpapp.ReadFrame(c.conn, c.max)
	if err != nil {
		return f, amqp.Performative{}, 0, err
	}
	p, n, err := amqpapp.DecodePerformative(f.Body)
	return f, p, n, err
}
