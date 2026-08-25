package adapter

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	amqpapp "github.com/enterprise/amqp-broker-core/internal/amqp/application"
	amqp "github.com/enterprise/amqp-broker-core/internal/amqp/domain"
	connapp "github.com/enterprise/amqp-broker-core/internal/connection/application"
	conn "github.com/enterprise/amqp-broker-core/internal/connection/domain"
	deliveryapp "github.com/enterprise/amqp-broker-core/internal/delivery/application"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	linkapp "github.com/enterprise/amqp-broker-core/internal/link/application"
	link "github.com/enterprise/amqp-broker-core/internal/link/domain"
	sessionapp "github.com/enterprise/amqp-broker-core/internal/session/application"
	session "github.com/enterprise/amqp-broker-core/internal/session/domain"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

type Server struct {
	Address     string
	MaxFrame    uint32
	IdleTimeout time.Duration
	TLSConfig   *tls.Config
	Auth        conn.Authenticator
	Connections *connapp.Registry
	Sessions    *sessionapp.Registry
	Links       *linkapp.Registry
	Delivery    *deliveryapp.Service
	Logger      *slog.Logger
	listener    net.Listener
	wg          sync.WaitGroup
	cancel      context.CancelFunc
}

func (s *Server) Start(ctx context.Context) error {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.Address)
	if err != nil {
		return fmt.Errorf("listen AMQP: %w", err)
	}
	if s.TLSConfig != nil {
		ln = tls.NewListener(ln, s.TLSConfig)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.listener = ln
	s.wg.Add(1)
	go s.accept(runCtx)
	return nil
}
func (s *Server) accept(ctx context.Context) {
	defer s.wg.Done()
	for {
		c, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				s.Logger.Error("amqp accept failed", "error", err)
			}
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.handle(ctx, c); err != nil && !errors.Is(err, io.EOF) {
				s.Logger.Warn("amqp connection closed", "remote", c.RemoteAddr().String(), "error", err)
			}
		}()
	}
}
func (s *Server) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type peer struct {
	server           *Server
	net              net.Conn
	connection       *conn.Connection
	identity         conn.Identity
	sessionIDs       map[uint16]string
	linkIDs          map[string]string
	deliveryConsumer map[uint32]string
}

func (s *Server) handle(ctx context.Context, nc net.Conn) error {
	defer nc.Close()
	id, err := randomID("conn")
	if err != nil {
		return err
	}
	c := conn.New(id, nc.RemoteAddr().String(), s.MaxFrame, s.IdleTimeout, time.Now())
	if err = s.Connections.Add(c); err != nil {
		return err
	}
	defer func() { s.Sessions.RemoveConnection(id); s.Connections.Remove(id); _ = c.Transition(conn.StateClosed) }()
	p := &peer{server: s, net: nc, connection: c, sessionIDs: map[uint16]string{}, linkIDs: map[string]string{}, deliveryConsumer: map[uint32]string{}}
	if err = p.handshake(ctx); err != nil {
		return err
	}
	for {
		if c.IdleTimeout > 0 {
			_ = nc.SetReadDeadline(time.Now().Add(c.IdleTimeout))
		}
		frame, err := amqpapp.ReadFrame(nc, c.MaxFrame)
		if err != nil {
			return err
		}
		c.Touch(time.Now())
		if err = p.process(ctx, frame); err != nil {
			return err
		}
		if c.Snapshot().State == conn.StateClosed {
			return nil
		}
	}
}
func (p *peer) handshake(ctx context.Context) error {
	header := make([]byte, 8)
	if _, err := io.ReadFull(p.net, header); err != nil {
		return err
	}
	protocol, err := amqp.ValidateProtocolHeader(header)
	if err != nil {
		return err
	}
	if protocol == 3 {
		if p.server.Auth == nil {
			return errors.New("SASL requested but no authenticator configured")
		}
		if _, err = p.net.Write(amqp.SASLHeader); err != nil {
			return err
		}
		symbols := make([]any, 0, len(p.server.Auth.Mechanisms()))
		for _, m := range p.server.Auth.Mechanisms() {
			symbols = append(symbols, amqp.Symbol(m))
		}
		mech := amqp.Performative{Descriptor: amqp.DescriptorSASLMechanisms, Fields: []any{symbols}}
		if err = p.write(0, amqp.FrameTypeSASL, mech, nil); err != nil {
			return err
		}
		frame, err := amqpapp.ReadFrame(p.net, p.connection.MaxFrame)
		if err != nil {
			return err
		}
		init, _, err := amqpapp.DecodePerformative(frame.Body)
		if err != nil || init.Descriptor != amqp.DescriptorSASLInit {
			return errors.New("expected SASL init")
		}
		identity, authErr := p.server.Auth.Authenticate(ctx, init.String(0), init.Binary(1))
		code := uint8(0)
		if authErr != nil {
			code = 1
		}
		outcome := amqp.Performative{Descriptor: amqp.DescriptorSASLOutcome, Fields: []any{code}}
		if err = p.write(0, amqp.FrameTypeSASL, outcome, nil); err != nil {
			return err
		}
		if authErr != nil {
			return authErr
		}
		p.identity = identity
		if _, err = io.ReadFull(p.net, header); err != nil {
			return err
		}
		protocol, err = amqp.ValidateProtocolHeader(header)
		if err != nil || protocol != 0 {
			return errors.New("expected AMQP protocol header after SASL")
		}
	}
	if _, err = p.net.Write(amqp.AMQPHeader); err != nil {
		return err
	}
	return p.connection.Transition(conn.StateHeader)
}
func (p *peer) process(ctx context.Context, frame amqp.Frame) error {
	perf, n, err := amqpapp.DecodePerformative(frame.Body)
	if err != nil {
		return fmt.Errorf("decode frame on channel %d: %w", frame.Channel, err)
	}
	payload := frame.Body[n:]
	switch perf.Descriptor {
	case amqp.DescriptorOpen:
		return p.onOpen(perf)
	case amqp.DescriptorBegin:
		return p.onBegin(frame.Channel, perf)
	case amqp.DescriptorAttach:
		return p.onAttach(ctx, frame.Channel, perf)
	case amqp.DescriptorFlow:
		return p.onFlow(ctx, frame.Channel, perf)
	case amqp.DescriptorTransfer:
		return p.onTransfer(ctx, frame.Channel, perf, payload)
	case amqp.DescriptorDisposition:
		return p.onDisposition(ctx, perf)
	case amqp.DescriptorDetach:
		return p.onDetach(ctx, frame.Channel, perf)
	case amqp.DescriptorEnd:
		return p.onEnd(frame.Channel)
	case amqp.DescriptorClose:
		return p.onClose()
	default:
		return fmt.Errorf("performative %s not valid in connection state", perf.Name())
	}
}
func (p *peer) onOpen(perf amqp.Performative) error {
	if p.connection.Snapshot().State != conn.StateHeader {
		return errors.New("duplicate open")
	}
	p.connection.ContainerID = perf.String(0)
	if peerMax := perf.Uint(2); peerMax >= 512 && peerMax < p.connection.MaxFrame {
		p.connection.MaxFrame = peerMax
	}
	if err := p.connection.Transition(conn.StateOpen); err != nil {
		return err
	}
	reply := amqp.Performative{Descriptor: amqp.DescriptorOpen, Fields: []any{"amqp-broker-core", nil, p.connection.MaxFrame, uint16(65535), uint32(p.connection.IdleTimeout / time.Millisecond)}}
	return p.write(0, amqp.FrameTypeAMQP, reply, nil)
}
func (p *peer) onBegin(channel uint16, _ amqp.Performative) error {
	if p.connection.Snapshot().State != conn.StateOpen {
		return errors.New("begin before open")
	}
	id, err := randomID("ssn")
	if err != nil {
		return err
	}
	s := session.New(id, p.connection.ID, channel, channel, time.Now())
	if err = s.Begin(); err != nil {
		return err
	}
	if err = p.server.Sessions.Add(s); err != nil {
		return err
	}
	if err = p.connection.AddSession(channel, id); err != nil {
		return err
	}
	p.sessionIDs[channel] = id
	reply := amqp.Performative{Descriptor: amqp.DescriptorBegin, Fields: []any{uint16(channel), uint32(0), uint32(2048), uint32(2048)}}
	return p.write(channel, amqp.FrameTypeAMQP, reply, nil)
}
func (p *peer) onAttach(ctx context.Context, channel uint16, perf amqp.Performative) error {
	sessionID, ok := p.sessionIDs[channel]
	if !ok {
		return errors.New("attach on unmapped session")
	}
	handle := perf.Uint(1)
	remoteRole := perf.Bool(2)
	address := perf.String(5)
	if address == "" {
		address = perf.String(6)
	}
	if address == "" {
		return errors.New("attach source or target address required")
	}
	id, err := randomID("lnk")
	if err != nil {
		return err
	}
	role := link.RoleReceiver
	if remoteRole {
		role = link.RoleSender
	}
	l, err := link.New(id, sessionID, perf.String(0), address, handle, role, time.Now())
	if err != nil {
		return err
	}
	if err = l.Attach(); err != nil {
		return err
	}
	if err = p.server.Links.Add(l); err != nil {
		return err
	}
	s, _ := p.server.Sessions.Get(sessionID)
	if err = s.Attach(handle, id); err != nil {
		return err
	}
	p.linkIDs[fmt.Sprintf("%d:%d", channel, handle)] = id
	if remoteRole {
		prefetch := uint32(100)
		consumer, err := p.server.Delivery.Register(ctx, id, address, prefetch, delivery.AtLeastOnce)
		if err != nil {
			return err
		}
		l.Grant(consumer.Credit)
	}
	reply := amqp.Performative{Descriptor: amqp.DescriptorAttach, Fields: []any{perf.String(0), handle, !remoteRole, nil, nil, perf.Field(5), perf.Field(6)}}
	return p.write(channel, amqp.FrameTypeAMQP, reply, nil)
}
func (p *peer) onFlow(ctx context.Context, channel uint16, perf amqp.Performative) error {
	sessionID, ok := p.sessionIDs[channel]
	if !ok {
		return errors.New("flow on unknown session")
	}
	s, _ := p.server.Sessions.Get(sessionID)
	s.Flow(perf.Uint(1), perf.Uint(3))
	handle := perf.Uint(4)
	l, found := p.server.Links.ByHandle(sessionID, handle)
	if !found {
		return nil
	}
	credit := perf.Uint(6)
	l.Grant(credit)
	if l.Role != link.RoleSender {
		return nil
	}
	for i := uint32(0); i < credit; i++ {
		msg, deliveryID, err := p.server.Delivery.Acquire(ctx, l.ID)
		if err != nil {
			if err.Error() == "no message available" {
				return nil
			}
			return err
		}
		if err = l.Consume(uint64(len(msg.Body))); err != nil {
			return err
		}
		fields := []any{handle, deliveryID, []byte(msg.ID), uint32(0), false, false, nil, nil, nil, nil, false}
		transfer := amqp.Performative{Descriptor: amqp.DescriptorTransfer, Fields: fields}
		if err = p.write(channel, amqp.FrameTypeAMQP, transfer, amqpapp.EncodeDataSection(msg.Body)); err != nil {
			return err
		}
		p.deliveryConsumer[deliveryID] = l.ID
	}
	return nil
}
func (p *peer) onTransfer(ctx context.Context, channel uint16, perf amqp.Performative, payload []byte) error {
	sessionID, ok := p.sessionIDs[channel]
	if !ok {
		return errors.New("transfer on unknown session")
	}
	s, _ := p.server.Sessions.Get(sessionID)
	if err := s.ConsumeIncoming(); err != nil {
		return err
	}
	handle := perf.Uint(0)
	id, ok := p.linkIDs[fmt.Sprintf("%d:%d", channel, handle)]
	if !ok {
		return errors.New("transfer on unknown link")
	}
	l, _ := p.server.Links.Get(id)
	if l.Role != link.RoleReceiver {
		return errors.New("transfer sent on sender link")
	}
	body, err := amqpapp.DecodeDataSection(payload)
	if err != nil {
		return err
	}
	if err = l.Consume(uint64(len(body))); err != nil {
		return err
	}
	msgs, err := p.server.Delivery.Publish(ctx, delivery.Publish{Address: l.Address, Body: body, Settled: perf.Bool(4)})
	if err != nil {
		return err
	}
	deliveryID := perf.Uint(1)
	if !perf.Bool(4) {
		state := map[string]any{"descriptor": byte(0x24), "value": []any{}}
		disp := amqp.Performative{Descriptor: amqp.DescriptorDisposition, Fields: []any{true, deliveryID, nil, true, state}}
		if err = p.write(channel, amqp.FrameTypeAMQP, disp, nil); err != nil {
			return err
		}
	}
	p.server.Logger.Info("message transferred", "connection_id", p.connection.ID, "address", l.Address, "copies", len(msgs))
	return nil
}
func (p *peer) onDisposition(ctx context.Context, perf amqp.Performative) error {
	first := perf.Uint(1)
	last := first
	if perf.Field(2) != nil {
		last = perf.Uint(2)
	}
	for id := first; id <= last; id++ {
		consumer := p.deliveryConsumer[id]
		if consumer != "" {
			_ = p.server.Delivery.Settle(ctx, consumer, id, delivery.StateAccepted)
			delete(p.deliveryConsumer, id)
		}
		if id == ^uint32(0) {
			break
		}
	}
	return nil
}
func (p *peer) onDetach(ctx context.Context, channel uint16, perf amqp.Performative) error {
	sessionID, ok := p.sessionIDs[channel]
	if !ok {
		return errors.New("detach on unknown session")
	}
	handle := perf.Uint(0)
	k := fmt.Sprintf("%d:%d", channel, handle)
	id, ok := p.linkIDs[k]
	if !ok {
		return errors.New("unknown link handle")
	}
	if l, found := p.server.Links.Get(id); found {
		l.Detach()
		if l.Role == link.RoleSender {
			_ = p.server.Delivery.Disconnect(ctx, id)
		}
	}
	p.server.Links.Remove(id)
	if s, found := p.server.Sessions.Get(sessionID); found {
		s.Detach(handle)
	}
	delete(p.linkIDs, k)
	reply := amqp.Performative{Descriptor: amqp.DescriptorDetach, Fields: []any{handle, true}}
	return p.write(channel, amqp.FrameTypeAMQP, reply, nil)
}
func (p *peer) onEnd(channel uint16) error {
	id, ok := p.sessionIDs[channel]
	if !ok {
		return errors.New("end on unknown session")
	}
	s, _ := p.server.Sessions.Get(id)
	_ = s.End()
	p.server.Links.RemoveSession(id)
	p.server.Sessions.Remove(id)
	p.connection.RemoveSession(channel)
	delete(p.sessionIDs, channel)
	return p.write(channel, amqp.FrameTypeAMQP, amqp.Performative{Descriptor: amqp.DescriptorEnd, Fields: []any{}}, nil)
}
func (p *peer) onClose() error {
	_ = p.connection.Transition(conn.StateClosing)
	if err := p.write(0, amqp.FrameTypeAMQP, amqp.Performative{Descriptor: amqp.DescriptorClose, Fields: []any{}}, nil); err != nil {
		return err
	}
	return p.connection.Transition(conn.StateClosed)
}
func (p *peer) write(channel uint16, typ byte, perf amqp.Performative, payload []byte) error {
	frame, err := amqpapp.FrameFor(channel, perf, payload)
	if err != nil {
		return err
	}
	frame.Type = typ
	_ = p.net.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return amqpapp.WriteFrame(p.net, frame, p.connection.MaxFrame)
}
func randomID(prefix string) (string, error) {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + "_" + hex.EncodeToString(b), nil
}
func readUint32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
