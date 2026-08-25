package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	transaction "github.com/enterprise/amqp-broker-core/internal/transaction/domain"
	"os"
	"path/filepath"
)

type Snapshot struct {
	Version      int                       `json:"version"`
	Addresses    []address.Address         `json:"addresses"`
	Bindings     []address.Binding         `json:"bindings"`
	Messages     []delivery.Message        `json:"messages"`
	Consumers    []delivery.Consumer       `json:"consumers"`
	Transactions []transaction.Transaction `json:"transactions"`
}

func (m *Memory) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s := Snapshot{Version: 1}
	for _, a := range m.addresses {
		s.Addresses = append(s.Addresses, a)
	}
	for _, b := range m.bindings {
		s.Bindings = append(s.Bindings, cloneBinding(b))
	}
	for _, msg := range m.messages {
		copyMsg := cloneMessage(msg)
		copyMsg.Body = nil
		s.Messages = append(s.Messages, copyMsg)
	}
	for _, c := range m.consumers {
		c.Unsettled = cloneUnsettled(c.Unsettled)
		s.Consumers = append(s.Consumers, c)
	}
	for _, tx := range m.transactions {
		s.Transactions = append(s.Transactions, cloneTransaction(tx))
	}
	return s, nil
}
func (m *Memory) Restore(ctx context.Context, s Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Version != 1 {
		return fmt.Errorf("unsupported snapshot version %d", s.Version)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addresses = map[string]address.Address{}
	m.bindings = map[string]address.Binding{}
	m.messages = map[string]delivery.Message{}
	m.consumers = map[string]delivery.Consumer{}
	m.transactions = map[string]transaction.Transaction{}
	for _, a := range s.Addresses {
		m.addresses[a.Name] = a
	}
	for _, b := range s.Bindings {
		m.bindings[b.ID] = cloneBinding(b)
	}
	for _, msg := range s.Messages {
		if msg.State == delivery.StateAcquired && msg.TransactionID == "" {
			msg.State = delivery.StateReleased
			msg.Redelivered = true
		}
		m.messages[msg.ID] = cloneMessage(msg)
	}
	for _, c := range s.Consumers {
		for _, id := range c.Unsettled {
			if msg, ok := m.messages[id]; ok {
				msg.State = delivery.StateReleased
				msg.Redelivered = true
				m.messages[id] = msg
			}
		}
		c.Connected = false
		c.Credit = 0
		c.Unsettled = map[uint32]string{}
		m.consumers[c.ID] = c
	}
	for _, tx := range s.Transactions {
		m.transactions[tx.ID] = cloneTransaction(tx)
	}
	return nil
}

type SnapshotFile struct {
	path string
}

func NewSnapshotFile(dir string) *SnapshotFile {
	return &SnapshotFile{path: filepath.Join(dir, "metadata.json")}
}
func (f *SnapshotFile) Load(ctx context.Context, m *Memory) error {
	b, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read metadata snapshot: %w", err)
	}
	var s Snapshot
	if err = json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("decode metadata snapshot: %w", err)
	}
	return m.Restore(ctx, s)
}
func (f *SnapshotFile) Save(ctx context.Context, m *Memory) error {
	s, err := m.Snapshot(ctx)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(f.path), 0750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), "metadata-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err = tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, f.path); err != nil {
		return err
	}
	return nil
}
