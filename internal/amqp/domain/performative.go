package domain

import (
	"errors"
	"fmt"
)

type Performative struct {
	Descriptor byte
	Fields     []any
}

func (p Performative) Name() string {
	if n, ok := Names[p.Descriptor]; ok {
		return n
	}
	return fmt.Sprintf("unknown-0x%x", p.Descriptor)
}
func (p Performative) Field(i int) any {
	if i < 0 || i >= len(p.Fields) {
		return nil
	}
	return p.Fields[i]
}
func (p Performative) String(i int) string {
	v := p.Field(i)
	switch x := v.(type) {
	case string:
		return x
	case Symbol:
		return string(x)
	default:
		return ""
	}
}
func (p Performative) Uint(i int) uint32 {
	v := p.Field(i)
	switch x := v.(type) {
	case uint32:
		return x
	case uint16:
		return uint32(x)
	case uint8:
		return uint32(x)
	case uint64:
		return uint32(x)
	default:
		return 0
	}
}
func (p Performative) Bool(i int) bool     { v := p.Field(i); b, _ := v.(bool); return b }
func (p Performative) Binary(i int) []byte { v := p.Field(i); b, _ := v.([]byte); return b }

type Symbol string

func (p Performative) Validate() error {
	min := map[byte]int{DescriptorOpen: 1, DescriptorBegin: 0, DescriptorAttach: 3, DescriptorFlow: 0, DescriptorTransfer: 1, DescriptorDisposition: 2, DescriptorDetach: 1, DescriptorEnd: 0, DescriptorClose: 0, DescriptorSASLMechanisms: 1, DescriptorSASLInit: 1, DescriptorSASLChallenge: 1, DescriptorSASLResponse: 1, DescriptorSASLOutcome: 1}
	n, ok := min[p.Descriptor]
	if !ok {
		return fmt.Errorf("unknown performative descriptor 0x%x", p.Descriptor)
	}
	if len(p.Fields) < n {
		return fmt.Errorf("%s requires at least %d fields", p.Name(), n)
	}
	switch p.Descriptor {
	case DescriptorOpen:
		if p.String(0) == "" {
			return errors.New("open container-id required")
		}
	case DescriptorAttach:
		if p.String(0) == "" {
			return errors.New("attach name required")
		}
	case DescriptorTransfer:
		if p.Field(0) == nil {
			return errors.New("transfer handle required")
		}
	}
	return nil
}
