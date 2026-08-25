package domain

import "context"

type Identity struct {
	Subject    string
	Mechanism  string
	Attributes map[string]string
}
type Authenticator interface {
	Mechanisms() []string
	Authenticate(context.Context, string, []byte) (Identity, error)
}
