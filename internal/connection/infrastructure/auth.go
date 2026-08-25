package infrastructure

import (
	"bytes"
	"context"
	"errors"
	domain "github.com/enterprise/amqp-broker-core/internal/connection/domain"
)

type StaticAuthenticator struct {
	Username       string
	Password       string
	AllowAnonymous bool
}

func (a StaticAuthenticator) Mechanisms() []string {
	if a.AllowAnonymous {
		return []string{"ANONYMOUS", "PLAIN"}
	}
	return []string{"PLAIN"}
}
func (a StaticAuthenticator) Authenticate(ctx context.Context, mechanism string, response []byte) (domain.Identity, error) {
	if err := ctx.Err(); err != nil {
		return domain.Identity{}, err
	}
	switch mechanism {
	case "ANONYMOUS":
		if !a.AllowAnonymous {
			return domain.Identity{}, errors.New("anonymous authentication disabled")
		}
		return domain.Identity{Subject: "anonymous", Mechanism: mechanism}, nil
	case "PLAIN":
		parts := bytes.Split(response, []byte{0})
		if len(parts) != 3 {
			return domain.Identity{}, errors.New("invalid PLAIN response")
		}
		if string(parts[1]) != a.Username || string(parts[2]) != a.Password {
			return domain.Identity{}, errors.New("invalid credentials")
		}
		return domain.Identity{Subject: string(parts[1]), Mechanism: mechanism}, nil
	default:
		return domain.Identity{}, errors.New("unsupported SASL mechanism")
	}
}
