package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	address "github.com/enterprise/amqp-broker-core/internal/address/domain"
	"time"
)

type PostgresAddresses struct{ db *sql.DB }

func NewPostgresAddresses(db *sql.DB) *PostgresAddresses { return &PostgresAddresses{db: db} }
func (p *PostgresAddresses) Create(ctx context.Context, a address.Address) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO addresses(name,kind,durable,max_length,default_ttl_ns,dead_letter,paused,created_at,version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, a.Name, a.Kind, a.Durable, a.MaxLength, int64(a.DefaultTTL), nullable(a.DeadLetter), a.Paused, a.CreatedAt, a.Version)
	if err != nil {
		return fmt.Errorf("insert address: %w", err)
	}
	return nil
}
func (p *PostgresAddresses) Get(ctx context.Context, name string) (address.Address, error) {
	var a address.Address
	var ttl int64
	var dead sql.NullString
	err := p.db.QueryRowContext(ctx, `SELECT name,kind,durable,max_length,default_ttl_ns,dead_letter,paused,created_at,version FROM addresses WHERE name=$1`, name).Scan(&a.Name, &a.Kind, &a.Durable, &a.MaxLength, &ttl, &dead, &a.Paused, &a.CreatedAt, &a.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return a, address.ErrNotFound
	}
	if err != nil {
		return a, fmt.Errorf("select address: %w", err)
	}
	a.DefaultTTL = time.Duration(ttl)
	a.DeadLetter = dead.String
	return a, nil
}
func (p *PostgresAddresses) List(ctx context.Context) ([]address.Address, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT name,kind,durable,max_length,default_ttl_ns,dead_letter,paused,created_at,version FROM addresses ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []address.Address{}
	for rows.Next() {
		var a address.Address
		var ttl int64
		var dead sql.NullString
		if err = rows.Scan(&a.Name, &a.Kind, &a.Durable, &a.MaxLength, &ttl, &dead, &a.Paused, &a.CreatedAt, &a.Version); err != nil {
			return nil, err
		}
		a.DefaultTTL = time.Duration(ttl)
		a.DeadLetter = dead.String
		out = append(out, a)
	}
	return out, rows.Err()
}
func (p *PostgresAddresses) SetPaused(ctx context.Context, name string, paused bool) (address.Address, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE addresses SET paused=$2,version=version+1 WHERE name=$1`, name, paused)
	if err != nil {
		return address.Address{}, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return address.Address{}, address.ErrNotFound
	}
	return p.Get(ctx, name)
}
func (p *PostgresAddresses) Delete(ctx context.Context, name string) error {
	res, err := p.db.ExecContext(ctx, `DELETE FROM addresses WHERE name=$1`, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return address.ErrNotFound
	}
	return nil
}
func (p *PostgresAddresses) CreateBinding(ctx context.Context, b address.Binding) error {
	raw, err := jsonMap(b.Filter)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(ctx, `INSERT INTO bindings(id,source,destination,routing_key,filter,priority,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, b.ID, b.Source, b.Destination, b.RoutingKey, raw, b.Priority, b.CreatedAt)
	return err
}
func (p *PostgresAddresses) ListBindings(ctx context.Context, source string) ([]address.Binding, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id,source,destination,routing_key,filter,priority,created_at FROM bindings WHERE ($1='' OR source=$1) ORDER BY priority DESC,id`, source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []address.Binding{}
	for rows.Next() {
		var b address.Binding
		var raw []byte
		if err = rows.Scan(&b.ID, &b.Source, &b.Destination, &b.RoutingKey, &raw, &b.Priority, &b.CreatedAt); err != nil {
			return nil, err
		}
		if err = parseJSONMap(raw, &b.Filter); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
func (p *PostgresAddresses) DeleteBinding(ctx context.Context, id string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM bindings WHERE id=$1`, id)
	return err
}
func nullable(v string) any {
	if v == "" {
		return nil
	}
	return v
}
