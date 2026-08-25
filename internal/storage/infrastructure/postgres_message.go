package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	"time"
)

type PostgresMessages struct{ db *sql.DB }

func NewPostgresMessages(db *sql.DB) *PostgresMessages { return &PostgresMessages{db: db} }
func (p *PostgresMessages) Put(ctx context.Context, m delivery.Message) error {
	headers, err := json.Marshal(m.Headers)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(ctx, `INSERT INTO messages(id,address,routing_key,headers,priority,created_at,expires_at,state,delivery_count,redelivered,transaction_id,log_offset) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, m.ID, m.Address, m.RoutingKey, headers, m.Priority, m.CreatedAt, nullTime(m.ExpiresAt), m.State, m.DeliveryCount, m.Redelivered, nullable(m.TransactionID), m.LogOffset)
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}
func (p *PostgresMessages) Get(ctx context.Context, id string) (delivery.Message, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id,address,routing_key,headers,priority,created_at,expires_at,state,delivery_count,redelivered,transaction_id,log_offset FROM messages WHERE id=$1`, id)
	m, err := scanMessage(row.Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return m, delivery.ErrMessageNotFound
	}
	return m, err
}
func (p *PostgresMessages) Update(ctx context.Context, m delivery.Message) error {
	headers, _ := json.Marshal(m.Headers)
	res, err := p.db.ExecContext(ctx, `UPDATE messages SET address=$2,routing_key=$3,headers=$4,priority=$5,expires_at=$6,state=$7,delivery_count=$8,redelivered=$9,transaction_id=$10,log_offset=$11 WHERE id=$1`, m.ID, m.Address, m.RoutingKey, headers, m.Priority, nullTime(m.ExpiresAt), m.State, m.DeliveryCount, m.Redelivered, nullable(m.TransactionID), m.LogOffset)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return delivery.ErrMessageNotFound
	}
	return nil
}
func (p *PostgresMessages) Delete(ctx context.Context, id string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM messages WHERE id=$1`, id)
	return err
}
func (p *PostgresMessages) ListReady(ctx context.Context, address string, limit int) ([]delivery.Message, error) {
	query := `SELECT id,address,routing_key,headers,priority,created_at,expires_at,state,delivery_count,redelivered,transaction_id,log_offset FROM messages WHERE address=$1 AND state IN ('available','released') ORDER BY priority DESC,created_at`
	args := []any{address}
	if limit > 0 {
		query += ` LIMIT $2`
		args = append(args, limit)
	}
	return p.query(ctx, query, args...)
}
func (p *PostgresMessages) ListDead(ctx context.Context, limit int) ([]delivery.Message, error) {
	query := `SELECT id,address,routing_key,headers,priority,created_at,expires_at,state,delivery_count,redelivered,transaction_id,log_offset FROM messages WHERE state IN ('dead-lettered','expired') ORDER BY created_at`
	args := []any{}
	if limit > 0 {
		query += ` LIMIT $1`
		args = append(args, limit)
	}
	return p.query(ctx, query, args...)
}
func (p *PostgresMessages) Depth(ctx context.Context, address string) (int, error) {
	var n int
	err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM messages WHERE address=$1 AND (state IN ('available','released') OR (state='acquired' AND transaction_id IS NULL))`, address).Scan(&n)
	return n, err
}
func (p *PostgresMessages) All(ctx context.Context) ([]delivery.Message, error) {
	return p.query(ctx, `SELECT id,address,routing_key,headers,priority,created_at,expires_at,state,delivery_count,redelivered,transaction_id,log_offset FROM messages ORDER BY created_at`)
}
func (p *PostgresMessages) query(ctx context.Context, q string, args ...any) ([]delivery.Message, error) {
	rows, err := p.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []delivery.Message{}
	for rows.Next() {
		m, err := scanMessage(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

type scanner func(...any) error

func scanMessage(scan scanner) (delivery.Message, error) {
	var m delivery.Message
	var headers []byte
	var expires sql.NullTime
	var tx sql.NullString
	err := scan(&m.ID, &m.Address, &m.RoutingKey, &headers, &m.Priority, &m.CreatedAt, &expires, &m.State, &m.DeliveryCount, &m.Redelivered, &tx, &m.LogOffset)
	if err != nil {
		return m, err
	}
	m.ExpiresAt = expires.Time
	m.TransactionID = tx.String
	if err = json.Unmarshal(headers, &m.Headers); err != nil {
		return m, err
	}
	return m, nil
}
func nullTime(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v
}
func jsonMap(v map[string]string) ([]byte, error)       { return json.Marshal(v) }
func parseJSONMap(b []byte, v *map[string]string) error { return json.Unmarshal(b, v) }
