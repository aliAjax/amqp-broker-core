package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID   string         `yaml:"node_id"`
	HTTP     Listener       `yaml:"http"`
	GRPC     Listener       `yaml:"grpc"`
	AMQP     AMQP           `yaml:"amqp"`
	Auth     Auth           `yaml:"auth"`
	Storage  Storage        `yaml:"storage"`
	Limits   Limits         `yaml:"limits"`
	Workers  Workers        `yaml:"workers"`
	Shutdown time.Duration  `yaml:"-"`
	Raw      map[string]any `yaml:"-"`
}

type Listener struct {
	Address string `yaml:"address"`
}
type AMQP struct {
	Address        string        `yaml:"address"`
	MaxFrameSize   uint32        `yaml:"max_frame_size"`
	IdleTimeout    time.Duration `yaml:"-"`
	IdleTimeoutRaw string        `yaml:"idle_timeout"`
	MaxConnections int           `yaml:"max_connections"`
}
type Auth struct {
	Token          string `yaml:"token"`
	AllowAnonymous bool   `yaml:"allow_anonymous"`
}
type Storage struct {
	Driver  string `yaml:"driver"`
	DataDir string `yaml:"data_dir"`
	DSN     string `yaml:"dsn"`
}
type Limits struct {
	BodyBytes         int64 `yaml:"body_bytes"`
	RequestsPerSecond int   `yaml:"requests_per_second"`
}
type Workers struct {
	SweepInterval    time.Duration `yaml:"-"`
	SweepIntervalRaw string        `yaml:"sweep_interval"`
	LeaseTTL         time.Duration `yaml:"-"`
	LeaseTTLRaw      string        `yaml:"lease_ttl"`
}

func Default() Config {
	return Config{NodeID: "node-1", HTTP: Listener{Address: ":8080"}, GRPC: Listener{Address: ":9090"}, AMQP: AMQP{Address: ":5672", MaxFrameSize: 1 << 20, IdleTimeout: 30 * time.Second, MaxConnections: 1024}, Auth: Auth{AllowAnonymous: true}, Storage: Storage{Driver: "file", DataDir: "./data"}, Limits: Limits{BodyBytes: 1 << 20, RequestsPerSecond: 200}, Workers: Workers{SweepInterval: time.Second, LeaseTTL: 15 * time.Second}, Shutdown: 10 * time.Second}
}

func Load(path string) (Config, error) {
	c := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return c, fmt.Errorf("read config: %w", err)
		}
		if err = yaml.Unmarshal(b, &c); err != nil {
			return c, fmt.Errorf("parse yaml: %w", err)
		}
	}
	if c.AMQP.IdleTimeoutRaw != "" {
		d, e := time.ParseDuration(c.AMQP.IdleTimeoutRaw)
		if e != nil {
			return c, fmt.Errorf("amqp idle timeout: %w", e)
		}
		c.AMQP.IdleTimeout = d
	}
	if c.Workers.SweepIntervalRaw != "" {
		d, e := time.ParseDuration(c.Workers.SweepIntervalRaw)
		if e != nil {
			return c, fmt.Errorf("sweep interval: %w", e)
		}
		c.Workers.SweepInterval = d
	}
	if c.Workers.LeaseTTLRaw != "" {
		d, e := time.ParseDuration(c.Workers.LeaseTTLRaw)
		if e != nil {
			return c, fmt.Errorf("lease ttl: %w", e)
		}
		c.Workers.LeaseTTL = d
	}
	override(&c)
	return c, c.Validate()
}

func override(c *Config) {
	stringEnv("BROKER_NODE_ID", &c.NodeID)
	stringEnv("BROKER_HTTP_ADDRESS", &c.HTTP.Address)
	stringEnv("BROKER_GRPC_ADDRESS", &c.GRPC.Address)
	stringEnv("BROKER_AMQP_ADDRESS", &c.AMQP.Address)
	stringEnv("BROKER_AUTH_TOKEN", &c.Auth.Token)
	stringEnv("BROKER_STORAGE_DRIVER", &c.Storage.Driver)
	stringEnv("BROKER_DATA_DIR", &c.Storage.DataDir)
	stringEnv("BROKER_POSTGRES_DSN", &c.Storage.DSN)
	if v := os.Getenv("BROKER_ALLOW_ANONYMOUS"); v != "" {
		if b, e := strconv.ParseBool(v); e == nil {
			c.Auth.AllowAnonymous = b
		}
	}
	if v := os.Getenv("BROKER_MAX_CONNECTIONS"); v != "" {
		if n, e := strconv.Atoi(v); e == nil {
			c.AMQP.MaxConnections = n
		}
	}
	if v := os.Getenv("BROKER_MAX_FRAME_SIZE"); v != "" {
		if n, e := strconv.ParseUint(v, 10, 32); e == nil {
			c.AMQP.MaxFrameSize = uint32(n)
		}
	}
}
func stringEnv(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}
func (c Config) Validate() error {
	var all []error
	if strings.TrimSpace(c.NodeID) == "" {
		all = append(all, errors.New("node_id is required"))
	}
	if c.HTTP.Address == "" || c.GRPC.Address == "" || c.AMQP.Address == "" {
		all = append(all, errors.New("all listener addresses are required"))
	}
	if c.AMQP.MaxFrameSize < 512 {
		all = append(all, errors.New("max_frame_size must be at least 512"))
	}
	if c.AMQP.MaxConnections < 1 {
		all = append(all, errors.New("max_connections must be positive"))
	}
	if c.Storage.Driver != "memory" && c.Storage.Driver != "file" && c.Storage.Driver != "postgres" {
		all = append(all, errors.New("storage driver must be memory, file, or postgres"))
	}
	if c.Storage.Driver == "postgres" && c.Storage.DSN == "" {
		all = append(all, errors.New("postgres dsn is required"))
	}
	return errors.Join(all...)
}
