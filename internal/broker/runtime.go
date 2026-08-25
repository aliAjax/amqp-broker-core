package broker

import (
	"context"
	"database/sql"
	"fmt"
	addressapp "github.com/enterprise/amqp-broker-core/internal/address/application"
	amqpserver "github.com/enterprise/amqp-broker-core/internal/amqp/adapter"
	clusterapp "github.com/enterprise/amqp-broker-core/internal/cluster/application"
	cluster "github.com/enterprise/amqp-broker-core/internal/cluster/domain"
	clusterinfra "github.com/enterprise/amqp-broker-core/internal/cluster/infrastructure"
	connapp "github.com/enterprise/amqp-broker-core/internal/connection/application"
	conninfra "github.com/enterprise/amqp-broker-core/internal/connection/infrastructure"
	deliveryapp "github.com/enterprise/amqp-broker-core/internal/delivery/application"
	delivery "github.com/enterprise/amqp-broker-core/internal/delivery/domain"
	linkapp "github.com/enterprise/amqp-broker-core/internal/link/application"
	routingapp "github.com/enterprise/amqp-broker-core/internal/routing/application"
	routing "github.com/enterprise/amqp-broker-core/internal/routing/domain"
	sessionapp "github.com/enterprise/amqp-broker-core/internal/session/application"
	storage "github.com/enterprise/amqp-broker-core/internal/storage/infrastructure"
	transactionapp "github.com/enterprise/amqp-broker-core/internal/transaction/application"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Runtime struct {
	Config       ConfigLike
	Addresses    *addressapp.Service
	Delivery     *deliveryapp.Service
	Transactions *transactionapp.Service
	Connections  *connapp.Registry
	Sessions     *sessionapp.Registry
	Links        *linkapp.Registry
	Cluster      *clusterapp.Service
	Coordinator  *clusterinfra.Coordinator
	Memory       *storage.Memory
	Snapshot     *storage.SnapshotFile
	BodyLog      delivery.BodyLog
	AMQP         *amqpserver.Server
	logger       *slog.Logger
	mu           sync.Mutex
}
type ConfigLike interface{ GetNodeID() string }
type Settings struct {
	NodeID         string
	AMQPAddress    string
	MaxFrame       uint32
	IdleTimeout    time.Duration
	MaxConnections int
	DataDir        string
	LeaseTTL       time.Duration
	Shards         uint32
	Logger         *slog.Logger
	AuthToken      string
	AllowAnonymous bool
}

func Build(settings Settings) (*Runtime, error) {
	if settings.Logger == nil {
		settings.Logger = slog.Default()
	}
	if settings.Shards == 0 {
		settings.Shards = 32
	}
	if settings.MaxConnections == 0 {
		settings.MaxConnections = 1024
	}
	if settings.MaxFrame == 0 {
		settings.MaxFrame = 1 << 20
	}
	if settings.LeaseTTL <= 0 {
		settings.LeaseTTL = 15 * time.Second
	}
	mem := storage.NewMemory()
	snap := storage.NewSnapshotFile(settings.DataDir)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := snap.Load(ctx, mem); err != nil {
		return nil, err
	}
	logStore, err := storage.OpenFileLog(filepath.Clean(settings.DataDir))
	if err != nil {
		return nil, err
	}
	addresses := addressapp.NewService(mem, mem, nil)
	router := routingapp.NewRouter(mem, mem, routing.StandardMatcher{})
	deliveries := deliveryapp.NewService(storage.NewDeliveryMemory(mem), mem, mem, router, logStore, nil)
	tx := transactionapp.NewService(storage.NewTransactionMemory(mem), deliveries, nil, 30*time.Second)
	coord := clusterinfra.NewCoordinator()
	_ = coord.RegisterNode(ctx, cluster.Node{ID: settings.NodeID, Capacity: int(settings.Shards)})
	cl := clusterapp.NewService(settings.NodeID, settings.Shards, settings.LeaseTTL, coord)
	connections := connapp.NewRegistry(settings.MaxConnections)
	sessions := sessionapp.NewRegistry()
	links := linkapp.NewRegistry()
	runtime := &Runtime{Addresses: addresses, Delivery: deliveries, Transactions: tx, Connections: connections, Sessions: sessions, Links: links, Cluster: cl, Coordinator: coord, Memory: mem, Snapshot: snap, BodyLog: logStore, logger: settings.Logger}
	runtime.AMQP = &amqpserver.Server{Address: settings.AMQPAddress, MaxFrame: settings.MaxFrame, IdleTimeout: settings.IdleTimeout, Auth: conninfra.StaticAuthenticator{AllowAnonymous: settings.AllowAnonymous}, Connections: connections, Sessions: sessions, Links: links, Delivery: deliveries, Logger: settings.Logger}
	runtime.Config = settingsConfig{node: settings.NodeID}
	return runtime, nil
}

type settingsConfig struct{ node string }

func (s settingsConfig) GetNodeID() string { return s.node }
func (r *Runtime) Start(ctx context.Context) error {
	if err := r.AMQP.Start(ctx); err != nil {
		return err
	}
	return nil
}
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.AMQP != nil {
		if err := r.AMQP.Close(ctx); err != nil {
			return err
		}
	}
	if r.Snapshot != nil {
		if err := r.Snapshot.Save(ctx, r.Memory); err != nil {
			return err
		}
	}
	if r.BodyLog != nil {
		return r.BodyLog.Close()
	}
	return nil
}
func (r *Runtime) Sweep(ctx context.Context) (int, error) {
	a, err := r.Delivery.SweepExpired(ctx)
	if err != nil {
		return a, err
	}
	t, err := r.Transactions.Sweep(ctx)
	return a + t, err
}
func (r *Runtime) Status(ctx context.Context) map[string]any {
	leases, _ := r.Cluster.Leases(ctx)
	addresses, _ := r.Addresses.List(ctx)
	return map[string]any{"node_id": r.Config.GetNodeID(), "addresses": len(addresses), "connections": r.Connections.Count(), "leases": len(leases), "storage": "memory+filelog"}
}
func _(*sql.DB, *os.File, fmt.Stringer) {}
