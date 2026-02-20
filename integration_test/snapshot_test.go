package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestConnector_SnapshotInitialMode(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateBooksTable(t, db)
	_, err := db.ExecContext(ctx, `INSERT INTO books (title, author) VALUES ('B1','A1'), ('B2','A2'), ('B3','A3')`)
	require.NoError(t, err)

	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled: true,
				Mode:    cdcconfig.SnapshotModeInitial,
			},
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "books_snapshot_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					{Name: "books", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "books_snapshot_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{LogLevel: slog.LevelInfo},
		},
		RabbitMQ: config.RabbitMQ{
			URL: fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort),
			Exchange: config.ExchangeConfig{
				Name:    "books_snapshot_exchange",
				Type:    "topic",
				Durable: true,
			},
			Queues: []config.QueueConfig{
				{Name: "books_snapshot_queue", Durable: true, Bindings: []string{"public.books.*"}},
			},
			TableRoutingKeyMapping: map[string]string{
				"public.books": "public.books",
			},
			PublisherBatchTickerDuration: 100 * time.Millisecond,
			PublisherBatchSize:           10,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	// Ensure at least one snapshot event reached destination.
	msg := mustConsumeOne(t, "books_snapshot_queue")
	require.NotEmpty(t, msg.Body)
}

func TestConnector_SnapshotOnlyMode(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateBooksTable(t, db)
	_, err := db.ExecContext(ctx, `INSERT INTO books (title, author) VALUES ('SO1','SA1')`)
	require.NoError(t, err)

	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Snapshot: cdcconfig.SnapshotConfig{
				Enabled: true,
				Mode:    cdcconfig.SnapshotModeSnapshotOnly,
				Tables: publication.Tables{
					{Name: "books", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "books_snapshot_only_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					{Name: "books", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "books_snapshot_only_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{LogLevel: slog.LevelInfo},
		},
		RabbitMQ: config.RabbitMQ{
			URL:      fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort),
			Exchange: config.ExchangeConfig{Name: "books_snapshot_only_exchange", Type: "topic", Durable: true},
			Queues: []config.QueueConfig{
				{Name: "books_snapshot_only_queue", Durable: true, Bindings: []string{"public.books.*"}},
			},
			TableRoutingKeyMapping: map[string]string{
				"public.books": "public.books",
			},
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	connector.Start(ctx)
	msg := mustConsumeOne(t, "books_snapshot_only_queue")
	require.NotEmpty(t, msg.Body)
}

func mustCreateBooksTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS books (
			id SERIAL PRIMARY KEY,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)
}
