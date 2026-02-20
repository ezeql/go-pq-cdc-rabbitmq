package integration

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnector_InsertOperation(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	cfg := newTestConfig("users_insert_exchange", "users_insert_queue", "users_insert_slot", "users_insert_pub")
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	_, err = db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('Insert User', 'insert@acme.io')`)
	require.NoError(t, err)

	msg := mustConsumeOne(t, "users_insert_queue")
	var data map[string]any
	require.NoError(t, json.Unmarshal(msg.Body, &data))
	assert.Equal(t, "INSERT", data["operation"])
	assert.Equal(t, "Insert User", data["name"])
}

func TestConnector_UpdateOperation(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	var id int
	require.NoError(t, db.QueryRowContext(ctx, `INSERT INTO users (name, email) VALUES ('Before', 'before@acme.io') RETURNING id`).Scan(&id))

	cfg := newTestConfig("users_update_exchange", "users_update_queue", "users_update_slot", "users_update_pub")
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	_, err = db.ExecContext(ctx, `UPDATE users SET name='After', email='after@acme.io' WHERE id=$1`, id)
	require.NoError(t, err)

	msg := mustConsumeOne(t, "users_update_queue")
	var data map[string]any
	require.NoError(t, json.Unmarshal(msg.Body, &data))
	assert.Equal(t, "UPDATE", data["operation"])
	assert.Equal(t, "After", data["name"])
}

func TestConnector_DeleteOperation(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	var id int
	require.NoError(t, db.QueryRowContext(ctx, `INSERT INTO users (name, email) VALUES ('Delete Me', 'delete@acme.io') RETURNING id`).Scan(&id))

	cfg := newTestConfig("users_delete_exchange", "users_delete_queue", "users_delete_slot", "users_delete_pub")
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	_, err = db.ExecContext(ctx, `DELETE FROM users WHERE id=$1`, id)
	require.NoError(t, err)

	msg := mustConsumeOne(t, "users_delete_queue")
	var data map[string]any
	require.NoError(t, json.Unmarshal(msg.Body, &data))
	assert.Equal(t, "DELETE", data["operation"])
	assert.Equal(t, "Delete Me", data["name"])
}

func TestConnector_AckMechanism(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	// Use default batch size (10) and a fast ticker to flush quickly.
	// Batch size 2 caused a race between the batch-size trigger and the ticker,
	// making this test flaky.
	cfg := newTestConfig("users_ack_exchange", "users_ack_queue", "users_ack_slot", "users_ack_pub")

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	// Insert two rows; they will be batched and flushed by the ticker (100ms).
	_, err = db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('A1', 'a1@acme.io')`)
	require.NoError(t, err)
	_ = mustConsumeOne(t, "users_ack_queue")

	_, err = db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('A2', 'a2@acme.io')`)
	require.NoError(t, err)
	_ = mustConsumeOne(t, "users_ack_queue")

	// Give time for the batch to flush and ACK the WAL position to PostgreSQL.
	time.Sleep(2 * time.Second)
	connector.Close()

	// Wait for the replication slot to become inactive so connector2 can acquire it.
	time.Sleep(2 * time.Second)

	// Insert while no connector is running — must be captured by the second connector.
	_, err = db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('A3', 'a3@acme.io')`)
	require.NoError(t, err)

	connector2, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector2.Close()
	go connector2.Start(ctx)
	readyCtx2, cancel2 := withTimeout(ctx)
	defer cancel2()
	require.NoError(t, connector2.WaitUntilReady(readyCtx2))

	msg := mustConsumeOne(t, "users_ack_queue")
	assert.NotEmpty(t, msg.Body)

	// Verify the message is A3, not a replayed A1/A2.
	var data map[string]any
	require.NoError(t, json.Unmarshal(msg.Body, &data))
	assert.Equal(t, "A3", data["name"])
}

func TestConnector_FanOut(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	cfg := newTestConfig("users_fanout_exchange", "users_fanout_queue_1", "users_fanout_slot", "users_fanout_pub")
	cfg.RabbitMQ.Queues = append(cfg.RabbitMQ.Queues, config.QueueConfig{
		Name:    "users_fanout_queue_2",
		Durable: true,
		Bindings: []string{
			"public.users.*",
		},
	})

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	_, err = db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('Fanout', 'fanout@acme.io')`)
	require.NoError(t, err)

	msg1 := mustConsumeOne(t, "users_fanout_queue_1")
	msg2 := mustConsumeOne(t, "users_fanout_queue_2")
	assert.NotEmpty(t, msg1.Body)
	assert.NotEmpty(t, msg2.Body)
}

func TestConnector_RoutingKeyFiltering(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	cfg := newTestConfig("users_filter_exchange", "users_filter_insert_queue", "users_filter_slot", "users_filter_pub")
	cfg.RabbitMQ.Queues = []config.QueueConfig{
		{Name: "users_filter_insert_queue", Durable: true, Bindings: []string{"public.users.INSERT"}},
		{Name: "users_filter_delete_queue", Durable: true, Bindings: []string{"public.users.DELETE"}},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	var id int
	require.NoError(t, db.QueryRowContext(ctx, `INSERT INTO users (name, email) VALUES ('Filter', 'filter@acme.io') RETURNING id`).Scan(&id))
	_, err = db.ExecContext(ctx, `DELETE FROM users WHERE id=$1`, id)
	require.NoError(t, err)

	insertMsg := mustConsumeOne(t, "users_filter_insert_queue")
	deleteMsg := mustConsumeOne(t, "users_filter_delete_queue")
	assert.NotEmpty(t, insertMsg.Body)
	assert.NotEmpty(t, deleteMsg.Body)
}

func newTestConfig(exchange, queue, slotName, publicationName string) config.Connector {
	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	return config.Connector{
		CDC: cdcconfig.Config{
			Host:      Infra.PostgresHost,
			Port:      postgresPort,
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              publicationName,
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        slotName,
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{LogLevel: slog.LevelInfo},
		},
		RabbitMQ: config.RabbitMQ{
			URL: fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort),
			Exchange: config.ExchangeConfig{
				Name:       exchange,
				Type:       "topic",
				Durable:    true,
				AutoDelete: false,
			},
			Queues: []config.QueueConfig{
				{Name: queue, Durable: true, Bindings: []string{"public.users.*"}},
			},
			TableRoutingKeyMapping: map[string]string{
				"public.users": "public.users",
			},
			PublisherBatchTickerDuration: 100 * time.Millisecond,
			PublisherBatchSize:           10,
		},
	}
}

func handler(msg *cdc.Message) []rabbitmq.PublishMessage {
	if !msg.Type.IsInsert() && !msg.Type.IsUpdate() && !msg.Type.IsSnapshot() && !msg.Type.IsDelete() {
		return nil
	}
	data := msg.NewData
	if msg.Type.IsDelete() {
		data = msg.OldData
	}
	data["operation"] = string(msg.Type)
	body, _ := json.Marshal(data)
	return []rabbitmq.PublishMessage{{Body: body, ContentType: "application/json"}}
}

func mustOpenDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", Infra.PostgresHost, Infra.PostgresPort))
	require.NoError(t, err)
	return db
}

func mustCreateUsersTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT,
			created_on TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	require.NoError(t, err)
}

func mustConsumeOne(t *testing.T, queue string) amqp.Delivery {
	t.Helper()
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort))
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()

	msgCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	msg, err := rabbitmq.ConsumeOne(msgCtx, ch, queue)
	require.NoError(t, err)
	return msg
}

func withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 30*time.Second)
}
