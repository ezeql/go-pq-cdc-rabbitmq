package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// nopLogger discards all log output.
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

var _ logger.Logger = nopLogger{}

// silenceStdout redirects os.Stdout to /dev/null and returns a restore function.
// This suppresses the hardcoded fmt.Println("used config: ...") in go-pq-cdc.
func silenceStdout() func() {
	orig := os.Stdout
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return func() {}
	}
	os.Stdout = devNull
	return func() {
		os.Stdout = orig
		devNull.Close()
	}
}

// BenchmarkPipeline measures end-to-end throughput: PG INSERT -> CDC -> RabbitMQ consume.
// Each b.N operation represents one message flowing through the entire pipeline.
// Use `go test -bench=BenchmarkPipeline -benchmem -count=6 -timeout=600s ./integration_test/...`
// and compare across commits with benchstat.
func BenchmarkPipeline(b *testing.B) {
	if Infra == nil {
		b.Skip("test infrastructure not available (run via TestMain)")
	}

	ctx := context.Background()
	db := mustOpenBenchDB(b)
	defer db.Close()

	mustExec(b, db, `CREATE TABLE IF NOT EXISTS bench_events (
		id SERIAL PRIMARY KEY,
		payload TEXT NOT NULL,
		created_on TIMESTAMPTZ DEFAULT NOW()
	)`)
	mustExec(b, db, `ALTER TABLE bench_events REPLICA IDENTITY FULL`)

	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:     Infra.PostgresHost,
			Port:     postgresPort,
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Metric:   cdcconfig.MetricConfig{Port: 18081},
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "bench_pipeline_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
				},
				Tables: publication.Tables{
					{Name: "bench_events", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "bench_pipeline_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{Logger: nopLogger{}},
		},
		RabbitMQ: config.RabbitMQ{
			URL: fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort),
			Exchange: config.ExchangeConfig{
				Name:    "bench.pipeline",
				Type:    "topic",
				Durable: true,
			},
			Queues: []config.QueueConfig{
				{Name: "bench_pipeline_queue", Durable: true, Bindings: []string{"public.bench_events.*"}},
			},
			TableRoutingKeyMapping: map[string]string{
				"public.bench_events": "public.bench_events",
			},
			PublisherBatchTickerDuration: 100 * time.Millisecond,
			PublisherBatchSize:           2000,
		},
	}

	benchHandler := func(msg *cdc.Message) []rabbitmq.PublishMessage {
		body, _ := json.Marshal(msg.NewData)
		return []rabbitmq.PublishMessage{{Body: body, ContentType: "application/json"}}
	}

	restore := silenceStdout()
	connector, err := cdc.NewConnector(ctx, cfg, benchHandler)
	if err != nil {
		restore()
		b.Fatal("NewConnector:", err)
	}
	defer connector.Close()

	go connector.Start(ctx)
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := connector.WaitUntilReady(readyCtx); err != nil {
		restore()
		b.Fatal("WaitUntilReady:", err)
	}
	restore()

	rmqConn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort))
	if err != nil {
		b.Fatal("amqp.Dial:", err)
	}
	defer rmqConn.Close()

	ch, err := rmqConn.Channel()
	if err != nil {
		b.Fatal("Channel:", err)
	}
	defer ch.Close()

	// Purge any leftover messages from prior runs.
	_, _ = ch.QueuePurge("bench_pipeline_queue", false)

	msgs, err := ch.Consume("bench_pipeline_queue", "bench-consumer", true, false, false, false, nil)
	if err != nil {
		b.Fatal("Consume:", err)
	}

	// Drain any stale messages before starting the benchmark.
	drainDone := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case <-msgs:
		case <-drainDone:
			break drain
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Insert b.N rows into PostgreSQL.
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(ctx, `INSERT INTO bench_events (payload) VALUES ($1)`, fmt.Sprintf("bench-%d", i)); err != nil {
			b.Fatal("INSERT:", err)
		}
	}

	// Wait until all b.N messages arrive in RabbitMQ.
	received := 0
	timeout := time.NewTimer(120 * time.Second)
	defer timeout.Stop()
	for received < b.N {
		select {
		case <-msgs:
			received++
		case <-timeout.C:
			b.Fatalf("timeout: received %d of %d messages", received, b.N)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/sec")
}

// BenchmarkPipelineBatch is like BenchmarkPipeline but inserts rows in batches
// of 100 using a single multi-row INSERT, testing bulk throughput.
func BenchmarkPipelineBatch(b *testing.B) {
	if Infra == nil {
		b.Skip("test infrastructure not available (run via TestMain)")
	}

	ctx := context.Background()
	db := mustOpenBenchDB(b)
	defer db.Close()

	mustExec(b, db, `CREATE TABLE IF NOT EXISTS bench_batch_events (
		id SERIAL PRIMARY KEY,
		payload TEXT NOT NULL,
		created_on TIMESTAMPTZ DEFAULT NOW()
	)`)
	mustExec(b, db, `ALTER TABLE bench_batch_events REPLICA IDENTITY FULL`)

	postgresPort, _ := strconv.Atoi(Infra.PostgresPort)
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:     Infra.PostgresHost,
			Port:     postgresPort,
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Metric:   cdcconfig.MetricConfig{Port: 18082},
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "bench_batch_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
				},
				Tables: publication.Tables{
					{Name: "bench_batch_events", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "bench_batch_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Logger: cdcconfig.LoggerConfig{Logger: nopLogger{}},
		},
		RabbitMQ: config.RabbitMQ{
			URL: fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort),
			Exchange: config.ExchangeConfig{
				Name:    "bench.batch",
				Type:    "topic",
				Durable: true,
			},
			Queues: []config.QueueConfig{
				{Name: "bench_batch_queue", Durable: true, Bindings: []string{"public.bench_batch_events.*"}},
			},
			TableRoutingKeyMapping: map[string]string{
				"public.bench_batch_events": "public.bench_batch_events",
			},
			PublisherBatchTickerDuration: 100 * time.Millisecond,
			PublisherBatchSize:           2000,
		},
	}

	benchHandler := func(msg *cdc.Message) []rabbitmq.PublishMessage {
		body, _ := json.Marshal(msg.NewData)
		return []rabbitmq.PublishMessage{{Body: body, ContentType: "application/json"}}
	}

	restore := silenceStdout()
	connector, err := cdc.NewConnector(ctx, cfg, benchHandler)
	if err != nil {
		restore()
		b.Fatal("NewConnector:", err)
	}
	defer connector.Close()

	go connector.Start(ctx)
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := connector.WaitUntilReady(readyCtx); err != nil {
		restore()
		b.Fatal("WaitUntilReady:", err)
	}
	restore()

	rmqConn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%s/", Infra.RabbitHost, Infra.RabbitPort))
	if err != nil {
		b.Fatal("amqp.Dial:", err)
	}
	defer rmqConn.Close()

	ch, err := rmqConn.Channel()
	if err != nil {
		b.Fatal("Channel:", err)
	}
	defer ch.Close()

	_, _ = ch.QueuePurge("bench_batch_queue", false)

	msgs, err := ch.Consume("bench_batch_queue", "bench-batch-consumer", true, false, false, false, nil)
	if err != nil {
		b.Fatal("Consume:", err)
	}

	drainDone := time.After(500 * time.Millisecond)
drain:
	for {
		select {
		case <-msgs:
		case <-drainDone:
			break drain
		}
	}

	// Build a batch INSERT with 100 rows per statement.
	const batchSize = 100
	totalMessages := b.N
	if totalMessages < batchSize {
		totalMessages = batchSize
	}
	// Round up to nearest batchSize.
	totalMessages = ((totalMessages + batchSize - 1) / batchSize) * batchSize

	b.ResetTimer()
	b.ReportAllocs()

	for offset := 0; offset < totalMessages; offset += batchSize {
		query := "INSERT INTO bench_batch_events (payload) VALUES "
		for j := 0; j < batchSize; j++ {
			if j > 0 {
				query += ","
			}
			query += fmt.Sprintf("('batch-%d')", offset+j)
		}
		if _, err := db.ExecContext(ctx, query); err != nil {
			b.Fatal("batch INSERT:", err)
		}
	}

	received := 0
	timeout := time.NewTimer(120 * time.Second)
	defer timeout.Stop()
	for received < totalMessages {
		select {
		case <-msgs:
			received++
		case <-timeout.C:
			b.Fatalf("timeout: received %d of %d messages", received, totalMessages)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(totalMessages)/b.Elapsed().Seconds(), "msgs/sec")
}

func mustOpenBenchDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("postgres", fmt.Sprintf(
		"postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable",
		Infra.PostgresHost, Infra.PostgresPort,
	))
	if err != nil {
		b.Fatal("sql.Open:", err)
	}
	return db
}

func mustExec(b *testing.B, db *sql.DB, query string) {
	b.Helper()
	if _, err := db.ExecContext(context.Background(), query); err != nil {
		b.Fatal("exec:", err)
	}
}
