package benchmark

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	_ "github.com/lib/pq"
)

func BenchmarkThroughput(b *testing.B) {
	pgHost := envOrDefault("BENCH_POSTGRES_HOST", "localhost")
	pgPort := envOrDefault("BENCH_POSTGRES_PORT", "5432")
	rmqHost := envOrDefault("BENCH_RABBITMQ_HOST", "localhost")
	rmqPort := envOrDefault("BENCH_RABBITMQ_PORT", "5672")

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://cdc_user:cdc_pass@%s:%s/cdc_db?sslmode=disable", pgHost, pgPort))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, _ = db.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS benchmark_events (id SERIAL PRIMARY KEY, payload TEXT NOT NULL, created_on TIMESTAMPTZ DEFAULT NOW())`)

	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:     pgHost,
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "benchmark_pub",
				Operations: publication.Operations{
					publication.OperationInsert,
				},
				Tables: publication.Tables{
					{Name: "benchmark_events", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "benchmark_slot",
				SlotActivityCheckerInterval: 3000,
			},
		},
		RabbitMQ: config.RabbitMQ{
			URL: fmt.Sprintf("amqp://guest:guest@%s:%s/", rmqHost, rmqPort),
			Exchange: config.ExchangeConfig{
				Name:    "benchmark.events",
				Type:    "topic",
				Durable: true,
			},
			TableRoutingKeyMapping: map[string]string{
				"public.benchmark_events": "public.benchmark_events",
			},
			PublisherBatchSize:           2000,
			PublisherBatchTickerDuration: 500 * time.Millisecond,
		},
	}

	connector, err := cdc.NewConnector(context.Background(), cfg, func(msg *cdc.Message) []rabbitmq.PublishMessage {
		return []rabbitmq.PublishMessage{{Body: []byte(fmt.Sprintf("%v", msg.NewData["payload"])), ContentType: "text/plain"}}
	})
	if err != nil {
		b.Fatal(err)
	}
	defer connector.Close()
	go connector.Start(context.Background())
	_ = connector.WaitUntilReady(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.ExecContext(context.Background(), `INSERT INTO benchmark_events (payload) VALUES ($1)`, fmt.Sprintf("event-%d", i)); err != nil {
			b.Fatal(err)
		}
	}
}

func envOrDefault(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
