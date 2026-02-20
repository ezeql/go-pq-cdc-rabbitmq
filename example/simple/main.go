package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()

	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:     "127.0.0.1",
			Username: "cdc_user",
			Password: "cdc_pass",
			Database: "cdc_db",
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{
					{Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull},
					{Name: "orders", ReplicaIdentity: publication.ReplicaIdentityFull},
				},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{Port: 8081},
			Logger: cdcconfig.LoggerConfig{LogLevel: slog.LevelInfo},
		},
		RabbitMQ: config.RabbitMQ{
			URL: "amqp://guest:guest@localhost:5672/",
			Exchange: config.ExchangeConfig{
				Name:    "cdc.events",
				Type:    "topic",
				Durable: true,
			},
			TableRoutingKeyMapping: map[string]string{
				"public.users":  "public.users",
				"public.orders": "public.orders",
			},
			PublisherBatchTickerDuration: 200 * time.Millisecond,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}
	defer connector.Close()
	connector.Start(ctx)
}

func handler(msg *cdc.Message) []rabbitmq.PublishMessage {
	slog.Info("change captured", "table", msg.TableName, "type", msg.Type)
	if !msg.Type.IsInsert() && !msg.Type.IsUpdate() && !msg.Type.IsSnapshot() && !msg.Type.IsDelete() {
		return nil
	}
	data := msg.NewData
	if msg.Type.IsDelete() {
		data = msg.OldData
	}
	data["operation"] = string(msg.Type)
	body, _ := json.Marshal(data)
	return []rabbitmq.PublishMessage{
		{
			Body:        body,
			ContentType: "application/json",
		},
	}
}
