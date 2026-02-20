package config_test

import (
	"math"
	"testing"
	"time"

	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/stretchr/testify/assert"
)

func TestSetDefault_ZeroValue_PopulatesAllDefaults(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{}
	cfg.SetDefault()

	assert.Equal(t, "amqp://guest:guest@localhost:5672/", cfg.RabbitMQ.URL)
	assert.Equal(t, 30*time.Second, cfg.RabbitMQ.ConnectionTimeout)
	assert.Equal(t, 10*time.Second, cfg.RabbitMQ.Heartbeat)
	assert.Equal(t, "topic", cfg.RabbitMQ.Exchange.Type)
	assert.Equal(t, "cdc.events", cfg.RabbitMQ.Exchange.Name)
	assert.True(t, cfg.RabbitMQ.Exchange.Durable)
	assert.Equal(t, 2000, cfg.RabbitMQ.PublisherBatchSize)
	assert.Equal(t, "1mb", cfg.RabbitMQ.PublisherBatchBytes)
	assert.Equal(t, 10*time.Second, cfg.RabbitMQ.PublisherBatchTickerDuration)
	assert.Equal(t, math.MaxInt, cfg.RabbitMQ.PublisherMaxRetries)
	assert.Equal(t, 1*time.Second, cfg.RabbitMQ.ReconnectInterval)
	assert.Equal(t, 30*time.Second, cfg.RabbitMQ.ReconnectMaxInterval)
	assert.Equal(t, "{{.TableNamespace}}.{{.TableName}}.{{.Operation}}", cfg.RabbitMQ.RoutingKeyTemplate)
}

func TestSetDefault_ExplicitValues_ArePreserved(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			URL:                          "amqp://prod:secret@rmq.internal:5672/vhost",
			ConnectionTimeout:            5 * time.Second,
			Heartbeat:                    3 * time.Second,
			Exchange:                     config.ExchangeConfig{Name: "my.exchange", Type: "direct"},
			PublisherBatchSize:           500,
			PublisherBatchBytes:          "5mb",
			PublisherBatchTickerDuration: 2 * time.Second,
			PublisherMaxRetries:          10,
			ReconnectInterval:            500 * time.Millisecond,
			ReconnectMaxInterval:         10 * time.Second,
			RoutingKeyTemplate:           "custom.{{.TableName}}",
		},
	}
	cfg.SetDefault()

	assert.Equal(t, "amqp://prod:secret@rmq.internal:5672/vhost", cfg.RabbitMQ.URL)
	assert.Equal(t, 5*time.Second, cfg.RabbitMQ.ConnectionTimeout)
	assert.Equal(t, 3*time.Second, cfg.RabbitMQ.Heartbeat)
	assert.Equal(t, "direct", cfg.RabbitMQ.Exchange.Type)
	assert.Equal(t, "my.exchange", cfg.RabbitMQ.Exchange.Name)
	assert.Equal(t, 500, cfg.RabbitMQ.PublisherBatchSize)
	assert.Equal(t, "5mb", cfg.RabbitMQ.PublisherBatchBytes)
	assert.Equal(t, 2*time.Second, cfg.RabbitMQ.PublisherBatchTickerDuration)
	assert.Equal(t, 10, cfg.RabbitMQ.PublisherMaxRetries)
	assert.Equal(t, 500*time.Millisecond, cfg.RabbitMQ.ReconnectInterval)
	assert.Equal(t, 10*time.Second, cfg.RabbitMQ.ReconnectMaxInterval)
	assert.Equal(t, "custom.{{.TableName}}", cfg.RabbitMQ.RoutingKeyTemplate)
}

func TestSetDefault_ExchangeDurable_AlwaysForced(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			Exchange: config.ExchangeConfig{Durable: false},
		},
	}
	cfg.SetDefault()

	// SetDefault unconditionally sets Durable = true.
	assert.True(t, cfg.RabbitMQ.Exchange.Durable)
}

func TestSetDefault_QueuesDurable_AlwaysForced(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			Queues: []config.QueueConfig{
				{Name: "q1", Durable: false},
				{Name: "q2", Durable: false},
			},
		},
	}
	cfg.SetDefault()

	for _, q := range cfg.RabbitMQ.Queues {
		assert.True(t, q.Durable, "queue %s should be durable", q.Name)
	}
}

func TestSetDefault_Idempotent(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{}
	cfg.SetDefault()

	// Capture state after first call.
	url := cfg.RabbitMQ.URL
	batchSize := cfg.RabbitMQ.PublisherBatchSize

	// Call again -- values should not change.
	cfg.SetDefault()
	assert.Equal(t, url, cfg.RabbitMQ.URL)
	assert.Equal(t, batchSize, cfg.RabbitMQ.PublisherBatchSize)
}
