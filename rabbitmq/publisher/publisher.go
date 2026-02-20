package publisher

import (
	"fmt"
	"math"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/ezeql/go-pq-cdc-rabbitmq/internal/bytesize"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
)

type Publisher struct {
	PublisherBatch *Batch
}

func NewPublisher(
	client rabbitmq.Client,
	cfg *config.Connector,
	responseHandler rabbitmq.ResponseHandler,
) (Publisher, error) {
	batchBytes, err := bytesize.ParseSize(cfg.RabbitMQ.PublisherBatchBytes)
	if err != nil {
		return Publisher{}, fmt.Errorf("publisherBatchBytes parse: %w", err)
	}
	if batchBytes > bytesize.Size(math.MaxInt64) {
		return Publisher{}, fmt.Errorf("publisherBatchBytes exceeds maximum: %d", batchBytes)
	}

	return Publisher{
		PublisherBatch: newBatch(
			cfg.RabbitMQ.PublisherBatchTickerDuration,
			cfg.RabbitMQ.PublisherBatchSize,
			int64(batchBytes), //nolint:gosec // G115: guarded by the check above
			cfg.RabbitMQ.PublisherMaxRetries,
			cfg.RabbitMQ.Exchange.Name,
			responseHandler,
			client,
			cfg.CDC.Slot.Name,
		),
	}, nil
}

func (p *Publisher) StartBatch() {
	p.PublisherBatch.StartBatchTicker()
}

func (p *Publisher) Produce(
	ctx *replication.ListenerContext,
	eventTime time.Time,
	messages []rabbitmq.PublishMessage,
	isLastChunk bool,
) {
	p.PublisherBatch.AddEvents(ctx, messages, eventTime, isLastChunk)
}

func (p *Publisher) Close() {
	p.PublisherBatch.Close()
}

func (p *Publisher) GetMetric() Metric {
	return p.PublisherBatch.metric
}

func (p *Publisher) HasPendingMessages() bool {
	return p.PublisherBatch.HasPendingMessages()
}
