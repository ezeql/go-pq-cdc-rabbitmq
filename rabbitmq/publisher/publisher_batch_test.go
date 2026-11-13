package publisher

import (
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func countingCtx(n *int) *replication.ListenerContext {
	return &replication.ListenerContext{Ack: func() error { *n++; return nil }}
}

func TestBatchAckWaitsForPendingRows(t *testing.T) {
	b := newBatch(time.Hour, 100, 1<<20, 1, "ex", nil, nil, "slot")
	defer b.batchTicker.Stop()

	var direct int
	b.Ack(countingCtx(&direct))
	if direct != 1 {
		t.Fatalf("idle batch must ack at once, got %d acks", direct)
	}

	var row, skipped int
	b.AddEvents(countingCtx(&row), []rabbitmq.PublishMessage{{Body: []byte("x")}}, time.Now(), true)
	b.Ack(countingCtx(&skipped))
	if row != 0 || skipped != 0 {
		t.Fatalf("nothing may be acked before the flush, got row=%d skipped=%d", row, skipped)
	}
	if len(b.acks) != 2 {
		t.Fatalf("both deliveries must wait for the flush, got %d", len(b.acks))
	}
}

type noChannelClient struct{}

func (noChannelClient) Channel() *amqp.Channel           { return nil }
func (noChannelClient) NotifyReconnect() <-chan struct{} { return nil }
func (noChannelClient) Close() error                     { return nil }

type nopHandler struct{}

func (nopHandler) OnSuccess(*rabbitmq.ResponseHandlerContext) {}
func (nopHandler) OnError(*rabbitmq.ResponseHandlerContext)   {}

// A failed flush must keep the rows and ack nothing, or the next successful
// flush would confirm past rows RabbitMQ never accepted.
func TestBatchFailedFlushKeepsRowsAndAcks(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	b := newBatch(time.Hour, 100, 1<<20, 1, "ex", nopHandler{}, noChannelClient{}, "slot")
	defer b.batchTicker.Stop()

	var row, skipped int
	b.AddEvents(countingCtx(&row), []rabbitmq.PublishMessage{{Body: []byte("x")}}, time.Now(), true)
	b.FlushMessages()
	b.Ack(countingCtx(&skipped))

	if row != 0 || skipped != 0 {
		t.Fatalf("nothing may be acked after a failed flush, got row=%d skipped=%d", row, skipped)
	}
	if len(b.messages) != 1 || len(b.acks) != 2 {
		t.Fatalf("failed batch must be kept for retry, got messages=%d acks=%d", len(b.messages), len(b.acks))
	}

	b.Close()
	b.FlushMessages() // must return at once once closed
}
