package publisher

import (
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
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
