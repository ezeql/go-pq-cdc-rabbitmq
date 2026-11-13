package publisher

import (
	"context"
	"errors"
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

type fakeConfirm struct {
	ack     bool
	pending bool
}

func (f fakeConfirm) WaitContext(ctx context.Context) (bool, error) {
	if f.pending {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return f.ack, nil
}

func TestWaitConfirms(t *testing.T) {
	open := func() bool { return false }
	closed := func() bool { return true }

	acks, err := waitConfirms([]confirmation{fakeConfirm{ack: true}, fakeConfirm{ack: false}}, time.Second, open)
	if err != nil || !acks[0] || acks[1] {
		t.Fatalf("broker nack must be reported per row, got %v %v", acks, err)
	}

	if _, err = waitConfirms([]confirmation{fakeConfirm{ack: false}}, time.Second, closed); !errors.Is(err, amqp.ErrClosed) {
		t.Fatalf("nack on a closed channel must be retryable ErrClosed, got %v", err)
	}

	if _, err = waitConfirms([]confirmation{fakeConfirm{ack: true}, fakeConfirm{pending: true}}, 10*time.Millisecond, open); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing confirm must time out, got %v", err)
	}
}

func TestIsPermanent(t *testing.T) {
	if !isPermanent(&amqp.Error{Code: amqp.NotFound}) {
		t.Fatal("404 from the broker cannot be fixed by retrying")
	}
	if isPermanent(amqp.ErrClosed) || isPermanent(context.DeadlineExceeded) {
		t.Fatal("disconnects and timeouts must be retried")
	}
}
