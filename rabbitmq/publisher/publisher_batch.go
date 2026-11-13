package publisher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Batch struct {
	metric          Metric
	client          rabbitmq.Client
	responseHandler rabbitmq.ResponseHandler
	batchTicker     *time.Ticker
	lastChannel     *amqp.Channel
	chClose         chan *amqp.Error
	defaultExchange string
	messages        []rabbitmq.PublishMessage
	// acks holds every delivered message not yet acked. go-pq-cdc confirms the
	// WAL end on idle keepalives only once each delivery has been acked.
	acks                []*replication.ListenerContext
	routingKeys         []string
	batchLimit          int
	currentMessageBytes int64
	maxRetries          int
	batchBytes          int64
	batchTickerDuration time.Duration
	flushLock           sync.Mutex
	// pending is true from AddEvents until a flush succeeds.
	pending bool
	closed  bool
	done    chan struct{}
	// closing makes a flush stop retrying once Close has started.
	closing atomic.Bool
}

const (
	confirmTimeout  = 15 * time.Second
	maxRetryBackoff = 5 * time.Second
)

func newBatch(
	batchTime time.Duration,
	batchLimit int,
	batchBytes int64,
	maxRetries int,
	defaultExchange string,
	responseHandler rabbitmq.ResponseHandler,
	client rabbitmq.Client,
	slotName string,
) *Batch {
	return &Batch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		metric:              NewMetric(slotName),
		messages:            make([]rabbitmq.PublishMessage, 0, batchLimit),
		routingKeys:         make([]string, 0, batchLimit),
		batchLimit:          batchLimit,
		batchBytes:          batchBytes,
		maxRetries:          maxRetries,
		responseHandler:     responseHandler,
		defaultExchange:     defaultExchange,
		client:              client,
		done:                make(chan struct{}),
	}
}

func (b *Batch) StartBatchTicker() {
	go func() {
		for {
			select {
			case <-b.batchTicker.C:
				b.FlushMessages()
			case <-b.done:
				return
			}
		}
	}()
}

// Close flushes what is buffered and stops publishing. Rows delivered after
// Close stay unacked and are redelivered on restart.
func (b *Batch) Close() {
	b.closing.Store(true)
	b.FlushMessages()
	b.flushLock.Lock()
	defer b.flushLock.Unlock()
	if !b.closed {
		b.closed = true
		b.batchTicker.Stop()
		close(b.done)
	}
}

// Ack acks a delivery that published nothing (unmapped table, empty handler
// result, Relation/Truncate/logical message). Acking it while earlier rows are
// still unpublished would confirm past them, so it then waits for their flush.
func (b *Batch) Ack(ctx *replication.ListenerContext) {
	b.flushLock.Lock()
	if !b.pending {
		b.flushLock.Unlock()
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}
	b.acks = append(b.acks, ctx)
	shouldFlush := len(b.acks) >= b.batchLimit
	b.flushLock.Unlock()

	if shouldFlush {
		b.FlushMessages()
	}
}

func (b *Batch) AddEvents(
	ctx *replication.ListenerContext,
	messages []rabbitmq.PublishMessage,
	eventTime time.Time,
	isLastChunk bool,
) {
	b.flushLock.Lock()
	for i := range messages {
		b.messages = append(b.messages, messages[i])
		b.routingKeys = append(b.routingKeys, messages[i].RoutingKey)
		b.currentMessageBytes += totalSizeOfMessage(&messages[i])
	}
	b.pending = true
	if isLastChunk {
		b.acks = append(b.acks, ctx)
	}
	shouldFlush := len(b.messages) >= b.batchLimit || b.currentMessageBytes >= b.batchBytes || len(b.acks) >= b.batchLimit
	b.flushLock.Unlock()

	if isLastChunk {
		b.metric.SetProcessLatency(time.Since(eventTime).Nanoseconds())
	}
	if shouldFlush {
		b.FlushMessages()
	}
}

func (b *Batch) FlushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if b.closed || len(b.messages) == 0 {
		return
	}

	var (
		flushSuccess bool
		acks         []bool
		err          error
	)
	started := time.Now()

	for attempt := 0; attempt < b.maxRetries; attempt++ {
		acks, err = b.publishAndConfirm()
		if err == nil {
			flushSuccess = b.handleConfirmations(acks)
			break
		}
		logger.Error("batch publish failed", "attempt", attempt+1, "error", err)
		if b.closing.Load() || isPermanent(err) {
			break
		}
		time.Sleep(min(time.Duration(attempt+1)*100*time.Millisecond, maxRetryBackoff))
	}

	b.metric.SetBulkRequestProcessLatency(time.Since(started).Nanoseconds())

	// Shutting down: the batch stays unacked and is redelivered on restart; a
	// fatal-error panic here would skip closing the stream and the client.
	if !flushSuccess && err != nil && !b.closing.Load() {
		b.handleResponseError(err)
	}

	if flushSuccess {
		b.messages = b.messages[:0]
		b.routingKeys = b.routingKeys[:0]
		b.currentMessageBytes = 0
		b.pending = false
		for _, ctx := range b.acks {
			if ackErr := ctx.Ack(); ackErr != nil {
				logger.Error("ack", "error", ackErr)
			}
		}
		clear(b.acks)
		b.acks = b.acks[:0]
	} else {
		// Keep the batch: dropping it would let the next successful flush
		// confirm past rows RabbitMQ never accepted. Retried on the next flush,
		// so rows already confirmed in this batch may be published twice.
		logger.Warn("flush failed, keeping batch for retry, nothing acked")
	}

	b.batchTicker.Reset(b.batchTickerDuration)
}

func (b *Batch) publishAndConfirm() ([]bool, error) {
	ch := b.client.Channel()
	if ch == nil {
		return nil, fmt.Errorf("rabbitmq channel is nil")
	}

	closeCh := b.closeNotify(ch)

	// One deferred confirmation per publish: no shared listener, so a confirm
	// left over from a timed-out attempt can never be counted for another row.
	confirms := make([]confirmation, len(b.messages))
	for i := range b.messages {
		ex := b.defaultExchange
		if b.messages[i].Exchange != "" {
			ex = b.messages[i].Exchange
		}
		dc, err := ch.PublishWithDeferredConfirm(ex, b.routingKeys[i], false, false, amqp.Publishing{
			ContentType:  b.messages[i].ContentType,
			DeliveryMode: getDeliveryMode(b.messages[i].DeliveryMode),
			Headers:      amqp.Table(b.messages[i].Headers),
			Body:         b.messages[i].Body,
			MessageId:    b.messages[i].MessageID,
			Timestamp:    b.messages[i].Timestamp,
			Type:         b.messages[i].Type,
			AppId:        b.messages[i].AppID,
		})
		if err != nil {
			return nil, err
		}
		if dc == nil {
			return nil, fmt.Errorf("rabbitmq channel is not in confirm mode")
		}
		confirms[i] = dc
	}
	acks, err := waitConfirms(confirms, confirmTimeout, ch.IsClosed)
	if errors.Is(err, amqp.ErrClosed) {
		// Prefer the broker's reason (e.g. 404 for a missing exchange) so a
		// permanent error is not retried forever as a plain disconnect.
		select {
		case e := <-closeCh:
			if e != nil {
				return nil, e
			}
		default:
		}
	}
	return acks, err
}

// isPermanent reports channel errors a retry cannot fix.
func isPermanent(err error) bool {
	var e *amqp.Error
	return errors.As(err, &e) && (e.Code == amqp.NotFound || e.Code == amqp.AccessRefused || e.Code == amqp.PreconditionFailed)
}

// closeNotify returns a close listener registered once per channel; amqp091
// sends at most one error to it, so the 1-slot buffer never blocks it.
func (b *Batch) closeNotify(ch *amqp.Channel) chan *amqp.Error {
	if ch != b.lastChannel {
		b.lastChannel = ch
		b.chClose = ch.NotifyClose(make(chan *amqp.Error, 1))
	}
	return b.chClose
}

type confirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

// waitConfirms waits for every confirmation under one deadline. amqp091 nacks
// every pending confirmation when the channel closes, so a nack on a closed
// channel is returned as amqp.ErrClosed (retried), not as a broker nack.
func waitConfirms(confirms []confirmation, timeout time.Duration, closed func() bool) ([]bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	acks := make([]bool, len(confirms))
	for i, c := range confirms {
		ack, err := c.WaitContext(ctx)
		if err != nil {
			return nil, fmt.Errorf("waiting for publisher confirms: %w", err)
		}
		if !ack && closed() {
			return nil, amqp.ErrClosed
		}
		acks[i] = ack
	}
	return acks, nil
}

func (b *Batch) handleConfirmations(acks []bool) bool {
	flushSuccess := true
	successCounts := make(map[string]float64, 4)
	errCounts := make(map[string]float64, 4)
	var rhCtx rabbitmq.ResponseHandlerContext

	for i, ack := range acks {
		if ack {
			successCounts[b.routingKeys[i]]++
			if b.responseHandler != nil {
				rhCtx.Message = &b.messages[i]
				rhCtx.Err = nil
				b.responseHandler.OnSuccess(&rhCtx)
			}
			continue
		}
		flushSuccess = false
		errCounts[b.routingKeys[i]]++
		if b.responseHandler != nil {
			rhCtx.Message = &b.messages[i]
			rhCtx.Err = fmt.Errorf("publisher nack, routing_key=%s", b.routingKeys[i])
			b.responseHandler.OnError(&rhCtx)
		}
	}

	for rk, count := range successCounts {
		b.metric.AddSuccessOp(rk, count)
	}
	for rk, count := range errCounts {
		b.metric.AddErrOp(rk, count)
	}
	return flushSuccess
}

func (b *Batch) handleResponseError(err error) {
	errCounts := make(map[string]float64, 4)
	var rhCtx rabbitmq.ResponseHandlerContext
	for i := range b.messages {
		errCounts[b.routingKeys[i]]++
		if b.responseHandler != nil {
			rhCtx.Message = &b.messages[i]
			rhCtx.Err = err
			b.responseHandler.OnError(&rhCtx)
		}
	}
	for rk, count := range errCounts {
		b.metric.AddErrOp(rk, count)
	}
}

func getDeliveryMode(deliveryMode uint8) uint8 {
	if deliveryMode == 0 {
		return amqp.Persistent
	}
	return deliveryMode
}

func totalSizeOfMessage(msg *rabbitmq.PublishMessage) int64 {
	size := len(msg.Body) + len(msg.RoutingKey) + len(msg.ContentType) + len(msg.MessageID) + len(msg.Type) + len(msg.AppID)
	for k, v := range msg.Headers {
		size += len(k)
		size += estimateHeaderValueSize(v)
	}
	return int64(size)
}

func estimateHeaderValueSize(v any) int {
	switch value := v.(type) {
	case string:
		return len(value)
	case []byte:
		return len(value)
	case bool:
		return 1
	case int, int8, int16, int32, int64:
		return 8
	case uint, uint8, uint16, uint32, uint64:
		return 8
	case float32, float64:
		return 8
	default:
		return 16
	}
}
