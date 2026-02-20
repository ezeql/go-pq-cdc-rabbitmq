package publisher

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Batch struct {
	metric              Metric
	client              rabbitmq.Client
	responseHandler     rabbitmq.ResponseHandler
	batchTicker         *time.Ticker
	lastAckCtx          *replication.ListenerContext
	lastChannel         *amqp.Channel
	confirmTimeout      *time.Timer
	confirmsCh          chan amqp.Confirmation
	defaultExchange     string
	messages            []rabbitmq.PublishMessage
	routingKeys         []string
	batchLimit          int
	currentMessageBytes int64
	maxRetries          int
	batchBytes          int64
	batchTickerDuration time.Duration
	flushLock           sync.Mutex
	pendingFlag         atomic.Bool
	messageCount        atomic.Int32
}

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
	t := time.NewTimer(15 * time.Second)
	t.Stop()
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
		confirmTimeout:      t,
	}
}

func (b *Batch) StartBatchTicker() {
	go func() {
		for range b.batchTicker.C {
			b.FlushMessages()
		}
	}()
}

func (b *Batch) Close() {
	b.batchTicker.Stop()
	b.FlushMessages()
}

// HasPendingMessages returns true if there are unacknowledged messages.
// Uses atomics instead of the flush mutex to avoid lock contention on the hot path.
func (b *Batch) HasPendingMessages() bool {
	return b.messageCount.Load() > 0 || b.pendingFlag.Load()
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
	b.pendingFlag.Store(true)
	b.messageCount.Store(int32(min(len(b.messages), math.MaxInt32))) //nolint:gosec // G115: clamped to MaxInt32
	if isLastChunk {
		b.lastAckCtx = ctx
	}
	shouldFlush := len(b.messages) >= b.batchLimit || b.currentMessageBytes >= b.batchBytes
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

	if len(b.messages) == 0 {
		return
	}

	var (
		flushSuccess bool
		confirmed    []amqp.Confirmation
		err          error
	)
	started := time.Now()

	for attempt := 0; attempt < b.maxRetries; attempt++ {
		confirmed, err = b.publishAndConfirm()
		if err == nil {
			flushSuccess = b.handleConfirmations(confirmed)
			break
		}
		logger.Error("batch publish failed", "attempt", attempt+1, "error", err)
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}

	b.metric.SetBulkRequestProcessLatency(time.Since(started).Nanoseconds())

	if !flushSuccess && err != nil {
		b.handleResponseError(err)
	}

	b.messages = b.messages[:0]
	b.routingKeys = b.routingKeys[:0]
	b.currentMessageBytes = 0
	b.messageCount.Store(0)

	if flushSuccess {
		b.pendingFlag.Store(false)
		if b.lastAckCtx != nil {
			if ackErr := b.lastAckCtx.Ack(); ackErr != nil {
				logger.Error("ack", "error", ackErr)
			}
			b.lastAckCtx = nil
		}
	} else {
		logger.Warn("flush failed, skipping ACK to preserve message ordering")
	}

	b.batchTicker.Reset(b.batchTickerDuration)
}

// getOrCreateConfirmsCh returns a persistent confirms listener for the current channel.
// If the underlying AMQP channel has changed (e.g. after reconnect), it creates a new listener.
// This avoids the amqp091-go listener accumulation bug where each NotifyPublish call
// appends a new listener and the blocking send in confirms.confirm() deadlocks when
// old listener channels fill up.
func (b *Batch) getOrCreateConfirmsCh(ch *amqp.Channel) chan amqp.Confirmation {
	if b.lastChannel == ch && b.confirmsCh != nil {
		return b.confirmsCh
	}
	// Channel changed (reconnect or first call) — register a fresh listener.
	// Size buffer to batch limit to avoid backpressure on the amqp091-go confirm dispatch goroutine.
	bufSize := b.batchLimit
	if bufSize > 4096 {
		bufSize = 4096
	}
	if bufSize < 256 {
		bufSize = 256
	}
	b.confirmsCh = make(chan amqp.Confirmation, bufSize)
	ch.NotifyPublish(b.confirmsCh)
	b.lastChannel = ch
	return b.confirmsCh
}

func (b *Batch) publishAndConfirm() ([]amqp.Confirmation, error) {
	ch := b.client.Channel()
	if ch == nil {
		return nil, fmt.Errorf("rabbitmq channel is nil")
	}

	confirmsCh := b.getOrCreateConfirmsCh(ch)

	for i := range b.messages {
		ex := b.defaultExchange
		if b.messages[i].Exchange != "" {
			ex = b.messages[i].Exchange
		}
		if err := ch.PublishWithContext(context.Background(), ex, b.routingKeys[i], false, false, amqp.Publishing{
			ContentType:  b.messages[i].ContentType,
			DeliveryMode: getDeliveryMode(b.messages[i].DeliveryMode),
			Headers:      amqp.Table(b.messages[i].Headers),
			Body:         b.messages[i].Body,
			MessageId:    b.messages[i].MessageID,
			Timestamp:    b.messages[i].Timestamp,
			Type:         b.messages[i].Type,
			AppId:        b.messages[i].AppID,
		}); err != nil {
			return nil, err
		}
	}

	confirmed := make([]amqp.Confirmation, 0, len(b.messages))
	if !b.confirmTimeout.Stop() {
		select {
		case <-b.confirmTimeout.C:
		default:
		}
	}
	b.confirmTimeout.Reset(15 * time.Second)
	defer b.confirmTimeout.Stop()
	for i := 0; i < len(b.messages); i++ {
		select {
		case c := <-confirmsCh:
			confirmed = append(confirmed, c)
		case <-b.confirmTimeout.C:
			return nil, fmt.Errorf("timeout while waiting publisher confirms")
		}
	}
	return confirmed, nil
}

func (b *Batch) handleConfirmations(confirmed []amqp.Confirmation) bool {
	flushSuccess := true
	successCounts := make(map[string]float64, 4)
	errCounts := make(map[string]float64, 4)
	var rhCtx rabbitmq.ResponseHandlerContext

	for i := range confirmed {
		if confirmed[i].Ack {
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
			rhCtx.Err = fmt.Errorf("publisher nack, delivery_tag=%d", confirmed[i].DeliveryTag)
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
