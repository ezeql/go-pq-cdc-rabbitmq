package rabbitmq

import (
	"errors"
	"fmt"
	"io"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishMessage struct {
	Timestamp    time.Time
	Headers      map[string]any
	Exchange     string
	RoutingKey   string
	ContentType  string
	MessageID    string
	Type         string
	AppID        string
	Body         []byte
	DeliveryMode uint8
}

type ResponseHandlerContext struct {
	Message *PublishMessage
	Err     error
}

type ResponseHandler interface {
	OnSuccess(ctx *ResponseHandlerContext)
	OnError(ctx *ResponseHandlerContext)
}

type DefaultResponseHandler struct{}

func (drh *DefaultResponseHandler) OnSuccess(_ *ResponseHandlerContext) {}

func (drh *DefaultResponseHandler) OnError(ctx *ResponseHandlerContext) {
	if isFatalError(ctx.Err) {
		logger.Error("permanent error on rabbitmq while flush messages", "error", ctx.Err)
		panic(fmt.Errorf("permanent error on RabbitMQ side %w", ctx.Err))
	}
	logger.Error("batch publisher flush", "error", ctx.Err)
}

func isFatalError(err error) bool {
	var e *amqp.Error
	ok := errors.As(err, &e)
	if ok {
		switch e.Code {
		case amqp.NotFound, amqp.AccessRefused, amqp.PreconditionFailed:
			return true
		default:
			return false
		}
	}

	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return false
	}
	return true
}
