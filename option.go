package cdc

import (
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	"github.com/prometheus/client_golang/prometheus"
)

type Option func(Connector)
type Options []Option

func (ops Options) Apply(c Connector) {
	for _, op := range ops {
		op(c)
	}
}

func mustConnector(c Connector) *connector {
	conn, ok := c.(*connector)
	if !ok {
		panic("option can only be applied to cdc.connector")
	}
	return conn
}

func WithResponseHandler(respHandler rabbitmq.ResponseHandler) Option {
	return func(c Connector) {
		mustConnector(c).responseHandler = respHandler
	}
}

func WithPrometheusMetrics(collectors []prometheus.Collector) Option {
	return func(c Connector) {
		mustConnector(c).metrics = collectors
	}
}

func WithLogger(l logger.Logger) Option {
	return func(c Connector) {
		mustConnector(c).cfg.CDC.Logger.Logger = l
	}
}
