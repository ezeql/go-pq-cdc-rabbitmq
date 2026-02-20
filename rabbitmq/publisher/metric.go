package publisher

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "go_pq_cdc_rabbitmq"

type Metric interface {
	SetProcessLatency(latency int64)
	SetBulkRequestProcessLatency(latency int64)
	PrometheusCollectors() []prometheus.Collector
	AddSuccessOp(routingKey string, count float64)
	AddErrOp(routingKey string, count float64)
}

var hostname, _ = os.Hostname()

type metric struct {
	processLatencyNs            prometheus.Gauge
	bulkRequestProcessLatencyNs prometheus.Gauge
	totalSuccess                *prometheus.CounterVec
	totalErr                    *prometheus.CounterVec
	slotName                    string
}

func NewMetric(slotName string) Metric {
	return &metric{
		slotName: slotName,
		processLatencyNs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "process_latency",
			Name:      "current",
			Help:      "latest rabbitmq connector process latency in nanoseconds",
			ConstLabels: prometheus.Labels{
				"host":      hostname,
				"slot_name": slotName,
			},
		}),
		bulkRequestProcessLatencyNs: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "bulk_request_process_latency",
			Name:      "current",
			Help:      "latest rabbitmq connector bulk request process latency in nanoseconds",
			ConstLabels: prometheus.Labels{
				"host":      hostname,
				"slot_name": slotName,
			},
		}),
		totalSuccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "publish",
			Name:      "total",
			Help:      "total number of successful publish operation to rabbitmq",
		}, []string{"slot_name", "routing_key", "host"}),
		totalErr: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "err",
			Name:      "total",
			Help:      "total number of error in publish operation to rabbitmq",
		}, []string{"slot_name", "routing_key", "host"}),
	}
}

func (m *metric) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.processLatencyNs,
		m.bulkRequestProcessLatencyNs,
		m.totalSuccess,
		m.totalErr,
	}
}

func (m *metric) SetProcessLatency(latency int64) {
	m.processLatencyNs.Set(float64(latency))
}

func (m *metric) SetBulkRequestProcessLatency(latency int64) {
	m.bulkRequestProcessLatencyNs.Set(float64(latency))
}

func (m *metric) AddSuccessOp(routingKey string, count float64) {
	m.totalSuccess.WithLabelValues(m.slotName, routingKey, hostname).Add(count)
}

func (m *metric) AddErrOp(routingKey string, count float64) {
	m.totalErr.WithLabelValues(m.slotName, routingKey, hostname).Add(count)
}
