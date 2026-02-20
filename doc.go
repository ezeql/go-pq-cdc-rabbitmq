// Package cdc provides a PostgreSQL Change Data Capture (CDC) connector
// that streams row-level changes to RabbitMQ.
//
// It wraps the go-pq-cdc logical replication library and publishes INSERT,
// UPDATE, and DELETE events as AMQP messages to a configurable exchange,
// with optional routing-key mapping per table.
//
// # Basic usage
//
//	conn, err := cdc.NewConnector(ctx, config.Connector{...}, handler)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer conn.Close()
//	conn.Start(ctx)
//
// The handler function receives a [Message] for every captured row change
// and returns the list of [rabbitmq.PublishMessage] values to publish.
// Returning nil or an empty slice silently skips the event.
//
// # Configuration
//
// All RabbitMQ connection and publisher settings live in [config.Connector].
// Call [config.Connector.SetDefault] to apply sane defaults before use.
//
// # Routing keys
//
// By default each message is published with the routing key
// "<schema>.<table>". Use [config.RabbitMQ.TableRoutingKeyMapping] to override
// the mapping per table.
//
// # Metrics
//
// Pass [WithPrometheusMetrics] to expose Prometheus counters and gauges
// under the "go_pq_cdc_rabbitmq" namespace.
package cdc
