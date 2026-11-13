# go-pq-cdc-rabbitmq

RabbitMQ connector for [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc).

`go-pq-cdc-rabbitmq` streams PostgreSQL logical replication events to RabbitMQ with batching and publisher confirms. It follows the same architecture used by `go-pq-cdc-kafka` and `go-pq-cdc-elasticsearch`.

## Features

- PostgreSQL CDC via `go-pq-cdc`
- RabbitMQ topic exchange publishing
- Routing key mapping (`schema.table.operation`)
- Batch publishing by size, bytes, and ticker
- Publisher confirms before PostgreSQL ACK
- Connection recovery with exponential backoff
- Snapshot mode support (initial and snapshot_only)
- Prometheus metrics

## Install

```sh
go get github.com/ezeql/go-pq-cdc-rabbitmq
```

## Usage

```go
package main

import (
    "context"
    "encoding/json"
    "log/slog"
    "time"

    cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
    "github.com/ezeql/go-pq-cdc-rabbitmq/config"
    "github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
    cdcconfig "github.com/Trendyol/go-pq-cdc/config"
    "github.com/Trendyol/go-pq-cdc/pq/publication"
    "github.com/Trendyol/go-pq-cdc/pq/slot"
)

func main() {
    cfg := config.Connector{
        CDC: cdcconfig.Config{
            Host:     "127.0.0.1",
            Username: "cdc_user",
            Password: "cdc_pass",
            Database: "cdc_db",
            Publication: publication.Config{
                CreateIfNotExists: true,
                Name:              "cdc_publication",
                Operations: publication.Operations{
                    publication.OperationInsert,
                    publication.OperationUpdate,
                    publication.OperationDelete,
                },
                Tables: publication.Tables{
                    {Name: "users", ReplicaIdentity: publication.ReplicaIdentityFull},
                },
            },
            Slot: slot.Config{
                CreateIfNotExists: true,
                Name:              "cdc_slot",
            },
        },
        RabbitMQ: config.RabbitMQ{
            URL: "amqp://guest:guest@localhost:5672/",
            Exchange: config.ExchangeConfig{
                Name:    "cdc.events",
                Type:    "topic",
                Durable: true,
            },
            TableRoutingKeyMapping: map[string]string{
                "public.users": "public.users",
            },
            PublisherBatchTickerDuration: 200 * time.Millisecond,
        },
    }

    connector, err := cdc.NewConnector(context.Background(), cfg, func(msg *cdc.Message) []rabbitmq.PublishMessage {
        if !msg.Type.IsInsert() && !msg.Type.IsUpdate() && !msg.Type.IsSnapshot() && !msg.Type.IsDelete() {
            return nil
        }
        data := msg.NewData
        if msg.Type.IsDelete() {
            data = msg.OldData
        }
        data["operation"] = string(msg.Type)
        payload, _ := json.Marshal(data)
        return []rabbitmq.PublishMessage{{Body: payload, ContentType: "application/json"}}
    })
    if err != nil {
        slog.Error("new connector", "error", err)
        return
    }
    defer connector.Close()
    connector.Start(context.Background())
}
```

## Exchange and Routing

By default, each event resolves to a routing key:

- `{schema}.{table}.{operation}`
- Example: `public.users.INSERT`

Consumer services can bind their own queues independently:

- `public.users.*` for all user operations
- `#.DELETE` for all delete operations
- `public.orders.INSERT` for a single operation

This allows fan-out scenarios where multiple workers receive the same event copy.

With `tableRoutingKeyMapping` set, only mapped tables are published; changes to
any other table are acked and skipped. A static value gets `.{operation}`
appended (`public.users` → `public.users.INSERT`); a template value
(`{{.TableNamespace}}`, `{{.TableName}}`, `{{.Operation}}`) is used as rendered.
Templates are checked when the connector is created.

Partitions without their own mapping use the longest mapped prefix of their
name split on `_` (`events_archive_2024` → `events_archive`). The match is by
name only, so an unrelated table such as `users_audit` also routes as `users`
when `users` is mapped.

## Delivery guarantees

Delivery is at-least-once. A replication position is acked only after RabbitMQ
confirmed every row at or below it; a failed or unconfirmed batch is kept and
retried, so consumers can see duplicates and should be idempotent. A broker
nack is fatal with the default response handler.

`cdc.heartbeat` is rejected: go-pq-cdc acks heartbeat rows itself, which
confirms past rows still buffered in the publisher. Since go-pq-cdc v1.14 an
idle slot advances on keepalives, which covers what the heartbeat was for.

Known gap in go-pq-cdc, outside this connector's control:

- **Initial snapshot:** go-pq-cdc marks a snapshot chunk done once its rows are
  handed over, before they are published. A crash in that window loses those
  rows; the end of the snapshot flushes what is buffered.

## Integration Tests

Integration tests use testcontainers:

- PostgreSQL with logical replication enabled
- RabbitMQ management image

Run:

```sh
make test/integration
```

## Benchmarks

Benchmark stack:

- PostgreSQL
- RabbitMQ
- Prometheus
- Grafana

Start stack:

```sh
cd benchmark && docker compose up -d
```

Run benchmark:

```sh
make test/benchmark
```

## Metrics

- `go_pq_cdc_rabbitmq_process_latency_current`
- `go_pq_cdc_rabbitmq_bulk_request_process_latency_current`
- `go_pq_cdc_rabbitmq_publish_total`
- `go_pq_cdc_rabbitmq_err_total`

## Compatibility

- PostgreSQL 15+ (CI covers 15 to 18)
- RabbitMQ 3+
