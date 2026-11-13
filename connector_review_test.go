package cdc

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq/publisher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRoutingTemplate_FailsWhenTemplateCannotRender(t *testing.T) {
	_, err := parseRoutingTemplate("k", "{{.Table}}.x") // parses, but no such field
	require.Error(t, err)
	_, err = parseRoutingTemplate("k", "{{.TableName")
	require.Error(t, err)
	_, err = parseRoutingTemplate("k", "{{.TableNamespace}}.{{.TableName}}.{{.Operation}}")
	require.NoError(t, err)
}

func TestFindParentTable_PrefersLongestMappedPrefix(t *testing.T) {
	cfg := config.Connector{RabbitMQ: config.RabbitMQ{TableRoutingKeyMapping: map[string]string{
		"public.events":         "ev",
		"public.events_archive": "ev.archive",
	}}}
	c := newRoutingTestConnector(cfg)
	assert.Equal(t, "public.events_archive", c.findParentTable("public", "events_archive_2024"))
	assert.Equal(t, "public.events", c.findParentTable("public", "events_2024"))
}

// Snapshot BEGIN/END markers have no table and no row: they must be acked and
// never reach the user handler (which would see a nil NewData map).
func TestListener_SkipsSnapshotMarkers(t *testing.T) {
	cfg := config.Connector{}
	cfg.SetDefault()
	pub, err := publisher.NewPublisher(nil, &cfg, &rabbitmq.DefaultResponseHandler{})
	require.NoError(t, err)
	defer pub.PublisherBatch.Close()

	c := newRoutingTestConnector(cfg)
	c.publisher = pub
	c.handler = func(*Message) []rabbitmq.PublishMessage {
		t.Fatal("handler called for a snapshot marker")
		return nil
	}

	for _, ev := range []format.SnapshotEventType{format.SnapshotEventTypeBegin, format.SnapshotEventTypeEnd} {
		acked := false
		c.listener(&replication.ListenerContext{
			Message: &format.Snapshot{EventType: ev},
			Ack:     func() error { acked = true; return nil },
		})
		assert.True(t, acked, "%s marker not acked", ev)
	}
}

// A header amqp cannot encode must fail before any chunk of the event is
// buffered: a later chunk failing would republish the earlier ones forever.
func TestListener_RejectsInvalidHeadersBeforeBuffering(t *testing.T) {
	cfg := config.Connector{}
	cfg.SetDefault()
	cfg.RabbitMQ.PublisherBatchSize = 1 // two events -> two chunks
	pub, err := publisher.NewPublisher(nil, &cfg, &rabbitmq.DefaultResponseHandler{})
	require.NoError(t, err)
	defer pub.PublisherBatch.Close()

	c := newRoutingTestConnector(cfg)
	c.publisher = pub
	c.handler = func(*Message) []rabbitmq.PublishMessage {
		return []rabbitmq.PublishMessage{{Body: []byte("ok")}, {Headers: map[string]any{"lsn": uint64(1)}}}
	}

	// With a nil client, buffering the first chunk would flush and panic on a
	// nil pointer; getting the header error proves validation ran first.
	defer func() {
		err, _ := recover().(error)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid AMQP headers")
	}()
	c.listener(&replication.ListenerContext{
		Message: &format.Insert{TableNamespace: "public", TableName: "users"},
		Ack:     func() error { return nil },
	})
}
