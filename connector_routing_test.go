package cdc

import (
	"strings"
	"testing"
	"text/template"

	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRoutingTestConnector builds a minimal connector with only the fields
// needed for routing key resolution. No AMQP or CDC dependencies.
func newRoutingTestConnector(cfg config.Connector) *connector {
	c := &connector{
		cfg:              &cfg,
		staticMappings:   make(map[string]string, len(cfg.RabbitMQ.TableRoutingKeyMapping)),
		templateMappings: make(map[string]*template.Template, len(cfg.RabbitMQ.TableRoutingKeyMapping)),
	}
	for k, v := range cfg.RabbitMQ.TableRoutingKeyMapping {
		if strings.Contains(v, "{{") {
			t, err := template.New("routingKey:" + k).Parse(v)
			if err != nil {
				panic(err)
			}
			c.templateMappings[k] = t
		} else {
			c.staticMappings[k] = v
		}
	}
	return c
}

// --- resolveTableToRoutingKey ---

func TestResolveRoutingKey_DefaultTemplate_NoMapping(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, ok := c.resolveTableToRoutingKey("public.users", "public", "users", InsertMessage)
	require.True(t, ok)
	assert.Equal(t, "public.users.INSERT", key)

	key, ok = c.resolveTableToRoutingKey("myschema.orders", "myschema", "orders", DeleteMessage)
	require.True(t, ok)
	assert.Equal(t, "myschema.orders.DELETE", key)
}

func TestResolveRoutingKey_CustomTemplate_NoMapping(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			RoutingKeyTemplate: "cdc.{{.TableName}}.{{.Operation}}",
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, ok := c.resolveTableToRoutingKey("public.users", "public", "users", UpdateMessage)
	require.True(t, ok)
	assert.Equal(t, "cdc.users.UPDATE", key)
}

func TestResolveRoutingKey_StaticMapping(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users":  "user-events",
				"public.orders": "order-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, ok := c.resolveTableToRoutingKey("public.users", "public", "users", InsertMessage)
	require.True(t, ok)
	assert.Equal(t, "user-events.INSERT", key)

	key, ok = c.resolveTableToRoutingKey("public.orders", "public", "orders", DeleteMessage)
	require.True(t, ok)
	assert.Equal(t, "order-events.DELETE", key)
}

func TestResolveRoutingKey_TemplateMappingValue(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users": "{{.TableNamespace}}.{{.TableName}}.{{.Operation}}",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, ok := c.resolveTableToRoutingKey("public.users", "public", "users", InsertMessage)
	require.True(t, ok)
	assert.Equal(t, "public.users.INSERT", key)
}

func TestResolveRoutingKey_UnmappedTable_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users": "user-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	_, ok := c.resolveTableToRoutingKey("public.products", "public", "products", InsertMessage)
	assert.False(t, ok)
}

func TestResolveRoutingKey_CachesResult(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users": "user-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	// First call populates cache.
	key1, ok1 := c.resolveTableToRoutingKey("public.users", "public", "users", InsertMessage)
	require.True(t, ok1)

	// Second call should return the same result from cache.
	key2, ok2 := c.resolveTableToRoutingKey("public.users", "public", "users", InsertMessage)
	require.True(t, ok2)
	assert.Equal(t, key1, key2)

	// Verify cache entry exists.
	_, loaded := c.routingKeyCache.Load("public.users|INSERT")
	assert.True(t, loaded)
}

func TestResolveRoutingKey_DifferentOperations_DifferentKeys(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users": "user-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	ops := []struct {
		op   MessageType
		want string
	}{
		{InsertMessage, "user-events.INSERT"},
		{UpdateMessage, "user-events.UPDATE"},
		{DeleteMessage, "user-events.DELETE"},
		{SnapshotMessage, "user-events.SNAPSHOT"},
	}
	for _, tt := range ops {
		key, ok := c.resolveTableToRoutingKey("public.users", "public", "users", tt.op)
		require.True(t, ok, "operation %s", tt.op)
		assert.Equal(t, tt.want, key, "operation %s", tt.op)
	}
}

// --- findParentTable (partition table lookup) ---

func TestFindParentTable_SingleSegmentName_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users": "user-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	// "users" has no underscores, so no parent candidates.
	assert.Empty(t, c.findParentTable("public", "users"))
}

func TestFindParentTable_PartitionedTable_FindsParent(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.orders": "order-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	// "orders_2024_01" should match parent "orders".
	parent := c.findParentTable("public", "orders_2024_01")
	assert.Equal(t, "public.orders", parent)
}

func TestFindParentTable_DeepPartition_FindsFirstMatch(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.events_archive": "archive-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	// "events_archive_2024_q1" should match "events_archive" (not "events").
	parent := c.findParentTable("public", "events_archive_2024_q1")
	assert.Equal(t, "public.events_archive", parent)
}

func TestFindParentTable_NoMappingMatch_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.users": "user-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	// "logs_2024_01" has no parent in the mapping.
	assert.Empty(t, c.findParentTable("public", "logs_2024_01"))
}

func TestResolveRoutingKey_PartitionTable_InheritsParentMapping(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{
		RabbitMQ: config.RabbitMQ{
			TableRoutingKeyMapping: map[string]string{
				"public.orders": "order-events",
			},
		},
	}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, ok := c.resolveTableToRoutingKey("public.orders_2024_01", "public", "orders_2024_01", InsertMessage)
	require.True(t, ok)
	assert.Equal(t, "order-events.INSERT", key)
}

// --- renderTemplate ---

func TestRenderTemplate_AllFields(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, err := c.renderTemplate(
		"{{.TableNamespace}}.{{.TableName}}.{{.Operation}}",
		"myschema", "orders", DeleteMessage,
	)
	require.NoError(t, err)
	assert.Equal(t, "myschema.orders.DELETE", key)
}

func TestRenderTemplate_PartialFields(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	key, err := c.renderTemplate("events.{{.TableName}}", "public", "users", InsertMessage)
	require.NoError(t, err)
	assert.Equal(t, "events.users", key)
}

func TestRenderTemplate_InvalidTemplate_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := config.Connector{}
	cfg.SetDefault()
	c := newRoutingTestConnector(cfg)

	_, err := c.renderTemplate("{{.Invalid", "public", "users", InsertMessage)
	assert.Error(t, err)
}
