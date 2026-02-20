package cdc

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"text/template"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	"github.com/ezeql/go-pq-cdc-rabbitmq/internal/sliceutil"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq/publisher"
	"github.com/prometheus/client_golang/prometheus"
)

// operationSuffix avoids per-message string concatenation when building routing keys.
var operationSuffix = map[MessageType]string{
	InsertMessage:   ".INSERT",
	UpdateMessage:   ".UPDATE",
	DeleteMessage:   ".DELETE",
	SnapshotMessage: ".SNAPSHOT",
}

// bufPool reuses bytes.Buffers for template rendering to avoid per-call allocations.
var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

type Connector interface {
	Start(ctx context.Context)
	WaitUntilReady(ctx context.Context) error
	Close()
}

// routingKeyResult stores a cached routing key resolution result.
type routingKeyResult struct {
	key   string
	found bool
}

type connector struct {
	cdc                  cdc.Connector
	client               rabbitmq.Client
	responseHandler      rabbitmq.ResponseHandler
	staticMappings       map[string]string
	templateMappings     map[string]*template.Template
	publisher            publisher.Publisher
	handler              Handler
	cfg                  *config.Connector
	readyCh              chan struct{}
	partitionCache       sync.Map
	routingKeyCache      sync.Map
	routingTemplateCache sync.Map
	metrics              []prometheus.Collector
	readyOnce            sync.Once
	closeOnce            sync.Once
}

func NewConnector(ctx context.Context, cfg config.Connector, handler Handler, options ...Option) (Connector, error) {
	cfg.SetDefault()
	rm := &connector{
		cfg:     &cfg,
		handler: handler,
		readyCh: make(chan struct{}),
	}

	// Pre-scan TableRoutingKeyMapping to separate static values from templates.
	rm.staticMappings = make(map[string]string, len(cfg.RabbitMQ.TableRoutingKeyMapping))
	rm.templateMappings = make(map[string]*template.Template, len(cfg.RabbitMQ.TableRoutingKeyMapping))
	for k, v := range cfg.RabbitMQ.TableRoutingKeyMapping {
		if strings.Contains(v, "{{") {
			t, err := template.New("routingKey:" + k).Parse(v)
			if err != nil {
				return nil, fmt.Errorf("invalid routing key template for %q: %w", k, err)
			}
			rm.templateMappings[k] = t
		} else {
			rm.staticMappings[k] = v
		}
	}

	Options(options).Apply(rm)

	pqCDC, err := cdc.NewConnector(ctx, rm.cfg.CDC, rm.listener)
	if err != nil {
		return nil, err
	}
	rm.cdc = pqCDC
	rm.cfg.CDC = *pqCDC.GetConfig()

	rmqClient, err := rabbitmq.NewClient(rm.cfg)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq new client: %w", err)
	}
	rm.client = rmqClient

	if rm.responseHandler == nil {
		rm.responseHandler = &rabbitmq.DefaultResponseHandler{}
	}

	rm.publisher, err = publisher.NewPublisher(rmqClient, rm.cfg, rm.responseHandler)
	if err != nil {
		logger.Error("rabbitmq new publisher", "error", err)
		return nil, err
	}

	pqCDC.SetMetricCollectors(rm.publisher.GetMetric().PrometheusCollectors()...)
	pqCDC.SetMetricCollectors(rm.metrics...)
	return rm, nil
}

func (c *connector) Start(ctx context.Context) {
	if c.cfg.CDC.IsSnapshotOnlyMode() {
		logger.Info("starting snapshot-only mode")
		logger.Info("bulk process started")
		c.publisher.StartBatch()
		c.signalReady()
		c.cdc.Start(ctx)
		logger.Info("snapshot-only mode completed")
		return
	}

	go func() {
		logger.Info("waiting for connector start...")
		if err := c.cdc.WaitUntilReady(ctx); err != nil {
			panic(err)
		}
		logger.Info("bulk process started")
		c.publisher.StartBatch()
		c.signalReady()
	}()
	c.cdc.Start(ctx)
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	c.closeOnce.Do(func() {
		c.signalReady()
		c.cdc.Close()
		c.publisher.Close()
		if err := c.client.Close(); err != nil {
			logger.Error("rabbitmq client close", "error", err)
		}
	})
}

func (c *connector) listener(ctx *replication.ListenerContext) {
	var msg *Message
	switch m := ctx.Message.(type) {
	case *format.Insert:
		msg = NewInsertMessage(m)
	case *format.Update:
		msg = NewUpdateMessage(m)
	case *format.Delete:
		msg = NewDeleteMessage(m)
	case *format.Snapshot:
		msg = NewSnapshotMessage(m)
	default:
		return
	}

	fullTableName := c.getFullTableName(msg.TableNamespace, msg.TableName)
	routingKey, ok := c.resolveTableToRoutingKey(fullTableName, msg.TableNamespace, msg.TableName, msg.Type)
	if !ok {
		if c.publisher.HasPendingMessages() {
			logger.Warn("skipping ACK - pending messages in batch", "table", fullTableName)
			return
		}
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}

	events := c.handler(msg)
	if len(events) == 0 {
		if c.publisher.HasPendingMessages() {
			logger.Warn("skipping ACK - pending messages in batch", "table", fullTableName)
			return
		}
		if err := ctx.Ack(); err != nil {
			logger.Error("ack", "error", err)
		}
		return
	}

	for i := range events {
		if events[i].RoutingKey == "" {
			events[i].RoutingKey = routingKey
		}
	}

	batchSizeLimit := c.cfg.RabbitMQ.PublisherBatchSize
	if len(events) > batchSizeLimit {
		chunks := sliceutil.ChunkWithSize(events, batchSizeLimit)
		lastChunkIndex := len(chunks) - 1
		for idx, chunk := range chunks {
			c.publisher.Produce(ctx, msg.EventTime, chunk, idx == lastChunkIndex)
		}
		return
	}
	c.publisher.Produce(ctx, msg.EventTime, events, true)
}

func (c *connector) resolveTableToRoutingKey(fullTableName, tableNamespace, tableName string, operation MessageType) (string, bool) {
	// Check the routing key cache first (covers both template and static mapping paths).
	cacheKey := fullTableName + "|" + string(operation)
	if cached, ok := c.routingKeyCache.Load(cacheKey); ok {
		result := cached.(routingKeyResult)
		return result.key, result.found
	}

	key, found := c.resolveTableToRoutingKeySlow(fullTableName, tableNamespace, tableName, operation)
	c.routingKeyCache.Store(cacheKey, routingKeyResult{key: key, found: found})
	return key, found
}

// resolveTableToRoutingKeySlow is the uncached routing key resolution path.
func (c *connector) resolveTableToRoutingKeySlow(fullTableName, tableNamespace, tableName string, operation MessageType) (string, bool) {
	mapping := c.cfg.RabbitMQ.TableRoutingKeyMapping
	if len(mapping) == 0 {
		key, err := c.renderTemplate(c.cfg.RabbitMQ.RoutingKeyTemplate, tableNamespace, tableName, operation)
		return key, err == nil
	}

	lookupKey := fullTableName
	_, exists := mapping[lookupKey]
	if !exists {
		if t, ok := timescaledb.HyperTables.Load(fullTableName); ok {
			parentName := t.(string)
			if _, ok := mapping[parentName]; ok {
				lookupKey = parentName
				exists = true
			}
		}
		if !exists {
			parent := c.getParentTableName(fullTableName, tableNamespace, tableName)
			if parent != "" {
				if _, ok := mapping[parent]; ok {
					lookupKey = parent
					exists = true
				}
			}
		}
	}
	if !exists {
		return "", false
	}

	// Use pre-scanned template/static maps to avoid runtime strings.Contains.
	if t, ok := c.templateMappings[lookupKey]; ok {
		key, err := c.renderParsedTemplate(t, tableNamespace, tableName, operation)
		return key, err == nil
	}
	if base, ok := c.staticMappings[lookupKey]; ok {
		return base + operationSuffix[operation], true
	}

	// Fallback for dynamically discovered mappings (e.g. partition/hypertable parents
	// that map to a different key than the one originally in TableRoutingKeyMapping).
	base := mapping[lookupKey]
	if strings.Contains(base, "{{") {
		key, err := c.renderTemplate(base, tableNamespace, tableName, operation)
		return key, err == nil
	}
	return base + operationSuffix[operation], true
}

func (c *connector) getParentTableName(fullTableName, tableNamespace, tableName string) string {
	if cachedValue, found := c.partitionCache.Load(fullTableName); found {
		parentName, ok := cachedValue.(string)
		if !ok {
			logger.Error("invalid cache value type for table", "table", fullTableName)
			return ""
		}
		return parentName
	}

	parentTableName := c.findParentTable(tableNamespace, tableName)
	c.partitionCache.Store(fullTableName, parentTableName)
	return parentTableName
}

func (c *connector) findParentTable(tableNamespace, tableName string) string {
	tableParts := strings.Split(tableName, "_")
	if len(tableParts) <= 1 {
		return ""
	}
	for i := 1; i < len(tableParts); i++ {
		parentNameCandidate := strings.Join(tableParts[:i], "_")
		fullParentName := c.getFullTableName(tableNamespace, parentNameCandidate)
		if _, exists := c.cfg.RabbitMQ.TableRoutingKeyMapping[fullParentName]; exists {
			return fullParentName
		}
	}
	return ""
}

func (c *connector) getFullTableName(tableNamespace, tableName string) string {
	return tableNamespace + "." + tableName
}

func (c *connector) renderTemplate(tpl, schema, table string, op MessageType) (string, error) {
	t, err := c.getOrCreateRoutingTemplate(tpl)
	if err != nil {
		return "", err
	}
	return c.renderParsedTemplate(t, schema, table, op)
}

func (c *connector) renderParsedTemplate(t *template.Template, schema, table string, op MessageType) (string, error) {
	data := struct {
		TableNamespace string
		TableName      string
		Operation      string
	}{
		TableNamespace: schema,
		TableName:      table,
		Operation:      string(op),
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if err := t.Execute(buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (c *connector) getOrCreateRoutingTemplate(tpl string) (*template.Template, error) {
	if v, ok := c.routingTemplateCache.Load(tpl); ok {
		t, castOK := v.(*template.Template)
		if castOK {
			return t, nil
		}
	}

	parsed, err := template.New("routingKey").Parse(tpl)
	if err != nil {
		return nil, err
	}
	actual, _ := c.routingTemplateCache.LoadOrStore(tpl, parsed)
	t, _ := actual.(*template.Template)
	return t, nil
}

func (c *connector) signalReady() {
	c.readyOnce.Do(func() {
		close(c.readyCh)
	})
}
