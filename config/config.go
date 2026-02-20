package config

import (
	"math"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
)

type TLSConfig struct {
	CACert   []byte `yaml:"caCert" mapstructure:"caCert"`
	Cert     []byte `yaml:"cert" mapstructure:"cert"`
	Key      []byte `yaml:"key" mapstructure:"key"`
	Enabled  bool   `yaml:"enabled" mapstructure:"enabled"`
	Insecure bool   `yaml:"insecure" mapstructure:"insecure"`
}

type DeadLetterConfig struct {
	Exchange   string `yaml:"exchange" mapstructure:"exchange"`
	RoutingKey string `yaml:"routingKey" mapstructure:"routingKey"`
	QueueName  string `yaml:"queueName" mapstructure:"queueName"`
	Enabled    bool   `yaml:"enabled" mapstructure:"enabled"`
}

type ExchangeConfig struct {
	Arguments  map[string]any    `yaml:"arguments" mapstructure:"arguments"`
	DeadLetter *DeadLetterConfig `yaml:"deadLetter" mapstructure:"deadLetter"`
	Name       string            `yaml:"name" mapstructure:"name"`
	Type       string            `yaml:"type" mapstructure:"type"`
	Durable    bool              `yaml:"durable" mapstructure:"durable"`
	AutoDelete bool              `yaml:"autoDelete" mapstructure:"autoDelete"`
}

type QueueConfig struct {
	Arguments  map[string]any `yaml:"arguments" mapstructure:"arguments"`
	Name       string         `yaml:"name" mapstructure:"name"`
	Bindings   []string       `yaml:"bindings" mapstructure:"bindings"`
	Durable    bool           `yaml:"durable" mapstructure:"durable"`
	AutoDelete bool           `yaml:"autoDelete" mapstructure:"autoDelete"`
	Exclusive  bool           `yaml:"exclusive" mapstructure:"exclusive"`
	NoWait     bool           `yaml:"noWait" mapstructure:"noWait"`
}

type RabbitMQ struct {
	Exchange                     ExchangeConfig    `yaml:"exchange" mapstructure:"exchange"`
	TableRoutingKeyMapping       map[string]string `yaml:"tableRoutingKeyMapping" mapstructure:"tableRoutingKeyMapping"`
	ConnectionName               string            `yaml:"connectionName" mapstructure:"connectionName"`
	PublisherBatchBytes          string            `yaml:"publisherBatchBytes" mapstructure:"publisherBatchBytes"`
	RoutingKeyTemplate           string            `yaml:"routingKeyTemplate" mapstructure:"routingKeyTemplate"`
	URL                          string            `yaml:"url" mapstructure:"url"`
	TLS                          TLSConfig         `yaml:"tls" mapstructure:"tls"`
	Queues                       []QueueConfig     `yaml:"queues" mapstructure:"queues"`
	Heartbeat                    time.Duration     `yaml:"heartbeat" mapstructure:"heartbeat"`
	PublisherBatchSize           int               `yaml:"publisherBatchSize" mapstructure:"publisherBatchSize"`
	ConnectionTimeout            time.Duration     `yaml:"connectionTimeout" mapstructure:"connectionTimeout"`
	PublisherBatchTickerDuration time.Duration     `yaml:"publisherBatchTickerDuration" mapstructure:"publisherBatchTickerDuration"`
	PublisherMaxRetries          int               `yaml:"publisherMaxRetries" mapstructure:"publisherMaxRetries"`
	ReconnectInterval            time.Duration     `yaml:"reconnectInterval" mapstructure:"reconnectInterval"`
	ReconnectMaxInterval         time.Duration     `yaml:"reconnectMaxInterval" mapstructure:"reconnectMaxInterval"`
	ReconnectMaxElapsed          time.Duration     `yaml:"reconnectMaxElapsed" mapstructure:"reconnectMaxElapsed"`
}

type Connector struct {
	RabbitMQ RabbitMQ         `yaml:"rabbitmq" mapstructure:"rabbitmq"`
	CDC      cdcconfig.Config `yaml:"cdc" mapstructure:"cdc"`
}

func (c *Connector) SetDefault() {
	if c.RabbitMQ.URL == "" {
		c.RabbitMQ.URL = "amqp://guest:guest@localhost:5672/"
	}
	if c.RabbitMQ.ConnectionTimeout == 0 {
		c.RabbitMQ.ConnectionTimeout = 30 * time.Second
	}
	if c.RabbitMQ.Heartbeat == 0 {
		c.RabbitMQ.Heartbeat = 10 * time.Second
	}
	if c.RabbitMQ.Exchange.Type == "" {
		c.RabbitMQ.Exchange.Type = "topic"
	}
	if c.RabbitMQ.Exchange.Name == "" {
		c.RabbitMQ.Exchange.Name = "cdc.events"
	}
	c.RabbitMQ.Exchange.Durable = true
	if c.RabbitMQ.PublisherBatchSize == 0 {
		c.RabbitMQ.PublisherBatchSize = 2000
	}
	if c.RabbitMQ.PublisherBatchBytes == "" {
		c.RabbitMQ.PublisherBatchBytes = "1mb"
	}
	if c.RabbitMQ.PublisherBatchTickerDuration == 0 {
		c.RabbitMQ.PublisherBatchTickerDuration = 10 * time.Second
	}
	if c.RabbitMQ.PublisherMaxRetries == 0 {
		c.RabbitMQ.PublisherMaxRetries = math.MaxInt
	}
	if c.RabbitMQ.ReconnectInterval == 0 {
		c.RabbitMQ.ReconnectInterval = 1 * time.Second
	}
	if c.RabbitMQ.ReconnectMaxInterval == 0 {
		c.RabbitMQ.ReconnectMaxInterval = 30 * time.Second
	}
	if c.RabbitMQ.RoutingKeyTemplate == "" {
		c.RabbitMQ.RoutingKeyTemplate = "{{.TableNamespace}}.{{.TableName}}.{{.Operation}}"
	}
	for i := range c.RabbitMQ.Queues {
		c.RabbitMQ.Queues[i].Durable = true
	}
}
