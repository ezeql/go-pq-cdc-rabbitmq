package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client interface {
	Channel() *amqp.Channel
	NotifyReconnect() <-chan struct{}
	Close() error
}

type client struct {
	cfg         *config.Connector
	conn        *amqp.Connection
	channel     *amqp.Channel
	connCloseCh chan *amqp.Error
	chCloseCh   chan *amqp.Error
	reconnectCh chan struct{}
	closeCh     chan struct{}
	mu          sync.RWMutex
}

func NewClient(cfg *config.Connector) (Client, error) {
	c := &client{
		cfg:         cfg,
		reconnectCh: make(chan struct{}, 1),
		closeCh:     make(chan struct{}),
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	go c.reconnectLoop()
	return c, nil
}

func (c *client) Channel() *amqp.Channel {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channel
}

func (c *client) NotifyReconnect() <-chan struct{} {
	return c.reconnectCh
}

func (c *client) Close() error {
	select {
	case <-c.closeCh:
	default:
		close(c.closeCh)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			firstErr = err
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *client) reconnectLoop() {
	for {
		select {
		case err := <-c.connCloseCh:
			if err != nil {
				logger.Error("rabbitmq connection closed", "error", err)
			}
			c.reconnect()
		case err := <-c.chCloseCh:
			if err != nil {
				logger.Error("rabbitmq channel closed", "error", err)
			}
			c.reconnect()
		case <-c.closeCh:
			return
		}
	}
}

func (c *client) reconnect() {
	backoff := c.cfg.RabbitMQ.ReconnectInterval
	maxBackoff := c.cfg.RabbitMQ.ReconnectMaxInterval
	maxElapsed := c.cfg.RabbitMQ.ReconnectMaxElapsed
	started := time.Now()

	for {
		select {
		case <-c.closeCh:
			return
		default:
		}

		if maxElapsed > 0 && time.Since(started) > maxElapsed {
			logger.Error("rabbitmq reconnect elapsed timeout exceeded", "max_elapsed", maxElapsed)
			return
		}

		sleep := backoff + rand.N(backoff/4+1) //nolint:gosec // G404: jitter does not require cryptographic randomness
		logger.Warn("attempting rabbitmq reconnection", "retry_in", sleep)
		time.Sleep(sleep)

		if err := c.connect(); err != nil {
			logger.Error("rabbitmq reconnect attempt failed", "error", err)
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		logger.Info("rabbitmq reconnected")
		select {
		case c.reconnectCh <- struct{}{}:
		default:
		}
		return
	}
}

func (c *client) connect() error {
	conn, err := c.dial()
	if err != nil {
		return fmt.Errorf("rabbitmq dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("rabbitmq channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq confirm mode: %w", err)
	}

	if err := c.declareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("rabbitmq topology: %w", err)
	}

	c.mu.Lock()
	prevCh := c.channel
	prevConn := c.conn
	c.conn = conn
	c.channel = ch
	c.connCloseCh = conn.NotifyClose(make(chan *amqp.Error, 1))
	c.chCloseCh = ch.NotifyClose(make(chan *amqp.Error, 1))
	c.mu.Unlock()

	if prevCh != nil {
		_ = prevCh.Close()
	}
	if prevConn != nil {
		_ = prevConn.Close()
	}
	return nil
}

func (c *client) dial() (*amqp.Connection, error) {
	cfg := amqp.Config{
		Heartbeat: c.cfg.RabbitMQ.Heartbeat,
		Dial:      amqp.DefaultDial(c.cfg.RabbitMQ.ConnectionTimeout),
		Properties: amqp.Table{
			"connection_name": c.cfg.RabbitMQ.ConnectionName,
		},
	}

	if c.cfg.RabbitMQ.TLS.Enabled {
		tlsCfg := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: c.cfg.RabbitMQ.TLS.Insecure, //nolint:gosec
		}
		if len(c.cfg.RabbitMQ.TLS.CACert) > 0 {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(c.cfg.RabbitMQ.TLS.CACert)
			tlsCfg.RootCAs = pool
		}
		if len(c.cfg.RabbitMQ.TLS.Cert) > 0 && len(c.cfg.RabbitMQ.TLS.Key) > 0 {
			cert, err := tls.X509KeyPair(c.cfg.RabbitMQ.TLS.Cert, c.cfg.RabbitMQ.TLS.Key)
			if err != nil {
				return nil, err
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		cfg.TLSClientConfig = tlsCfg
	}

	return amqp.DialConfig(c.cfg.RabbitMQ.URL, cfg)
}

func (c *client) declareTopology(ch *amqp.Channel) error {
	ex := c.cfg.RabbitMQ.Exchange
	if err := ch.ExchangeDeclare(
		ex.Name,
		ex.Type,
		ex.Durable,
		ex.AutoDelete,
		false,
		false,
		amqp.Table(ex.Arguments),
	); err != nil {
		return err
	}

	for _, q := range c.cfg.RabbitMQ.Queues {
		if _, err := ch.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			q.NoWait,
			amqp.Table(q.Arguments),
		); err != nil {
			return err
		}
		for _, binding := range q.Bindings {
			if err := ch.QueueBind(
				q.Name,
				binding,
				ex.Name,
				false,
				nil,
			); err != nil {
				return err
			}
		}
	}

	if ex.DeadLetter != nil && ex.DeadLetter.Enabled {
		if err := ch.ExchangeDeclare(ex.DeadLetter.Exchange, "topic", true, false, false, false, nil); err != nil {
			return err
		}
		if ex.DeadLetter.QueueName != "" {
			if _, err := ch.QueueDeclare(ex.DeadLetter.QueueName, true, false, false, false, nil); err != nil {
				return err
			}
			if err := ch.QueueBind(
				ex.DeadLetter.QueueName,
				ex.DeadLetter.RoutingKey,
				ex.DeadLetter.Exchange,
				false,
				nil,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

func ConsumeOne(ctx context.Context, ch *amqp.Channel, queue string) (amqp.Delivery, error) {
	msgs, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		return amqp.Delivery{}, err
	}
	select {
	case msg := <-msgs:
		return msg, nil
	case <-ctx.Done():
		return amqp.Delivery{}, ctx.Err()
	}
}
