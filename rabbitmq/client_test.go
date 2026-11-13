package rabbitmq

import (
	"strings"
	"testing"

	"github.com/ezeql/go-pq-cdc-rabbitmq/config"
)

func TestDialRejectsInsecureTLSConfig(t *testing.T) {
	for name, tc := range map[string]struct {
		url    string
		tls    config.TLSConfig
		errSub string
	}{
		"plaintext scheme": {url: "amqp://localhost:5672/", tls: config.TLSConfig{Enabled: true}, errSub: "amqps://"},
		"bad CA PEM":       {url: "amqps://localhost:5671/", tls: config.TLSConfig{Enabled: true, CACert: []byte("nope")}, errSub: "caCert"},
		"cert without key": {url: "amqps://localhost:5671/", tls: config.TLSConfig{Enabled: true, Cert: []byte("x")}, errSub: "tls"},
	} {
		t.Run(name, func(t *testing.T) {
			c := &client{cfg: &config.Connector{RabbitMQ: config.RabbitMQ{URL: tc.url, TLS: tc.tls}}}
			_, err := c.dial()
			if err == nil || !strings.Contains(err.Error(), tc.errSub) {
				t.Fatalf("want error containing %q, got %v", tc.errSub, err)
			}
		})
	}
}
