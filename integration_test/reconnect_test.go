package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	"github.com/stretchr/testify/require"
)

func TestConnector_ConnectionRecovery(t *testing.T) {
	t.Skip("container restart behavior is environment-sensitive; run manually in CI with docker")

	ctx := context.Background()
	cfg := newTestConfig("reconnect_exchange", "reconnect_queue", "reconnect_slot", "reconnect_pub")
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	require.NoError(t, Infra.RabbitContainer.Stop(ctx, nil))
	time.Sleep(3 * time.Second)
	require.NoError(t, Infra.RabbitContainer.Start(ctx))
	time.Sleep(5 * time.Second)
}
