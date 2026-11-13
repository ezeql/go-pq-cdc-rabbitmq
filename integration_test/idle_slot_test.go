package integration

import (
	"context"
	"testing"
	"time"

	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go-pq-cdc confirms the server WAL end on an idle keepalive only once every
// delivered message has been acked. A single lost ack (a non-last row of a
// batch, a Relation message, a filtered row) pins the slot and WAL piles up.
func TestConnector_IdleSlotFollowsWALEnd(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t)
	defer db.Close()
	mustCreateUsersTable(t, db)

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS idle_unrelated (id serial PRIMARY KEY, payload text)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `ALTER SYSTEM SET wal_sender_timeout = '2s'`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `SELECT pg_reload_conf()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `ALTER SYSTEM RESET wal_sender_timeout`)
		_, _ = db.ExecContext(ctx, `SELECT pg_reload_conf()`)
		_, _ = db.ExecContext(ctx, `DROP TABLE IF EXISTS idle_unrelated`)
	})

	cfg := newTestConfig("users_idle_exchange", "users_idle_queue", "users_idle_slot", "users_idle_pub")
	connector, err := cdc.NewConnector(ctx, cfg, handler)
	require.NoError(t, err)
	defer connector.Close()
	go connector.Start(ctx)
	readyCtx, cancel := withTimeout(ctx)
	defer cancel()
	require.NoError(t, connector.WaitUntilReady(readyCtx))

	// One transaction, three rows: one batch, one Relation message.
	_, err = db.ExecContext(ctx, `INSERT INTO users (name, email) VALUES ('I1','i1@acme.io'),('I2','i2@acme.io'),('I3','i3@acme.io')`)
	require.NoError(t, err)
	conn, err := amqp.Dial(cfg.RabbitMQ.URL)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	defer ch.Close()
	require.Eventually(t, func() bool {
		q, err := ch.QueueDeclarePassive("users_idle_queue", true, false, false, false, nil)
		return err == nil && q.Messages == 3
	}, 20*time.Second, 100*time.Millisecond, "rows were not published")

	// WAL the publication never sees, across segment switches.
	_, err = db.ExecContext(ctx, `INSERT INTO idle_unrelated (payload) SELECT 'row-' || g FROM generate_series(1, 1000) g`)
	require.NoError(t, err)
	for range 3 {
		_, err = db.ExecContext(ctx, `SELECT pg_switch_wal()`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO idle_unrelated (payload) VALUES ('tick')`)
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		var near bool
		err := db.QueryRowContext(ctx, `
			SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) <
			       (SELECT setting::numeric FROM pg_settings WHERE name = 'wal_segment_size')
			FROM pg_replication_slots WHERE slot_name = 'users_idle_slot'`).Scan(&near)
		return err == nil && near
	}, 20*time.Second, 200*time.Millisecond, "confirmed_flush_lsn did not follow the WAL end while idle")
}
