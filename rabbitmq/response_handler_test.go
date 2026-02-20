package rabbitmq

import (
	"errors"
	"io"
	"syscall"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestIsFatalError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err   error
		name  string
		fatal bool
	}{
		// AMQP errors that should be fatal (no point retrying).
		{
			name:  "amqp NotFound",
			err:   &amqp.Error{Code: amqp.NotFound, Reason: "not found"},
			fatal: true,
		},
		{
			name:  "amqp AccessRefused",
			err:   &amqp.Error{Code: amqp.AccessRefused, Reason: "access refused"},
			fatal: true,
		},
		{
			name:  "amqp PreconditionFailed",
			err:   &amqp.Error{Code: amqp.PreconditionFailed, Reason: "precondition failed"},
			fatal: true,
		},

		// AMQP errors that are transient (should retry).
		{
			name:  "amqp ConnectionForced",
			err:   &amqp.Error{Code: amqp.ConnectionForced, Reason: "connection forced"},
			fatal: false,
		},
		{
			name:  "amqp ChannelError",
			err:   &amqp.Error{Code: amqp.ChannelError, Reason: "channel error"},
			fatal: false,
		},
		{
			name:  "amqp InternalError",
			err:   &amqp.Error{Code: amqp.InternalError, Reason: "internal error"},
			fatal: false,
		},

		// Network/IO errors that are transient.
		{
			name:  "unexpected EOF",
			err:   io.ErrUnexpectedEOF,
			fatal: false,
		},
		{
			name:  "connection refused",
			err:   syscall.ECONNREFUSED,
			fatal: false,
		},
		{
			name:  "connection reset",
			err:   syscall.ECONNRESET,
			fatal: false,
		},
		{
			name:  "broken pipe",
			err:   syscall.EPIPE,
			fatal: false,
		},

		// Wrapped transient errors should still be non-fatal.
		{
			name:  "wrapped ECONNRESET",
			err:   errors.Join(errors.New("write failed"), syscall.ECONNRESET),
			fatal: false,
		},
		{
			name:  "wrapped unexpected EOF",
			err:   errors.Join(errors.New("read body"), io.ErrUnexpectedEOF),
			fatal: false,
		},

		// Unknown errors default to fatal (fail-safe).
		{
			name:  "unknown error is fatal",
			err:   errors.New("something completely unexpected"),
			fatal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.fatal, isFatalError(tt.err))
		})
	}
}
