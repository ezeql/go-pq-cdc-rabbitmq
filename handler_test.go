package cdc_test

import (
	"encoding/json"
	"testing"
	"time"

	cdc "github.com/ezeql/go-pq-cdc-rabbitmq"
	"github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// userLifecycleHandler demonstrates routing CDC events by operation type.
// Each case returns a PublishMessage that would land in a different queue
// based on the routing key pattern configured in the connector:
//
//	welcome_queue       <- public.users.INSERT   (send welcome email)
//	notification_queue  <- public.users.UPDATE   (push notification)
//	audit_queue         <- public.users.DELETE   (compliance audit log)
func userLifecycleHandler(msg *cdc.Message) []rabbitmq.PublishMessage {
	switch msg.Type {
	case cdc.InsertMessage:
		// Consumer: send welcome email, create default settings, start onboarding drip.
		return publish(msg.NewData, msg.Type)

	case cdc.UpdateMessage:
		// Consumer: push "profile updated" notification, refresh search index, bust cache.
		return publish(msg.NewData, msg.Type)

	case cdc.DeleteMessage:
		// Consumer: write compliance audit log, trigger GDPR data purge, revoke sessions.
		return publish(msg.OldData, msg.Type)

	default:
		return nil
	}
}

func publish(data map[string]any, op cdc.MessageType) []rabbitmq.PublishMessage {
	data["operation"] = string(op)
	body, _ := json.Marshal(data)
	return []rabbitmq.PublishMessage{{Body: body, ContentType: "application/json"}}
}

// TestHandler_InsertEmitsWelcomeMessage verifies that a new user INSERT
// produces a message suitable for the welcome_queue consumer.
func TestHandler_InsertEmitsWelcomeMessage(t *testing.T) {
	msg := &cdc.Message{
		EventTime:      time.Now(),
		TableName:      "users",
		TableNamespace: "public",
		NewData:        map[string]any{"id": 1, "name": "Alice", "email": "alice@acme.io"},
		Type:           cdc.InsertMessage,
	}

	out := userLifecycleHandler(msg)

	require.Len(t, out, 1)
	assert.Equal(t, "application/json", out[0].ContentType)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(out[0].Body, &payload))
	assert.Equal(t, "INSERT", payload["operation"])
	assert.Equal(t, "Alice", payload["name"])
	assert.Equal(t, "alice@acme.io", payload["email"])
}

// TestHandler_UpdateEmitsNotification verifies that a profile UPDATE
// produces a message suitable for the notification_queue consumer.
func TestHandler_UpdateEmitsNotification(t *testing.T) {
	msg := &cdc.Message{
		EventTime:      time.Now(),
		TableName:      "users",
		TableNamespace: "public",
		OldData:        map[string]any{"id": 1, "name": "Alice", "email": "alice@acme.io"},
		NewData:        map[string]any{"id": 1, "name": "Alice Smith", "email": "alice.smith@acme.io"},
		Type:           cdc.UpdateMessage,
	}

	out := userLifecycleHandler(msg)

	require.Len(t, out, 1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(out[0].Body, &payload))
	assert.Equal(t, "UPDATE", payload["operation"])
	assert.Equal(t, "Alice Smith", payload["name"])
	assert.Equal(t, "alice.smith@acme.io", payload["email"])
}

// TestHandler_DeleteEmitsAuditRecord verifies that an account DELETE
// produces a message with the OLD data suitable for the audit_queue consumer.
func TestHandler_DeleteEmitsAuditRecord(t *testing.T) {
	msg := &cdc.Message{
		EventTime:      time.Now(),
		TableName:      "users",
		TableNamespace: "public",
		OldData:        map[string]any{"id": 1, "name": "Alice Smith", "email": "alice.smith@acme.io"},
		NewData:        nil,
		Type:           cdc.DeleteMessage,
	}

	out := userLifecycleHandler(msg)

	require.Len(t, out, 1)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(out[0].Body, &payload))
	assert.Equal(t, "DELETE", payload["operation"])
	assert.Equal(t, "Alice Smith", payload["name"])
	assert.Equal(t, "alice.smith@acme.io", payload["email"])
}
