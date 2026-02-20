package cdc

import "github.com/ezeql/go-pq-cdc-rabbitmq/rabbitmq"

type Handler func(msg *Message) []rabbitmq.PublishMessage
