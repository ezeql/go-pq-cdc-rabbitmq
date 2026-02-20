package integration

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
)

type TestInfrastructure struct {
	PostgresContainer testcontainers.Container
	RabbitContainer   testcontainers.Container
	PostgresHost      string
	PostgresPort      string
	RabbitHost        string
	RabbitPort        string
	RabbitMgmtPort    string
}

func (ti *TestInfrastructure) Cleanup(ctx context.Context) error {
	if ti.RabbitContainer != nil {
		if err := ti.RabbitContainer.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate rabbitmq container: %w", err)
		}
	}
	if ti.PostgresContainer != nil {
		if err := ti.PostgresContainer.Terminate(ctx); err != nil {
			return fmt.Errorf("failed to terminate postgres container: %w", err)
		}
	}
	return nil
}
