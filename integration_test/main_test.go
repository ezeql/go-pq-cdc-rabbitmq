package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var Infra *TestInfrastructure

var (
	PostgresTestImage = "POSTGRES_TEST_IMAGE"
	defaultVersion    = "16.2"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error

	Infra, err = setupTestInfrastructure(ctx)
	if err != nil {
		log.Fatal("setup test infrastructure:", err)
	}

	code := m.Run()
	if err := Infra.Cleanup(ctx); err != nil {
		log.Printf("cleanup error: %v", err)
	}
	os.Exit(code)
}

func setupTestInfrastructure(ctx context.Context) (*TestInfrastructure, error) {
	postgresVersion := os.Getenv(PostgresTestImage)
	if postgresVersion == "" {
		postgresVersion = defaultVersion
	}

	postgresPort := "5432/tcp"
	postgresReq := testcontainers.ContainerRequest{
		Image:        fmt.Sprintf("postgres:%s", postgresVersion),
		ExposedPorts: []string{postgresPort},
		Env: map[string]string{
			"POSTGRES_USER":             "cdc_user",
			"POSTGRES_PASSWORD":         "cdc_pass",
			"POSTGRES_DB":               "cdc_db",
			"POSTGRES_HOST_AUTH_METHOD": "trust",
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: postgresReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}

	postgresHost, err := postgresContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres host: %w", err)
	}

	postgresMappedPort, err := postgresContainer.MappedPort(ctx, nat.Port(postgresPort))
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres mapped port: %w", err)
	}

	rabbitReq := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
		WaitingFor: wait.ForLog("Server startup complete").
			WithStartupTimeout(90 * time.Second),
	}

	rabbitContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: rabbitReq,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start rabbitmq container: %w", err)
	}

	rabbitMappedPort, err := rabbitContainer.MappedPort(ctx, "5672")
	if err != nil {
		return nil, fmt.Errorf("failed to get rabbitmq mapped port: %w", err)
	}
	rabbitMgmtMappedPort, err := rabbitContainer.MappedPort(ctx, "15672")
	if err != nil {
		return nil, fmt.Errorf("failed to get rabbitmq mgmt mapped port: %w", err)
	}

	return &TestInfrastructure{
		PostgresHost:      postgresHost,
		PostgresPort:      postgresMappedPort.Port(),
		RabbitHost:        "localhost",
		RabbitPort:        rabbitMappedPort.Port(),
		RabbitMgmtPort:    rabbitMgmtMappedPort.Port(),
		PostgresContainer: postgresContainer,
		RabbitContainer:   rabbitContainer,
	}, nil
}
