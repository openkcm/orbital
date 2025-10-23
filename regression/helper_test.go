package regression_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"

	_ "github.com/lib/pq"

	stdsql "database/sql"

	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
	"github.com/openkcm/orbital/store/sql"
)

const (
	pgUserName = "postgres"
	pgPwd      = "postgres"
	mqUsername = "guest"
	mqPwd      = "guest"
)

// testEnvironment holds all the test containers and information needed for testing.
type testEnvironment struct {
	postgres postgresContainer
	rabbitMQ rabbitMQContainer
}

// postgresContainer holds the Postgres container and its connection details.
type postgresContainer struct {
	container testcontainers.Container
	url       string
}

// rabbitMQContainer holds the RabbitMQ container and its connection URL.
type rabbitMQContainer struct {
	container testcontainers.Container
	url       string
}

// setupEnv creates all necessary containers for testing.
func setupEnv(ctx context.Context, t *testing.T, dbName string) (*testEnvironment, error) {
	t.Helper()

	slog.SetLogLoggerLevel(slog.LevelInfo)

	env := &testEnvironment{
		postgres: postgresContainer{},
		rabbitMQ: rabbitMQContainer{},
	}

	errGrp := errgroup.Group{}

	errGrp.Go(func() error {
		t.Log("Starting Postgres container...")

		pgContainer, err := postgres.Run(ctx, "postgres:17-alpine",
			postgres.WithDatabase(dbName),
			postgres.WithUsername(pgUserName),
			postgres.WithPassword(pgPwd),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			return fmt.Errorf("failed to start postgres container: %w", err)
		}
		env.postgres.container = pgContainer

		host, err := pgContainer.Host(ctx)
		if err != nil {
			return fmt.Errorf("failed to get postgres host: %w", err)
		}

		pgPort, err := pgContainer.MappedPort(ctx, "5432")
		if err != nil {
			return fmt.Errorf("failed to get postgres port: %w", err)
		}

		env.postgres.url = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			host, pgPort.Port(), pgUserName, pgPwd, dbName)

		return nil
	})

	errGrp.Go(func() error {
		t.Log("Starting RabbitMQ container...")

		rabbitContainer, err := rabbitmq.Run(ctx, "rabbitmq:4-management",
			testcontainers.WithWaitStrategy(
				wait.ForAll(
					wait.ForLog("Server startup complete").WithStartupTimeout(60*time.Second),
					wait.ForListeningPort("5672/tcp").WithStartupTimeout(60*time.Second),
				),
			),
			rabbitmq.WithAdminPassword("guest"),
			rabbitmq.WithAdminUsername("guest"),
		)
		if err != nil {
			return fmt.Errorf("failed to start rabbitmq container: %w", err)
		}
		env.rabbitMQ.container = rabbitContainer

		rabbitHost, err := rabbitContainer.Host(ctx)
		if err != nil {
			return fmt.Errorf("error while getting rabbitmq host: %w", err)
		}

		rabbitPort, err := rabbitContainer.MappedPort(ctx, "5672")
		if err != nil {
			return fmt.Errorf("error while getting rabbitmq port: %w", err)
		}

		env.rabbitMQ.url = fmt.Sprintf("amqp://%s:%s@%s/", mqUsername, mqPwd, net.JoinHostPort(rabbitHost, rabbitPort.Port()))

		return nil
	})

	if err := errGrp.Wait(); err != nil {
		return nil, fmt.Errorf("failed to set up test environment: %w", err)
	}

	return env, nil
}

// Cleanup closes the database connection and terminates the containers.
func (env *testEnvironment) Cleanup(ctx context.Context) error {
	err := env.postgres.container.Terminate(ctx)
	if err != nil {
		return fmt.Errorf("failed to terminate Postgres container: %w", err)
	}

	err = env.rabbitMQ.container.Terminate(ctx)
	if err != nil {
		return fmt.Errorf("failed to terminate RabbitMQ container: %w", err)
	}

	return nil
}

// createAMQPClient creates an AMQP client for communication.
func createAMQPClient(ctx context.Context, env *testEnvironment, target, source string) (*amqp.Client, error) {
	connInfo := amqp.ConnectionInfo{
		URL:    env.rabbitMQ.url,
		Target: target,
		Source: source,
	}

	client, err := amqp.NewClient(ctx, codec.JSON{}, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create AMQP client: %w", err)
	}

	return client, nil
}

func createOrbitalStore(ctx context.Context, env *testEnvironment) (*sql.SQL, *stdsql.DB, error) {
	db, err := stdsql.Open("postgres", env.postgres.url)
	if err != nil {
		return nil, nil, err
	}
	store, err := sql.New(ctx, db)

	return store, db, err
}

func envIntOrDefault(k string, d int) int {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return d
	}
	return i
}
