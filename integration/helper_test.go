package integration_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"

	_ "github.com/lib/pq"

	stdsql "database/sql"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
	"github.com/openkcm/orbital/store/sql"
)

// testEnvironment holds all the test containers and information needed for testing.
type testEnvironment struct {
	postgres postgresContainer
	rabbitMQ rabbitMQContainer
}

// postgresContainer holds the Postgres container and its connection details.
type postgresContainer struct {
	container testcontainers.Container
	host      string
	port      string
	db        *stdsql.DB
}

// rabbitMQContainer holds the RabbitMQ container and its connection URL.
type rabbitMQContainer struct {
	container testcontainers.Container
	url       string
}

// managerConfig holds configuration for the manager.
type managerConfig struct {
	taskResolveFunc       orbital.TaskResolveFunc
	jobConfirmFunc        orbital.JobConfirmFunc
	managerTargets        map[string]orbital.ManagerTarget
	jobDoneEventFunc      orbital.JobTerminatedEventFunc
	jobCanceledEventFunc  orbital.JobTerminatedEventFunc
	jobFailedEventFunc    orbital.JobTerminatedEventFunc
	maxReconcileCount     int64
	backoffMaxIntervalSec int64
}

// operatorConfig holds configuration for the operator.
type operatorConfig struct {
	handlers map[string]orbital.Handler
}

// setupTestEnvironment creates all necessary containers for testing.
func setupTestEnvironment(ctx context.Context, t *testing.T) (*testEnvironment, error) {
	t.Helper()

	env := &testEnvironment{
		postgres: postgresContainer{},
		rabbitMQ: rabbitMQContainer{},
	}

	g := errgroup.Group{}

	g.Go(func() error {
		t.Log("Starting Postgres container...")

		pgContainer, err := postgres.Run(ctx, "postgres:17-alpine",
			postgres.WithDatabase("orbital"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			return fmt.Errorf("failed to start postgres container: %w", err)
		}
		env.postgres.container = pgContainer

		pgHost, err := pgContainer.Host(ctx)
		if err != nil {
			return fmt.Errorf("failed to get postgres host: %w", err)
		}
		env.postgres.host = pgHost

		pgPort, err := pgContainer.MappedPort(ctx, "5432")
		if err != nil {
			return fmt.Errorf("failed to get postgres port: %w", err)
		}
		env.postgres.port = pgPort.Port()

		postgresURL := fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=orbital sslmode=disable",
			pgHost, pgPort.Port())

		db, err := stdsql.Open("postgres", postgresURL)
		if err != nil {
			return fmt.Errorf("failed to connect to postgres: %w", err)
		}
		env.postgres.db = db

		return nil
	})

	g.Go(func() error {
		t.Log("Starting RabbitMQ container...")

		rabbitContainer, err := rabbitmq.Run(ctx, "rabbitmq:4-management",
			rabbitmq.WithAdminUsername("guest"),
			rabbitmq.WithAdminPassword("guest"),
			testcontainers.WithWaitStrategy(
				wait.ForAll(
					wait.ForLog("Server startup complete").WithStartupTimeout(60*time.Second),
					wait.ForListeningPort("5672/tcp").WithStartupTimeout(60*time.Second),
				),
			),
		)
		if err != nil {
			return fmt.Errorf("failed to start rabbitmq container: %w", err)
		}
		env.rabbitMQ.container = rabbitContainer

		rabbitHost, err := rabbitContainer.Host(ctx)
		if err != nil {
			return fmt.Errorf("failed to get rabbitmq host: %w", err)
		}

		rabbitPort, err := rabbitContainer.MappedPort(ctx, "5672")
		if err != nil {
			return fmt.Errorf("failed to get rabbitmq port: %w", err)
		}

		env.rabbitMQ.url = fmt.Sprintf("amqp://guest:guest@%s/", net.JoinHostPort(rabbitHost, rabbitPort.Port()))

		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed to set up test environment: %w", err)
	}

	return env, nil
}

// Cleanup closes the database connection and terminates the containers.
func (env *testEnvironment) Cleanup(ctx context.Context) error {
	err := env.postgres.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close Postgres DB connection: %w", err)
	}

	err = env.postgres.container.Terminate(ctx)
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
func createAMQPClient(ctx context.Context, rabbitURL, target, source string) (*amqp.Client, error) {
	connInfo := amqp.ConnectionInfo{
		URL:    rabbitURL,
		Target: target,
		Source: source,
	}

	client, err := amqp.NewClient(ctx, codec.JSON{}, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create AMQP client: %w", err)
	}

	return client, nil
}

// closeClient closes the AMQP client connection.
func closeClient(ctx context.Context, t *testing.T, client *amqp.Client) {
	t.Helper()

	err := client.Close(ctx)
	assert.NoError(t, err, "failed to close AMQP client")
}

// createAndStartManager creates and starts a manager instance.
func createAndStartManager(ctx context.Context, t *testing.T, store *sql.SQL, config managerConfig) (*orbital.Manager, error) {
	t.Helper()

	repo := orbital.NewRepository(store)

	managerOpts := []orbital.ManagerOptsFunc{
		orbital.WithJobConfirmFunc(config.jobConfirmFunc),
		orbital.WithTargets(config.managerTargets),
		orbital.WithJobDoneEventFunc(config.jobDoneEventFunc),
		orbital.WithJobCanceledEventFunc(config.jobCanceledEventFunc),
		orbital.WithJobFailedEventFunc(config.jobFailedEventFunc),
	}

	manager, err := orbital.NewManager(repo, config.taskResolveFunc, managerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create manager: %w", err)
	}

	manager.Config.ConfirmJobWorkerConfig.ExecInterval = 200 * time.Millisecond
	manager.Config.CreateTasksWorkerConfig.ExecInterval = 200 * time.Millisecond
	manager.Config.ReconcileWorkerConfig.ExecInterval = 300 * time.Millisecond
	manager.Config.NotifyWorkerConfig.ExecInterval = 200 * time.Millisecond
	manager.Config.ConfirmJobAfter = 100 * time.Millisecond
	if config.backoffMaxIntervalSec != 0 {
		manager.Config.BackoffMaxIntervalSec = config.backoffMaxIntervalSec
	}
	if config.maxReconcileCount != 0 {
		manager.Config.MaxReconcileCount = config.maxReconcileCount
	}

	err = manager.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start manager: %w", err)
	}

	return manager, nil
}

// createAndStartOperator creates and starts an operator instance.
func createAndStartOperator(ctx context.Context, t *testing.T, client orbital.Responder, config operatorConfig) error {
	t.Helper()

	operator, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
	if err != nil {
		return fmt.Errorf("failed to create operator: %w", err)
	}

	return addHandlerAndListen(ctx, t, config, operator)
}

func createAndStartOperatorWithTarget(ctx context.Context, t *testing.T, target orbital.OperatorTarget, config operatorConfig) error {
	t.Helper()

	operator, err := orbital.NewOperator(target)
	if err != nil {
		return fmt.Errorf("failed to create operator: %w", err)
	}

	return addHandlerAndListen(ctx, t, config, operator)
}

func addHandlerAndListen(ctx context.Context, t *testing.T, config operatorConfig, operator *orbital.Operator) error {
	t.Helper()

	for taskType, handler := range config.handlers {
		err := operator.RegisterHandler(taskType, handler)
		if err != nil {
			return fmt.Errorf("failed to register handler for %s: %w", taskType, err)
		}
	}

	go operator.ListenAndRespond(ctx)

	return nil
}

// createTestJob creates a job with test data.
func createTestJob(ctx context.Context, t *testing.T, manager *orbital.Manager, jobType string, data []byte) (orbital.Job, error) {
	t.Helper()

	job := orbital.NewJob(jobType, data)
	createdJob, err := manager.PrepareJob(ctx, job)
	if err != nil {
		return orbital.Job{}, fmt.Errorf("failed to prepare job: %w", err)
	}

	t.Logf("Created job %s with type %s", createdJob.ID, jobType)
	return createdJob, nil
}
