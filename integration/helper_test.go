//go:build integration
// +build integration

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/lib/pq"

	stdsql "database/sql"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
	"github.com/openkcm/orbital/store/sql"
)

// TestEnvironment holds all the test containers and clients.
type TestEnvironment struct {
	PostgresContainer testcontainers.Container
	RabbitMQContainer testcontainers.Container
	PostgresURL       string
	RabbitMQURL       string
	DB                *stdsql.DB
	Store             *sql.SQL
	AMQPClients       []*amqp.AMQP       // Track all AMQP clients for cleanup
	Managers          []*orbital.Manager // Track managers for proper shutdown
	mu                sync.Mutex         // Protect concurrent access to slices
}

// ManagerConfig holds configuration for the manager.
type ManagerConfig struct {
	TaskResolveFunc        orbital.TaskResolveFunc
	JobConfirmFunc         orbital.JobConfirmFunc
	TargetClients          map[string]orbital.Initiator
	JobTerminatedEventFunc orbital.JobTerminatedEventFunc
}

// OperatorConfig holds configuration for the operator.
type OperatorConfig struct {
	Handlers map[string]orbital.Handler
}

// setupTestEnvironment creates all necessary containers for testing.
func setupTestEnvironment(t *testing.T, ctx context.Context) (*TestEnvironment, error) {
	t.Helper()

	env := &TestEnvironment{
		AMQPClients: make([]*amqp.AMQP, 0),
		Managers:    make([]*orbital.Manager, 0),
	}

	pgContainer, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("orbital"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start postgres container: %w", err)
	}
	env.PostgresContainer = pgContainer

	pgHost, err := pgContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres host: %w", err)
	}

	pgPort, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		return nil, fmt.Errorf("failed to get postgres port: %w", err)
	}

	env.PostgresURL = fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=orbital sslmode=disable",
		pgHost, pgPort.Port())

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
		return nil, fmt.Errorf("failed to start rabbitmq container: %w", err)
	}
	env.RabbitMQContainer = rabbitContainer

	rabbitHost, err := rabbitContainer.Host(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get rabbitmq host: %w", err)
	}

	rabbitPort, err := rabbitContainer.MappedPort(ctx, "5672")
	if err != nil {
		return nil, fmt.Errorf("failed to get rabbitmq port: %w", err)
	}

	env.RabbitMQURL = fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitHost, rabbitPort.Port())

	err = waitForRabbitMQReady(ctx, env.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("RabbitMQ not ready: %w", err)
	}

	db, err := stdsql.Open("postgres", env.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	env.DB = db

	store, err := sql.New(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize store: %w", err)
	}
	env.Store = store

	return env, nil
}

// Cleanup tears down all containers.
func (env *TestEnvironment) Cleanup(ctx context.Context) {
	env.mu.Lock()

	env.Managers = nil

	env.AMQPClients = nil
	env.mu.Unlock()

	if env.DB != nil {
		env.DB.Close()
	}

	time.Sleep(500 * time.Millisecond)

	if env.RabbitMQContainer != nil {
		env.RabbitMQContainer.Terminate(ctx)
	}
	if env.PostgresContainer != nil {
		env.PostgresContainer.Terminate(ctx)
	}
}

// createManager creates and starts a manager instance.
func createManager(t *testing.T, ctx context.Context, env *TestEnvironment, config ManagerConfig) (*orbital.Manager, error) {
	t.Helper()

	repo := orbital.NewRepository(env.Store)

	managerOpts := []orbital.ManagerOptsFunc{
		orbital.WithJobConfirmFunc(config.JobConfirmFunc),
		orbital.WithTargetClients(config.TargetClients),
		orbital.WithJobTerminatedEventFunc(config.JobTerminatedEventFunc),
	}

	manager, err := orbital.NewManager(repo, config.TaskResolveFunc, managerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create manager: %w", err)
	}

	manager.Config.ConfirmJobWorkerConfig.ExecInterval = 200 * time.Millisecond
	manager.Config.CreateTasksWorkerConfig.ExecInterval = 200 * time.Millisecond
	manager.Config.ReconcileWorkerConfig.ExecInterval = 300 * time.Millisecond
	manager.Config.NotifyWorkerConfig.ExecInterval = 200 * time.Millisecond
	manager.Config.ConfirmJobDelay = 100 * time.Millisecond

	err = manager.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start manager: %w", err)
	}

	env.mu.Lock()
	env.Managers = append(env.Managers, manager)
	env.mu.Unlock()

	return manager, nil
}

// createOperator creates and starts an operator instance.
func createOperator(t *testing.T, ctx context.Context, responder orbital.Responder, config OperatorConfig) (*orbital.Operator, error) {
	t.Helper()

	operator, err := orbital.NewOperator(responder)
	if err != nil {
		return nil, fmt.Errorf("failed to create operator: %w", err)
	}

	for taskType, handler := range config.Handlers {
		err = operator.RegisterHandler(taskType, handler)
		if err != nil {
			return nil, fmt.Errorf("failed to register handler for %s: %w", taskType, err)
		}
	}

	go operator.ListenAndRespond(ctx)

	return operator, nil
}

// createAMQPClient creates an AMQP client for communication.
func createAMQPClient(ctx context.Context, env *TestEnvironment, rabbitURL, target, source string) (*amqp.AMQP, error) {
	connInfo := amqp.ConnectionInfo{
		URL:    rabbitURL,
		Target: target,
		Source: source,
	}

	client, err := amqp.NewClient(ctx, codec.JSON{}, connInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to create AMQP client: %w", err)
	}

	env.mu.Lock()
	env.AMQPClients = append(env.AMQPClients, client)
	env.mu.Unlock()

	return client, nil
}

// runTestWithEnvironment runs a test function with proper environment setup and cleanup.
func runTestWithEnvironment(t *testing.T, testFunc func(t *testing.T, ctx context.Context, env *TestEnvironment)) {
	t.Helper()

	envCtx, envCancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer envCancel()

	env, err := setupTestEnvironment(t, envCtx)
	require.NoError(t, err)

	testCtx, testCancel := context.WithTimeout(envCtx, 2*time.Minute)

	defer func() {
		testCancel()
		time.Sleep(300 * time.Millisecond)
		env.Cleanup(envCtx)
	}()

	testFunc(t, testCtx, env)
}

// waitForRabbitMQReady waits for RabbitMQ to be ready to accept connections.
func waitForRabbitMQReady(ctx context.Context, rabbitURL string) error {
	timeout := 30 * time.Second
	startTime := time.Now()

	for time.Since(startTime) < timeout {
		testClient, err := createAMQPClient(ctx, &TestEnvironment{AMQPClients: make([]*amqp.AMQP, 0)}, rabbitURL, "test-queue", "test-source")
		if err == nil && testClient != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return fmt.Errorf("RabbitMQ not ready within %v", timeout)
}

// createTestJob creates a job with test data.
func createTestJob(t *testing.T, ctx context.Context, manager *orbital.Manager, jobType string, data []byte) (orbital.Job, error) {
	job := orbital.NewJob(jobType, data)
	createdJob, err := manager.PrepareJob(ctx, job)
	if err != nil {
		return orbital.Job{}, fmt.Errorf("failed to prepare job: %w", err)
	}

	t.Logf("Created job %s with type %s", createdJob.ID, jobType)
	return createdJob, nil
}
