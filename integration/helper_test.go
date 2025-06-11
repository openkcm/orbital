package integration

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
	"github.com/openkcm/orbital/store/sql"
)

// TestEnvironment holds all the test containers and clients
type TestEnvironment struct {
	PostgresContainer testcontainers.Container
	RabbitMQContainer testcontainers.Container
	PostgresURL       string
	RabbitMQURL       string
	DB                *stdsql.DB
	Store             *sql.SQL
	AMQPClients       []*amqp.AMQP // Track all AMQP clients for cleanup
	mu                sync.Mutex   // Protect concurrent access to AMQPClients
}

// setupTestEnvironment creates all necessary containers for testing
func setupTestEnvironment(t *testing.T, ctx context.Context) (*TestEnvironment, error) {
	env := &TestEnvironment{
		AMQPClients: make([]*amqp.AMQP, 0),
	}

	// Setup PostgreSQL
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:17-alpine"),
		postgres.WithDatabase("orbital"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
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

	// Setup RabbitMQ with better wait strategy
	rabbitContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:4-management"),
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

	// Wait for RabbitMQ to be fully ready
	err = waitForRabbitMQReady(ctx, env.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("RabbitMQ not ready: %w", err)
	}

	// Initialize database connection
	db, err := stdsql.Open("postgres", env.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	env.DB = db

	// Initialize store
	store, err := sql.New(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize store: %w", err)
	}
	env.Store = store

	return env, nil
}

// Cleanup tears down all containers
func (env *TestEnvironment) Cleanup(ctx context.Context) {
	// Close all AMQP connections first
	env.mu.Lock()
	for _, client := range env.AMQPClients {
		if client != nil {
			// Note: AMQP client doesn't have a Close method in the current implementation
			// This is a limitation of the current AMQP client design
		}
	}
	env.AMQPClients = nil
	env.mu.Unlock()

	// Close database connection
	if env.DB != nil {
		env.DB.Close()
	}

	// Give a moment for connections to close gracefully
	time.Sleep(100 * time.Millisecond)

	// Terminate containers
	if env.RabbitMQContainer != nil {
		env.RabbitMQContainer.Terminate(ctx)
	}
	if env.PostgresContainer != nil {
		env.PostgresContainer.Terminate(ctx)
	}
}

// ManagerConfig holds configuration for the manager
type ManagerConfig struct {
	TaskResolver   orbital.TaskResolverFunc
	JobConfirmFunc orbital.JobConfirmFunc
	TargetClients  map[string]orbital.Initiator
}

// createManager creates and starts a manager instance
func createManager(t *testing.T, ctx context.Context, env *TestEnvironment, config ManagerConfig) (*orbital.Manager, error) {
	repo := orbital.NewRepository(env.Store)

	managerOpts := []orbital.ManagerOptsFunc{
		orbital.WithJobConfirmFunc(config.JobConfirmFunc),
		orbital.WithTargetClients(config.TargetClients),
		orbital.WithProcessingJobDelay(100 * time.Millisecond),
	}

	manager, err := orbital.NewManager(repo, config.TaskResolver, managerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create manager: %w", err)
	}

	// Configure worker intervals for faster testing
	manager.Config.ConfirmJobWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.CreateTasksWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.ReconcileWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.NotifyWorkerConfig.ExecInterval = 100 * time.Millisecond
	manager.Config.ConfirmJobDelay = 100 * time.Millisecond

	// Start the manager
	err = manager.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start manager: %w", err)
	}

	return manager, nil
}

// OperatorConfig holds configuration for the operator
type OperatorConfig struct {
	Handlers map[string]orbital.Handler
}

// createOperator creates and starts an operator instance
func createOperator(t *testing.T, ctx context.Context, responder orbital.Responder, config OperatorConfig) (*orbital.Operator, error) {
	operator, err := orbital.NewOperator(responder)
	if err != nil {
		return nil, fmt.Errorf("failed to create operator: %w", err)
	}

	// Register handlers
	for taskType, handler := range config.Handlers {
		err = operator.RegisterHandler(taskType, handler)
		if err != nil {
			return nil, fmt.Errorf("failed to register handler for %s: %w", taskType, err)
		}
	}

	// Start listening
	go operator.ListenAndRespond(ctx)

	return operator, nil
}

// createAMQPClient creates an AMQP client for communication
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

	// Track the client for cleanup
	env.mu.Lock()
	env.AMQPClients = append(env.AMQPClients, client)
	env.mu.Unlock()

	return client, nil
}

// waitForJobStatus polls the job status until it matches the expected status or times out
func waitForJobStatus(t *testing.T, ctx context.Context, manager *orbital.Manager, jobID uuid.UUID, expectedStatus orbital.JobStatus, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				// Get final job state for debugging
				if job, found, err := manager.GetJob(ctx, jobID); err == nil && found {
					return fmt.Errorf("timeout waiting for job status %s, current status: %s, error: %s",
						expectedStatus, job.Status, job.ErrorMessage)
				}
				return fmt.Errorf("timeout waiting for job status %s", expectedStatus)
			}

			job, found, err := manager.GetJob(ctx, jobID)
			if err != nil {
				t.Logf("Error getting job %s: %v", jobID, err)
				continue
			}
			if !found {
				t.Logf("Job %s not found yet", jobID)
				continue
			}

			t.Logf("Job %s status: %s", jobID, job.Status)

			if job.Status == expectedStatus {
				return nil
			}

			// Check for failure states
			if job.Status == orbital.JobStatusFailed || job.Status == orbital.JobStatusAborted {
				if expectedStatus == orbital.JobStatusFailed || expectedStatus == orbital.JobStatusAborted {
					return nil // This is expected
				}
				return fmt.Errorf("job failed with status %s: %s", job.Status, job.ErrorMessage)
			}
		}
	}
}

// waitForRabbitMQReady waits for RabbitMQ to be ready to accept connections
func waitForRabbitMQReady(ctx context.Context, rabbitURL string) error {
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Try to create a connection
		testClient, err := createAMQPClient(ctx, &TestEnvironment{AMQPClients: make([]*amqp.AMQP, 0)}, rabbitURL, "test-queue", "test-source")
		if err == nil && testClient != nil {
			return nil // RabbitMQ is ready
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue trying
		}
	}

	return fmt.Errorf("RabbitMQ not ready within %v", timeout)
}

// createTestJob creates a job with test data
func createTestJob(t *testing.T, ctx context.Context, manager *orbital.Manager, jobType string, data []byte) (orbital.Job, error) {
	job := orbital.NewJob(jobType, data)
	createdJob, err := manager.PrepareJob(ctx, job)
	if err != nil {
		return orbital.Job{}, fmt.Errorf("failed to prepare job: %w", err)
	}

	t.Logf("Created job %s with type %s", createdJob.ID, jobType)
	return createdJob, nil
}
