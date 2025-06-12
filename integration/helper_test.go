//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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
	TaskResolver         orbital.TaskResolverFunc
	JobConfirmFunc       orbital.JobConfirmFunc
	TargetClients        map[string]orbital.Initiator
	TerminationEventFunc orbital.TerminationEventFunc
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
		orbital.WithProcessingJobDelay(100 * time.Millisecond),
	}

	manager, err := orbital.NewManager(repo, config.TaskResolver, managerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create manager: %w", err)
	}

	if config.TerminationEventFunc != nil {
		manager.JobTerminationEventFunc = config.TerminationEventFunc
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

// waitForJobStatus polls the job status until it matches the expected status or times out.
func waitForJobStatus(ctx context.Context, manager *orbital.Manager, jobID uuid.UUID, expectedStatus orbital.JobStatus, timeout time.Duration) error {
	startTime := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Since(startTime) > timeout {
				if job, found, err := manager.GetJob(ctx, jobID); err == nil && found {
					return fmt.Errorf("timeout after %v waiting for job status %s, current status: %s, error: %s",
						timeout, expectedStatus, job.Status, job.ErrorMessage)
				}
				return fmt.Errorf("timeout after %v waiting for job status %s", timeout, expectedStatus)
			}

			job, found, err := manager.GetJob(ctx, jobID)
			if err != nil {
				continue
			}
			if !found {
				continue
			}

			if job.Status == expectedStatus {
				return nil
			}

			if job.Status == orbital.JobStatusFailed || job.Status == orbital.JobStatusAborted {
				if expectedStatus == orbital.JobStatusFailed || expectedStatus == orbital.JobStatusAborted {
					return nil
				}
				return fmt.Errorf("job reached terminal state %s (expected %s): %s",
					job.Status, expectedStatus, job.ErrorMessage)
			}
		}
	}
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

// waitForTaskExecution waits for tasks to be executed (sent) with retry logic.
func waitForTaskExecution(ctx context.Context, manager *orbital.Manager, jobID uuid.UUID, expectedSentCount int64, timeout time.Duration) error {
	startTime := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Since(startTime) > timeout {
				tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
					JobID: jobID,
					Limit: 10,
				})
				if err == nil && len(tasks) > 0 {
					var taskInfo []string
					for _, task := range tasks {
						taskInfo = append(taskInfo, fmt.Sprintf("Task %s: SentCount=%d, LastSentAt=%d, Status=%s",
							task.ID, task.SentCount, task.LastSentAt, task.Status))
					}
					return fmt.Errorf("timeout after %v waiting for task execution (expected sent count: %d)\nTask details: %v",
						timeout, expectedSentCount, taskInfo)
				}
				return fmt.Errorf("timeout after %v waiting for task execution (expected sent count: %d)", timeout, expectedSentCount)
			}

			tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
				JobID: jobID,
				Limit: 10,
			})
			if err != nil {
				continue
			}

			if len(tasks) == 0 {
				continue
			}

			allTasksReady := true
			for _, task := range tasks {
				if task.SentCount < expectedSentCount || task.LastSentAt <= 0 {
					allTasksReady = false
					break
				}
			}

			if allTasksReady {
				return nil
			}
		}
	}
}

// waitForJobEventProcessed waits for a job event to be created and processed (isNotified = true).
func waitForJobEventProcessed(ctx context.Context, env *TestEnvironment, jobID uuid.UUID, timeout time.Duration) error {
	startTime := time.Now()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	repo := orbital.NewRepository(env.Store)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Since(startTime) > timeout {
				return fmt.Errorf("timeout after %v waiting for job event to be processed for job %s", timeout, jobID)
			}

			isNotified := true
			_, found, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{
				ID:         jobID,
				IsNotified: &isNotified,
			})
			if err != nil {
				continue
			}
			if found {
				return nil
			}
		}
	}
}
