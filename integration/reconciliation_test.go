package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openkcm/orbital"
)

// TestMinimalFlow tests the basic flow: create job → create 1 task → job processing → job done
func TestMinimalFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Setup test environment
	env, err := setupTestEnvironment(t, ctx)
	require.NoError(t, err, "failed to setup test environment")
	defer func() {
		// Cancel context first to stop all operations
		cancel()
		// Give time for graceful shutdown
		time.Sleep(500 * time.Millisecond)
		// Then cleanup containers
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		env.Cleanup(cleanupCtx)
	}()

	// Define task type and target
	const (
		taskType   = "test-task"
		taskTarget = "test-target"
	)

	// Create unique queue names for this test to avoid conflicts
	tasksQueue := "tasks-queue-minimal"
	responsesQueue := "responses-queue-minimal"

	// Create AMQP clients for manager and operator
	// Manager sends to tasks queue, receives from responses queue
	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err, "failed to create manager AMQP client")

	// Operator receives from tasks queue, sends to responses queue
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err, "failed to create operator AMQP client")

	// Create a channel to track operator execution (use once to avoid multiple calls)
	operatorDone := make(chan struct{})
	var operatorOnce sync.Once

	// Configure and create manager
	managerConfig := ManagerConfig{
		TaskResolver: func(ctx context.Context, job orbital.Job, cursor orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			t.Logf("TaskResolver called for job %s", job.ID)
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("task-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				},
				Done: true, // All tasks resolved
			}, nil
		},
		JobConfirmFunc: func(ctx context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			t.Logf("JobConfirmFunc called for job %s", job.ID)
			return orbital.JobConfirmResult{
				Confirmed: true,
			}, nil
		},
		TargetClients: map[string]orbital.Initiator{
			taskTarget: managerClient,
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err, "failed to create manager")

	// Configure and create operator
	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				// Use sync.Once to ensure we only signal completion once, even if called multiple times
				operatorOnce.Do(func() {
					close(operatorDone)
				})

				t.Logf("Handler called for task %s", req.TaskID)
				assert.Equal(t, taskType, req.Type)
				assert.Equal(t, []byte("task-data"), req.Data)

				// Simulate some work
				select {
				case <-ctx.Done():
					return orbital.HandlerResponse{}, ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}

				return orbital.HandlerResponse{
					WorkingState: []byte("task completed"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}

	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err, "failed to create operator")

	// Create a test job
	job, err := createTestJob(t, ctx, manager, "test-job", []byte("job-data"))
	require.NoError(t, err, "failed to create job")

	// Wait for operator to be called (with timeout)
	select {
	case <-operatorDone:
		t.Log("Operator completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	// Wait for job to complete
	err = waitForJobStatus(t, ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err, "job did not complete in time")

	// Verify final job state
	finalJob, found, err := manager.GetJob(ctx, job.ID)
	require.NoError(t, err, "failed to get final job")
	require.True(t, found, "job not found")
	assert.Equal(t, orbital.JobStatusDone, finalJob.Status)
	assert.Empty(t, finalJob.ErrorMessage)

	// Verify task was created and completed
	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err, "failed to list tasks")
	assert.Len(t, tasks, 1, "expected exactly one task")

	if len(tasks) > 0 {
		task := tasks[0]
		assert.Equal(t, orbital.TaskStatusDone, task.Status)
		assert.Equal(t, taskType, task.Type)
		assert.Equal(t, taskTarget, task.Target)
		assert.Equal(t, []byte("task-data"), task.Data)
		assert.Equal(t, []byte("task completed"), task.WorkingState)
	}

	t.Log("Test completed successfully!")
}

// TestMultipleTasksFlow tests a job with multiple tasks
func TestMultipleTasksFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Setup test environment
	env, err := setupTestEnvironment(t, ctx)
	require.NoError(t, err, "failed to setup test environment")
	defer func() {
		cancel()
		time.Sleep(500 * time.Millisecond)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		env.Cleanup(cleanupCtx)
	}()

	const (
		taskType1   = "task-type-1"
		taskType2   = "task-type-2"
		taskTarget1 = "target-1"
		taskTarget2 = "target-2"
	)

	// Create unique queue names
	queue1 := "queue-1-multi"
	response1 := "response-1-multi"
	queue2 := "queue-2-multi"
	response2 := "response-2-multi"

	// Create AMQP clients
	client1, err := createAMQPClient(ctx, env, env.RabbitMQURL, queue1, response1)
	require.NoError(t, err)
	operatorClient1, err := createAMQPClient(ctx, env, env.RabbitMQURL, response1, queue1)
	require.NoError(t, err)

	client2, err := createAMQPClient(ctx, env, env.RabbitMQURL, queue2, response2)
	require.NoError(t, err)
	operatorClient2, err := createAMQPClient(ctx, env, env.RabbitMQURL, response2, queue2)
	require.NoError(t, err)

	// Track operator executions with sync.Once to handle potential retries
	var operator1Once, operator2Once sync.Once
	operator1Done := make(chan struct{})
	operator2Done := make(chan struct{})

	// Channel to track when both are done
	allOperatorsDone := make(chan struct{})

	// Configure manager with multiple tasks
	managerConfig := ManagerConfig{
		TaskResolver: func(ctx context.Context, job orbital.Job, cursor orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("data-1"),
						Type:   taskType1,
						Target: taskTarget1,
					},
					{
						Data:   []byte("data-2"),
						Type:   taskType2,
						Target: taskTarget2,
					},
				},
				Done: true,
			}, nil
		},
		JobConfirmFunc: func(ctx context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Confirmed: true}, nil
		},
		TargetClients: map[string]orbital.Initiator{
			taskTarget1: client1,
			taskTarget2: client2,
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	// Create operators for each task type
	operatorConfig1 := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType1: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operator1Once.Do(func() {
					close(operator1Done)
				})
				t.Logf("Handler 1 called for task %s", req.TaskID)
				return orbital.HandlerResponse{
					WorkingState: []byte("task 1 done"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	_, err = createOperator(t, ctx, operatorClient1, operatorConfig1)
	require.NoError(t, err)

	operatorConfig2 := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType2: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operator2Once.Do(func() {
					close(operator2Done)
				})
				t.Logf("Handler 2 called for task %s", req.TaskID)
				return orbital.HandlerResponse{
					WorkingState: []byte("task 2 done"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	_, err = createOperator(t, ctx, operatorClient2, operatorConfig2)
	require.NoError(t, err)

	// Start a goroutine to signal when both operators are done
	go func() {
		<-operator1Done
		<-operator2Done
		close(allOperatorsDone)
	}()

	// Create job
	job, err := createTestJob(t, ctx, manager, "multi-task-job", []byte("job-data"))
	require.NoError(t, err)

	// Wait for all operators to complete
	select {
	case <-allOperatorsDone:
		t.Log("All operators completed")
	case <-time.After(30 * time.Second):
		t.Fatal("Not all operators completed within timeout")
	}

	// Wait for completion
	err = waitForJobStatus(t, ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	// Verify all tasks completed
	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err)
	assert.Len(t, tasks, 2)

	for _, task := range tasks {
		assert.Equal(t, orbital.TaskStatusDone, task.Status)
	}
}

// TestTaskFailureScenario tests how the system handles task failures
func TestTaskFailureScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env, err := setupTestEnvironment(t, ctx)
	require.NoError(t, err)
	defer func() {
		cancel()
		time.Sleep(500 * time.Millisecond)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		env.Cleanup(cleanupCtx)
	}()

	const (
		taskType   = "failing-task"
		taskTarget = "target"
	)

	// Create unique queue names
	tasksQueue := "tasks-failure"
	responsesQueue := "responses-failure"

	// Create AMQP clients
	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

	// Track operator execution
	operatorDone := make(chan struct{})
	var operatorOnce sync.Once

	// Configure manager
	managerConfig := ManagerConfig{
		TaskResolver: func(ctx context.Context, job orbital.Job, cursor orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("fail-task-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				},
				Done: true,
			}, nil
		},
		JobConfirmFunc: func(ctx context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Confirmed: true}, nil
		},
		TargetClients: map[string]orbital.Initiator{
			taskTarget: managerClient,
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	// Create operator that fails the task
	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operatorOnce.Do(func() {
					close(operatorDone)
				})
				t.Logf("Handler returning failure for task %s", req.TaskID)
				return orbital.HandlerResponse{
					WorkingState: []byte("task failed"),
					Result:       orbital.ResultFailed,
				}, nil
			},
		},
	}
	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	// Create job
	job, err := createTestJob(t, ctx, manager, "failing-job", []byte("job-data"))
	require.NoError(t, err)

	// Wait for operator to be called
	select {
	case <-operatorDone:
		t.Log("Operator completed with failure as expected")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	// Wait for job to fail
	err = waitForJobStatus(t, ctx, manager, job.ID, orbital.JobStatusFailed, 30*time.Second)
	require.NoError(t, err)

	// Verify job failed with correct error
	finalJob, found, err := manager.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, orbital.JobStatusFailed, finalJob.Status)
	assert.Equal(t, orbital.ErrMsgFailedTasks, finalJob.ErrorMessage)
}

// TestReconcileAfterSec tests the reconciliation delay functionality
func TestReconcileAfterSec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env, err := setupTestEnvironment(t, ctx)
	require.NoError(t, err)
	defer func() {
		cancel()
		time.Sleep(500 * time.Millisecond)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		env.Cleanup(cleanupCtx)
	}()

	const (
		taskType   = "reconcile-task"
		taskTarget = "target"
	)

	// Track handler invocations
	var handlerCalls int32
	var firstCallTime, secondCallTime time.Time
	var mu sync.Mutex
	operatorDone := make(chan struct{})

	// Create unique queue names
	tasksQueue := "tasks-reconcile"
	responsesQueue := "responses-reconcile"

	// Create AMQP clients
	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

	// Configure manager
	managerConfig := ManagerConfig{
		TaskResolver: func(ctx context.Context, job orbital.Job, cursor orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("reconcile-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				},
				Done: true,
			}, nil
		},
		JobConfirmFunc: func(ctx context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Confirmed: true}, nil
		},
		TargetClients: map[string]orbital.Initiator{
			taskTarget: managerClient,
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	// Create operator that requests reconciliation
	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				mu.Lock()
				defer mu.Unlock()

				calls := atomic.AddInt32(&handlerCalls, 1)
				t.Logf("Handler call #%d for task %s", calls, req.TaskID)

				if calls == 1 {
					firstCallTime = time.Now()
					// Request reconciliation after 2 seconds
					return orbital.HandlerResponse{
						WorkingState:      []byte("in progress"),
						Result:            orbital.ResultProcessing,
						ReconcileAfterSec: 2,
					}, nil
				}

				secondCallTime = time.Now()
				close(operatorDone)
				// Complete the task
				return orbital.HandlerResponse{
					WorkingState: []byte("completed"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	// Create job
	job, err := createTestJob(t, ctx, manager, "reconcile-job", []byte("job-data"))
	require.NoError(t, err)

	// Wait for second handler call
	select {
	case <-operatorDone:
		t.Log("Operator completed after reconciliation delay")
	case <-time.After(60 * time.Second):
		t.Fatal("Operator second call did not happen within timeout")
	}

	// Wait for job completion
	err = waitForJobStatus(t, ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	// Verify handler was called twice
	finalCalls := atomic.LoadInt32(&handlerCalls)
	assert.GreaterOrEqual(t, finalCalls, int32(2), "handler should be called at least twice")

	// Verify reconciliation delay
	if finalCalls >= 2 && !secondCallTime.IsZero() && !firstCallTime.IsZero() {
		delay := secondCallTime.Sub(firstCallTime)
		t.Logf("Reconciliation delay: %v", delay)
		assert.True(t, delay >= 2*time.Second, "reconciliation should happen after at least 2 seconds")
	}
}

// TestMultipleTaskRequestResponseCycles tests tasks that complete after multiple request/response cycles
func TestMultipleTaskRequestResponseCycles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	env, err := setupTestEnvironment(t, ctx)
	require.NoError(t, err)
	defer func() {
		cancel()
		time.Sleep(500 * time.Millisecond)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		env.Cleanup(cleanupCtx)
	}()

	const (
		taskType   = "multi-cycle-task"
		taskTarget = "target"
	)

	// Track handler invocations
	var handlerCalls int32
	operatorDone := make(chan struct{})
	expectedCycles := int32(3) // Task will take 3 cycles to complete

	// Create unique queue names
	tasksQueue := "tasks-multi-cycle"
	responsesQueue := "responses-multi-cycle"

	// Create AMQP clients
	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

	// Configure manager
	managerConfig := ManagerConfig{
		TaskResolver: func(ctx context.Context, job orbital.Job, cursor orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("multi-cycle-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				},
				Done: true,
			}, nil
		},
		JobConfirmFunc: func(ctx context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Confirmed: true}, nil
		},
		TargetClients: map[string]orbital.Initiator{
			taskTarget: managerClient,
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	// Create operator that processes through multiple cycles
	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				calls := atomic.AddInt32(&handlerCalls, 1)
				t.Logf("Handler call #%d for task %s", calls, req.TaskID)

				if calls < expectedCycles {
					// Continue processing - not done yet
					return orbital.HandlerResponse{
						WorkingState:      []byte(fmt.Sprintf("cycle %d in progress", calls)),
						Result:            orbital.ResultProcessing,
						ReconcileAfterSec: 1, // Quick reconciliation for testing
					}, nil
				}

				// Final cycle - complete the task
				select {
				case operatorDone <- struct{}{}:
				default:
				}

				return orbital.HandlerResponse{
					WorkingState: []byte("all cycles completed"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	// Create job
	job, err := createTestJob(t, ctx, manager, "multi-cycle-job", []byte("job-data"))
	require.NoError(t, err)

	// Wait for final handler call
	select {
	case <-operatorDone:
		t.Log("Operator completed after multiple cycles")
	case <-time.After(60 * time.Second):
		t.Fatal("Operator did not complete within timeout")
	}

	// Wait for job completion
	err = waitForJobStatus(t, ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	// Verify handler was called the expected number of times
	finalCalls := atomic.LoadInt32(&handlerCalls)
	assert.GreaterOrEqual(t, finalCalls, expectedCycles, "handler should be called at least %d times", expectedCycles)

	// Verify final task state
	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusDone, task.Status)
	assert.Equal(t, []byte("all cycles completed"), task.WorkingState)
}
