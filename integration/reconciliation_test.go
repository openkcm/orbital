package integration_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	stdsql "database/sql"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/store/sql"
)

// TestReconciliationFlows runs all reconciliation tests as subtests.
//
//nolint:tparallel
func TestReconciliationFlows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		test func(ctx context.Context, t *testing.T, env *testEnvironment, store *sql.SQL)
	}{
		{"reconcile", testReconcile},
		{"reconcile with multiple tasks", testReconcileWithMultipleTasks},
		{"handle failed task", testTaskFailureScenario},
		{"handle multiple cycles", testMultipleRequestResponseCycles},
	}

	ctx := t.Context()

	env, err := setupTestEnvironment(ctx, t)
	require.NoError(t, err, "failed to set up test environment")
	defer func() {
		err := env.Cleanup(ctx)
		assert.NoError(t, err, "failed to clean up test environment")
	}()

	t.Run("should", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				dbName := strings.ReplaceAll(tt.name, " ", "_")
				store, db := createStore(ctx, t, env, dbName)
				t.Cleanup(func() {
					err := db.Close()
					assert.NoError(t, err, "failed to close test database connection")
				})

				tt.test(ctx, t, env, store)
			})
		}
	})
}

// createStore creates a new test database and returns the store and database connection.
func createStore(ctx context.Context, t *testing.T, env *testEnvironment, name string) (*sql.SQL, *stdsql.DB) {
	t.Helper()

	_, err := env.postgres.db.ExecContext(ctx, "CREATE DATABASE "+name)
	assert.NoError(t, err, "failed to create test database")

	postgresURL := fmt.Sprintf("host=%s port=%s user=postgres password=postgres dbname=%s sslmode=disable",
		env.postgres.host, env.postgres.port, name)

	db, err := stdsql.Open("postgres", postgresURL)
	assert.NoError(t, err, "failed to connect to test database")

	store, err := sql.New(ctx, db)
	assert.NoError(t, err, "failed to create store for test")

	return store, db
}

// testReconcile tests the basic flow: create job → create 1 task → job processing → job done.
func testReconcile(ctx context.Context, t *testing.T, env *testEnvironment, store *sql.SQL) {
	t.Helper()

	const (
		taskType   = "test-task"
		taskTarget = "test-target"
	)

	tasksQueue := fmt.Sprintf("tasks-minimal-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-minimal-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env.rabbitMQ.url, tasksQueue, responsesQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, managerClient)

	operatorClient, err := createAMQPClient(ctx, env.rabbitMQ.url, responsesQueue, tasksQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, operatorClient)

	operatorDone := make(chan struct{})
	var operatorOnce sync.Once

	var jobDoneCalls, jobCanceledCalls, jobFailedCalls int32
	terminationDone := make(chan orbital.Job, 1)

	managerConfig := managerConfig{
		taskResolveFunc: func(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			t.Logf("TaskResolver called for job %s", job.ID)
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("task-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				},
				Done: true,
			}, nil
		},
		jobConfirmFunc: func(_ context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			t.Logf("JobConfirmFunc called for job %s", job.ID)
			return orbital.JobConfirmResult{Done: true}, nil
		},
		targetInitiators: map[string]orbital.Initiator{
			taskTarget: {Client: managerClient},
		},
		jobDoneEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobDoneCalls, 1)
			t.Logf("JobDoneEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobCanceledEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobCanceledCalls, 1)
			t.Logf("JobCanceledEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobFailedEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobFailedCalls, 1)
			t.Logf("JobFailedEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
	}

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := createAndStartManager(ctxCancel, t, store, managerConfig)
	assert.NoError(t, err)

	operatorConfig := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType: func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operatorOnce.Do(func() {
					close(operatorDone)
				})

				t.Logf("Handler called for task %s", req.TaskID)
				assert.Equal(t, taskType, req.Type)
				assert.Equal(t, []byte("task-data"), req.Data)

				time.Sleep(100 * time.Millisecond)

				return orbital.HandlerResponse{
					WorkingState: []byte("task completed"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}

	ctxCancel, cancel = context.WithCancel(ctx)
	defer cancel()
	err = createAndStartOperator(ctxCancel, t, operatorClient, operatorConfig)
	assert.NoError(t, err)

	job, err := createTestJob(ctx, t, manager, "test-job", []byte("job-data"))
	assert.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalJob, found, err := manager.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobStatusDone, finalJob.Status)
	assert.Empty(t, finalJob.ErrorMessage)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusDone, task.Status)
	assert.Equal(t, taskType, task.Type)
	assert.Equal(t, taskTarget, task.Target)
	assert.Equal(t, []byte("task-data"), task.Data)
	assert.Equal(t, []byte("task completed"), task.WorkingState)

	assert.Zero(t, task.ReconcileCount, "task reconcile count should be reset to zero")
	assert.Equal(t, int64(1), task.TotalSentCount)
	assert.Equal(t, int64(1), task.TotalReceivedCount)
	assert.Positive(t, task.LastReconciledAt, "last_reconciled_at should be set")
	assert.Equal(t, int64(0), task.ReconcileAfterSec, "reconcile_after_sec should be 0 for completed task")

	assert.Equal(t, int32(1), atomic.LoadInt32(&jobDoneCalls), "job done event function should be called exactly once")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobCanceledCalls), "job canceled event function should not be called")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobFailedCalls), "job failed event function should not be called")
}

// testReconcileWithMultipleTasks tests a job with multiple tasks.
func testReconcileWithMultipleTasks(ctx context.Context, t *testing.T, env *testEnvironment, store *sql.SQL) {
	t.Helper()

	const (
		taskType1   = "task-type-1"
		taskType2   = "task-type-2"
		taskTarget1 = "target-1"
		taskTarget2 = "target-2"
	)

	queue1 := fmt.Sprintf("queue-1-multi-%d", time.Now().UnixNano())
	response1 := fmt.Sprintf("response-1-multi-%d", time.Now().UnixNano())
	queue2 := fmt.Sprintf("queue-2-multi-%d", time.Now().UnixNano())
	response2 := fmt.Sprintf("response-2-multi-%d", time.Now().UnixNano())

	managerClient1, err := createAMQPClient(ctx, env.rabbitMQ.url, queue1, response1)
	assert.NoError(t, err)
	defer closeClient(ctx, t, managerClient1)
	operatorClient1, err := createAMQPClient(ctx, env.rabbitMQ.url, response1, queue1)
	assert.NoError(t, err)
	defer closeClient(ctx, t, operatorClient1)

	managerClient2, err := createAMQPClient(ctx, env.rabbitMQ.url, queue2, response2)
	assert.NoError(t, err)
	defer closeClient(ctx, t, managerClient2)
	operatorClient2, err := createAMQPClient(ctx, env.rabbitMQ.url, response2, queue2)
	assert.NoError(t, err)
	defer closeClient(ctx, t, operatorClient2)

	var operator1Once, operator2Once sync.Once
	operator1Done := make(chan struct{})
	operator2Done := make(chan struct{})
	allOperatorsDone := make(chan struct{})

	var jobDoneCalls, jobCanceledCalls, jobFailedCalls int32
	terminationDone := make(chan orbital.Job, 1)

	managerConfig := managerConfig{
		taskResolveFunc: func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
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
		jobConfirmFunc: func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Done: true}, nil
		},
		targetInitiators: map[string]orbital.Initiator{
			taskTarget1: {Client: managerClient1},
			taskTarget2: {Client: managerClient2},
		},
		jobDoneEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobDoneCalls, 1)
			t.Logf("JobDoneEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobCanceledEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobCanceledCalls, 1)
			t.Logf("JobCanceledEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobFailedEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobFailedCalls, 1)
			t.Logf("JobFailedEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
	}

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := createAndStartManager(ctxCancel, t, store, managerConfig)
	assert.NoError(t, err)

	operatorConfig1 := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType1: func(_ context.Context, _ orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operator1Once.Do(func() {
					close(operator1Done)
				})

				time.Sleep(100 * time.Millisecond)

				return orbital.HandlerResponse{
					WorkingState: []byte("task 1 done"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	ctxCancel, cancel = context.WithCancel(ctx)
	defer cancel()
	err = createAndStartOperator(ctxCancel, t, operatorClient1, operatorConfig1)
	assert.NoError(t, err)

	operatorConfig2 := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType2: func(_ context.Context, _ orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operator2Once.Do(func() {
					close(operator2Done)
				})

				time.Sleep(100 * time.Millisecond)

				return orbital.HandlerResponse{
					WorkingState: []byte("task 2 done"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	ctxCancel, cancel = context.WithCancel(ctx)
	defer cancel()
	err = createAndStartOperator(ctxCancel, t, operatorClient2, operatorConfig2)
	assert.NoError(t, err)

	go func() {
		<-operator1Done
		<-operator2Done
		close(allOperatorsDone)
	}()

	job, err := createTestJob(ctx, t, manager, "multi-task-job", []byte("job-data"))
	assert.NoError(t, err)

	select {
	case <-allOperatorsDone:
		t.Log("All operators completed")
	case <-time.After(30 * time.Second):
		t.Fatal("Not all operators completed within timeout")
	}

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for multi-task job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	job, found, err := manager.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobStatusDone, job.Status)
	assert.Empty(t, job.ErrorMessage)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	assert.NoError(t, err)
	assert.Len(t, tasks, 2)

	tasksByType := make(map[string]orbital.Task)
	for _, task := range tasks {
		tasksByType[task.Type] = task
	}

	task1, ok := tasksByType[taskType1]
	assert.True(t, ok)
	assert.Equal(t, orbital.TaskStatusDone, task1.Status)
	assert.Equal(t, []byte("data-1"), task1.Data)
	assert.Equal(t, []byte("task 1 done"), task1.WorkingState)
	assert.Zero(t, task1.ReconcileCount)
	assert.Equal(t, int64(1), task1.TotalSentCount)
	assert.Equal(t, int64(1), task1.TotalReceivedCount)

	task2, ok := tasksByType[taskType2]
	assert.True(t, ok)
	assert.Equal(t, orbital.TaskStatusDone, task2.Status)
	assert.Equal(t, []byte("data-2"), task2.Data)
	assert.Equal(t, []byte("task 2 done"), task2.WorkingState)
	assert.Zero(t, task2.ReconcileCount)
	assert.Equal(t, int64(1), task2.TotalSentCount)
	assert.Equal(t, int64(1), task2.TotalReceivedCount)

	assert.Equal(t, int32(1), atomic.LoadInt32(&jobDoneCalls), "job done event function should be called exactly once")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobCanceledCalls), "job canceled event function should not be called")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobFailedCalls), "job failed event function should not be called")
}

// testTaskFailureScenario tests how the system handles task failures.
func testTaskFailureScenario(ctx context.Context, t *testing.T, env *testEnvironment, store *sql.SQL) {
	t.Helper()

	const (
		taskType   = "failing-task"
		taskTarget = "target"
	)

	tasksQueue := fmt.Sprintf("tasks-failure-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-failure-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env.rabbitMQ.url, tasksQueue, responsesQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, managerClient)
	operatorClient, err := createAMQPClient(ctx, env.rabbitMQ.url, responsesQueue, tasksQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, operatorClient)

	operatorDone := make(chan struct{})
	var operatorOnce sync.Once

	var jobDoneCalls, jobCanceledCalls, jobFailedCalls int32
	terminationDone := make(chan orbital.Job, 1)

	managerConfig := managerConfig{
		taskResolveFunc: func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
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
		jobConfirmFunc: func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Done: true}, nil
		},
		targetInitiators: map[string]orbital.Initiator{
			taskTarget: {Client: managerClient},
		},
		jobDoneEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobDoneCalls, 1)
			t.Logf("JobDoneEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobCanceledEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobCanceledCalls, 1)
			t.Logf("JobCanceledEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobFailedEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobFailedCalls, 1)
			t.Logf("JobFailedEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
	}

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := createAndStartManager(ctxCancel, t, store, managerConfig)
	assert.NoError(t, err)

	operatorConfig := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType: func(_ context.Context, _ orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operatorOnce.Do(func() {
					close(operatorDone)
				})

				time.Sleep(100 * time.Millisecond)

				return orbital.HandlerResponse{
					WorkingState: []byte("task failed"),
					Result:       orbital.ResultFailed,
				}, nil
			},
		},
	}
	ctxCancel, cancel = context.WithCancel(ctx)
	defer cancel()
	err = createAndStartOperator(ctxCancel, t, operatorClient, operatorConfig)
	assert.NoError(t, err)

	job, err := createTestJob(ctx, t, manager, "failing-job", []byte("job-data"))
	assert.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed with failure as expected")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for failing job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusFailed, terminatedJob.Status)
		assert.Equal(t, orbital.ErrMsgFailedTasks, terminatedJob.ErrorMessage)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalJob, found, err := manager.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobStatusFailed, finalJob.Status)
	assert.Equal(t, orbital.ErrMsgFailedTasks, finalJob.ErrorMessage)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusFailed, task.Status)
	assert.Equal(t, []byte("task failed"), task.WorkingState)
	assert.Zero(t, task.ReconcileCount)
	assert.Equal(t, int64(1), task.TotalSentCount)
	assert.Equal(t, int64(1), task.TotalReceivedCount)

	assert.Equal(t, int32(0), atomic.LoadInt32(&jobDoneCalls), "job done event function should not be called")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobCanceledCalls), "job canceled event function should not be called")
	assert.Equal(t, int32(1), atomic.LoadInt32(&jobFailedCalls), "job failed event function should be called exactly once")
}

// testMultipleRequestResponseCycles tests tasks that complete after multiple request/response cycles.
func testMultipleRequestResponseCycles(ctx context.Context, t *testing.T, env *testEnvironment, store *sql.SQL) {
	t.Helper()

	const (
		taskType   = "multi-cycle-task"
		taskTarget = "target"
	)

	var handlerCalls int32
	operatorDone := make(chan struct{})
	expectedCycles := int64(3)

	var jobDoneCalls, jobCanceledCalls, jobFailedCalls int32
	terminationDone := make(chan orbital.Job, 1)

	tasksQueue := fmt.Sprintf("tasks-multi-cycle-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-multi-cycle-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env.rabbitMQ.url, tasksQueue, responsesQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, managerClient)
	operatorClient, err := createAMQPClient(ctx, env.rabbitMQ.url, responsesQueue, tasksQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, operatorClient)

	managerConfig := managerConfig{
		taskResolveFunc: func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
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
		jobConfirmFunc: func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{Done: true}, nil
		},
		targetInitiators: map[string]orbital.Initiator{
			taskTarget: {Client: managerClient},
		},
		jobDoneEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobDoneCalls, 1)
			t.Logf("JobDoneEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobCanceledEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobCanceledCalls, 1)
			t.Logf("JobCanceledEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobFailedEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobFailedCalls, 1)
			t.Logf("JobFailedEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
	}

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := createAndStartManager(ctxCancel, t, store, managerConfig)
	assert.NoError(t, err)

	workingStates := make([][]byte, 0, expectedCycles)
	var mu sync.Mutex

	operatorConfig := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType: func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				mu.Lock()
				defer mu.Unlock()

				calls := atomic.AddInt32(&handlerCalls, 1)
				t.Logf("Handler call #%d for task %s", calls, req.TaskID)

				time.Sleep(100 * time.Millisecond)

				if int64(calls) < expectedCycles {
					state := fmt.Appendf(nil, "cycle %d in progress", calls)
					workingStates = append(workingStates, state)
					return orbital.HandlerResponse{
						WorkingState:      state,
						Result:            orbital.ResultProcessing,
						ReconcileAfterSec: 1,
					}, nil
				}

				finalState := []byte("all cycles completed")
				workingStates = append(workingStates, finalState)
				close(operatorDone)

				return orbital.HandlerResponse{
					WorkingState: finalState,
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}
	ctxCancel, cancel = context.WithCancel(ctx)
	defer cancel()
	err = createAndStartOperator(ctxCancel, t, operatorClient, operatorConfig)
	assert.NoError(t, err)

	job, err := createTestJob(ctx, t, manager, "multi-cycle-job", []byte("job-data"))
	assert.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed after multiple cycles")
	case <-time.After(45 * time.Second):
		t.Fatal("Operator did not complete within timeout")
	}

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for multi-cycle job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalCalls := atomic.LoadInt32(&handlerCalls)
	assert.Equal(t, expectedCycles, int64(finalCalls), "handler should be called exactly %d times", expectedCycles)

	job, found, err := manager.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobStatusDone, job.Status)
	assert.Empty(t, job.ErrorMessage, "job should not have an error message")

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusDone, task.Status)
	assert.Equal(t, []byte("all cycles completed"), task.WorkingState)
	assert.Zero(t, task.ReconcileCount)
	assert.Equal(t, expectedCycles, task.TotalSentCount, "task should have sent %d times", expectedCycles)
	assert.Equal(t, expectedCycles, task.TotalReceivedCount, "task should have received %d times", expectedCycles)
	assert.Positive(t, task.LastReconciledAt, "last_reconciled_at should be set")
	assert.Equal(t, int64(0), task.ReconcileAfterSec, "reconcile_after_sec should be 0 for completed task")

	assert.Len(t, workingStates, int(expectedCycles), "should have tracked all working states")

	assert.Equal(t, int32(1), atomic.LoadInt32(&jobDoneCalls), "job done event function should be called exactly once")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobCanceledCalls), "job canceled event function should not be called")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobFailedCalls), "job failed event function should not be called")
}
