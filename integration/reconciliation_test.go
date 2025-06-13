//go:build integration
// +build integration

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openkcm/orbital"
)

// TestReconciliationFlows runs all reconciliation tests as subtests.
func TestReconciliationFlows(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T, ctx context.Context, env *TestEnvironment)
	}{
		{"Reconcile", testReconcile},
		{"Reconcile With MultipleTasks", testReconcileWithMultipleTasks},
		{"TaskFailureScenario", testTaskFailureScenario},
		{"ReconcileAfterSec", testReconcileAfterSec},
		{"MultipleRequestResponseCycles", testMultipleRequestResponseCycles},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTestWithEnvironment(t, tt.test)
		})
	}
}

// testReconcile tests the basic flow: create job → create 1 task → job processing → job done.
func testReconcile(t *testing.T, ctx context.Context, env *TestEnvironment) {
	const (
		taskType   = "test-task"
		taskTarget = "test-target"
	)

	tasksQueue := fmt.Sprintf("tasks-minimal-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-minimal-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)

	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

	operatorDone := make(chan struct{})
	var operatorOnce sync.Once

	var terminationCalls int32
	terminationDone := make(chan orbital.Job, 1)

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
				Done: true,
			}, nil
		},
		JobConfirmFunc: func(ctx context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			t.Logf("JobConfirmFunc called for job %s", job.ID)
			return orbital.JobConfirmResult{Confirmed: true}, nil
		},
		TargetClients: map[string]orbital.Initiator{
			taskTarget: managerClient,
		},
		TerminationEventFunc: func(ctx context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&terminationCalls, 1)
			t.Logf("TerminationEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)

			select {
			case terminationDone <- job:
			default:
			}
			return nil
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
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

	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	job, err := createTestJob(t, ctx, manager, "test-job", []byte("job-data"))
	require.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	err = waitForJobStatus(ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	err = waitForTaskExecution(ctx, manager, job.ID, 1, 15*time.Second)
	require.NoError(t, err)

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalJob, found, err := manager.GetJob(ctx, job.ID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, orbital.JobStatusDone, finalJob.Status)
	assert.Empty(t, finalJob.ErrorMessage)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusDone, task.Status)
	assert.Equal(t, taskType, task.Type)
	assert.Equal(t, taskTarget, task.Target)
	assert.Equal(t, []byte("task-data"), task.Data)
	assert.Equal(t, []byte("task completed"), task.WorkingState)

	assert.Equal(t, int64(1), task.SentCount, "task should be sent exactly once")
	assert.Positive(t, task.LastSentAt, "last_sent_at should be set")
	assert.Equal(t, int64(0), task.ReconcileAfterSec, "reconcile_after_sec should be 0 for completed task")

	assert.Equal(t, int32(1), atomic.LoadInt32(&terminationCalls), "termination event function should be called exactly once")
}

// testReconcileWithMultipleTasks tests a job with multiple tasks.
func testReconcileWithMultipleTasks(t *testing.T, ctx context.Context, env *TestEnvironment) {
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

	client1, err := createAMQPClient(ctx, env, env.RabbitMQURL, queue1, response1)
	require.NoError(t, err)
	operatorClient1, err := createAMQPClient(ctx, env, env.RabbitMQURL, response1, queue1)
	require.NoError(t, err)

	client2, err := createAMQPClient(ctx, env, env.RabbitMQURL, queue2, response2)
	require.NoError(t, err)
	operatorClient2, err := createAMQPClient(ctx, env, env.RabbitMQURL, response2, queue2)
	require.NoError(t, err)

	var operator1Once, operator2Once sync.Once
	operator1Done := make(chan struct{})
	operator2Done := make(chan struct{})
	allOperatorsDone := make(chan struct{})

	var terminationCalls int32
	terminationDone := make(chan orbital.Job, 1)

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
		TerminationEventFunc: func(ctx context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&terminationCalls, 1)
			t.Logf("TerminationEventFunc called for multi-task job %s (call #%d), status: %s", job.ID, calls, job.Status)

			select {
			case terminationDone <- job:
			default:
			}
			return nil
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	operatorConfig1 := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType1: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
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
	_, err = createOperator(t, ctx, operatorClient1, operatorConfig1)
	require.NoError(t, err)

	operatorConfig2 := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType2: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
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
	_, err = createOperator(t, ctx, operatorClient2, operatorConfig2)
	require.NoError(t, err)

	go func() {
		<-operator1Done
		<-operator2Done
		close(allOperatorsDone)
	}()

	job, err := createTestJob(t, ctx, manager, "multi-task-job", []byte("job-data"))
	require.NoError(t, err)

	select {
	case <-allOperatorsDone:
		t.Log("All operators completed")
	case <-time.After(30 * time.Second):
		t.Fatal("Not all operators completed within timeout")
	}

	err = waitForJobStatus(ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	err = waitForTaskExecution(ctx, manager, job.ID, 1, 20*time.Second)
	require.NoError(t, err)

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for multi-task job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err)
	assert.Len(t, tasks, 2)

	tasksByType := make(map[string]orbital.Task)
	for _, task := range tasks {
		tasksByType[task.Type] = task
	}

	task1, ok := tasksByType[taskType1]
	require.True(t, ok)
	assert.Equal(t, orbital.TaskStatusDone, task1.Status)
	assert.Equal(t, []byte("data-1"), task1.Data)
	assert.Equal(t, []byte("task 1 done"), task1.WorkingState)
	assert.Equal(t, int64(1), task1.SentCount)

	task2, ok := tasksByType[taskType2]
	require.True(t, ok)
	assert.Equal(t, orbital.TaskStatusDone, task2.Status)
	assert.Equal(t, []byte("data-2"), task2.Data)
	assert.Equal(t, []byte("task 2 done"), task2.WorkingState)
	assert.Equal(t, int64(1), task2.SentCount)

	assert.Equal(t, int32(1), atomic.LoadInt32(&terminationCalls), "termination event function should be called exactly once")
}

// testTaskFailureScenario tests how the system handles task failures.
func testTaskFailureScenario(t *testing.T, ctx context.Context, env *TestEnvironment) {
	const (
		taskType   = "failing-task"
		taskTarget = "target"
	)

	tasksQueue := fmt.Sprintf("tasks-failure-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-failure-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

	operatorDone := make(chan struct{})
	var operatorOnce sync.Once

	var terminationCalls int32
	terminationDone := make(chan orbital.Job, 1)

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
		TerminationEventFunc: func(ctx context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&terminationCalls, 1)
			t.Logf("TerminationEventFunc called for failing job %s (call #%d), status: %s", job.ID, calls, job.Status)

			select {
			case terminationDone <- job:
			default:
			}
			return nil
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
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
	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	job, err := createTestJob(t, ctx, manager, "failing-job", []byte("job-data"))
	require.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed with failure as expected")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	err = waitForJobStatus(ctx, manager, job.ID, orbital.JobStatusFailed, 30*time.Second)
	require.NoError(t, err)

	err = waitForTaskExecution(ctx, manager, job.ID, 1, 15*time.Second)
	require.NoError(t, err)

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
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, orbital.JobStatusFailed, finalJob.Status)
	assert.Equal(t, orbital.ErrMsgFailedTasks, finalJob.ErrorMessage)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusFailed, task.Status)
	assert.Equal(t, []byte("task failed"), task.WorkingState)
	assert.Equal(t, int64(1), task.SentCount)

	assert.Equal(t, int32(1), atomic.LoadInt32(&terminationCalls), "termination event function should be called exactly once")
}

// testReconcileAfterSec tests the reconciliation delay functionality.
func testReconcileAfterSec(t *testing.T, ctx context.Context, env *TestEnvironment) {
	const (
		taskType   = "reconcile-task"
		taskTarget = "target"
	)

	var handlerCalls int32
	handlerCallTimes := make([]time.Time, 0, 3)
	var mu sync.Mutex
	operatorDone := make(chan struct{})

	var terminationCalls int32
	terminationDone := make(chan orbital.Job, 1)

	tasksQueue := fmt.Sprintf("tasks-reconcile-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-reconcile-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

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
		TerminationEventFunc: func(ctx context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&terminationCalls, 1)
			t.Logf("TerminationEventFunc called for reconcile job %s (call #%d), status: %s", job.ID, calls, job.Status)

			select {
			case terminationDone <- job:
			default:
			}
			return nil
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	var taskID atomic.Value

	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				mu.Lock()
				defer mu.Unlock()

				if taskID.Load() == nil {
					taskID.Store(req.TaskID)
				}

				calls := atomic.AddInt32(&handlerCalls, 1)
				handlerCallTimes = append(handlerCallTimes, time.Now())
				t.Logf("Handler call #%d for task %s at %v", calls, req.TaskID, time.Now())

				time.Sleep(100 * time.Millisecond)

				var reconcileAfterSec int64

				if calls == 1 {
					reconcileAfterSec = time.Now().Unix() + 2
					return orbital.HandlerResponse{
						WorkingState:      []byte("in progress"),
						Result:            orbital.ResultProcessing,
						ReconcileAfterSec: 2,
					}, nil
				}

				if calls == 2 {
					if time.Now().Unix() < reconcileAfterSec {
						t.Errorf("reconciliation should happen after at least 2 seconds, but was called too early")
					}
					close(operatorDone)
					return orbital.HandlerResponse{
						WorkingState: []byte("completed"),
						Result:       orbital.ResultDone,
					}, nil
				}

				t.Errorf("Unexpected handler call #%d", calls)
				return orbital.HandlerResponse{}, errors.New("unexpected call")
			},
		},
	}
	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	job, err := createTestJob(t, ctx, manager, "reconcile-job", []byte("job-data"))
	require.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed after reconciliation delay")
	case <-time.After(45 * time.Second):
		t.Fatal("Operator second call did not happen within timeout")
	}

	err = waitForJobStatus(ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	err = waitForTaskExecution(ctx, manager, job.ID, 2, 20*time.Second)
	require.NoError(t, err)

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for reconcile job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalCalls := atomic.LoadInt32(&handlerCalls)
	assert.Equal(t, int32(2), finalCalls, "handler should be called exactly twice")

	if tid := taskID.Load(); tid != nil {
		tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
			JobID: job.ID,
			Limit: 10,
		})
		require.NoError(t, err)
		require.Len(t, tasks, 1)

		task := tasks[0]
		assert.Equal(t, tid.(uuid.UUID), task.ID)
		assert.Equal(t, orbital.TaskStatusDone, task.Status)
		assert.Equal(t, int64(2), task.SentCount, "task should be sent exactly twice")
		assert.Equal(t, []byte("completed"), task.WorkingState)
		assert.Equal(t, int64(0), task.ReconcileAfterSec, "reconcile_after_sec should be 0 for completed task")
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&terminationCalls), "termination event function should be called exactly once")
}

// testMultipleRequestResponseCycles tests tasks that complete after multiple request/response cycles.
func testMultipleRequestResponseCycles(t *testing.T, ctx context.Context, env *TestEnvironment) {
	const (
		taskType   = "multi-cycle-task"
		taskTarget = "target"
	)

	var handlerCalls int32
	operatorDone := make(chan struct{})
	expectedCycles := int64(3)

	var terminationCalls int32
	terminationDone := make(chan orbital.Job, 1)

	tasksQueue := fmt.Sprintf("tasks-multi-cycle-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-multi-cycle-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, tasksQueue, responsesQueue)
	require.NoError(t, err)
	operatorClient, err := createAMQPClient(ctx, env, env.RabbitMQURL, responsesQueue, tasksQueue)
	require.NoError(t, err)

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
		TerminationEventFunc: func(ctx context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&terminationCalls, 1)
			t.Logf("TerminationEventFunc called for multi-cycle job %s (call #%d), status: %s", job.ID, calls, job.Status)

			select {
			case terminationDone <- job:
			default:
			}
			return nil
		},
	}

	manager, err := createManager(t, ctx, env, managerConfig)
	require.NoError(t, err)

	workingStates := make([][]byte, 0, expectedCycles)
	var mu sync.Mutex

	operatorConfig := OperatorConfig{
		Handlers: map[string]orbital.Handler{
			taskType: func(ctx context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				mu.Lock()
				defer mu.Unlock()

				calls := atomic.AddInt32(&handlerCalls, 1)
				t.Logf("Handler call #%d for task %s", calls, req.TaskID)

				time.Sleep(100 * time.Millisecond)

				if int64(calls) < expectedCycles {
					state := []byte(fmt.Sprintf("cycle %d in progress", calls))
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
	_, err = createOperator(t, ctx, operatorClient, operatorConfig)
	require.NoError(t, err)

	job, err := createTestJob(t, ctx, manager, "multi-cycle-job", []byte("job-data"))
	require.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed after multiple cycles")
	case <-time.After(45 * time.Second):
		t.Fatal("Operator did not complete within timeout")
	}

	err = waitForJobStatus(ctx, manager, job.ID, orbital.JobStatusDone, 30*time.Second)
	require.NoError(t, err)

	err = waitForTaskExecution(ctx, manager, job.ID, expectedCycles, 25*time.Second)
	require.NoError(t, err)

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

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusDone, task.Status)
	assert.Equal(t, []byte("all cycles completed"), task.WorkingState)
	assert.Equal(t, expectedCycles, task.SentCount, "task should be sent %d times", expectedCycles)
	assert.Positive(t, task.LastSentAt, "last_sent_at should be set")
	assert.Equal(t, int64(0), task.ReconcileAfterSec, "reconcile_after_sec should be 0 for completed task")

	assert.Len(t, workingStates, int(expectedCycles), "should have tracked all working states")

	assert.Equal(t, int32(1), atomic.LoadInt32(&terminationCalls), "termination event function should be called exactly once")
}
