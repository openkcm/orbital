package orbital_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
	"github.com/openkcm/orbital/internal/retry"
)

var errSendFailed = errors.New("send failed")

type (
	// failedClient simulates a failed send operation.
	failedClient struct{}
	// successfulClient simulates a successful send operation.
	successfulClient struct{}
)

func (f *failedClient) SendTaskRequest(_ context.Context, _ orbital.TaskRequest) error {
	return errSendFailed
}

func (f *failedClient) ReceiveTaskResponse(_ context.Context) (orbital.TaskResponse, error) {
	return orbital.TaskResponse{}, nil
}

func (s *successfulClient) SendTaskRequest(_ context.Context, _ orbital.TaskRequest) error {
	return nil
}

func (s *successfulClient) ReceiveTaskResponse(_ context.Context) (orbital.TaskResponse, error) {
	return orbital.TaskResponse{}, nil
}

func TestReconcile(t *testing.T) {
	t.Run("should just return if there is no job ready for reconciliation", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
		)
		assert.NoError(t, err)

		// when
		err = orbital.Reconcile(subj)(ctx)

		// then
		assert.NoError(t, err)
	})

	t.Run("job status should", func(t *testing.T) {
		// given
		tt := []struct {
			name                  string
			jobStatus             orbital.JobStatus
			taskStatus            orbital.TaskStatus
			taskReconcileAfterSec int64
			expJobStatus          orbital.JobStatus
			isEventRecorded       bool
		}{
			{
				name:         "change to PROCESSING if the job was in READY status before",
				jobStatus:    orbital.JobStatusReady,
				taskStatus:   orbital.TaskStatusProcessing,
				expJobStatus: orbital.JobStatusProcessing,
			},
			{
				name:         "remain PROCESSING if a task is in PROCESSING status",
				jobStatus:    orbital.JobStatusProcessing,
				taskStatus:   orbital.TaskStatusProcessing,
				expJobStatus: orbital.JobStatusProcessing,
			},
			{
				name:         "remain PROCESSING if a task is in CREATED status",
				jobStatus:    orbital.JobStatusProcessing,
				taskStatus:   orbital.TaskStatusCreated,
				expJobStatus: orbital.JobStatusProcessing,
			},
			{
				name:            "change to FAILED if a task is in FAILED status",
				jobStatus:       orbital.JobStatusProcessing,
				taskStatus:      orbital.TaskStatusFailed,
				expJobStatus:    orbital.JobStatusFailed,
				isEventRecorded: true,
			},
			{
				name:            "change to DONE if all tasks are in DONE status",
				jobStatus:       orbital.JobStatusProcessing,
				taskStatus:      orbital.TaskStatusDone,
				expJobStatus:    orbital.JobStatusDone,
				isEventRecorded: true,
			},
			{
				name:                  "remain PROCESSING if a task is not reconcile ready",
				jobStatus:             orbital.JobStatusProcessing,
				taskStatus:            orbital.TaskStatusProcessing,
				taskReconcileAfterSec: 10,
				expJobStatus:          orbital.JobStatusProcessing,
			},
		}

		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				subj, err := orbital.NewManager(repo,
					mockTaskResolveFunc(),
					orbital.WithJobFailedEventFunc(mockTerminatedFunc()),
					orbital.WithJobDoneEventFunc(mockTerminatedFunc()),
				)
				assert.NoError(t, err)

				job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: tc.jobStatus,
				})
				assert.NoError(t, err)

				_, err = orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
					{
						JobID:             job.ID,
						Status:            tc.taskStatus,
						ReconcileAfterSec: tc.taskReconcileAfterSec,
					},
				})
				assert.NoError(t, err)

				// when
				err = orbital.Reconcile(subj)(ctx)

				// then
				assert.NoError(t, err)
				actJob, ok, err := orbital.GetRepoJob(repo)(ctx, job.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, tc.expJobStatus, actJob.Status)
				if tc.expJobStatus == orbital.JobStatusFailed {
					assert.Equal(t, orbital.ErrMsgFailedTasks, actJob.ErrorMessage)
				}
				if tc.isEventRecorded {
					actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: actJob.ID})
					assert.NoError(t, err)
					assert.True(t, ok)
					assert.Equal(t, actJob.ID, actEvent.ID)
				}
			})
		}
	})

	t.Run("should send task request and update task", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusProcessing,
		})
		assert.NoError(t, err)

		expType := "task-type"
		expData := []byte("task-data")
		expWorkingState := []byte("initial-state")
		expTarget := "target-1"
		expETag := "etag"
		ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
			{
				JobID:            job.ID,
				Type:             expType,
				Data:             expData,
				WorkingState:     expWorkingState,
				ETag:             expETag,
				Status:           orbital.TaskStatusCreated,
				Target:           expTarget,
				LastReconciledAt: 0,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)

		client, err := embedded.NewClient(func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			assert.Equal(t, ids[0], req.TaskID)
			assert.Equal(t, expType, req.Type)
			assert.Equal(t, expData, req.Data)
			assert.Equal(t, expWorkingState, req.WorkingState)
			assert.Equal(t, expETag, req.ETag)
			return orbital.TaskResponse{}, nil
		})
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithTargetClients(map[string]orbital.Initiator{
				expTarget: client,
			}),
		)
		assert.NoError(t, err)

		// when
		err = orbital.Reconcile(subj)(ctx)
		assert.NoError(t, err)
		_, err = client.ReceiveTaskResponse(ctx)
		assert.NoError(t, err)

		// then
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, orbital.TaskStatusProcessing, actTask.Status)
		assert.NotZero(t, actTask.LastReconciledAt)
		assert.Equal(t, int64(1), actTask.ReconcileCount)
		assert.Equal(t, int64(1), actTask.TotalSentCount)
		assert.Equal(t, int64(20), actTask.ReconcileAfterSec)
	})

	t.Run("reconcile_after_sec", func(t *testing.T) {
		tts := []struct {
			name   string
			client orbital.Initiator
		}{
			{
				name:   "should be incremented exponentially for each successful sent",
				client: &successfulClient{},
			},
			{
				name:   "should be incremented exponentially for unsuccessful sent",
				client: &failedClient{},
			},
		}
		for _, tc := range tts {
			t.Run(tc.name, func(t *testing.T) {
				// given
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: orbital.JobStatusProcessing,
				})
				assert.NoError(t, err)

				expTarget := "target-1"
				taskSentCount := int64(8) // simulate that the task was sent 8 times before
				ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
					{
						JobID:            job.ID,
						Type:             "type",
						Data:             []byte(""),
						ETag:             "etag",
						Status:           orbital.TaskStatusCreated,
						Target:           expTarget,
						ReconcileCount:   taskSentCount,
						LastReconciledAt: 0,
					},
				})
				assert.NoError(t, err)
				assert.Len(t, ids, 1)

				subj, err := orbital.NewManager(repo,
					mockTaskResolveFunc(),
					orbital.WithTargetClients(
						map[string]orbital.Initiator{
							expTarget: tc.client,
						},
					),
				)
				assert.NoError(t, err)

				// when
				err = orbital.Reconcile(subj)(ctx)

				// then
				assert.NoError(t, err)
				actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
				assert.NoError(t, err)
				assert.True(t, ok)
				expReconcileAfterSec := retry.ExponentialBackoffInterval(
					subj.Config.BackoffBaseIntervalSec,
					subj.Config.BackoffMaxIntervalSec,
					taskSentCount+1,
				)
				assert.Equal(t, expReconcileAfterSec, actTask.ReconcileAfterSec)
			})
		}
	})

	t.Run("should send the same ETag to the operator if the operator does not respond within ReconcileAfterSec", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusProcessing,
		})
		assert.NoError(t, err)
		expETag := "etag-initial"

		expTarget := "target-1"
		ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
			{
				JobID:            job.ID,
				Type:             "type",
				Data:             []byte("task-data"),
				WorkingState:     []byte("state"),
				Status:           orbital.TaskStatusCreated,
				Target:           "target-1",
				ETag:             expETag,
				LastReconciledAt: 0,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)

		operatorCalled := 0
		client, err := embedded.NewClient(func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			operatorCalled++
			assert.Equal(t, expETag, req.ETag)
			return orbital.TaskResponse{}, nil
		})
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithTargetClients(map[string]orbital.Initiator{
				expTarget: client,
			}),
		)
		assert.NoError(t, err)

		// when
		// calling Reconcile twice to simulate sending of the taskrequest
		for range 2 {
			err = orbital.Reconcile(subj)(ctx)
			assert.NoError(t, err)
			_, err = client.ReceiveTaskResponse(ctx)
			assert.NoError(t, err)
		}

		// then
		// check that the operator was called twice, once for each Reconcile call
		assert.Equal(t, 2, operatorCalled)
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, int64(2), actTask.ReconcileCount)
		assert.Equal(t, int64(2), actTask.TotalSentCount)
		assert.Equal(t, expETag, actTask.ETag)
	})

	t.Run("should update task if task request fails", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusProcessing,
		})
		assert.NoError(t, err)

		target := "target"
		ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
			{
				JobID:  job.ID,
				Status: orbital.TaskStatusCreated,
				Target: target,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)

		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithTargetClients(map[string]orbital.Initiator{
				target: &failedClient{},
			}),
		)
		assert.NoError(t, err)

		// when
		err = orbital.Reconcile(subj)(ctx)

		// then
		assert.NoError(t, err)
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Greater(t, actTask.LastReconciledAt, int64(1))
		assert.Equal(t, int64(1), actTask.ReconcileCount)
		assert.Equal(t, orbital.TaskStatusProcessing, actTask.Status)
		expReconcileAfterSec := retry.ExponentialBackoffInterval(subj.Config.BackoffBaseIntervalSec,
			subj.Config.BackoffMaxIntervalSec, 1)
		assert.Equal(t, expReconcileAfterSec, actTask.ReconcileAfterSec)
		assert.Equal(t, int64(0), actTask.TotalSentCount, "should not update total_sent_count on failed send")
	})

	t.Run("should not send task request if max sent count is reached", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusProcessing,
		})
		assert.NoError(t, err)

		beforeETag := uuid.NewString()
		ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
			{
				JobID:  job.ID,
				Status: orbital.TaskStatusCreated,
				ETag:   beforeETag,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
		)
		assert.NoError(t, err)

		subj.Config.MaxReconcileCount = 0 // set max sent count to 1 to simulate the case

		// when
		err = orbital.Reconcile(subj)(ctx)

		// then
		assert.NoError(t, err)
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.NotEqual(t, beforeETag, actTask.ETag)
		assert.Equal(t, orbital.TaskStatusFailed, actTask.Status)
	})

	t.Run("total_sent_count", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tcs := []struct {
			name                   string
			client                 orbital.Initiator
			isSentCountIncremented bool
		}{
			{
				name:                   "should be incremented after each successful send",
				client:                 &successfulClient{},
				isSentCountIncremented: true,
			},
			{
				name:                   "should not be incremented after a failed send",
				client:                 &failedClient{},
				isSentCountIncremented: false,
			},
		}

		for _, tc := range tcs {
			t.Run(tc.name, func(t *testing.T) {
				job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: orbital.JobStatusProcessing,
				})
				assert.NoError(t, err)

				target := "target-1"
				ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
					{
						JobID:  job.ID,
						Type:   "task-type",
						ETag:   "etag",
						Status: orbital.TaskStatusCreated,
						Target: target,
					},
				})
				assert.NoError(t, err)
				assert.Len(t, ids, 1)

				assert.NoError(t, err)

				subj, err := orbital.NewManager(repo,
					mockTaskResolveFunc(),
					orbital.WithTargetClients(map[string]orbital.Initiator{
						target: tc.client,
					}),
				)
				assert.NoError(t, err)

				for i := range 4 {
					// when
					err = orbital.Reconcile(subj)(ctx)

					// then
					assert.NoError(t, err)
					actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
					assert.NoError(t, err)
					assert.True(t, ok)
					if tc.isSentCountIncremented {
						assert.Equal(t, int64(i+1), actTask.TotalSentCount)
					} else {
						assert.Equal(t, int64(0), actTask.TotalSentCount)
					}
				}
			})
		}
	})
}

//nolint:gocognit
func TestProcessResponse(t *testing.T) {
	prevLastReconciledAt := int64(1000) // Simulate a previous last reconciled time
	t.Run("process response", func(t *testing.T) {
		tests := []struct {
			name                   string
			initialStoredTask      *orbital.Task
			taskResponse           func(taskID uuid.UUID) orbital.TaskResponse
			expTaskStatus          orbital.TaskStatus
			expWorkingState        []byte
			expTotalReceivedCount  int64
			expReconcileAfter      int64
			expReconcileCount      int64
			isLastReconcileAtEqual bool
			expErrorMessage        string
		}{
			{
				name: "successful response update",
				initialStoredTask: &orbital.Task{
					JobID:              uuid.New(),
					Type:               "task-type",
					Data:               []byte("task-data"),
					WorkingState:       []byte("initial-state"),
					Status:             orbital.TaskStatusProcessing,
					Target:             "target-1",
					ETag:               "etag-123",
					LastReconciledAt:   prevLastReconciledAt,
					TotalReceivedCount: 2,
					ReconcileCount:     4,
				},
				taskResponse: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:            taskID,
						Type:              "task-type",
						WorkingState:      []byte("updated-state"),
						ETag:              "etag-123",
						Status:            string(orbital.ResultDone),
						ReconcileAfterSec: 300,
					}
				},
				expTaskStatus:          orbital.TaskStatusDone,
				expWorkingState:        []byte("updated-state"),
				expReconcileAfter:      300,
				expTotalReceivedCount:  3,
				expReconcileCount:      0,
				isLastReconcileAtEqual: false,
			},
			{
				name: "should not update task with error message if the task response is not failed",
				initialStoredTask: &orbital.Task{
					JobID:            uuid.New(),
					Type:             "task-type",
					Data:             []byte("task-data"),
					WorkingState:     []byte("initial-state"),
					Status:           orbital.TaskStatusProcessing,
					Target:           "target-1",
					ETag:             "etag-123",
					LastReconciledAt: prevLastReconciledAt,
					ReconcileCount:   2,
				},
				taskResponse: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:            taskID,
						Type:              "task-type",
						WorkingState:      []byte("updated-state"),
						ETag:              "etag-123",
						Status:            string(orbital.ResultDone),
						ReconcileAfterSec: 300,
						ErrorMessage:      "some-error-message",
					}
				},
				expTaskStatus:          orbital.TaskStatusDone,
				expWorkingState:        []byte("updated-state"),
				expErrorMessage:        "",
				expReconcileAfter:      300,
				expTotalReceivedCount:  1,
				expReconcileCount:      0,
				isLastReconcileAtEqual: false,
			},
			{
				name: "stale etag - response discarded",
				initialStoredTask: &orbital.Task{
					JobID:            uuid.New(),
					Type:             "task-type",
					Data:             []byte("task-data"),
					WorkingState:     []byte("initial-state"),
					Status:           orbital.TaskStatusProcessing,
					Target:           "target-1",
					ETag:             "current-etag",
					ReconcileCount:   2,
					LastReconciledAt: prevLastReconciledAt,
				},
				taskResponse: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:       taskID,
						Type:         "task-type",
						WorkingState: []byte("should-not-update"),
						ETag:         "stale-etag",
						Status:       string(orbital.ResultDone),
					}
				},
				expTaskStatus:          orbital.TaskStatusProcessing,
				expWorkingState:        []byte("initial-state"),
				expTotalReceivedCount:  0,
				expReconcileCount:      2,
				isLastReconcileAtEqual: true,
			},
			{
				name:              "task not found",
				initialStoredTask: nil,
				taskResponse: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID: taskID,
						Type:   "task-type",
						ETag:   "etag-123",
						Status: string(orbital.ResultDone),
					}
				},
			},
			{
				name: "error status response",
				initialStoredTask: &orbital.Task{
					JobID:              uuid.New(),
					Type:               "task-type",
					Data:               []byte("task-data"),
					WorkingState:       []byte("initial-state"),
					Status:             orbital.TaskStatusProcessing,
					Target:             "target-1",
					ETag:               "etag-123",
					ReconcileCount:     4,
					TotalReceivedCount: 4,
					LastReconciledAt:   prevLastReconciledAt,
				},
				taskResponse: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:       taskID,
						Type:         "task-type",
						WorkingState: []byte("error-state"),
						ETag:         "etag-123",
						Status:       string(orbital.TaskStatusFailed),
						ErrorMessage: "Processing failed",
					}
				},
				expTaskStatus:          orbital.TaskStatusFailed,
				expErrorMessage:        "Processing failed",
				expWorkingState:        []byte("error-state"),
				expTotalReceivedCount:  5,
				expReconcileCount:      0,
				isLastReconcileAtEqual: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				var taskID uuid.UUID

				if tt.initialStoredTask != nil {
					taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{*tt.initialStoredTask})
					assert.NoError(t, err)
					taskID = taskIDs[0]
				}

				mgr, err := orbital.NewManager(repo, mockTaskResolveFunc())
				assert.NoError(t, err)

				response := tt.taskResponse(taskID)
				err = orbital.ProcessResponse(mgr)(ctx, response)
				assert.NoError(t, err)

				if response.TaskID != taskID {
					return
				}

				task, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
				if !found {
					return
				}
				assert.NoError(t, err)

				assert.Equal(t, tt.expTaskStatus, task.Status)
				assert.Equal(t, tt.expWorkingState, task.WorkingState)
				assert.Equal(t, tt.expTotalReceivedCount, task.TotalReceivedCount)
				assert.Equal(t, tt.expReconcileCount, task.ReconcileCount)
				if tt.isLastReconcileAtEqual {
					assert.Equal(t, prevLastReconciledAt, task.LastReconciledAt)
				} else {
					assert.Greater(t, task.LastReconciledAt, prevLastReconciledAt)
				}
				if tt.expReconcileAfter > 0 {
					assert.Equal(t, tt.expReconcileAfter, task.ReconcileAfterSec)
				}
				// Make sure the ETag is updated if the response was processed successfully
				assert.NotEqual(t, response.ETag, task.ETag)
				assert.Equal(t, tt.expErrorMessage, task.ErrorMessage)
			})
		}
	})
	t.Run("etag", func(t *testing.T) {
		tt := []struct {
			name               string
			taskETag           string
			taskResponseETag   string
			taskResponseStatus string
		}{
			{
				name:               "should be regenerated if Task and Taskresponse ETag are the same and TaskResponse status is DONE",
				taskETag:           "etag",
				taskResponseETag:   "etag",
				taskResponseStatus: string(orbital.ResultDone),
			},
			{
				name:               "should be regenerated if Task and Taskresponse ETag are the same and TaskResponse status is FAILED",
				taskETag:           "etag",
				taskResponseETag:   "etag",
				taskResponseStatus: string(orbital.ResultFailed),
			},
			{
				name:               "should be regenerated if Task and Taskresponse ETag are the same and TaskResponse status is CONTINUE",
				taskETag:           "etag",
				taskResponseETag:   "etag",
				taskResponseStatus: string(orbital.ResultProcessing),
			},
			{
				name:               "should not be regenerated if Task and Taskresponse Etag are different and TaskResponse status is DONE",
				taskETag:           "etag",
				taskResponseETag:   "different-etag",
				taskResponseStatus: string(orbital.ResultDone),
			},
			{
				name:               "should not be regenerated if Task and Taskresponse ETag are different and TaskResponse status is FAILED",
				taskETag:           "etag",
				taskResponseETag:   "different-etag",
				taskResponseStatus: string(orbital.ResultFailed),
			},
			{
				name:               "should not be regenerated if Task and Taskresponse ETag are different and TaskResponse status is CONTINUE",
				taskETag:           "etag",
				taskResponseETag:   "different-etag",
				taskResponseStatus: string(orbital.ResultProcessing),
			},
		}
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				task := orbital.Task{
					JobID:        uuid.New(),
					Type:         "task-type",
					Data:         []byte("task-data"),
					WorkingState: []byte("initial-state"),
					Status:       orbital.TaskStatusProcessing,
					Target:       "target-1",
					ETag:         tc.taskETag,
				}
				taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
				assert.NoError(t, err)
				assert.Len(t, taskIDs, 1)
				taskID := taskIDs[0]

				mgr, err := orbital.NewManager(repo, mockTaskResolveFunc())
				assert.NoError(t, err)

				response := orbital.TaskResponse{
					TaskID:            taskID,
					Type:              "task-type",
					WorkingState:      []byte("updated-state"),
					ETag:              tc.taskResponseETag,
					Status:            tc.taskResponseStatus,
					ReconcileAfterSec: 300,
				}
				processFunc := orbital.ProcessResponse(mgr)
				err = processFunc(ctx, response)
				assert.NoError(t, err)

				task, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
				assert.True(t, found)
				assert.NoError(t, err)

				if tc.taskETag == tc.taskResponseETag {
					assert.NotEqual(t, tc.taskETag, task.ETag)
				} else {
					assert.Equal(t, tc.taskETag, task.ETag)
				}
			})
		}
	})
	t.Run("received_count", func(t *testing.T) {
		t.Run("should be incremented for each successful receive (etag matches)", func(t *testing.T) {
			ctx := t.Context()
			db, store := createSQLStore(t)
			defer clearTables(t, db)
			repo := orbital.NewRepository(store)

			etag := "etag"
			task := orbital.Task{
				JobID:  uuid.New(),
				Type:   "task-type",
				Status: orbital.TaskStatusProcessing,
				ETag:   etag,
			}
			taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
			assert.NoError(t, err)
			assert.Len(t, taskIDs, 1)
			taskID := taskIDs[0]

			mgr, err := orbital.NewManager(repo, mockTaskResolveFunc())
			assert.NoError(t, err)
			for i := range 5 {
				response := orbital.TaskResponse{
					TaskID: taskID,
					Type:   "task-type",
					ETag:   etag,
					Status: string(orbital.ResultDone),
				}
				processFunc := orbital.ProcessResponse(mgr)
				err = processFunc(ctx, response)
				assert.NoError(t, err)

				task, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
				assert.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, int64(i+1), task.TotalReceivedCount)

				etag = task.ETag
			}
		})
		t.Run("should not be incremented for non-successful receive (etag doesn't match)", func(t *testing.T) {
			ctx := t.Context()
			db, store := createSQLStore(t)
			defer clearTables(t, db)
			repo := orbital.NewRepository(store)

			task := orbital.Task{
				JobID:  uuid.New(),
				Type:   "task-type",
				Status: orbital.TaskStatusProcessing,
				ETag:   "etag",
			}
			taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
			assert.NoError(t, err)
			assert.Len(t, taskIDs, 1)
			taskID := taskIDs[0]

			mgr, err := orbital.NewManager(repo, mockTaskResolveFunc())
			assert.NoError(t, err)
			response := orbital.TaskResponse{
				TaskID: taskID,
				Type:   "task-type",
				ETag:   "non-matching-etag",
				Status: string(orbital.ResultDone),
			}
			processFunc := orbital.ProcessResponse(mgr)
			err = processFunc(ctx, response)
			assert.NoError(t, err)

			task, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, int64(0), task.TotalReceivedCount)
		})
	})
}
