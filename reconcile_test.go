package orbital_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
	"github.com/openkcm/orbital/internal/retry"
)

//nolint:gocognit
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
			assert.Equal(t, job.ExternalID, req.ExternalID)
			assert.Equal(t, expData, req.Data)
			assert.Equal(t, expWorkingState, req.WorkingState)
			assert.Equal(t, expETag, req.ETag)
			return orbital.TaskResponse{}, nil
		})
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithTargets(map[string]orbital.TargetManager{
				expTarget: {Client: client},
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
				client: successfulClient(),
			},
			{
				name:   "should be incremented exponentially for unsuccessful sent",
				client: failedClient(),
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
					orbital.WithTargets(
						map[string]orbital.TargetManager{
							expTarget: {Client: tc.client},
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
				Target:           expTarget,
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
			orbital.WithTargets(map[string]orbital.TargetManager{
				expTarget: {Client: client},
			}),
		)
		assert.NoError(t, err)
		// making sure there is no delay in sending the task request
		subj.Config.BackoffMaxIntervalSec = 0

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

	t.Run("should send a task request only after the defined reconcile_after_sec", func(t *testing.T) {
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
		ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
			{
				JobID:            job.ID,
				Type:             "type",
				Data:             []byte("-data"),
				WorkingState:     []byte(""),
				Status:           orbital.TaskStatusCreated,
				Target:           expTarget,
				ETag:             "etag",
				LastReconciledAt: 0,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)
		taskID := ids[0]

		initiator, err := embedded.NewClient(func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{}, nil
		})
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithTargets(map[string]orbital.TargetManager{
				expTarget: {Client: initiator},
			}),
		)
		assert.NoError(t, err)
		// setting the backoff max interval to 5 seconds
		subj.Config.BackoffMaxIntervalSec = 5

		var startTime, execTime time.Time
		startTime = time.Now()

		// calling Reconcile 11 times with an interval of 500 Millisecond (5.5 seconds in total)
		for count := range 11 {
			// when
			err = orbital.Reconcile(subj)(ctx)
			assert.NoError(t, err)

			execTime = time.Now()
			time.Sleep(500 * time.Millisecond)

			actTask, ok, err := orbital.GetRepoTask(repo)(ctx, taskID)
			assert.NoError(t, err)
			assert.True(t, ok)

			// then
			if count >= 10 {
				assert.Equal(t, int64(2), actTask.TotalSentCount, "should not update reconcile count if reconcile_after_sec is not reached")
				assert.GreaterOrEqual(t, execTime.Sub(startTime), 5*time.Second)
			} else {
				assert.Equal(t, int64(1), actTask.TotalSentCount, "should update reconcile count if reconcile_after_sec is reached")
				assert.Less(t, execTime.Sub(startTime), 5*time.Second)
			}
		}
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
			orbital.WithTargets(map[string]orbital.TargetManager{
				target: {Client: failedClient()},
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
				client:                 successfulClient(),
				isSentCountIncremented: true,
			},
			{
				name:                   "should not be incremented after a failed send",
				client:                 failedClient(),
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
					orbital.WithTargets(map[string]orbital.TargetManager{
						target: {Client: tc.client},
					}),
				)
				assert.NoError(t, err)
				// making sure there is no delay in sending the task request
				subj.Config.BackoffMaxIntervalSec = 0

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

	t.Run("should update task to failed when no target client is present", func(t *testing.T) {
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
			orbital.WithTargets(map[string]orbital.TargetManager{}),
		)
		assert.NoError(t, err)

		// when
		err = orbital.Reconcile(subj)(ctx)

		// then
		assert.NoError(t, err)
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, orbital.TaskStatusFailed, actTask.Status)
		assert.Equal(t, int64(0), actTask.TotalSentCount)
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
	t.Run("task not found", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		mgr, err := orbital.NewManager(repo, mockTaskResolveFunc())
		assert.NoError(t, err)

		err = orbital.ProcessResponse(mgr)(ctx, orbital.TaskResponse{})

		assert.Error(t, err)
		assert.ErrorIs(t, err, orbital.ErrTaskNotFound)
	})
}

func TestReconciliationRaceCondition(t *testing.T) {
	db, store := createSQLStore(t)
	repo := orbital.NewRepository(store)

	target := "target-1"

	createInitJobAndTask := func(ctx context.Context) (uuid.UUID, uuid.UUID) {
		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusProcessing,
		})
		assert.NoError(t, err)

		task := orbital.Task{
			JobID:  job.ID,
			Type:   "task-type",
			Status: orbital.TaskStatusProcessing,
			Target: target,
			ETag:   "etag",
		}
		taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
		assert.NoError(t, err)
		assert.Len(t, taskIDs, 1)
		return job.ID, taskIDs[0]
	}

	t.Run("should wait for task reconcile transaction to complete before processing response", func(t *testing.T) {
		// given
		defer clearTables(t, db)

		ctx := t.Context()
		_, taskID := createInitJobAndTask(ctx)

		taskBefore, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
		assert.NoError(t, err)
		assert.True(t, found)

		response := orbital.TaskResponse{
			TaskID: taskID,
			Type:   "task-type",
			ETag:   "etag",
			Status: string(orbital.ResultDone),
		}

		responseStartChan := make(chan string)

		mgr, err := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithTargets(map[string]orbital.TargetManager{
				target: {
					Client: &mockInitiator{
						FnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
							// start the response processing
							responseStartChan <- "start response processing"
							// giving some time to ensure that the response processing waits for this send to finish
							time.Sleep(1 * time.Second)
							return nil
						},
					},
				},
			}),
		)
		assert.NoError(t, err)

		// when
		go func() {
			err := orbital.Reconcile(mgr)(ctx)
			assert.NoError(t, err)
		}()

		assert.Equal(t, "start response processing", <-responseStartChan)
		err = orbital.ProcessResponse(mgr)(ctx, response)
		assert.NoError(t, err)

		// then
		taskAfter, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
		assert.NoError(t, err)
		assert.True(t, found)
		// Since total_sent_count as increased by 1 it means ProcessResponse waited for the Reconcile to finish.
		assert.Equal(t, taskBefore.TotalSentCount+1, taskAfter.TotalSentCount)
		// Since total_received_count has increased by 1 it means ProcessResponse was successfully processed.
		assert.Equal(t, taskBefore.TotalReceivedCount+1, taskAfter.TotalReceivedCount)
	})

	t.Run("should not send task request for tasks locked for update", func(t *testing.T) {
		defer clearTables(t, db)

		ctx := t.Context()
		jobID, taskID := createInitJobAndTask(ctx)

		mgr, err := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithTargets(map[string]orbital.TargetManager{
				target: {
					Client: &mockInitiator{
						FnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
							assert.Fail(t, "should not send task request while the task is locked for update")
							return nil
						},
					},
				},
			}),
		)
		assert.NoError(t, err)

		ctxTimeout, cancelFn := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFn()

		err = repo.Store.Transaction(ctxTimeout, func(ctx context.Context, r orbital.Repository) error {
			task, ok, err := orbital.GetRepoTaskForUpdate(&r)(ctx, taskID)
			assert.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, jobID, task.JobID)

			err = orbital.Reconcile(mgr)(ctx)
			assert.NoError(t, err)

			return nil
		})
		assert.NoError(t, err)
	})
}

func TestHandleResponse(t *testing.T) {
	t.Run("should not fail if the initiator is nil for a target", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		initiator := orbital.TargetManager{Client: nil}

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
		)
		assert.NoError(t, err)

		// when then

		assert.NotPanics(t, func() {
			orbital.HandleResponses(subj)(ctx, initiator, "target")
		}, "should not panic if the initiator is nil for a target")
	})
}

var errSendFailed = errors.New("send failed")

type mockInitiator struct {
	FnSendTaskRequest     func(context.Context, orbital.TaskRequest) error
	FnReceiveTaskResponse func(context.Context) (orbital.TaskResponse, error)
	FnClose               func(context.Context) error
}

// ReceiveTaskResponse implements orbital.Initiator.
func (m *mockInitiator) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	return m.FnReceiveTaskResponse(ctx)
}

// SendTaskRequest implements orbital.Initiator.
func (m *mockInitiator) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	return m.FnSendTaskRequest(ctx, request)
}

func (m *mockInitiator) Close(ctx context.Context) error {
	if m.FnClose == nil {
		return nil
	}

	return m.FnClose(ctx)
}

var _ orbital.Initiator = &mockInitiator{}

func failedClient() orbital.Initiator {
	return &mockInitiator{
		FnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
			return errSendFailed
		},
		FnReceiveTaskResponse: func(_ context.Context) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{}, nil
		},
		FnClose: func(_ context.Context) error {
			return nil
		},
	}
}

func successfulClient() orbital.Initiator {
	return &mockInitiator{
		FnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
			return nil
		},
		FnReceiveTaskResponse: func(_ context.Context) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{}, nil
		},

		FnClose: func(_ context.Context) error {
			return nil
		},
	}
}

func assertMapContains(t *testing.T, m, contains map[string]string) {
	t.Helper()
	for expK, expVal := range contains {
		actVal, ok := m[expK]
		assert.True(t, ok, "should contain key [%s]", expK)
		if !ok {
			continue
		}
		assert.Equal(t, expVal, actVal)
	}
}

type mockRequestSigner struct {
	FnSign func(ctx context.Context, request orbital.TaskRequest) (orbital.Signature, error)
}

var _ orbital.TaskRequestSigner = &mockRequestSigner{}

func (m *mockRequestSigner) Sign(ctx context.Context, request orbital.TaskRequest) (orbital.Signature, error) {
	return m.FnSign(ctx, request)
}

type mockResponseVerifier struct {
	FnVerify func(ctx context.Context, response orbital.TaskResponse) error
}

var _ orbital.TaskResponseVerifier = &mockResponseVerifier{}

func (m *mockResponseVerifier) Verify(ctx context.Context, response orbital.TaskResponse) error {
	return m.FnVerify(ctx, response)
}
