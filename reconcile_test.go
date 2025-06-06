package orbital_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/interactortest"
)

var errSendFailed = errors.New("send failed")

func TestReconcile(t *testing.T) {
	t.Run("should just return if there is no job ready for reconciliation", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		subj, err := orbital.NewManager(repo,
			mockTaskResolverFunc(),
		)
		assert.NoError(t, err)

		// when
		err = orbital.Reconcile(subj)(ctx)

		// then
		assert.NoError(t, err)
	})

	t.Run("should update the job status to", func(t *testing.T) {
		// given
		tt := []struct {
			name            string
			jobStatus       orbital.JobStatus
			taskStatus      orbital.TaskStatus
			expJobStatus    orbital.JobStatus
			isEventRecorded bool
		}{
			{
				name:         "PROCESSING if job was in READY status before",
				jobStatus:    orbital.JobStatusReady,
				taskStatus:   orbital.TaskStatusProcessing,
				expJobStatus: orbital.JobStatusProcessing,
			},
			{
				name:            "FAILED if a task is in FAILED status",
				jobStatus:       orbital.JobStatusProcessing,
				taskStatus:      orbital.TaskStatusFailed,
				expJobStatus:    orbital.JobStatusFailed,
				isEventRecorded: true,
			},
			{
				name:            "DONE if all tasks are in DONE status",
				jobStatus:       orbital.JobStatusProcessing,
				taskStatus:      orbital.TaskStatusDone,
				expJobStatus:    orbital.JobStatusDone,
				isEventRecorded: true,
			},
		}

		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				subj, err := orbital.NewManager(repo,
					mockTaskResolverFunc(),
				)
				assert.NoError(t, err)

				job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: tc.jobStatus,
				})
				assert.NoError(t, err)

				_, err = orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
					{
						JobID:  job.ID,
						Status: tc.taskStatus,
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
				JobID:        job.ID,
				Type:         expType,
				Data:         expData,
				WorkingState: expWorkingState,
				ETag:         expETag,
				Status:       orbital.TaskStatusCreated,
				Target:       expTarget,
				LastSentAt:   0,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)

		initiator, err := interactortest.NewInitiator(func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			assert.Equal(t, ids[0], req.TaskID)
			assert.Equal(t, expType, req.Type)
			assert.Equal(t, expData, req.Data)
			assert.Equal(t, expWorkingState, req.WorkingState)
			assert.Equal(t, expETag, req.ETag)
			return orbital.TaskResponse{}, nil
		}, nil)
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolverFunc(),
			orbital.WithTargetClients(map[string]orbital.Initiator{
				expTarget: initiator,
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
		assert.Equal(t, orbital.TaskStatusProcessing, actTask.Status)
		assert.NotZero(t, actTask.LastSentAt)
		assert.Equal(t, int64(1), actTask.SentCount)
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
				JobID:        job.ID,
				Type:         "type",
				Data:         []byte("task-data"),
				WorkingState: []byte("state"),
				Status:       orbital.TaskStatusCreated,
				Target:       "target-1",
				ETag:         expETag,
				LastSentAt:   0,
			},
		})
		assert.NoError(t, err)
		assert.Len(t, ids, 1)

		operatorCalled := 0
		initiator, err := interactortest.NewInitiator(func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			operatorCalled++
			assert.Equal(t, expETag, req.ETag)
			return orbital.TaskResponse{}, nil
		}, nil)
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolverFunc(),
			orbital.WithTargetClients(map[string]orbital.Initiator{
				expTarget: initiator,
			}),
		)
		assert.NoError(t, err)

		// when
		// calling Reconcile twice to simulate sending of the taskrequest
		for range 2 {
			err = orbital.Reconcile(subj)(ctx)
			assert.NoError(t, err)
		}

		// then
		// check that the operator was called twice, once for each Reconcile call
		assert.Equal(t, 2, operatorCalled)
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, int64(2), actTask.SentCount)
		assert.Equal(t, expETag, actTask.ETag)
	})
	t.Run("should not update task if task request fails", func(t *testing.T) {
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

		sent := false
		initiator, err := interactortest.NewInitiator(func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
			sent = true
			return orbital.TaskResponse{}, errSendFailed
		}, nil)
		assert.NoError(t, err)

		subj, err := orbital.NewManager(repo,
			mockTaskResolverFunc(),
			orbital.WithTargetClients(map[string]orbital.Initiator{
				target: initiator,
			}),
		)
		assert.NoError(t, err)

		// when
		err = orbital.Reconcile(subj)(ctx)

		// then
		assert.NoError(t, err)
		assert.True(t, sent)
		actTask, ok, err := orbital.GetRepoTask(repo)(ctx, ids[0])
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, orbital.TaskStatusCreated, actTask.Status)
	})
}

//nolint:gocognit
func TestProcessResponse(t *testing.T) {
	t.Run("process response", func(t *testing.T) {
		tests := []struct {
			name                   string
			setupData              func(t *testing.T, repo *orbital.Repository) uuid.UUID
			response               func(taskID uuid.UUID) orbital.TaskResponse
			expectedTaskStatus     orbital.TaskStatus
			expectedWorkingState   []byte
			expectedReconcileAfter int64
			expectError            bool
			expectNoUpdate         bool
		}{
			{
				name: "successful response update",
				setupData: func(t *testing.T, repo *orbital.Repository) uuid.UUID {
					t.Helper()
					ctx := t.Context()
					task := orbital.Task{
						JobID:        uuid.New(),
						Type:         "task-type",
						Data:         []byte("task-data"),
						WorkingState: []byte("initial-state"),
						Status:       orbital.TaskStatusProcessing,
						Target:       "target-1",
						ETag:         "etag-123",
					}
					taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
					assert.NoError(t, err)
					return taskIDs[0]
				},
				response: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:            taskID,
						Type:              "task-type",
						WorkingState:      []byte("updated-state"),
						ETag:              "etag-123",
						Status:            string(orbital.ResultDone),
						ReconcileAfterSec: 300,
					}
				},
				expectedTaskStatus:     orbital.TaskStatusDone,
				expectedWorkingState:   []byte("updated-state"),
				expectedReconcileAfter: 300,
				expectError:            false,
			},
			{
				name: "stale etag - response discarded",
				setupData: func(t *testing.T, repo *orbital.Repository) uuid.UUID {
					t.Helper()
					ctx := t.Context()
					task := orbital.Task{
						JobID:        uuid.New(),
						Type:         "task-type",
						Data:         []byte("task-data"),
						WorkingState: []byte("initial-state"),
						Status:       orbital.TaskStatusProcessing,
						Target:       "target-1",
						ETag:         "current-etag",
					}
					taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
					assert.NoError(t, err)
					return taskIDs[0]
				},
				response: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:       taskID,
						Type:         "task-type",
						WorkingState: []byte("should-not-update"),
						ETag:         "stale-etag",
						Status:       string(orbital.ResultDone),
					}
				},
				expectedTaskStatus:   orbital.TaskStatusProcessing,
				expectedWorkingState: []byte("initial-state"),
				expectError:          false,
				expectNoUpdate:       true,
			},
			{
				name: "task not found",
				setupData: func(_ *testing.T, _ *orbital.Repository) uuid.UUID {
					return uuid.New()
				},
				response: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID: taskID,
						Type:   "task-type",
						ETag:   "etag-123",
						Status: string(orbital.ResultDone),
					}
				},
				expectError: false,
			},
			{
				name: "error status response",
				setupData: func(t *testing.T, repo *orbital.Repository) uuid.UUID {
					t.Helper()
					ctx := t.Context()
					task := orbital.Task{
						JobID:        uuid.New(),
						Type:         "task-type",
						Data:         []byte("task-data"),
						WorkingState: []byte("initial-state"),
						Status:       orbital.TaskStatusProcessing,
						Target:       "target-1",
						ETag:         "etag-123",
					}
					taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{task})
					assert.NoError(t, err)
					return taskIDs[0]
				},
				response: func(taskID uuid.UUID) orbital.TaskResponse {
					return orbital.TaskResponse{
						TaskID:       taskID,
						Type:         "task-type",
						WorkingState: []byte("error-state"),
						ETag:         "etag-123",
						Status:       string(orbital.TaskStatusFailed),
						ErrorMessage: "Processing failed",
					}
				},
				expectedTaskStatus:   orbital.TaskStatusFailed,
				expectedWorkingState: []byte("error-state"),
				expectError:          false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				taskID := tt.setupData(t, repo)

				mgr, err := orbital.NewManager(repo, mockTaskResolverFunc())
				assert.NoError(t, err)

				response := tt.response(taskID)
				processFunc := orbital.ProcessResponse(mgr)
				err = processFunc(ctx, response)

				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}

				if response.TaskID != taskID {
					return
				}

				task, found, err := orbital.GetRepoTask(repo)(ctx, taskID)
				if !found {
					return
				}
				assert.NoError(t, err)

				if !tt.expectNoUpdate {
					assert.Equal(t, tt.expectedTaskStatus, task.Status)
					assert.Equal(t, tt.expectedWorkingState, task.WorkingState)
					if tt.expectedReconcileAfter > 0 {
						assert.Equal(t, tt.expectedReconcileAfter, task.ReconcileAfterSec)
					}
					// Make sure the ETag is updated if the response was processed successfully
					assert.NotEqual(t, response.ETag, task.ETag)
				} else {
					assert.Equal(t, orbital.TaskStatusProcessing, task.Status)
					assert.Equal(t, []byte("initial-state"), task.WorkingState)
				}
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
				taskResponseStatus: string(orbital.ResultContinue),
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
				taskResponseStatus: string(orbital.ResultContinue),
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

				mgr, err := orbital.NewManager(repo, mockTaskResolverFunc())
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
}
