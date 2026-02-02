package orbital_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/internal/clock"
)

var errResourceNotFound = errors.New("resource not found")

func TestPrepareJob(t *testing.T) {
	// given
	tests := []struct {
		name     string
		existJob orbital.Job
		prepJob  orbital.Job
		expErr   error
	}{
		{
			name: "should prepare job with existing type but different external ID",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusCreated,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id-diff",
			},
			expErr: nil,
		},
		{
			name: "should prepare job with no external ID",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusCreated,
			},
			prepJob: orbital.Job{
				Type: "type",
			},
			expErr: nil,
		},
		{
			name: "should return error when preparing job with same type and external ID and existing job is created",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusCreated,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: orbital.ErrJobAlreadyExists,
		},
		{
			name: "should return error when preparing job with same type and external ID and existing job is confirming",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusConfirming,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: orbital.ErrJobAlreadyExists,
		},
		{
			name: "should return error when preparing job with same type and external ID and existing job is confirmed",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusConfirmed,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: orbital.ErrJobAlreadyExists,
		},
		{
			name: "should return error when preparing job with same type and external ID and existing job is resolving",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusResolving,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: orbital.ErrJobAlreadyExists,
		},
		{
			name: "should return error when preparing job with same type and external ID and existing job is ready",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusReady,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: orbital.ErrJobAlreadyExists,
		},
		{
			name: "should return error when preparing job with same type and external ID and existing job is processing",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusProcessing,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: orbital.ErrJobAlreadyExists,
		},
		{
			name: "should prepare job when existing job is done",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusDone,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: nil,
		},
		{
			name: "should prepare job when existing job is failed",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusFailed,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: nil,
		},
		{
			name: "should prepare job when existing job is canceled in resolving",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusResolveCanceled,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: nil,
		},
		{
			name: "should prepare job when existing job is canceled in confirming",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusConfirmCanceled,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: nil,
		},
		{
			name: "should prepare job when existing job is canceled by user",
			existJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
				Status:     orbital.JobStatusUserCanceled,
			},
			prepJob: orbital.Job{
				Type:       "type",
				ExternalID: "ext-id",
			},
			expErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, store := createSQLStore(t)
			defer clearTables(t, db)
			repo := orbital.NewRepository(store)

			subj, _ := orbital.NewManager(repo, mockTaskResolveFunc())

			ctx := t.Context()

			job, err := orbital.CreateRepoJob(repo)(ctx, tt.existJob)
			assert.NoError(t, err)
			assert.NotEqual(t, uuid.Nil, job.ID)

			// when
			job, err = subj.PrepareJob(ctx, tt.prepJob)

			// then
			if tt.expErr != nil {
				assert.Error(t, err)
				assert.ErrorIs(t, err, tt.expErr)
				assert.Equal(t, uuid.Nil, job.ID)
				return
			}
			assert.NoError(t, err)
			assert.NotEqual(t, uuid.Nil, job.ID)

			preparedJob, ok, err := subj.GetJob(ctx, job.ID)
			assert.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, orbital.JobStatusCreated, preparedJob.Status)
		})
	}
}

func TestNewManagerTaskResolverErr(t *testing.T) {
	t.Run("should not return error if task resolver is defined", func(t *testing.T) {
		// when
		_, err := orbital.NewManager(nil,
			mockTaskResolveFunc(),
		)

		// then
		assert.NoError(t, err)
	})

	t.Run("should return error if task resolver is nil", func(t *testing.T) {
		// when
		_, err := orbital.NewManager(nil,
			nil,
		)

		// then
		assert.Equal(t, orbital.ErrTaskResolverNotSet, err)
	})
}

func TestConfirmJob(t *testing.T) {
	t.Run("should change the job states", func(t *testing.T) {
		// given
		tts := []struct {
			name                string
			confirmFuncResponse func() (orbital.JobConfirmResult, error)
			expStatus           orbital.JobStatus
		}{
			{
				name: "for confirmation error",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{}, errResourceNotFound
				},
				expStatus: orbital.JobStatusCreated,
			},
			{
				name: "for canceled confirmation",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{IsCanceled: true}, nil
				},
				expStatus: orbital.JobStatusConfirmCanceled,
			},
			{
				name: "for unfinished confirmation",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{Done: false}, nil
				},
				expStatus: orbital.JobStatusConfirming,
			},
			{
				name: "for successful confirmation",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{Done: true}, nil
				},
				expStatus: orbital.JobStatusConfirmed,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				confirmFunc := func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
					return tt.confirmFuncResponse()
				}
				subj, _ := orbital.NewManager(repo,
					mockTaskResolveFunc(),
					orbital.WithJobConfirmFunc(confirmFunc),
				)
				subj.Config.ConfirmJobAfter = 10 * time.Millisecond

				job := orbital.NewJob("", nil)
				jobCreated, err := subj.PrepareJob(ctx, job)
				assert.NoError(t, err)

				// when
				time.Sleep(100 * time.Millisecond)
				err = orbital.ConfirmJob(subj)(ctx)
				assert.NoError(t, err)

				// then
				job, ok, err := subj.GetJob(ctx, jobCreated.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, tt.expStatus, job.Status)
			})
		}
	})
	t.Run("should set the job error message from confirm func when job is canceled", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		expErrMsg := "cancelled error message"

		confirmFunc := func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{
				CanceledErrorMessage: expErrMsg,
				IsCanceled:           true,
			}, nil
		}
		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithJobConfirmFunc(confirmFunc),
		)
		subj.Config.ConfirmJobAfter = 10 * time.Millisecond

		job := orbital.NewJob("", nil)
		jobCreated, err := subj.PrepareJob(ctx, job)
		assert.NoError(t, err)

		// when
		time.Sleep(100 * time.Millisecond)
		err = orbital.ConfirmJob(subj)(ctx)
		assert.NoError(t, err)

		// then
		job, ok, err := subj.GetJob(ctx, jobCreated.ID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, orbital.JobStatusConfirmCanceled, job.Status)
		assert.Equal(t, expErrMsg, job.ErrorMessage)
	})
	t.Run("should create job event from confirm func when job is canceled", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		confirmFunc := func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{
				IsCanceled: true,
			}, nil
		}
		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithJobConfirmFunc(confirmFunc),
			orbital.WithJobCanceledEventFunc(mockTerminatedFunc()),
		)
		subj.Config.ConfirmJobAfter = 10 * time.Millisecond

		job := orbital.NewJob("", nil)
		jobCreated, err := subj.PrepareJob(ctx, job)
		assert.NoError(t, err)

		// when
		time.Sleep(100 * time.Millisecond)
		err = orbital.ConfirmJob(subj)(ctx)
		assert.NoError(t, err)

		// then
		jobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{
			ID: jobCreated.ID,
		})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, jobCreated.ID, jobEvent.ID)
		assert.False(t, jobEvent.IsNotified)
	})

	t.Run("should not set the job error message from confirm func when job is confirmed", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		confirmFunc := func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			return orbital.JobConfirmResult{
				CanceledErrorMessage: "cancelled error message",
				Done:                 true,
			}, nil
		}
		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithJobConfirmFunc(confirmFunc),
		)
		subj.Config.ConfirmJobAfter = 10 * time.Millisecond

		job := orbital.NewJob("", nil)
		jobCreated, err := subj.PrepareJob(ctx, job)
		assert.NoError(t, err)

		// when
		time.Sleep(100 * time.Millisecond)
		err = orbital.ConfirmJob(subj)(ctx)
		assert.NoError(t, err)

		// then
		job, ok, err := subj.GetJob(ctx, jobCreated.ID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, orbital.JobStatusConfirmed, job.Status)
		assert.Empty(t, job.ErrorMessage)
	})

	t.Run("should respect retrieval mode and order by updatedAt in query", func(t *testing.T) {
		// given
		db, store := createSQLStore(t)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer func() {
			clearTables(t, db)
			cancel()
		}()

		repo := orbital.NewRepository(store)

		job := orbital.Job{Status: orbital.JobStatusCreated}
		createdJob1, err := orbital.CreateRepoJob(repo)(ctx, job)
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		_, err = orbital.CreateRepoJob(repo)(ctx, job)
		assert.NoError(t, err)

		callerChan := make(chan string)
		var isJob1Called atomic.Int32
		// make sure that the confirmFunc is called only once for the first job
		confirmFunc := func(_ context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			if job.ID == createdJob1.ID {
				isJob1Called.Add(1)
				callerChan <- "start second retrieval mode list"
				assert.Equal(t, "finish confirm func", <-callerChan)
			}
			return orbital.JobConfirmResult{Done: true}, nil
		}
		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithJobConfirmFunc(confirmFunc),
		)
		subj.Config.ConfirmJobAfter = 10 * time.Millisecond

		// when

		go func() {
			err := orbital.ConfirmJob(subj)(ctx)
			assert.NoError(t, err)
		}()
		assert.Equal(t, "start second retrieval mode list", <-callerChan)
		actJobs, err := orbital.ListRepoJobs(repo)(ctx, orbital.ListJobsQuery{
			RetrievalModeQueue: true,
			CreatedAt:          clock.NowUnixNano(),
		})
		assert.NoError(t, err)
		assert.Len(t, actJobs, 1)
		assert.NotEqual(t, createdJob1.ID, actJobs[0].ID)
		callerChan <- "finish confirm func"

		// then
		assert.Equal(t, int32(1), isJob1Called.Load())
	})
}

func TestCreateTasks(t *testing.T) {
	t.Run("should call task resolver", func(t *testing.T) {
		tt := []struct {
			name   string
			status orbital.JobStatus
		}{
			{
				name:   "if job status is confirmed",
				status: orbital.JobStatusConfirmed,
			},
			{
				name:   "if job status is resolving",
				status: orbital.JobStatusResolving,
			},
		}
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				// given
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				resolverCalled := 0
				resolverFunc := func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
					resolverCalled++
					return orbital.TaskResolverResult{
						TaskInfos: []orbital.TaskInfo{
							{
								Target: "target-1",
							},
						},
						Done: true,
					}, nil
				}
				targets := map[string]orbital.TargetManager{
					"target-1": {Client: nil},
				}
				subj, _ := orbital.NewManager(
					repo,
					resolverFunc,
					orbital.WithTargets(targets),
				)

				_, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: tc.status,
					Data:   []byte(`{"data": "data"}`),
				})

				assert.NoError(t, err)

				// when
				err = orbital.CreateTask(subj)(ctx)

				// then
				assert.NoError(t, err)
				assert.Equal(t, 1, resolverCalled)
			})
		}
	})

	t.Run("should create tasks based on task info", func(t *testing.T) {
		// given
		tt := []struct {
			name string
			info orbital.TaskInfo
		}{
			{
				name: "with empty fields",
				info: orbital.TaskInfo{
					Target: "target",
					Data:   []byte{},
					Type:   "target",
				},
			},
			{
				name: "with all fields set",
				info: orbital.TaskInfo{
					Target: "target",
					Data:   []byte("data"),
					Type:   "type",
				},
			},
		}

		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				resolverFunc := func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
					return orbital.TaskResolverResult{
						TaskInfos: []orbital.TaskInfo{tc.info},
						Done:      true,
					}, nil
				}

				targets := map[string]orbital.TargetManager{
					"target": {Client: nil},
				}
				subj, _ := orbital.NewManager(
					repo,
					resolverFunc,
					orbital.WithTargets(targets),
				)

				job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: orbital.JobStatusConfirmed,
				})
				assert.NoError(t, err)

				// when
				err = orbital.CreateTask(subj)(ctx)

				// then
				assert.NoError(t, err)
				actTasks, err := subj.ListTasks(ctx, orbital.ListTasksQuery{
					Status: orbital.TaskStatusCreated,
					Limit:  10,
				})
				assert.NoError(t, err)
				assert.Len(t, actTasks, 1)
				assert.Equal(t, job.ID, actTasks[0].JobID)
				assert.Equal(t, tc.info.Target, actTasks[0].Target)
				assert.Equal(t, tc.info.Data, actTasks[0].Data)
			})
		}
	})

	t.Run("should respect retrieval mode and order by updateAt", func(t *testing.T) {
		// given
		db, store := createSQLStore(t)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer func() {
			clearTables(t, db)
			cancel()
		}()

		repo := orbital.NewRepository(store)

		job := orbital.Job{
			Status: orbital.JobStatusConfirmed,
		}
		createdJob1, err := orbital.CreateRepoJob(repo)(ctx, job)
		assert.NoError(t, err)
		_, err = orbital.CreateRepoJob(repo)(ctx, job)
		assert.NoError(t, err)

		callerChan := make(chan string)
		var isJob1Called atomic.Int32
		resolverFunc := func(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			if job.ID == createdJob1.ID {
				isJob1Called.Add(1)
				callerChan <- "start second retrieval mode list"
				assert.Equal(t, "finish taskresolver func", <-callerChan)
			}
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Target: "target-1",
					},
				},
				Done: true,
			}, nil
		}
		targets := map[string]orbital.TargetManager{
			"target-1": {Client: nil},
		}
		subj, _ := orbital.NewManager(
			repo,
			resolverFunc,
			orbital.WithTargets(targets),
		)

		assert.NoError(t, err)

		// when
		go func() {
			err = orbital.CreateTask(subj)(ctx)
			assert.NoError(t, err)
		}()
		assert.Equal(t, "start second retrieval mode list", <-callerChan)
		actJobs, err := orbital.ListRepoJobs(repo)(ctx, orbital.ListJobsQuery{
			RetrievalModeQueue: true,
			CreatedAt:          clock.NowUnixNano(),
		})
		assert.NoError(t, err)
		assert.Len(t, actJobs, 1)
		assert.NotEqual(t, createdJob1.ID, actJobs[0].ID)
		callerChan <- "finish taskresolver func"

		// then
		assert.Equal(t, int32(1), isJob1Called.Load())
	})

	t.Run("should update job status", func(t *testing.T) {
		// given
		tts := []struct {
			name           string
			resolverResult orbital.TaskResolverResult
			expStatus      orbital.JobStatus
			expErrMsg      string
			expEvent       bool
		}{
			{
				name: "to READY if the all targets are resolved",
				resolverResult: orbital.TaskResolverResult{
					TaskInfos: []orbital.TaskInfo{
						{
							Target: "target-1",
						},
					},
					Done: true,
				},
				expStatus: orbital.JobStatusReady,
			},
			{
				name: "to READY if no targets are resolved but resolver is done",
				resolverResult: orbital.TaskResolverResult{
					Done: true,
				},
				expStatus: orbital.JobStatusReady,
			},
			{
				name: "to READY if an empty target list is returned and resolver is done",
				resolverResult: orbital.TaskResolverResult{
					TaskInfos: []orbital.TaskInfo{},
					Done:      true,
				},
				expStatus: orbital.JobStatusReady,
			},
			{
				name: "to FAILED if the target does not exist",
				resolverResult: orbital.TaskResolverResult{
					TaskInfos: []orbital.TaskInfo{
						{
							Target: "target-2",
						},
					},
					Done: false,
				},
				expStatus: orbital.JobStatusFailed,
				expErrMsg: fmt.Sprintf("%v target: %s", orbital.ErrNoClientForTarget, "target-2"),
				expEvent:  true,
			},
			{
				name: "to RESOLVING if not all targets are resolved yet",
				resolverResult: orbital.TaskResolverResult{
					TaskInfos: []orbital.TaskInfo{
						{
							Target: "target-1",
						},
					},
					Done: false,
				},
				expStatus: orbital.JobStatusResolving,
			},
			{
				name: "to RESOLVE_CANCELED if the job is canceled",
				resolverResult: orbital.TaskResolverResult{
					IsCanceled:           true,
					CanceledErrorMessage: "the job needs to be canceled",
				},
				expStatus: orbital.JobStatusResolveCanceled,
				expErrMsg: "the job needs to be canceled",
				expEvent:  true,
			},
			{
				name:      "to RESOLVE_CANCELED if neither targets are resolved nor the job is canceld",
				expStatus: orbital.JobStatusResolveCanceled,
				expErrMsg: "task resolver returned no tasks but not done, invalid state",
				expEvent:  true,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				ctx := t.Context()
				db, store := createSQLStore(t)
				defer clearTables(t, db)
				repo := orbital.NewRepository(store)

				job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Status: orbital.JobStatusConfirmed,
					Data:   make([]byte, 0),
				})
				assert.NoError(t, err)

				resolverCalled := 0
				resolverFunc := func(_ context.Context, j orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
					resolverCalled++
					assert.Equal(t, job.ID, j.ID)
					return tt.resolverResult, nil
				}

				targets := map[string]orbital.TargetManager{
					"target-1": {Client: nil},
				}
				subj, _ := orbital.NewManager(
					repo,
					resolverFunc,
					orbital.WithTargets(targets),
					orbital.WithJobCanceledEventFunc(mockTerminatedFunc()),
					orbital.WithJobFailedEventFunc(mockTerminatedFunc()),
				)

				// when
				err = orbital.CreateTask(subj)(ctx)

				// then
				assert.NoError(t, err)
				assert.Equal(t, 1, resolverCalled)

				actJob, ok, err := orbital.GetRepoJob(repo)(ctx, job.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, tt.expStatus, actJob.Status)
				assert.Equal(t, tt.expErrMsg, actJob.ErrorMessage)

				actJobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{
					ID: job.ID,
				})
				assert.NoError(t, err)
				assert.Equal(t, tt.expEvent, ok)
				if tt.expEvent {
					assert.Equal(t, job.ID, actJobEvent.ID)
					assert.False(t, actJobEvent.IsNotified)
				}
			})
		}
	})

	t.Run("should not update the job status if there is an error in task resolver", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusConfirmed,
		})
		assert.NoError(t, err)

		resolverFunc := func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			return orbital.TaskResolverResult{}, assert.AnError
		}

		targets := map[string]orbital.TargetManager{
			"target": {Client: nil},
		}
		subj, _ := orbital.NewManager(
			repo,
			resolverFunc,
			orbital.WithTargets(targets),
		)

		// ensure update time difference
		time.Sleep(time.Millisecond)

		// when
		err = orbital.CreateTask(subj)(ctx)

		// then
		assert.NoError(t, err)
		actJob, ok, err := orbital.GetRepoJob(repo)(ctx, job.ID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, orbital.JobStatusConfirmed, actJob.Status)
		assert.Greater(t, actJob.UpdatedAt, job.UpdatedAt)
	})

	t.Run("should update job Status to READY if resolverFunc resolves all targets in the second call", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusConfirmed,
			Data:   make([]byte, 0),
		})
		assert.NoError(t, err)

		resolverCalled := 0
		expCursor := orbital.TaskResolverCursor("cursor-1st-time")
		resolverFunc := func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			resolverCalled++
			return orbital.TaskResolverResult{
				Cursor: expCursor,
				TaskInfos: []orbital.TaskInfo{
					{
						Target: "target",
					},
				},
				Done: resolverCalled == 2,
			}, nil
		}

		targets := map[string]orbital.TargetManager{
			"target": {Client: nil},
		}
		subj, _ := orbital.NewManager(
			repo,
			resolverFunc,
			orbital.WithTargets(targets),
		)

		for createTaskCalledTimes := range 2 {
			// when
			err = orbital.CreateTask(subj)(ctx)

			// then
			assert.NoError(t, err)

			actCursor, _, err := orbital.GetRepoJobCursor(repo)(ctx, job.ID)
			assert.NoError(t, err)
			assert.Equal(t, expCursor, actCursor.Cursor)

			actJob, ok, err := orbital.GetRepoJob(repo)(ctx, job.ID)
			assert.NoError(t, err)
			assert.True(t, ok)
			if createTaskCalledTimes == 0 {
				assert.Equal(t, orbital.JobStatusResolving, actJob.Status)
			} else {
				assert.Equal(t, orbital.JobStatusReady, actJob.Status)
			}
		}
	})

	t.Run("should create task based on the number of targets returned by the task resolver", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		_, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Status: orbital.JobStatusConfirmed,
			Data:   []byte(`{"data": "data"}`),
		})
		assert.NoError(t, err)

		resolverCalled := 0
		resolverFunc := func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			resolverCalled++
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Target: fmt.Sprintf("target-%d", resolverCalled),
						Data:   fmt.Appendf(nil, "data-%d", resolverCalled),
						Type:   fmt.Sprintf("type-%d", resolverCalled),
					},
				},
				Done: resolverCalled == 2,
			}, nil
		}

		targets := map[string]orbital.TargetManager{
			"target-1": {Client: nil},
			"target-2": {Client: nil},
		}
		subj, _ := orbital.NewManager(
			repo,
			resolverFunc,
			orbital.WithTargets(targets),
		)

		for createTaskCalledTimes := range 2 {
			// when
			err = orbital.CreateTask(subj)(ctx)

			// then
			assert.NoError(t, err)
			actTasks, err := subj.ListTasks(ctx, orbital.ListTasksQuery{
				Status: orbital.TaskStatusCreated,
				Limit:  10,
			})
			sort.Slice(actTasks, func(i, j int) bool {
				return actTasks[i].Target < actTasks[j].Target
			})

			assert.NoError(t, err)
			assert.Len(t, actTasks, createTaskCalledTimes+1)
			totalTargetCreated := createTaskCalledTimes + 1

			for targetIndex := range totalTargetCreated {
				assert.Equal(t, fmt.Appendf(nil, "data-%d", targetIndex+1), actTasks[targetIndex].Data)
				assert.Equal(t, fmt.Sprintf("target-%d", targetIndex+1), actTasks[targetIndex].Target)
				assert.Equal(t, fmt.Sprintf("type-%d", targetIndex+1), actTasks[targetIndex].Type)
				// check if eTag is created
				eTag, err := uuid.Parse(actTasks[targetIndex].ETag)
				assert.NoError(t, err)
				assert.NotEqual(t, uuid.Nil, eTag)
			}
		}
	})
}

func TestListTasks(t *testing.T) {
	t.Run("should list all created tasks", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		taskToCreate := orbital.Task{
			JobID:             uuid.New(),
			Type:              "type",
			Data:              []byte("data"),
			WorkingState:      []byte("state"),
			LastReconciledAt:  1,
			ReconcileCount:    2,
			ReconcileAfterSec: 3,
			ETag:              "etag",
			Status:            orbital.TaskStatusCreated,
			Target:            "target-1",
			UpdatedAt:         4,
			CreatedAt:         5,
		}
		taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{taskToCreate})
		assert.NoError(t, err)
		assert.Len(t, taskIDs, 1)

		subj, err := orbital.NewManager(repo, func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			return orbital.TaskResolverResult{}, nil
		})
		assert.NoError(t, err)

		actTasks, err := subj.ListTasks(ctx, orbital.ListTasksQuery{
			Status: orbital.TaskStatusCreated,
			Limit:  10,
		})
		// then

		assert.NoError(t, err)
		assert.Len(t, actTasks, 1)
		assert.Equal(t, taskToCreate.Target, actTasks[0].Target)
		assert.Equal(t, taskToCreate.Data, actTasks[0].Data)
		assert.Equal(t, taskToCreate.Type, actTasks[0].Type)
		assert.Equal(t, taskToCreate.Status, actTasks[0].Status)
		assert.Equal(t, taskToCreate.WorkingState, actTasks[0].WorkingState)
		assert.Equal(t, taskToCreate.LastReconciledAt, actTasks[0].LastReconciledAt)
		assert.Equal(t, taskToCreate.ReconcileCount, actTasks[0].ReconcileCount)
		assert.Equal(t, taskToCreate.ReconcileAfterSec, actTasks[0].ReconcileAfterSec)
		assert.Equal(t, taskToCreate.ETag, actTasks[0].ETag)
	})
}

func TestStart(t *testing.T) {
	t.Run("should fail to start manager", func(t *testing.T) {
		t.Run("if any of target managers signing verifier is not set and signature check is enabled ", func(t *testing.T) {
			// given
			ctx := t.Context()

			db, store := createSQLStore(t)
			defer clearTables(t, db)

			repo := orbital.NewRepository(store)

			optsFunc := []orbital.ManagerOptsFunc{
				orbital.WithTargets(map[string]orbital.TargetManager{
					"target-1": {
						Client: &testInitiator{},
					},
					"target-2": {
						Client:             &testInitiator{},
						MustCheckSignature: true,
					},
				}),
			}

			subj, err := orbital.NewManager(repo,
				mockTaskResolveFunc(), optsFunc...,
			)
			assert.NoError(t, err)

			// when
			err = subj.Start(ctx)

			// then
			assert.ErrorIs(t, err, orbital.ErrManagerInvalidConfig)
		})

		t.Run("if manager is already started", func(t *testing.T) {
			// given
			db, store := createSQLStore(t)
			defer clearTables(t, db)
			repo := orbital.NewRepository(store)

			ctx := t.Context()

			subj, err := orbital.NewManager(repo, mockTaskResolveFunc())
			assert.NoError(t, err)

			// when
			assert.NoError(t, subj.Start(ctx))
			defer assert.NoError(t, subj.Stop(ctx))

			// then
			err = subj.Start(ctx)
			assert.ErrorIs(t, err, orbital.ErrManagerAlreadyStarted)
		})
	})
}

func TestManager_JovDoneEventFunc(t *testing.T) {
	t.Run("should call JobDoneEventFunc", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobID := uuid.New()
		_, err := orbital.CreateRepoJob(repo)(t.Context(), orbital.Job{ID: jobID, Status: orbital.JobStatusDone})
		assert.NoError(t, err)

		_, err = orbital.CreateRepoJobEvent(repo)(t.Context(), orbital.JobEvent{ID: jobID})
		assert.NoError(t, err)

		actCalled := 0
		wg := sync.WaitGroup{}
		wg.Add(1)
		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithJobDoneEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					defer wg.Done()
					actCalled++
					return nil
				}),
		)
		subj.Config.NotifyWorkerConfig.ExecInterval = 150 * time.Millisecond

		err = subj.Start(t.Context())
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, 1, actCalled)
	})
}

func TestCancel(t *testing.T) {
	t.Run("should cancel job", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobID := uuid.New()
		_, err := orbital.CreateRepoJob(repo)(t.Context(), orbital.Job{ID: jobID, Status: orbital.JobStatusCreated})
		assert.NoError(t, err)

		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
		)
		err = subj.CancelJob(t.Context(), jobID)
		assert.NoError(t, err)

		job, ok, err := subj.GetJob(t.Context(), jobID)
		assert.NoError(t, err)
		assert.True(t, ok)

		assert.Equal(t, orbital.JobStatusUserCanceled, job.Status)
		assert.Equal(t, "job has been canceled by the user", job.ErrorMessage)
	})
	t.Run("should not cancel job", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobID := uuid.New()
		_, err := orbital.CreateRepoJob(repo)(t.Context(), orbital.Job{ID: jobID, Status: orbital.JobStatusDone})
		assert.NoError(t, err)

		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
		)
		err = subj.CancelJob(t.Context(), jobID)
		assert.Error(t, err)

		job, ok, err := subj.GetJob(t.Context(), jobID)
		assert.NoError(t, err)
		assert.True(t, ok)

		assert.NotEqual(t, orbital.JobStatusUserCanceled, job.Status)
		assert.Empty(t, job.ErrorMessage)
	})
	t.Run("job not found", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		subj, _ := orbital.NewManager(repo,
			mockTaskResolveFunc(),
		)
		err := subj.CancelJob(t.Context(), uuid.New())
		assert.ErrorIs(t, err, orbital.ErrJobNotFound)
		assert.ErrorContains(t, err, "job not found")
	})
}

func TestManagerStop_Success(t *testing.T) {
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	subj, err := orbital.NewManager(repo, mockTaskResolveFunc())
	assert.NoError(t, err)

	ctx := t.Context()

	assert.NoError(t, subj.Start(ctx))

	assert.NoError(t, subj.Stop(ctx))
}

func TestManagerStop_NotStarted(t *testing.T) {
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	subj, err := orbital.NewManager(repo, mockTaskResolveFunc())
	assert.NoError(t, err)

	err = subj.Stop(t.Context())
	assert.ErrorIs(t, err, orbital.ErrManagerNotStarted)
}

func TestManagerStop_Idempotent(t *testing.T) {
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	subj, err := orbital.NewManager(repo, mockTaskResolveFunc())
	assert.NoError(t, err)

	ctx := t.Context()

	err = subj.Start(ctx)
	assert.NoError(t, err)

	err = subj.Stop(ctx)
	assert.NoError(t, err)

	err = subj.Stop(ctx)
	assert.NoError(t, err)

	err = subj.Stop(ctx)
	assert.NoError(t, err)
}

func TestManagerStop_ClosesAllClients(t *testing.T) {
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	var closeCalled atomic.Int32
	// Use buffered channel to avoid blocking
	readerStarted := make(chan struct{}, 2)

	mockClient := &testInitiator{
		fnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
			return nil
		},
		fnReceiveTaskResponse: func(ctx context.Context) (orbital.TaskResponse, error) {
			select {
			case readerStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return orbital.TaskResponse{}, ctx.Err()
		},
		fnClose: func(_ context.Context) error {
			closeCalled.Add(1)
			return nil
		},
	}

	targets := map[string]orbital.TargetManager{
		"target-1": {Client: mockClient},
		"target-2": {Client: mockClient},
	}

	subj, err := orbital.NewManager(repo, mockTaskResolveFunc(), orbital.WithTargets(targets))
	assert.NoError(t, err)

	ctx := t.Context()

	err = subj.Start(ctx)
	assert.NoError(t, err)

	<-readerStarted

	err = subj.Stop(ctx)
	assert.NoError(t, err)

	assert.Equal(t, int32(2), closeCalled.Load())
}

type testInitiator struct {
	fnSendTaskRequest     func(context.Context, orbital.TaskRequest) error
	fnReceiveTaskResponse func(context.Context) (orbital.TaskResponse, error)
	fnClose               func(context.Context) error
}

func (m *testInitiator) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	return m.fnSendTaskRequest(ctx, request)
}

func (m *testInitiator) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	return m.fnReceiveTaskResponse(ctx)
}

func (m *testInitiator) Close(ctx context.Context) error {
	if m.fnClose == nil {
		return nil
	}
	return m.fnClose(ctx)
}

var _ orbital.Initiator = &testInitiator{}

func TestManagerStop_GracefulShutdown(t *testing.T) {
	t.Run("should wait for in-flight work to complete before returning", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobID := uuid.New()
		_, err := orbital.CreateRepoJob(repo)(t.Context(), orbital.Job{ID: jobID, Status: orbital.JobStatusCreated})
		assert.NoError(t, err)

		workStarted := make(chan struct{})
		workCanComplete := make(chan struct{})
		var workCompleted atomic.Bool

		confirmFunc := func(_ context.Context, _ orbital.Job) (orbital.JobConfirmResult, error) {
			close(workStarted)
			<-workCanComplete
			workCompleted.Store(true)
			return orbital.JobConfirmResult{Done: true}, nil
		}

		subj, err := orbital.NewManager(repo,
			mockTaskResolveFunc(),
			orbital.WithJobConfirmFunc(confirmFunc),
		)
		assert.NoError(t, err)

		subj.Config.ConfirmJobWorkerConfig.ExecInterval = 10 * time.Millisecond
		subj.Config.ConfirmJobWorkerConfig.Timeout = 5 * time.Second

		ctx := t.Context()

		err = subj.Start(ctx)
		assert.NoError(t, err)

		<-workStarted

		stopDone := make(chan error)
		go func() {
			stopDone <- subj.Stop(ctx)
		}()

		select {
		case <-stopDone:
			t.Fatal("Stop returned before work completed")
		default:
		}

		close(workCanComplete)

		select {
		case err := <-stopDone:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Stop did not return within timeout")
		}

		assert.True(t, workCompleted.Load(), "work should have completed")
	})

	t.Run("should stop response readers gracefully", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		readerActive := make(chan struct{}, 1)
		var readerExited atomic.Bool

		mockClient := &testInitiator{
			fnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
				return nil
			},
			fnReceiveTaskResponse: func(ctx context.Context) (orbital.TaskResponse, error) {
				select {
				case readerActive <- struct{}{}:
				default:
				}
				<-ctx.Done()
				readerExited.Store(true)
				return orbital.TaskResponse{}, ctx.Err()
			},
			fnClose: func(_ context.Context) error {
				return nil
			},
		}

		targets := map[string]orbital.TargetManager{
			"target": {Client: mockClient},
		}

		subj, err := orbital.NewManager(repo, mockTaskResolveFunc(), orbital.WithTargets(targets))
		assert.NoError(t, err)

		ctx := t.Context()

		err = subj.Start(ctx)
		assert.NoError(t, err)

		<-readerActive

		err = subj.Stop(ctx)
		assert.NoError(t, err)

		assert.True(t, readerExited.Load(), "response reader should have exited")
	})

	t.Run("should handle stop with multiple workers and targets", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		var closeCallCount atomic.Int32
		var readersStarted atomic.Int32
		allReadersStarted := make(chan struct{})

		createMockClient := func() *testInitiator {
			return &testInitiator{
				fnSendTaskRequest: func(_ context.Context, _ orbital.TaskRequest) error {
					return nil
				},
				fnReceiveTaskResponse: func(ctx context.Context) (orbital.TaskResponse, error) {
					if readersStarted.Add(1) == 3 {
						close(allReadersStarted)
					}
					<-ctx.Done()
					return orbital.TaskResponse{}, ctx.Err()
				},
				fnClose: func(_ context.Context) error {
					closeCallCount.Add(1)
					return nil
				},
			}
		}

		targets := map[string]orbital.TargetManager{
			"target-1": {Client: createMockClient()},
			"target-2": {Client: createMockClient()},
			"target-3": {Client: createMockClient()},
		}

		subj, err := orbital.NewManager(repo, mockTaskResolveFunc(), orbital.WithTargets(targets))
		assert.NoError(t, err)

		ctx := t.Context()

		err = subj.Start(ctx)
		assert.NoError(t, err)

		<-allReadersStarted

		err = subj.Stop(ctx)
		assert.NoError(t, err)

		assert.Equal(t, int32(3), closeCallCount.Load())
	})
}
