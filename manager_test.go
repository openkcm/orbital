//go:build integration
// +build integration

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
)

var errResourceNotFound = errors.New("resource not found")

func TestPrepareJob(t *testing.T) {
	// given
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	subj, _ := orbital.NewManager(repo, mockTaskResolverFunc())

	job := orbital.NewJob("resource-data", []byte("type"))
	_, ok, err := subj.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.False(t, ok)

	// when
	createdJob, err := subj.PrepareJob(ctx, job)

	// then
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, createdJob.ID)

	preparedJob, ok, err := subj.GetJob(ctx, createdJob.ID)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, orbital.JobStatusCreated, preparedJob.Status)
}

func TestNewManagerTaskResolverErr(t *testing.T) {
	t.Run("should not return error if task resolver is defined", func(t *testing.T) {
		// when
		_, err := orbital.NewManager(nil,
			mockTaskResolverFunc(),
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
				name: "for failing confirmation",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{}, errResourceNotFound
				},
				expStatus: orbital.JobStatusCreated,
			},
			{
				name: "for non confirmation",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{Confirmed: false}, nil
				},
				expStatus: orbital.JobStatusCanceled,
			},
			{
				name: "successful",
				confirmFuncResponse: func() (orbital.JobConfirmResult, error) {
					return orbital.JobConfirmResult{Confirmed: true}, nil
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
					mockTaskResolverFunc(),
					orbital.WithJobConfirmFunc(confirmFunc),
				)
				subj.Config.ConfirmJobDelay = 100 * time.Millisecond

				job := orbital.NewJob("", nil)
				jobCreated, err := subj.PrepareJob(ctx, job)
				assert.NoError(t, err)

				// when
				time.Sleep(1 * time.Second)
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

		time.Sleep(time.Second)
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
			return orbital.JobConfirmResult{Confirmed: true}, nil
		}
		subj, _ := orbital.NewManager(repo,
			mockTaskResolverFunc(),
			orbital.WithJobConfirmFunc(confirmFunc),
		)
		subj.Config.ConfirmJobDelay = 100 * time.Millisecond

		// when

		go func() {
			err := orbital.ConfirmJob(subj)(ctx)
			assert.NoError(t, err)
		}()
		assert.Equal(t, "start second retrieval mode list", <-callerChan)
		actJobs, err := orbital.ListRepoJobs(repo)(ctx, orbital.ListJobsQuery{
			RetrievalModeQueue: true,
			CreatedAt:          utcUnix(),
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
				subj, _ := orbital.NewManager(
					repo,
					resolverFunc,
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
		subj, _ := orbital.NewManager(
			repo,
			resolverFunc,
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
			CreatedAt:          utcUnix(),
		})
		assert.NoError(t, err)
		assert.Len(t, actJobs, 1)
		assert.NotEqual(t, createdJob1.ID, actJobs[0].ID)
		callerChan <- "finish taskresolver func"

		// then
		assert.Equal(t, int32(1), isJob1Called.Load())
	})

	t.Run("should update job Status", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name           string
			resolverResult orbital.TaskResolverResult
			expStatus      orbital.JobStatus
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
				name: "to RESOLVING if the all targets are not resolved",
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
		}
		for _, tt := range tts {
			// given
			job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
				Status: orbital.JobStatusConfirmed,
				Data:   make([]byte, 0),
			})
			assert.NoError(t, err)

			resolverFunc := func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
				return tt.resolverResult, nil
			}
			subj, _ := orbital.NewManager(repo,
				resolverFunc,
			)

			// when
			err = orbital.CreateTask(subj)(ctx)

			// then
			assert.NoError(t, err)
			actJob, ok, err := orbital.GetRepoJob(repo)(ctx, job.ID)
			assert.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, tt.expStatus, actJob.Status)
		}
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

		subj, _ := orbital.NewManager(repo,
			resolverFunc,
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
						Data:   []byte(fmt.Sprintf("data-%d", resolverCalled)),
						Type:   fmt.Sprintf("type-%d", resolverCalled),
					},
				},
				Done: resolverCalled == 2,
			}, nil
		}

		subj, _ := orbital.NewManager(repo,
			resolverFunc,
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
				assert.Equal(t, []byte(fmt.Sprintf("data-%d", targetIndex+1)), actTasks[targetIndex].Data)
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
			LastSentAt:        1,
			SentCount:         2,
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
		assert.Equal(t, taskToCreate.LastSentAt, actTasks[0].LastSentAt)
		assert.Equal(t, taskToCreate.SentCount, actTasks[0].SentCount)
		assert.Equal(t, taskToCreate.ReconcileAfterSec, actTasks[0].ReconcileAfterSec)
		assert.Equal(t, taskToCreate.ETag, actTasks[0].ETag)
	})
}

func mockTaskResolverFunc() orbital.TaskResolverFunc {
	return func(_ context.Context, _ orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
		return orbital.TaskResolverResult{}, nil
	}
}

func TestStart(t *testing.T) {
	t.Run("should call JobTerminationEventFunc", func(t *testing.T) {
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobID := uuid.New()
		_, err := orbital.CreateRepoJob(repo)(t.Context(), orbital.Job{ID: jobID})
		assert.NoError(t, err)

		_, err = orbital.CreateRepoJobEvent(repo)(t.Context(), orbital.JobEvent{ID: jobID})
		assert.NoError(t, err)

		subj, _ := orbital.NewManager(repo,
			mockTaskResolverFunc(),
		)
		subj.Config.NotifyWorkerConfig.ExecInterval = 150 * time.Millisecond

		actCalled := 0
		wg := sync.WaitGroup{}
		wg.Add(1)
		subj.JobTerminationEventFunc = func(_ context.Context, _ orbital.Job) error {
			defer wg.Done()
			actCalled++
			return nil
		}
		err = subj.Start(t.Context())
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, 1, actCalled)
	})
}
