package orbital_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/internal/clock"
)

var errTransaction = errors.New("transaction error")

func TestRepoPrepare(t *testing.T) {
	ctx := t.Context()
	db, _ := createSQLStore(t)

	// Check if the jobs table is created
	var jobTableCount int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_tables WHERE tablename='jobs'").Scan(&jobTableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, jobTableCount)

	// Check if the tasks table is created
	var taskTableCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_tables WHERE tablename='tasks'").Scan(&taskTableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, taskTableCount)

	// Check if the job_cursor table is created
	var jobCursorCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pg_tables WHERE tablename='job_cursor'").Scan(&jobCursorCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, jobCursorCount)
}

func TestRepoCreateJob(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	job := orbital.Job{
		Type:   "job-type",
		Data:   []byte("data"),
		Status: "status",
	}
	createdJob, err := orbital.CreateRepoJob(repo)(ctx, job)
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, createdJob.ID)

	fetchedJob, ok, err := orbital.GetRepoJob(repo)(ctx, createdJob.ID)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, job.Type, fetchedJob.Type)
	assert.Equal(t, job.Status, fetchedJob.Status)
	assert.Equal(t, job.Data, fetchedJob.Data)
}

func TestRepoGetJob(t *testing.T) {
	t.Run("should return the job if it exists", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Data:         []byte("data"),
			Type:         "type",
			Status:       "status",
			ErrorMessage: "error-message",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, createdJob.ID)

		fetchedJob, ok, err := orbital.GetRepoJob(repo)(ctx, createdJob.ID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, createdJob.ID, fetchedJob.ID)
		assert.Equal(t, createdJob.Data, fetchedJob.Data)
		assert.Equal(t, createdJob.Type, fetchedJob.Type)
		assert.Equal(t, createdJob.Status, fetchedJob.Status)
		assert.Equal(t, createdJob.ErrorMessage, fetchedJob.ErrorMessage)
	})

	t.Run("should return false if the job ID is not there", func(t *testing.T) {
		ctx := t.Context()
		_, store := createSQLStore(t)
		repo := orbital.NewRepository(store)

		fetchedJob, ok, err := orbital.GetRepoJob(repo)(ctx, uuid.New())
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, uuid.Nil, fetchedJob.ID)
	})
}

func TestRepoUpdateJobState(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	job, err := orbital.CreateRepoJob(repo)(t.Context(), orbital.Job{
		ID:     [16]byte{},
		Data:   []byte("resource-data"),
		Type:   "type",
		Status: orbital.JobStatusCreated,
	})
	assert.NoError(t, err)

	job.Status = orbital.JobStatusConfirmed

	time.Sleep(1 * time.Microsecond) // Ensure the updated_at timestamp is different
	err = orbital.UpdateRepoJob(repo)(ctx, job)
	assert.NoError(t, err)

	updatedJob, ok, err := orbital.GetRepoJob(repo)(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, orbital.JobStatusConfirmed, updatedJob.Status)
	assert.Equal(t, job.CreatedAt, updatedJob.CreatedAt)
	assert.Equal(t, job.Data, updatedJob.Data)
	assert.Equal(t, job.Type, updatedJob.Type)
	assert.NotEqual(t, job.UpdatedAt, updatedJob.UpdatedAt)
}

func TestRepoListJobs(t *testing.T) {
	t.Run("should list jobs", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobs := make([]orbital.Job, 2)
		now := clock.NowUnixNano()
		for i := range jobs {
			jobs[i].Status = orbital.JobStatusCreated
			jobs[i].CreatedAt = now
			jobs[i].Data = []byte("data")
			job, err := orbital.CreateRepoJob(repo)(ctx, jobs[i])
			assert.NoError(t, err)
			jobs[i].ID = job.ID
		}
		// mirrors the order of jobs when queried
		sort.Slice(jobs, func(i, j int) bool {
			return jobs[i].ID.String() < jobs[j].ID.String()
		})

		tests := []struct {
			name    string
			query   orbital.ListJobsQuery
			expJobs []orbital.Job
		}{
			{
				name:    "List created jobs",
				query:   orbital.ListJobsQuery{Status: orbital.JobStatusCreated, CreatedAt: now, Limit: 10},
				expJobs: jobs,
			},
			{
				name:    "List confirmed jobs",
				query:   orbital.ListJobsQuery{Status: orbital.JobStatusConfirmed, CreatedAt: now, Limit: 10},
				expJobs: []orbital.Job{},
			},
			{
				name:    "List jobs with limit",
				query:   orbital.ListJobsQuery{Status: orbital.JobStatusCreated, CreatedAt: now, Limit: 1},
				expJobs: []orbital.Job{jobs[0]},
			},
			{
				name:    "List jobs with delayed createdAt",
				query:   orbital.ListJobsQuery{Status: orbital.JobStatusCreated, CreatedAt: now - int64(30*time.Second), Limit: 10},
				expJobs: []orbital.Job{},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				jobs, err := orbital.ListRepoJobs(repo)(ctx, tt.query)
				assert.NoError(t, err)
				assert.Len(t, jobs, len(tt.expJobs))
				for i, job := range jobs {
					assert.Equal(t, tt.expJobs[i].ID, job.ID)
				}
			})
		}
	})

	t.Run("should fetch different records in a transaction while queue mode is enabled", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJobs := make([]orbital.Job, 0, 2)
		jobToCreate := make([]orbital.Job, 2)
		now := clock.NowUnixNano()
		for i := range jobToCreate {
			now++
			jobToCreate[i].Status = orbital.JobStatusCreated
			jobToCreate[i].CreatedAt = now
			jobToCreate[i].Data = []byte("")
			createdJob, err := orbital.CreateRepoJob(repo)(ctx, jobToCreate[i])
			assert.NoError(t, err)
			createdJobs = append(createdJobs, createdJob)
		}
		transactor1 := make(chan string)
		defer close(transactor1)

		transactor2 := make(chan string)
		defer close(transactor2)

		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			assert.Equal(t, "transactor 1 start", <-transactor1)
			err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
				defer wg.Done()
				expJobs, err := orbital.ListRepoJobs(&repo)(ctx, orbital.ListJobsQuery{
					Status:             orbital.JobStatusCreated,
					CreatedAt:          now,
					Limit:              1,
					RetrievalModeQueue: true,
				})
				transactor1 <- "transactor 1 wait"
				assert.Equal(t, "transactor 1 continue", <-transactor1)
				assert.NoError(t, err)
				assert.Len(t, expJobs, 1)
				// should fetch the first job
				assert.Equal(t, createdJobs[0], expJobs[0])
				return nil
			})
			assert.NoError(t, err)
		}()

		go func() {
			assert.Equal(t, "transactor 2 start", <-transactor2)
			err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
				defer wg.Done()
				expJobs, err := orbital.ListRepoJobs(&repo)(ctx, orbital.ListJobsQuery{
					Status:             orbital.JobStatusCreated,
					CreatedAt:          now,
					Limit:              1,
					RetrievalModeQueue: true,
				})
				assert.NoError(t, err)
				assert.Len(t, expJobs, 1)
				// should fetch the second job
				assert.Equal(t, createdJobs[1], expJobs[0])
				transactor2 <- "transactor 2 finished"
				return nil
			})
			assert.NoError(t, err)
		}()

		transactor1 <- "transactor 1 start"
		assert.Equal(t, "transactor 1 wait", <-transactor1)
		transactor2 <- "transactor 2 start"
		assert.Equal(t, "transactor 2 finished", <-transactor2)
		transactor1 <- "transactor 1 continue"

		wg.Wait()
	})
}

func TestRepoCreateTasks(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	jobID := uuid.New()
	taskType := "type"

	tasks := make([]orbital.Task, 0, 3)
	for index := range 3 {
		tasks = append(tasks, orbital.Task{
			JobID:          jobID,
			Type:           taskType,
			WorkingState:   fmt.Append([]byte("working-state-"), index),
			ReconcileCount: int64(index),
			ETag:           fmt.Sprintf("etag-%v", index),
			Status:         orbital.TaskStatusCreated,
			Target:         fmt.Sprintf("target-%v", index),
			ErrorMessage:   fmt.Sprintf("error-message-%v", index),
		})
	}

	taskIDs, err := orbital.CreateRepoTasks(repo)(ctx, tasks)
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, taskIDs)

	for index, taskID := range taskIDs {
		fetchedTask, ok, err := orbital.GetRepoTask(repo)(t.Context(), taskID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, jobID, fetchedTask.JobID)
		assert.Equal(t, taskType, fetchedTask.Type)
		assert.Equal(t, fmt.Sprintf("working-state-%v", index), string(fetchedTask.WorkingState))
		assert.Equal(t, int64(index), fetchedTask.ReconcileCount)
		assert.Equal(t, fmt.Sprintf("etag-%v", index), fetchedTask.ETag)
		assert.Equal(t, fmt.Sprintf("error-message-%v", index), fetchedTask.ErrorMessage)
		assert.Equal(t, orbital.TaskStatusCreated, fetchedTask.Status)
	}
}

func TestRepoListTasks(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	jobID1 := uuid.New()
	jobID2 := uuid.New()
	jobID3 := uuid.New()
	tasks := []orbital.Task{
		{
			JobID:          jobID1,
			CreatedAt:      1,
			WorkingState:   []byte("working-state-1"),
			ReconcileCount: 2,
			ETag:           "etag-1",
			Status:         "created",
			Target:         "target-1",
		},
		{
			JobID:          jobID2,
			CreatedAt:      2,
			WorkingState:   []byte("working-state-2"),
			ReconcileCount: 3,
			ETag:           "etag-2",
			Status:         "created",
			Target:         "target-2",
		},
		{
			JobID:          jobID3,
			CreatedAt:      3,
			WorkingState:   []byte("working-state-3"),
			ReconcileCount: 3,
			ETag:           "etag-3",
			Status:         "resolving",
			Target:         "target-3",
		},
	}

	ids, err := orbital.CreateRepoTasks(repo)(ctx, tasks)
	assert.NoError(t, err)

	tts := []struct {
		name  string
		query orbital.ListTasksQuery
		expID []uuid.UUID
	}{
		{
			name:  "should fetch all created tasks",
			query: orbital.ListTasksQuery{Status: orbital.TaskStatus("created"), Limit: 10},
			expID: []uuid.UUID{ids[0], ids[1]},
		},
		{
			name:  "should fetch only 1 created tasks",
			query: orbital.ListTasksQuery{Status: orbital.TaskStatus("created"), Limit: 1},
			expID: []uuid.UUID{ids[0]},
		},
		{
			name:  "should fetch no tasks for an unknown status",
			query: orbital.ListTasksQuery{Status: orbital.TaskStatus("unknown"), Limit: 10},
			expID: []uuid.UUID{},
		},
		{
			name:  "should fetch all tasks if the state is not present",
			query: orbital.ListTasksQuery{Limit: 10},
			expID: []uuid.UUID{ids[0], ids[1], ids[2]},
		},
		{
			name:  "should fetch a task with a JobID",
			query: orbital.ListTasksQuery{Limit: 10, JobID: jobID2},
			expID: []uuid.UUID{ids[1]},
		},
		{
			name:  "should fetch the first task with only limit",
			query: orbital.ListTasksQuery{Limit: 1},
			expID: []uuid.UUID{ids[0]},
		},
		{
			name:  "should fetch all task with a status",
			query: orbital.ListTasksQuery{Limit: 10, Status: "resolving"},
			expID: []uuid.UUID{ids[2]},
		},
		{
			name:  "should fetch all tasks by statuses",
			query: orbital.ListTasksQuery{Limit: 10, StatusIn: []orbital.TaskStatus{"created", "resolving"}},
			expID: []uuid.UUID{ids[0], ids[1], ids[2]},
		},
		{
			name:  "should fetch all tasks that are ready to reconcile",
			query: orbital.ListTasksQuery{Limit: 10, IsReconcileReady: true},
			expID: []uuid.UUID{ids[0], ids[1], ids[2]},
		},
	}
	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			// when
			tasks, err = orbital.ListRepoTasks(repo)(t.Context(),
				tt.query,
			)

			// then
			assert.NoError(t, err)

			assert.Len(t, tasks, len(tt.expID))
			for index := range tt.expID {
				assert.Equal(t, tt.expID[index], tasks[index].ID)
			}
		})
	}
}

func TestRepoCreateJobCursor(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	expCursor := orbital.JobCursor{
		ID:     uuid.New(),
		Cursor: "cursor",
	}

	jobCursor, err := orbital.CreateRepoJobCursor(repo)(ctx, expCursor)
	assert.NoError(t, err)
	assert.Equal(t, expCursor.ID, jobCursor.ID)

	actCursor, found, err := orbital.GetRepoJobCursor(repo)(ctx, jobCursor.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, expCursor.ID, actCursor.ID)
	assert.Equal(t, expCursor.Cursor, actCursor.Cursor)
}

func TestRepoGetJobCursor(t *testing.T) {
	t.Run("should return the jobcursor if it exists", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJobCursor, err := orbital.CreateRepoJobCursor(repo)(ctx, orbital.JobCursor{
			Cursor: "cursor",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, createdJobCursor.ID)

		fetchedCursor, ok, err := orbital.GetRepoJobCursor(repo)(ctx, createdJobCursor.ID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, createdJobCursor.ID, fetchedCursor.ID)
	})
	t.Run("should return false if the job ID is not there", func(t *testing.T) {
		ctx := t.Context()
		_, store := createSQLStore(t)
		repo := orbital.NewRepository(store)

		actCursor, found, err := orbital.GetRepoJobCursor(repo)(ctx, uuid.New())
		assert.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, orbital.JobCursor{}, actCursor)
	})
}

func TestRepoUpdateJobCursor(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	jobCursor, err := orbital.CreateRepoJobCursor(repo)(t.Context(), orbital.JobCursor{
		ID:     uuid.New(),
		Cursor: "cursor",
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Microsecond) // Ensure the updated_at timestamp is different
	jobCursor.Cursor = "cursor-1"
	err = orbital.UpdateRepoJobCursor(repo)(ctx, jobCursor)
	assert.NoError(t, err)

	actCursor, _, err := orbital.GetRepoJobCursor(repo)(ctx, jobCursor.ID)
	assert.NoError(t, err)
	assert.Equal(t, orbital.TaskResolverCursor("cursor-1"), actCursor.Cursor)
	assert.Equal(t, jobCursor.CreatedAt, actCursor.CreatedAt)
	assert.NotEqual(t, jobCursor.UpdatedAt, actCursor.UpdatedAt)
}

func TestRepoTransaction(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
		job, err := orbital.CreateRepoJob(&repo)(ctx, orbital.Job{
			Data: []byte("data"),
		})
		if err != nil {
			return err
		}

		job.Status = orbital.JobStatusConfirmed
		return orbital.UpdateRepoJob(&repo)(ctx, job)
	})
	assert.NoError(t, err)

	jobs, err := orbital.ListRepoJobs(repo)(ctx, orbital.ListJobsQuery{
		Status:    orbital.JobStatusConfirmed,
		CreatedAt: clock.NowUnixNano(),
		Limit:     10,
	})
	assert.NoError(t, err)
	assert.Len(t, jobs, 1)
	assert.Equal(t, orbital.JobStatusConfirmed, jobs[0].Status)
}

func TestRepoTransactionError(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
		_, err := orbital.CreateRepoJob(&repo)(ctx, orbital.Job{
			Data: make([]byte, 0),
		})
		if err != nil {
			return err
		}
		return errTransaction
	})
	assert.Error(t, err)
	assert.Equal(t, errTransaction, err)

	jobs, err := orbital.ListRepoJobs(repo)(t.Context(), orbital.ListJobsQuery{
		Status:    orbital.JobStatusCreated,
		CreatedAt: clock.NowUnixNano(),
		Limit:     10,
	})
	assert.NoError(t, err)
	assert.Empty(t, jobs)
}

func TestRepoTransactionRaceCondition(t *testing.T) {
	t.Run("should not fetch records in RetrievalMode which are being updated in a transaction", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		transactor1 := make(chan string)
		defer close(transactor1)

		transactor2 := make(chan string)
		defer close(transactor2)

		wg := sync.WaitGroup{}
		wg.Add(2)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Type:   "job-type",
			Status: orbital.JobStatusCreated,
		})
		assert.NoError(t, err)
		/**
			  The first transaction starts and updates the status of an existing job to "aborted" with RetrievalMoedQueue `false`
			  but it does not commit immediately. Instead, it signals that it is waiting, allowing the second transactionany  to start.
		    The second transaction attempts to list jobs with the "created" status with RetrievalMode `true`, expecting to see none.
			  **/
		go func() {
			assert.Equal(t, "1st transaction starts", <-transactor1)
			defer wg.Done()
			err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
				createdJob.Status = orbital.JobStatusResolveCanceled
				err := orbital.UpdateRepoJob(&repo)(ctx, createdJob)
				assert.NoError(t, err)

				transactor1 <- "1st transaction waits"
				assert.Equal(t, "1st transaction continues", <-transactor1)

				return nil
			})
			assert.NoError(t, err)
			transactor1 <- "1st transaction ends"
		}()

		go func() {
			assert.Equal(t, "2nd transaction starts", <-transactor2)
			defer wg.Done()

			err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
				// should read initial created job
				result, err := orbital.ListRepoJobs(&repo)(ctx, orbital.ListJobsQuery{
					Status:             orbital.JobStatusCreated,
					RetrievalModeQueue: true,
					CreatedAt:          clock.NowUnixNano(),
					Limit:              10,
				})
				assert.NoError(t, err)
				assert.Empty(t, result)

				transactor2 <- "2nd transaction finished"
				return err
			})
			assert.NoError(t, err)
		}()

		transactor1 <- "1st transaction starts"
		assert.Equal(t, "1st transaction waits", <-transactor1)
		transactor2 <- "2nd transaction starts"
		assert.Equal(t, "2nd transaction finished", <-transactor2)
		transactor1 <- "1st transaction continues"
		assert.Equal(t, "1st transaction ends", <-transactor1)

		wg.Wait()

		// should fetch the updated job for the first transaction
		jobs, err := orbital.ListRepoJobs(repo)(ctx, orbital.ListJobsQuery{
			Status:    orbital.JobStatusResolveCanceled,
			CreatedAt: clock.NowUnixNano(),
			Limit:     10,
		})
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
	})
	t.Run("transaction should timeout gracefully", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		transactor1 := make(chan string)
		defer close(transactor1)

		transactor2 := make(chan string)
		defer close(transactor2)

		wg := sync.WaitGroup{}
		wg.Add(2)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Type:   "job-type",
			Status: orbital.JobStatusCreated,
		})
		assert.NoError(t, err)
		go func() {
			defer wg.Done()
			assert.Equal(t, "1st transaction starts", <-transactor1)
			ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()
			err := orbital.Transaction(repo)(ctxTimeout, func(ctx context.Context, repo orbital.Repository) error {
				createdJob.Status = orbital.JobStatusResolveCanceled
				err := orbital.UpdateRepoJob(&repo)(ctx, createdJob)
				assert.NoError(t, err)

				transactor1 <- "1st transaction waits"
				assert.Equal(t, "1st transaction continues", <-transactor1)

				return nil
			})
			// error should happen cause of timeout
			assert.Error(t, err)
			transactor1 <- "1st transaction ends"
		}()

		go func() {
			assert.Equal(t, "2nd transaction starts", <-transactor2)
			defer wg.Done()

			err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
				// should update the job as cancel
				createdJob.Status = orbital.JobStatusConfirmCanceled
				err := orbital.UpdateRepoJob(&repo)(ctx, createdJob)
				assert.NoError(t, err)

				transactor2 <- "2nd transaction finished"
				return err
			})
			assert.NoError(t, err)
		}()

		transactor1 <- "1st transaction starts"
		assert.Equal(t, "1st transaction waits", <-transactor1)
		transactor2 <- "2nd transaction starts"
		assert.Equal(t, "2nd transaction finished", <-transactor2)
		transactor1 <- "1st transaction continues"
		assert.Equal(t, "1st transaction ends", <-transactor1)

		wg.Wait()

		// should fetch the updated job for the second transaction
		jobs, err := orbital.ListRepoJobs(repo)(ctx, orbital.ListJobsQuery{
			Status:    orbital.JobStatusConfirmCanceled,
			CreatedAt: clock.NowUnixNano(),
			Limit:     10,
		})
		assert.NoError(t, err)
		assert.Len(t, jobs, 1)
	})
}

func TestRepoCreateJobEvent(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	jobEvent := orbital.JobEvent{
		ID:         uuid.New(),
		IsNotified: true,
	}
	createdJobEvent, err := orbital.CreateRepoJobEvent(repo)(ctx, jobEvent)
	assert.NoError(t, err)
	assert.NotEqual(t, uuid.Nil, createdJobEvent.ID)

	fetchedJobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: jobEvent.ID})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, jobEvent.IsNotified, fetchedJobEvent.IsNotified)
}

func TestRepoGetJobEvent(t *testing.T) {
	t.Run("should return the jobevent if it exists", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJobEvent, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{
			IsNotified: true,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, createdJobEvent.ID)

		fetchedJobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJobEvent.ID})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, createdJobEvent.ID, fetchedJobEvent.ID)
		assert.Equal(t, createdJobEvent.IsNotified, fetchedJobEvent.IsNotified)
	})

	t.Run("should return false if the job ID is not there", func(t *testing.T) {
		ctx := t.Context()
		_, store := createSQLStore(t)
		repo := orbital.NewRepository(store)

		fetchedJobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: uuid.New()})
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, uuid.Nil, fetchedJobEvent.ID)
	})
}

func TestRepoUpdateJobEvent(t *testing.T) {
	ctx := t.Context()
	db, store := createSQLStore(t)
	defer clearTables(t, db)
	repo := orbital.NewRepository(store)

	jobEvent, err := orbital.CreateRepoJobEvent(repo)(t.Context(), orbital.JobEvent{
		IsNotified: false,
	})
	assert.NoError(t, err)

	jobEvent.IsNotified = true

	time.Sleep(1 * time.Microsecond) // Ensure the updated_at timestamp is different
	err = orbital.UpdateRepoJobEvent(repo)(ctx, jobEvent)
	assert.NoError(t, err)

	updatedJobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: jobEvent.ID})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, jobEvent.IsNotified, updatedJobEvent.IsNotified)
}

func TestRepoGetJobForUpdate(t *testing.T) {
	t.Run("should return the job if it exists", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		job, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Data:         []byte("data"),
			Type:         "type",
			Status:       "status",
			ErrorMessage: "error-message",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, job.ID)

		fetchedJob, ok, err := orbital.GetRepoJobForUpdate(repo)(ctx, job.ID)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, job.ID, fetchedJob.ID)
		assert.Equal(t, job.Data, fetchedJob.Data)
		assert.Equal(t, job.Type, fetchedJob.Type)
		assert.Equal(t, job.Status, fetchedJob.Status)
		assert.Equal(t, job.ErrorMessage, fetchedJob.ErrorMessage)
	})

	t.Run("should return false if the job ID is not there", func(t *testing.T) {
		ctx := t.Context()
		_, store := createSQLStore(t)
		repo := orbital.NewRepository(store)

		fetchedJob, ok, err := orbital.GetRepoJobForUpdate(repo)(ctx, uuid.New())
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, uuid.Nil, fetchedJob.ID)
	})

	t.Run("transaction should get updated job after waiting for other transaction to finish", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		transactor1 := make(chan string)
		defer close(transactor1)

		transactor2 := make(chan string)
		defer close(transactor2)

		wg := sync.WaitGroup{}
		wg.Add(2)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Type:   "job-type",
			Status: orbital.JobStatusCreated,
		})
		assert.NoError(t, err)
		go func() {
			defer wg.Done()
			assert.Equal(t, "1st transaction starts", <-transactor1)
			err := orbital.Transaction(repo)(t.Context(), func(ctx context.Context, repo orbital.Repository) error {
				fetchJob, ok, err := orbital.GetRepoJobForUpdate(&repo)(ctx, createdJob.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, fetchJob.ID)

				createdJob.Data = []byte("updated-data")

				err = orbital.UpdateRepoJob(&repo)(ctx, createdJob)
				assert.NoError(t, err)

				transactor1 <- "1st transaction waits"
				return nil
			})
			assert.NoError(t, err)
		}()

		go func() {
			assert.Equal(t, "2nd transaction starts", <-transactor2)
			defer wg.Done()

			err := orbital.Transaction(repo)(ctx, func(ctx context.Context, repo orbital.Repository) error {
				fetchJob, ok, err := orbital.GetRepoJobForUpdate(&repo)(ctx, createdJob.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, fetchJob.ID)
				assert.Equal(t, []byte("updated-data"), fetchJob.Data)

				return nil
			})
			assert.NoError(t, err)
		}()

		transactor1 <- "1st transaction starts"
		time.Sleep(100 * time.Millisecond)
		transactor2 <- "2nd transaction starts"

		assert.Equal(t, "1st transaction waits", <-transactor1)
		wg.Wait()
	})
}
