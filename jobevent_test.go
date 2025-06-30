package orbital_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestRecordJobEvent(t *testing.T) {
	t.Run("should not record job event if job status ", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                 string
			jobStatus            orbital.JobStatus
			jobDoneEventFunc     orbital.JobTerminatedEventFunc
			jobFailedEventFunc   orbital.JobTerminatedEventFunc
			jobCanceledEventFunc orbital.JobTerminatedEventFunc
		}{
			{
				name:                 "is not terminal status JobStatusCreated",
				jobStatus:            orbital.JobStatusCreated,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is not terminal status JobStatusConfirmed",
				jobStatus:            orbital.JobStatusConfirmed,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is not terminal status JobStatusResolving",
				jobStatus:            orbital.JobStatusResolving,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is not terminal status JobStatusReady",
				jobStatus:            orbital.JobStatusReady,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is not terminal status JobStatusProcessing",
				jobStatus:            orbital.JobStatusProcessing,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:             "is a terminal status JobStatusDone and jobDoneEventFunc is nil",
				jobStatus:        orbital.JobStatusDone,
				jobDoneEventFunc: nil,
			},
			{
				name:               "is a terminal status JobStatusFailed and jobFailedEventFunc is nil",
				jobStatus:          orbital.JobStatusFailed,
				jobFailedEventFunc: nil,
			},
			{
				name:                 "is a terminal status JobStatusResolveCanceled and jobCanceledEventFunc is nil",
				jobStatus:            orbital.JobStatusResolveCanceled,
				jobCanceledEventFunc: nil,
			},
			{
				name:                 "is a terminal status JobStatusConfirmCanceled and jobCanceledEventFunc is nil",
				jobStatus:            orbital.JobStatusConfirmCanceled,
				jobCanceledEventFunc: nil,
			},
			{
				name:                 "is a terminal status JobStatusUserCanceled and jobCanceledEventFunc is nil",
				jobStatus:            orbital.JobStatusUserCanceled,
				jobCanceledEventFunc: nil,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(tt.jobDoneEventFunc),
					orbital.WithJobFailedEventFunc(tt.jobFailedEventFunc),
					orbital.WithJobCanceledEventFunc(tt.jobCanceledEventFunc),
				)
				jobID := uuid.New()
				// when
				err := orbital.RecordJobTerminatedEvent(subj)(ctx, *repo, orbital.Job{ID: jobID, Status: tt.jobStatus})
				assert.NoError(t, err)

				// then
				_, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: jobID})
				assert.NoError(t, err)
				assert.False(t, ok)
			})
		}
	})

	t.Run("should record job event if job status ", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                 string
			jobStatus            orbital.JobStatus
			jobDoneEventFunc     orbital.JobTerminatedEventFunc
			jobFailedEventFunc   orbital.JobTerminatedEventFunc
			jobCanceledEventFunc orbital.JobTerminatedEventFunc
		}{
			{
				name:             "is a terminal status JobStatusDone and jobDoneEventFunc is not nil",
				jobStatus:        orbital.JobStatusDone,
				jobDoneEventFunc: mockTerminatedFunc(),
			},
			{
				name:               "is a terminal status JobStatusFailed and jobFailedEventFunc is not nil",
				jobStatus:          orbital.JobStatusFailed,
				jobFailedEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is a terminal status JobStatusResolveCanceled and jobCanceledEventFunc is not nil",
				jobStatus:            orbital.JobStatusResolveCanceled,
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is a terminal status JobStatusConfirmCanceled and jobCanceledEventFunc is not nil",
				jobStatus:            orbital.JobStatusConfirmCanceled,
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "is a terminal status JobStatusUserCanceled and jobCanceledEventFunc is not nil",
				jobStatus:            orbital.JobStatusUserCanceled,
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(tt.jobDoneEventFunc),
					orbital.WithJobFailedEventFunc(tt.jobFailedEventFunc),
					orbital.WithJobCanceledEventFunc(tt.jobCanceledEventFunc),
				)
				jobID := uuid.New()

				// when
				err := orbital.RecordJobTerminatedEvent(subj)(ctx, *repo, orbital.Job{ID: jobID, Status: tt.jobStatus})
				assert.NoError(t, err)

				// then
				jobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: jobID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, jobID, jobEvent.ID)
				assert.False(t, jobEvent.IsNotified)
			})
		}
	})

	t.Run("should set jobevent `isNotified` to false when an existing jobevent has `isNotified` as true", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                 string
			jobStatus            orbital.JobStatus
			jobDoneEventFunc     orbital.JobTerminatedEventFunc
			jobFailedEventFunc   orbital.JobTerminatedEventFunc
			jobCanceledEventFunc orbital.JobTerminatedEventFunc
		}{
			{
				name:             "for JobStatusDone",
				jobStatus:        orbital.JobStatusDone,
				jobDoneEventFunc: mockTerminatedFunc(),
			},
			{
				name:               "for JobStatusFailed",
				jobStatus:          orbital.JobStatusFailed,
				jobFailedEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "for JobStatusResolveCanceled",
				jobStatus:            orbital.JobStatusResolveCanceled,
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "for JobStatusConfirmCanceled",
				jobStatus:            orbital.JobStatusConfirmCanceled,
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "for JobStatusUserCanceled",
				jobStatus:            orbital.JobStatusUserCanceled,
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(tt.jobDoneEventFunc),
					orbital.WithJobFailedEventFunc(tt.jobFailedEventFunc),
					orbital.WithJobCanceledEventFunc(tt.jobCanceledEventFunc),
				)
				jobID := uuid.New()

				// create a job event with isNotified = true
				_, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: jobID, IsNotified: true})
				assert.NoError(t, err)

				// when
				err = orbital.RecordJobTerminatedEvent(subj)(ctx, *repo, orbital.Job{ID: jobID, Status: tt.jobStatus})
				assert.NoError(t, err)

				// then
				jobEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: jobID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, jobID, jobEvent.ID)
				assert.False(t, jobEvent.IsNotified)
			})
		}
	})
}

func TestSendJobEvent(t *testing.T) {
	t.Run("should not send event if there are no jobevents", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobTerminationCalled := 0
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobDoneEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					jobTerminationCalled++
					return nil
				}),
			orbital.WithJobCanceledEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					jobTerminationCalled++
					return nil
				}),
			orbital.WithJobFailedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					jobTerminationCalled++
					return nil
				}),
		)

		// when
		err := orbital.SendJobTerminatedEvent(subj)(ctx)

		// then
		assert.NoError(t, err)
		assert.Equal(t, 0, jobTerminationCalled)
	})

	t.Run("should return error if there are no jobs for the jobevent", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		_, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{IsNotified: false})
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc())

		// when
		err = orbital.SendJobTerminatedEvent(subj)(ctx)

		// then
		assert.Error(t, err)
	})

	t.Run("should send event if there are jobevents for the job with terminal status", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                        string
			jobStatus                   orbital.JobStatus
			expCallJobDoneEventFunc     int
			expCallJobFailedEventFunc   int
			expCallJobCanceledEventFunc int
		}{
			{
				name:                    "JobStatusDone",
				jobStatus:               orbital.JobStatusDone,
				expCallJobDoneEventFunc: 1,
			},
			{
				name:                      "JobStatusFailed",
				jobStatus:                 orbital.JobStatusFailed,
				expCallJobFailedEventFunc: 1,
			},
			{
				name:                        "JobStatusUserCanceled",
				jobStatus:                   orbital.JobStatusUserCanceled,
				expCallJobCanceledEventFunc: 1,
			},
			{
				name:                        "JobStatusConfirmCanceled",
				jobStatus:                   orbital.JobStatusConfirmCanceled,
				expCallJobCanceledEventFunc: 1,
			},
			{
				name:                        "JobStatusResolveCanceled",
				jobStatus:                   orbital.JobStatusResolveCanceled,
				expCallJobCanceledEventFunc: 1,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Data:         []byte("something"),
					Type:         "type",
					Status:       tt.jobStatus,
					ErrorMessage: "error",
				})
				assert.NoError(t, err)

				_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
				assert.NoError(t, err)

				var actCallJobFailedEventFunc, actCallJobDoneEventFunc, actCallJobCanceledEventFunc int
				var actSendJob orbital.Job
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(
						func(_ context.Context, job orbital.Job) error {
							actCallJobDoneEventFunc++
							actSendJob = job
							return nil
						}),
					orbital.WithJobCanceledEventFunc(
						func(_ context.Context, job orbital.Job) error {
							actCallJobCanceledEventFunc++
							actSendJob = job
							return nil
						}),
					orbital.WithJobFailedEventFunc(
						func(_ context.Context, job orbital.Job) error {
							actCallJobFailedEventFunc++
							actSendJob = job
							return nil
						}),
				)

				// when
				err = orbital.SendJobTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				assert.Equal(t, createdJob, actSendJob)
				assert.Equal(t, tt.expCallJobDoneEventFunc, actCallJobDoneEventFunc)
				assert.Equal(t, tt.expCallJobFailedEventFunc, actCallJobFailedEventFunc)
				assert.Equal(t, tt.expCallJobCanceledEventFunc, actCallJobCanceledEventFunc)
				assert.NoError(t, clearJobEventTable(ctx, db))
			})
		}
	})

	t.Run("should send event and update jobevent `isNotified` to true", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name      string
			jobStatus orbital.JobStatus
		}{
			{
				name:      "JobStatusDone",
				jobStatus: orbital.JobStatusDone,
			},
			{
				name:      "JobStatusFailed",
				jobStatus: orbital.JobStatusFailed,
			},
			{
				name:      "JobStatusUserCanceled",
				jobStatus: orbital.JobStatusUserCanceled,
			},
			{
				name:      "JobStatusConfirmCanceled",
				jobStatus: orbital.JobStatusConfirmCanceled,
			},
			{
				name:      "JobStatusResolveCanceled",
				jobStatus: orbital.JobStatusResolveCanceled,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Data:         []byte("something"),
					Type:         "type",
					Status:       tt.jobStatus,
					ErrorMessage: "error",
				})
				assert.NoError(t, err)

				_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return nil
						}),
					orbital.WithJobCanceledEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return nil
						}),
					orbital.WithJobFailedEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return nil
						}),
				)

				// when
				err = orbital.SendJobTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJob.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, actEvent.ID)
				assert.True(t, actEvent.IsNotified)
				assert.NoError(t, clearJobEventTable(ctx, db))
			})
		}
	})

	t.Run("should not update jobevent `isNotified` to true if the Job terminal event func return an error", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name      string
			jobStatus orbital.JobStatus
		}{
			{
				name:      "JobStatusDone",
				jobStatus: orbital.JobStatusDone,
			},
			{
				name:      "JobStatusFailed",
				jobStatus: orbital.JobStatusFailed,
			},
			{
				name:      "JobStatusUserCanceled",
				jobStatus: orbital.JobStatusUserCanceled,
			},
			{
				name:      "JobStatusConfirmCanceled",
				jobStatus: orbital.JobStatusConfirmCanceled,
			},
			{
				name:      "JobStatusResolveCanceled",
				jobStatus: orbital.JobStatusResolveCanceled,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Data:         []byte("something"),
					Type:         "type",
					Status:       tt.jobStatus,
					ErrorMessage: "error",
				})
				assert.NoError(t, err)

				_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return assert.AnError // simulate an error
						}),
					orbital.WithJobCanceledEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return assert.AnError // simulate an error
						}),
					orbital.WithJobFailedEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return assert.AnError // simulate an error
						}),
				)

				// when
				err = orbital.SendJobTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJob.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, actEvent.ID)
				assert.False(t, actEvent.IsNotified)
				assert.NoError(t, clearJobEventTable(ctx, db))
			})
		}
	})

	t.Run("should update jobevent updateAt to latest timestamp if the Job terminal eventFunc return an error", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name      string
			jobStatus orbital.JobStatus
		}{
			{
				name:      "JobStatusDone",
				jobStatus: orbital.JobStatusDone,
			},
			{
				name:      "JobStatusFailed",
				jobStatus: orbital.JobStatusFailed,
			},
			{
				name:      "JobStatusUserCanceled",
				jobStatus: orbital.JobStatusUserCanceled,
			},
			{
				name:      "JobStatusConfirmCanceled",
				jobStatus: orbital.JobStatusConfirmCanceled,
			},
			{
				name:      "JobStatusResolveCanceled",
				jobStatus: orbital.JobStatusResolveCanceled,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Data:         []byte("something"),
					Type:         "type",
					Status:       tt.jobStatus,
					ErrorMessage: "error",
				})
				assert.NoError(t, err)

				createdEvent, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return assert.AnError // simulate an error
						}),
					orbital.WithJobCanceledEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return assert.AnError // simulate an error
						}),
					orbital.WithJobFailedEventFunc(
						func(_ context.Context, _ orbital.Job) error {
							return assert.AnError // simulate an error
						}),
				)

				// when
				time.Sleep(1 * time.Second) // ensure the updateAt will changes
				err = orbital.SendJobTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJob.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, actEvent.ID)
				assert.False(t, actEvent.IsNotified)
				assert.Less(t, createdEvent.UpdatedAt, actEvent.UpdatedAt)
				assert.NoError(t, clearJobEventTable(ctx, db))
			})
		}
	})

	t.Run("should not sent event if there are no event func configured", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                 string
			jobStatus            orbital.JobStatus
			jobDoneEventFunc     orbital.JobTerminatedEventFunc
			jobFailedEventFunc   orbital.JobTerminatedEventFunc
			jobCanceledEventFunc orbital.JobTerminatedEventFunc
		}{
			{
				name:                 "JobStatusDone",
				jobStatus:            orbital.JobStatusDone,
				jobDoneEventFunc:     nil,
				jobCanceledEventFunc: mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
			},
			{
				name:                 "JobStatusFailed",
				jobStatus:            orbital.JobStatusFailed,
				jobFailedEventFunc:   nil,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobCanceledEventFunc: mockTerminatedFunc(),
			},
			{
				name:                 "JobStatusUserCanceled",
				jobStatus:            orbital.JobStatusUserCanceled,
				jobCanceledEventFunc: nil,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
			},
			{
				name:                 "JobStatusConfirmCanceled",
				jobStatus:            orbital.JobStatusConfirmCanceled,
				jobCanceledEventFunc: nil,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
			},
			{
				name:                 "JobStatusResolveCanceled",
				jobStatus:            orbital.JobStatusResolveCanceled,
				jobCanceledEventFunc: nil,
				jobDoneEventFunc:     mockTerminatedFunc(),
				jobFailedEventFunc:   mockTerminatedFunc(),
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Data:         []byte("something"),
					Type:         "type",
					Status:       tt.jobStatus,
					ErrorMessage: "error",
				})
				assert.NoError(t, err)

				_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(tt.jobDoneEventFunc),
					orbital.WithJobCanceledEventFunc(tt.jobCanceledEventFunc),
					orbital.WithJobFailedEventFunc(tt.jobFailedEventFunc),
				)

				// when
				err = orbital.SendJobTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJob.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, actEvent.ID)
				assert.False(t, actEvent.IsNotified)
				assert.NoError(t, clearJobEventTable(ctx, db))
			})
		}
	})

	t.Run("should update jobEvent updateAt if there are no event func configured", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		jobEventFunc := func(_ context.Context, _ orbital.Job) error {
			return nil
		}

		tts := []struct {
			name                 string
			jobStatus            orbital.JobStatus
			jobDoneEventFunc     orbital.JobTerminatedEventFunc
			jobFailedEventFunc   orbital.JobTerminatedEventFunc
			jobCanceledEventFunc orbital.JobTerminatedEventFunc
		}{
			{
				name:                 "JobStatusDone",
				jobStatus:            orbital.JobStatusDone,
				jobDoneEventFunc:     nil,
				jobCanceledEventFunc: jobEventFunc,
				jobFailedEventFunc:   jobEventFunc,
			},
			{
				name:                 "JobStatusFailed",
				jobStatus:            orbital.JobStatusFailed,
				jobFailedEventFunc:   nil,
				jobDoneEventFunc:     jobEventFunc,
				jobCanceledEventFunc: jobEventFunc,
			},
			{
				name:                 "JobStatusUserCanceled",
				jobStatus:            orbital.JobStatusUserCanceled,
				jobCanceledEventFunc: nil,
				jobDoneEventFunc:     jobEventFunc,
				jobFailedEventFunc:   jobEventFunc,
			},
			{
				name:                 "JobStatusConfirmCanceled",
				jobStatus:            orbital.JobStatusConfirmCanceled,
				jobCanceledEventFunc: nil,
				jobDoneEventFunc:     jobEventFunc,
				jobFailedEventFunc:   jobEventFunc,
			},
			{
				name:                 "JobStatusResolveCanceled",
				jobStatus:            orbital.JobStatusResolveCanceled,
				jobCanceledEventFunc: nil,
				jobDoneEventFunc:     jobEventFunc,
				jobFailedEventFunc:   jobEventFunc,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
					Data:         []byte("something"),
					Type:         "type",
					Status:       tt.jobStatus,
					ErrorMessage: "error",
				})
				assert.NoError(t, err)

				createdEvent, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobDoneEventFunc(tt.jobDoneEventFunc),
					orbital.WithJobCanceledEventFunc(tt.jobCanceledEventFunc),
					orbital.WithJobFailedEventFunc(tt.jobFailedEventFunc),
				)

				// when
				time.Sleep(1 * time.Second) // ensure the updateAt will change
				err = orbital.SendJobTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJob.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, createdJob.ID, actEvent.ID)
				assert.False(t, actEvent.IsNotified)
				assert.Less(t, createdEvent.UpdatedAt, actEvent.UpdatedAt)
				assert.NoError(t, clearJobEventTable(ctx, db))
			})
		}
	})
}
