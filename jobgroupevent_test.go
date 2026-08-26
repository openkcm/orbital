package orbital_test

import (
	"context"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestRecordJobGroupEvent(t *testing.T) {
	t.Run("should not record job group event if job group status is not terminal", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name           string
			jobGroupStatus orbital.JobGroupStatus
		}{
			{
				name:           "JobGroupStatusCreated",
				jobGroupStatus: orbital.JobGroupStatusCreated,
			},
			{
				name:           "JobGroupStatusProcessing",
				jobGroupStatus: orbital.JobGroupStatusProcessing,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(mockJobGroupTerminatedFunc()),
					orbital.WithJobGroupFailedEventFunc(mockJobGroupTerminatedFunc()),
					orbital.WithJobGroupCanceledEventFunc(mockJobGroupTerminatedFunc()),
				)
				groupID := uuid.New()

				// when
				err := orbital.RecordJobGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.jobGroupStatus})
				assert.NoError(t, err)

				// then
				_, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
				assert.NoError(t, err)
				assert.False(t, ok)
			})
		}
	})

	t.Run("should not record job group event if job group status is terminal but corresponding eventFunc is nil", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                      string
			jobGroupStatus            orbital.JobGroupStatus
			jobGroupDoneEventFunc     orbital.JobGroupTerminatedEventFunc
			jobGroupFailedEventFunc   orbital.JobGroupTerminatedEventFunc
			jobGroupCanceledEventFunc orbital.JobGroupTerminatedEventFunc
		}{
			{
				name:                  "JobGroupStatusDone and jobGroupDoneEventFunc is nil",
				jobGroupStatus:        orbital.JobGroupStatusDone,
				jobGroupDoneEventFunc: nil,
			},
			{
				name:                    "JobGroupStatusFailed and jobGroupFailedEventFunc is nil",
				jobGroupStatus:          orbital.JobGroupStatusFailed,
				jobGroupFailedEventFunc: nil,
			},
			{
				name:                      "JobGroupStatusCanceled and jobGroupCanceledEventFunc is nil",
				jobGroupStatus:            orbital.JobGroupStatusCanceled,
				jobGroupCanceledEventFunc: nil,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(tt.jobGroupDoneEventFunc),
					orbital.WithJobGroupFailedEventFunc(tt.jobGroupFailedEventFunc),
					orbital.WithJobGroupCanceledEventFunc(tt.jobGroupCanceledEventFunc),
				)
				groupID := uuid.New()

				// when
				err := orbital.RecordJobGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.jobGroupStatus})
				assert.NoError(t, err)

				// then
				_, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
				assert.NoError(t, err)
				assert.False(t, ok)
			})
		}
	})

	t.Run("should record job group event if job group status is terminal and eventFunc is set", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                      string
			jobGroupStatus            orbital.JobGroupStatus
			jobGroupDoneEventFunc     orbital.JobGroupTerminatedEventFunc
			jobGroupFailedEventFunc   orbital.JobGroupTerminatedEventFunc
			jobGroupCanceledEventFunc orbital.JobGroupTerminatedEventFunc
		}{
			{
				name:                  "JobGroupStatusDone",
				jobGroupStatus:        orbital.JobGroupStatusDone,
				jobGroupDoneEventFunc: mockJobGroupTerminatedFunc(),
			},
			{
				name:                    "JobGroupStatusFailed",
				jobGroupStatus:          orbital.JobGroupStatusFailed,
				jobGroupFailedEventFunc: mockJobGroupTerminatedFunc(),
			},
			{
				name:                      "JobGroupStatusCanceled",
				jobGroupStatus:            orbital.JobGroupStatusCanceled,
				jobGroupCanceledEventFunc: mockJobGroupTerminatedFunc(),
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(tt.jobGroupDoneEventFunc),
					orbital.WithJobGroupFailedEventFunc(tt.jobGroupFailedEventFunc),
					orbital.WithJobGroupCanceledEventFunc(tt.jobGroupCanceledEventFunc),
				)
				groupID := uuid.New()

				// when
				err := orbital.RecordJobGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.jobGroupStatus})
				assert.NoError(t, err)

				// then
				event, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, groupID, event.ID)
				assert.False(t, event.IsNotified)
			})
		}
	})

	t.Run("should set job group event isNotified to false when existing event has isNotified as true", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                      string
			jobGroupStatus            orbital.JobGroupStatus
			jobGroupDoneEventFunc     orbital.JobGroupTerminatedEventFunc
			jobGroupFailedEventFunc   orbital.JobGroupTerminatedEventFunc
			jobGroupCanceledEventFunc orbital.JobGroupTerminatedEventFunc
		}{
			{
				name:                  "for JobGroupStatusDone",
				jobGroupStatus:        orbital.JobGroupStatusDone,
				jobGroupDoneEventFunc: mockJobGroupTerminatedFunc(),
			},
			{
				name:                    "for JobGroupStatusFailed",
				jobGroupStatus:          orbital.JobGroupStatusFailed,
				jobGroupFailedEventFunc: mockJobGroupTerminatedFunc(),
			},
			{
				name:                      "for JobGroupStatusCanceled",
				jobGroupStatus:            orbital.JobGroupStatusCanceled,
				jobGroupCanceledEventFunc: mockJobGroupTerminatedFunc(),
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(tt.jobGroupDoneEventFunc),
					orbital.WithJobGroupFailedEventFunc(tt.jobGroupFailedEventFunc),
					orbital.WithJobGroupCanceledEventFunc(tt.jobGroupCanceledEventFunc),
				)
				groupID := uuid.New()

				// create a job group event with isNotified = true
				_, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: groupID, IsNotified: true})
				assert.NoError(t, err)

				// when
				err = orbital.RecordJobGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.jobGroupStatus})
				assert.NoError(t, err)

				// then
				event, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, groupID, event.ID)
				assert.False(t, event.IsNotified)
			})
		}
	})

	t.Run("should not modify event if it already exists with isNotified as false", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		// given
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobGroupDoneEventFunc(mockJobGroupTerminatedFunc()),
		)
		groupID := uuid.New()

		// create a job group event with isNotified = false
		createdEvent, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: groupID, IsNotified: false})
		assert.NoError(t, err)

		// when
		err = orbital.RecordJobGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: orbital.JobGroupStatusDone})
		assert.NoError(t, err)

		// then
		event, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, groupID, event.ID)
		assert.False(t, event.IsNotified)
		assert.Equal(t, createdEvent.UpdatedAt, event.UpdatedAt)
	})
}

func TestSendJobGroupEvent(t *testing.T) {
	t.Run("should not send event if there are no job group events", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		groupTerminationCalled := 0
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobGroupDoneEventFunc(
				func(_ context.Context, _ orbital.JobGroup) error {
					groupTerminationCalled++
					return nil
				}),
			orbital.WithJobGroupCanceledEventFunc(
				func(_ context.Context, _ orbital.JobGroup) error {
					groupTerminationCalled++
					return nil
				}),
			orbital.WithJobGroupFailedEventFunc(
				func(_ context.Context, _ orbital.JobGroup) error {
					groupTerminationCalled++
					return nil
				}),
		)

		// when
		err := orbital.SendJobGroupTerminatedEvent(subj)(ctx)

		// then
		assert.NoError(t, err)
		assert.Equal(t, 0, groupTerminationCalled)
	})

	t.Run("should return error if there is no job group for the job group event", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		_, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{IsNotified: false})
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc())

		// when
		err = orbital.SendJobGroupTerminatedEvent(subj)(ctx)

		// then
		assert.Error(t, err)
	})

	t.Run("should invoke the correct callback and mark event as notified", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                             string
			jobGroupStatus                   orbital.JobGroupStatus
			expCallJobGroupDoneEventFunc     int
			expCallJobGroupFailedEventFunc   int
			expCallJobGroupCanceledEventFunc int
		}{
			{
				name:                         "JobGroupStatusDone",
				jobGroupStatus:               orbital.JobGroupStatusDone,
				expCallJobGroupDoneEventFunc: 1,
			},
			{
				name:                           "JobGroupStatusFailed",
				jobGroupStatus:                 orbital.JobGroupStatusFailed,
				expCallJobGroupFailedEventFunc: 1,
			},
			{
				name:                             "JobGroupStatusCanceled",
				jobGroupStatus:                   orbital.JobGroupStatusCanceled,
				expCallJobGroupCanceledEventFunc: 1,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdGroup, err := orbital.CreateJobGroup(repo)(ctx, orbital.JobGroup{
					Type:   "test-job-group",
					Status: tt.jobGroupStatus,
				})
				assert.NoError(t, err)

				_, err = orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: createdGroup.ID, IsNotified: false})
				assert.NoError(t, err)

				var actCallJobGroupDoneEventFunc, actCallJobGroupFailedEventFunc, actCallJobGroupCanceledEventFunc int
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							actCallJobGroupDoneEventFunc++
							assert.Equal(t, createdGroup.ID, group.ID)
							assert.Equal(t, tt.jobGroupStatus, group.Status)
							return nil
						}),
					orbital.WithJobGroupCanceledEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							actCallJobGroupCanceledEventFunc++
							assert.Equal(t, createdGroup.ID, group.ID)
							assert.Equal(t, tt.jobGroupStatus, group.Status)
							return nil
						}),
					orbital.WithJobGroupFailedEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							actCallJobGroupFailedEventFunc++
							assert.Equal(t, createdGroup.ID, group.ID)
							assert.Equal(t, tt.jobGroupStatus, group.Status)
							return nil
						}),
				)

				// when
				err = orbital.SendJobGroupTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				assert.Equal(t, tt.expCallJobGroupDoneEventFunc, actCallJobGroupDoneEventFunc)
				assert.Equal(t, tt.expCallJobGroupFailedEventFunc, actCallJobGroupFailedEventFunc)
				assert.Equal(t, tt.expCallJobGroupCanceledEventFunc, actCallJobGroupCanceledEventFunc)

				actEvent, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: createdGroup.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.True(t, actEvent.IsNotified)
				assert.NoError(t, clearJobGroupEventsTable(ctx, db))
				assert.NoError(t, clearJobGroupsTable(ctx, db))
			})
		}
	})

	t.Run("should not mark as notified and should bump updatedAt if event func returns an error", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name           string
			jobGroupStatus orbital.JobGroupStatus
		}{
			{
				name:           "JobGroupStatusDone",
				jobGroupStatus: orbital.JobGroupStatusDone,
			},
			{
				name:           "JobGroupStatusFailed",
				jobGroupStatus: orbital.JobGroupStatusFailed,
			},
			{
				name:           "JobGroupStatusCanceled",
				jobGroupStatus: orbital.JobGroupStatusCanceled,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdGroup, err := orbital.CreateJobGroup(repo)(ctx, orbital.JobGroup{
					Type:   "test-job-group",
					Status: tt.jobGroupStatus,
				})
				assert.NoError(t, err)

				createdEvent, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: createdGroup.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							assert.Equal(t, createdGroup.ID, group.ID)
							return assert.AnError
						}),
					orbital.WithJobGroupCanceledEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							assert.Equal(t, createdGroup.ID, group.ID)
							return assert.AnError
						}),
					orbital.WithJobGroupFailedEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							assert.Equal(t, createdGroup.ID, group.ID)
							return assert.AnError
						}),
				)

				// when
				time.Sleep(1 * time.Microsecond) // ensure the updatedAt will change
				err = orbital.SendJobGroupTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				actEvent, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: createdGroup.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.False(t, actEvent.IsNotified)
				assert.Less(t, createdEvent.UpdatedAt, actEvent.UpdatedAt)
				assert.NoError(t, clearJobGroupEventsTable(ctx, db))
				assert.NoError(t, clearJobGroupsTable(ctx, db))
			})
		}
	})

	t.Run("should not call callback and should bump updatedAt if no event func is configured for the status", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                      string
			jobGroupStatus            orbital.JobGroupStatus
			jobGroupDoneEventFunc     orbital.JobGroupTerminatedEventFunc
			jobGroupFailedEventFunc   orbital.JobGroupTerminatedEventFunc
			jobGroupCanceledEventFunc orbital.JobGroupTerminatedEventFunc
		}{
			{
				name:                      "JobGroupStatusDone",
				jobGroupStatus:            orbital.JobGroupStatusDone,
				jobGroupDoneEventFunc:     nil,
				jobGroupCanceledEventFunc: mockJobGroupTerminatedFunc(),
				jobGroupFailedEventFunc:   mockJobGroupTerminatedFunc(),
			},
			{
				name:                      "JobGroupStatusFailed",
				jobGroupStatus:            orbital.JobGroupStatusFailed,
				jobGroupFailedEventFunc:   nil,
				jobGroupDoneEventFunc:     mockJobGroupTerminatedFunc(),
				jobGroupCanceledEventFunc: mockJobGroupTerminatedFunc(),
			},
			{
				name:                      "JobGroupStatusCanceled",
				jobGroupStatus:            orbital.JobGroupStatusCanceled,
				jobGroupCanceledEventFunc: nil,
				jobGroupDoneEventFunc:     mockJobGroupTerminatedFunc(),
				jobGroupFailedEventFunc:   mockJobGroupTerminatedFunc(),
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdGroup, err := orbital.CreateJobGroup(repo)(ctx, orbital.JobGroup{
					Type:   "test-job-group",
					Status: tt.jobGroupStatus,
				})
				assert.NoError(t, err)

				createdEvent, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: createdGroup.ID, IsNotified: false})
				assert.NoError(t, err)

				callbackCalled := 0
				countingFunc := func(_ context.Context, _ orbital.JobGroup) error {
					callbackCalled++
					return nil
				}

				// override non-nil funcs with counting versions
				doneFunc := tt.jobGroupDoneEventFunc
				if doneFunc != nil {
					doneFunc = countingFunc
				}
				failedFunc := tt.jobGroupFailedEventFunc
				if failedFunc != nil {
					failedFunc = countingFunc
				}
				canceledFunc := tt.jobGroupCanceledEventFunc
				if canceledFunc != nil {
					canceledFunc = countingFunc
				}

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithJobGroupDoneEventFunc(doneFunc),
					orbital.WithJobGroupCanceledEventFunc(canceledFunc),
					orbital.WithJobGroupFailedEventFunc(failedFunc),
				)

				// when
				time.Sleep(1 * time.Microsecond) // ensure the updatedAt will change
				err = orbital.SendJobGroupTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				assert.Equal(t, 0, callbackCalled)

				actEvent, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: createdGroup.ID})
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.False(t, actEvent.IsNotified)
				assert.Less(t, createdEvent.UpdatedAt, actEvent.UpdatedAt)
				assert.NoError(t, clearJobGroupEventsTable(ctx, db))
				assert.NoError(t, clearJobGroupsTable(ctx, db))
			})
		}
	})
}
