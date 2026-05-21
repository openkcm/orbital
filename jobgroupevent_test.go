package orbital_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

func TestRecordGroupEvent(t *testing.T) {
	t.Run("should not record group event if group status is not terminal", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name        string
			groupStatus orbital.GroupStatus
		}{
			{
				name:        "GroupStatusCreated",
				groupStatus: orbital.GroupStatusCreated,
			},
			{
				name:        "GroupStatusProcessing",
				groupStatus: orbital.GroupStatusProcessing,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(mockGroupTerminatedFunc()),
					orbital.WithGroupFailedEventFunc(mockGroupTerminatedFunc()),
					orbital.WithGroupCanceledEventFunc(mockGroupTerminatedFunc()),
				)
				groupID := uuid.New()

				// when
				err := orbital.RecordGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.groupStatus})
				assert.NoError(t, err)

				// then
				_, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
				assert.NoError(t, err)
				assert.False(t, ok)
			})
		}
	})

	t.Run("should not record group event if group status is terminal but corresponding eventFunc is nil", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                   string
			groupStatus            orbital.GroupStatus
			groupDoneEventFunc     orbital.GroupTerminatedEventFunc
			groupFailedEventFunc   orbital.GroupTerminatedEventFunc
			groupCanceledEventFunc orbital.GroupTerminatedEventFunc
		}{
			{
				name:               "GroupStatusDone and groupDoneEventFunc is nil",
				groupStatus:        orbital.GroupStatusDone,
				groupDoneEventFunc: nil,
			},
			{
				name:                 "GroupStatusFailed and groupFailedEventFunc is nil",
				groupStatus:          orbital.GroupStatusFailed,
				groupFailedEventFunc: nil,
			},
			{
				name:                   "GroupStatusCanceled and groupCanceledEventFunc is nil",
				groupStatus:            orbital.GroupStatusCanceled,
				groupCanceledEventFunc: nil,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(tt.groupDoneEventFunc),
					orbital.WithGroupFailedEventFunc(tt.groupFailedEventFunc),
					orbital.WithGroupCanceledEventFunc(tt.groupCanceledEventFunc),
				)
				groupID := uuid.New()

				// when
				err := orbital.RecordGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.groupStatus})
				assert.NoError(t, err)

				// then
				_, ok, err := orbital.GetJobGroupEvent(repo)(ctx, orbital.ListJobGroupEventQuery{ID: groupID})
				assert.NoError(t, err)
				assert.False(t, ok)
			})
		}
	})

	t.Run("should record group event if group status is terminal and eventFunc is set", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                   string
			groupStatus            orbital.GroupStatus
			groupDoneEventFunc     orbital.GroupTerminatedEventFunc
			groupFailedEventFunc   orbital.GroupTerminatedEventFunc
			groupCanceledEventFunc orbital.GroupTerminatedEventFunc
		}{
			{
				name:               "GroupStatusDone",
				groupStatus:        orbital.GroupStatusDone,
				groupDoneEventFunc: mockGroupTerminatedFunc(),
			},
			{
				name:                 "GroupStatusFailed",
				groupStatus:          orbital.GroupStatusFailed,
				groupFailedEventFunc: mockGroupTerminatedFunc(),
			},
			{
				name:                   "GroupStatusCanceled",
				groupStatus:            orbital.GroupStatusCanceled,
				groupCanceledEventFunc: mockGroupTerminatedFunc(),
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(tt.groupDoneEventFunc),
					orbital.WithGroupFailedEventFunc(tt.groupFailedEventFunc),
					orbital.WithGroupCanceledEventFunc(tt.groupCanceledEventFunc),
				)
				groupID := uuid.New()

				// when
				err := orbital.RecordGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.groupStatus})
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

	t.Run("should set group event isNotified to false when existing event has isNotified as true", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                   string
			groupStatus            orbital.GroupStatus
			groupDoneEventFunc     orbital.GroupTerminatedEventFunc
			groupFailedEventFunc   orbital.GroupTerminatedEventFunc
			groupCanceledEventFunc orbital.GroupTerminatedEventFunc
		}{
			{
				name:               "for GroupStatusDone",
				groupStatus:        orbital.GroupStatusDone,
				groupDoneEventFunc: mockGroupTerminatedFunc(),
			},
			{
				name:                 "for GroupStatusFailed",
				groupStatus:          orbital.GroupStatusFailed,
				groupFailedEventFunc: mockGroupTerminatedFunc(),
			},
			{
				name:                   "for GroupStatusCanceled",
				groupStatus:            orbital.GroupStatusCanceled,
				groupCanceledEventFunc: mockGroupTerminatedFunc(),
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(tt.groupDoneEventFunc),
					orbital.WithGroupFailedEventFunc(tt.groupFailedEventFunc),
					orbital.WithGroupCanceledEventFunc(tt.groupCanceledEventFunc),
				)
				groupID := uuid.New()

				// create a group event with isNotified = true
				_, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: groupID, IsNotified: true})
				assert.NoError(t, err)

				// when
				err = orbital.RecordGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: tt.groupStatus})
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
			orbital.WithGroupDoneEventFunc(mockGroupTerminatedFunc()),
		)
		groupID := uuid.New()

		// create a group event with isNotified = false
		createdEvent, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: groupID, IsNotified: false})
		assert.NoError(t, err)

		// when
		err = orbital.RecordGroupTerminatedEvent(subj)(ctx, *repo, orbital.JobGroup{ID: groupID, Status: orbital.GroupStatusDone})
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

func TestSendGroupEvent(t *testing.T) {
	t.Run("should not send event if there are no group events", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		groupTerminationCalled := 0
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithGroupDoneEventFunc(
				func(_ context.Context, _ orbital.JobGroup) error {
					groupTerminationCalled++
					return nil
				}),
			orbital.WithGroupCanceledEventFunc(
				func(_ context.Context, _ orbital.JobGroup) error {
					groupTerminationCalled++
					return nil
				}),
			orbital.WithGroupFailedEventFunc(
				func(_ context.Context, _ orbital.JobGroup) error {
					groupTerminationCalled++
					return nil
				}),
		)

		// when
		err := orbital.SendGroupTerminatedEvent(subj)(ctx)

		// then
		assert.NoError(t, err)
		assert.Equal(t, 0, groupTerminationCalled)
	})

	t.Run("should return error if there is no group for the group event", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		_, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{IsNotified: false})
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc())

		// when
		err = orbital.SendGroupTerminatedEvent(subj)(ctx)

		// then
		assert.Error(t, err)
	})

	t.Run("should invoke the correct callback and mark event as notified", func(t *testing.T) {
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		tts := []struct {
			name                          string
			groupStatus                   orbital.GroupStatus
			expCallGroupDoneEventFunc     int
			expCallGroupFailedEventFunc   int
			expCallGroupCanceledEventFunc int
		}{
			{
				name:                      "GroupStatusDone",
				groupStatus:               orbital.GroupStatusDone,
				expCallGroupDoneEventFunc: 1,
			},
			{
				name:                        "GroupStatusFailed",
				groupStatus:                 orbital.GroupStatusFailed,
				expCallGroupFailedEventFunc: 1,
			},
			{
				name:                          "GroupStatusCanceled",
				groupStatus:                   orbital.GroupStatusCanceled,
				expCallGroupCanceledEventFunc: 1,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdGroup, err := orbital.CreateJobGroup(repo)(ctx, orbital.JobGroup{
					Type:   "test-group",
					Status: tt.groupStatus,
				})
				assert.NoError(t, err)

				_, err = orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: createdGroup.ID, IsNotified: false})
				assert.NoError(t, err)

				var actCallGroupDoneEventFunc, actCallGroupFailedEventFunc, actCallGroupCanceledEventFunc int
				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							actCallGroupDoneEventFunc++
							assert.Equal(t, createdGroup.ID, group.ID)
							assert.Equal(t, tt.groupStatus, group.Status)
							return nil
						}),
					orbital.WithGroupCanceledEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							actCallGroupCanceledEventFunc++
							assert.Equal(t, createdGroup.ID, group.ID)
							assert.Equal(t, tt.groupStatus, group.Status)
							return nil
						}),
					orbital.WithGroupFailedEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							actCallGroupFailedEventFunc++
							assert.Equal(t, createdGroup.ID, group.ID)
							assert.Equal(t, tt.groupStatus, group.Status)
							return nil
						}),
				)

				// when
				err = orbital.SendGroupTerminatedEvent(subj)(ctx)

				// then
				assert.NoError(t, err)
				assert.Equal(t, tt.expCallGroupDoneEventFunc, actCallGroupDoneEventFunc)
				assert.Equal(t, tt.expCallGroupFailedEventFunc, actCallGroupFailedEventFunc)
				assert.Equal(t, tt.expCallGroupCanceledEventFunc, actCallGroupCanceledEventFunc)

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
			name        string
			groupStatus orbital.GroupStatus
		}{
			{
				name:        "GroupStatusDone",
				groupStatus: orbital.GroupStatusDone,
			},
			{
				name:        "GroupStatusFailed",
				groupStatus: orbital.GroupStatusFailed,
			},
			{
				name:        "GroupStatusCanceled",
				groupStatus: orbital.GroupStatusCanceled,
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdGroup, err := orbital.CreateJobGroup(repo)(ctx, orbital.JobGroup{
					Type:   "test-group",
					Status: tt.groupStatus,
				})
				assert.NoError(t, err)

				createdEvent, err := orbital.CreateJobGroupEvent(repo)(ctx, orbital.JobGroupEvent{ID: createdGroup.ID, IsNotified: false})
				assert.NoError(t, err)

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							assert.Equal(t, createdGroup.ID, group.ID)
							return assert.AnError
						}),
					orbital.WithGroupCanceledEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							assert.Equal(t, createdGroup.ID, group.ID)
							return assert.AnError
						}),
					orbital.WithGroupFailedEventFunc(
						func(_ context.Context, group orbital.JobGroup) error {
							assert.Equal(t, createdGroup.ID, group.ID)
							return assert.AnError
						}),
				)

				// when
				time.Sleep(1 * time.Microsecond) // ensure the updatedAt will change
				err = orbital.SendGroupTerminatedEvent(subj)(ctx)

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
			name                   string
			groupStatus            orbital.GroupStatus
			groupDoneEventFunc     orbital.GroupTerminatedEventFunc
			groupFailedEventFunc   orbital.GroupTerminatedEventFunc
			groupCanceledEventFunc orbital.GroupTerminatedEventFunc
		}{
			{
				name:                   "GroupStatusDone",
				groupStatus:            orbital.GroupStatusDone,
				groupDoneEventFunc:     nil,
				groupCanceledEventFunc: mockGroupTerminatedFunc(),
				groupFailedEventFunc:   mockGroupTerminatedFunc(),
			},
			{
				name:                   "GroupStatusFailed",
				groupStatus:            orbital.GroupStatusFailed,
				groupFailedEventFunc:   nil,
				groupDoneEventFunc:     mockGroupTerminatedFunc(),
				groupCanceledEventFunc: mockGroupTerminatedFunc(),
			},
			{
				name:                   "GroupStatusCanceled",
				groupStatus:            orbital.GroupStatusCanceled,
				groupCanceledEventFunc: nil,
				groupDoneEventFunc:     mockGroupTerminatedFunc(),
				groupFailedEventFunc:   mockGroupTerminatedFunc(),
			},
		}
		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				createdGroup, err := orbital.CreateJobGroup(repo)(ctx, orbital.JobGroup{
					Type:   "test-group",
					Status: tt.groupStatus,
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
				doneFunc := tt.groupDoneEventFunc
				if doneFunc != nil {
					doneFunc = countingFunc
				}
				failedFunc := tt.groupFailedEventFunc
				if failedFunc != nil {
					failedFunc = countingFunc
				}
				canceledFunc := tt.groupCanceledEventFunc
				if canceledFunc != nil {
					canceledFunc = countingFunc
				}

				subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
					orbital.WithGroupDoneEventFunc(doneFunc),
					orbital.WithGroupCanceledEventFunc(canceledFunc),
					orbital.WithGroupFailedEventFunc(failedFunc),
				)

				// when
				time.Sleep(1 * time.Microsecond) // ensure the updatedAt will change
				err = orbital.SendGroupTerminatedEvent(subj)(ctx)

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
