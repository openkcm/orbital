package orbital_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
)

var (
	isTrue  = true
	isFalse = false
)

func TestRecordJobEvent(t *testing.T) {
	t.Run("should not record event if JobTerminatedEventFunc is nil", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(nil), // explicitly set to nil
		)

		// when
		err := orbital.RecordJobEvent(subj)(ctx, *repo, uuid.New())
		assert.NoError(t, err)

		// then
		_, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{IsNotified: &isFalse})
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("should record event if JobTerminatedEventFunc is not nil and jobevent is not present", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error { // set to a non-nil function
					return nil
				}),
		)

		jobID := uuid.New()

		// when
		err := orbital.RecordJobEvent(subj)(ctx, *repo, jobID)
		assert.NoError(t, err)

		// then
		event, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{IsNotified: &isFalse})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, jobID, event.ID)
		assert.False(t, event.IsNotified)
	})

	t.Run("should update jobevents `isNotified` to false if jobevent is already present and `isNotified` is true", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)
		jobID := uuid.New()
		_, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: jobID, IsNotified: true}) // create a job event with isNotified = true
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					return nil
				}),
		)

		// when
		err = orbital.RecordJobEvent(subj)(ctx, *repo, jobID)
		assert.NoError(t, err)

		// then
		event, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{IsNotified: &isFalse})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, jobID, event.ID)
		assert.False(t, event.IsNotified)
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
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					jobTerminationCalled++
					return nil
				}),
		)

		// when
		err := orbital.SendJobEvent(subj)(ctx)

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
		jobTerminationCalled := 0
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					jobTerminationCalled++
					return nil
				}),
		)

		// when
		err = orbital.SendJobEvent(subj)(ctx)

		// then
		assert.Error(t, err)
		assert.Equal(t, 0, jobTerminationCalled)
	})
	t.Run("should send event if there are jobs for the jobevent", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{
			Data:         []byte("something"),
			Type:         "type",
			Status:       "status",
			ErrorMessage: "error",
		})
		assert.NoError(t, err)

		_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
		assert.NoError(t, err)
		jobTerminationCalled := 0
		var actSendJob orbital.Job
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, job orbital.Job) error {
					jobTerminationCalled++
					actSendJob = job
					return nil
				}),
		)

		// when
		err = orbital.SendJobEvent(subj)(ctx)

		// then
		assert.NoError(t, err)
		assert.Equal(t, 1, jobTerminationCalled)
		assert.Equal(t, createdJob, actSendJob)
	})

	t.Run("should send event and update jobevent `isNotified` to true", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{})
		assert.NoError(t, err)

		_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					return nil
				}),
		)

		// when
		err = orbital.SendJobEvent(subj)(ctx)

		// then
		assert.NoError(t, err)
		actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{IsNotified: &isTrue})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, createdJob.ID, actEvent.ID)
		assert.True(t, actEvent.IsNotified)
	})

	t.Run("should not update jobevent `isNotified` to true if the JobTerminatedEventFunc return an error", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{})
		assert.NoError(t, err)

		_, err = orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					return assert.AnError // simulate an error
				}),
		)

		// when
		err = orbital.SendJobEvent(subj)(ctx)

		// then
		assert.NoError(t, err)

		actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{IsNotified: &isFalse})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, createdJob.ID, actEvent.ID)
		assert.False(t, actEvent.IsNotified)
	})

	t.Run("should update jobevent updateAt to latest timestamp if the JobTerminatedEventFunc return an error", func(t *testing.T) {
		// this makes sure that the jobevent is updated even if the JobTerminatedEventFunc returns an error
		// so that the jobevent can be retried later.
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		createdJob, err := orbital.CreateRepoJob(repo)(ctx, orbital.Job{})
		assert.NoError(t, err)

		createdEvent, err := orbital.CreateRepoJobEvent(repo)(ctx, orbital.JobEvent{ID: createdJob.ID, IsNotified: false})
		assert.NoError(t, err)
		subj, _ := orbital.NewManager(repo, mockTaskResolveFunc(),
			orbital.WithJobTerminatedEventFunc(
				func(_ context.Context, _ orbital.Job) error {
					return assert.AnError // simulate an error
				}),
		)

		// when
		time.Sleep(1 * time.Second) // ensure the updateAt will change
		err = orbital.SendJobEvent(subj)(ctx)

		// then
		assert.NoError(t, err)

		actEvent, ok, err := orbital.GetRepoJobEvent(repo)(ctx, orbital.JobEventQuery{ID: createdJob.ID})
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.False(t, actEvent.IsNotified)
		assert.Less(t, createdEvent.UpdatedAt, actEvent.UpdatedAt)
	})
}
