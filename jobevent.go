package orbital

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

var errJobNotFound = errors.New("job not found")

// JobEvent represents an event related to a job, used for tracking and processing job state changes.
type JobEvent struct {
	ID         uuid.UUID
	IsNotified bool
	UpdatedAt  int64
	CreatedAt  int64
}

// recordJobTerminationEvent records a job termination event in the repository.
// It checks if the JobTerminatedEventFunc is set; if not, it returns immediately.
// If the job event does not exist, it creates a new one. If the event exists and
// its IsNotified field is true, it sets IsNotified to false and updates the event.
// Returns an error if any repository operation fails.
func (m *Manager) recordJobTerminationEvent(ctx context.Context, repo Repository, jobID uuid.UUID) error {
	if m.jobTerminatedEventFunc == nil {
		return nil
	}
	event, ok, err := repo.getJobEvent(ctx, JobEventQuery{ID: jobID})
	if err != nil {
		return err
	}
	if !ok {
		_, err = repo.createJobEvent(ctx, JobEvent{ID: jobID})
		return err
	}
	if event.IsNotified {
		event.IsNotified = false
		return repo.updateJobEvent(ctx, event)
	}
	return nil
}

// sendJobTerminationEvent attempts to send a job event using the SendJobEventFunc callback.
// It retrieves the next unsent job event from the repository, fetches the associated job,
// and invokes the SendJobEventFunc. If sending is successful, it marks the event as sent.
// Returns an error if any step fails, or nil if there are no events to send or the callback is not set.
func (m *Manager) sendJobTerminationEvent(ctx context.Context) error {
	if m.jobTerminatedEventFunc == nil {
		return nil
	}
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		isNotified := false
		event, ok, err := repo.getJobEvent(ctx, JobEventQuery{
			IsNotified:         &isNotified,
			RetrievalModeQueue: true,
			OrderByUpdatedAt:   true,
			Limit:              1,
		})
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		job, ok, err := repo.getJob(ctx, event.ID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("job %s: %w", event.ID, errJobNotFound)
		}
		err = m.jobTerminatedEventFunc(ctx, job)
		if err != nil {
			slog.Error("failed to send job termination event", slog.String("jobID", job.ID.String()), slog.Any("error", err))
		}
		event.IsNotified = err == nil
		return repo.updateJobEvent(ctx, event)
	})
}
