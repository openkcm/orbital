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

// recordJobTerminatedEvent records a job termination event in the repository.
// It checks if an event function is registered for the given job. If not, it returns nil.
// If the job event does not exist, it creates a new event entry.
// If the event exists and has been notified, it resets the notification flag
// and updates the event in the repository.
// Returns an error if any repository operation fails.
func (m *Manager) recordJobTerminatedEvent(ctx context.Context, repo Repository, job Job) error {
	eventFunc := m.eventFunc(job)
	if eventFunc == nil {
		return nil
	}
	event, ok, err := repo.getJobEvent(ctx, JobEventQuery{ID: job.ID})
	if err != nil {
		return err
	}
	if !ok {
		_, err = repo.createJobEvent(ctx, JobEvent{ID: job.ID})
		return err
	}
	if event.IsNotified {
		event.IsNotified = false
		return repo.updateJobEvent(ctx, event)
	}
	return nil
}

// sendJobTerminatedEvent sends a notification for a job that has reached a
// terminal state (done, failed, or canceled). It retrieves the next job event
// that has not yet been notified, fetches the associated job, and invokes the
// registered event function callback if available. After sending the event or
// if no callback is registered, it updates the event as notified in the
// repository. Returns an error if any repository operation or callback fails.
func (m *Manager) sendJobTerminatedEvent(ctx context.Context) error {
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

		eventFunc := m.eventFunc(job)
		if eventFunc == nil {
			return repo.updateJobEvent(ctx, event)
		}
		return m.sendAndUpdateEvent(ctx, repo, eventFunc, job, event)
	})
}

// eventFunc returns the appropriate JobTerminatedEventFunc based on the job's
// terminal status. It selects the event function for done, canceled, or failed
// job states.
//
//nolint:exhaustive
func (m *Manager) eventFunc(job Job) JobTerminatedEventFunc {
	switch job.Status {
	case JobStatusDone:
		return m.jobDoneEventFunc
	case JobStatusResolveCanceled, JobStatusConfirmCanceled, JobStatusUserCanceled:
		return m.jobCanceledEventFunc
	case JobStatusFailed:
		return m.jobFailedEventFunc
	default:
		slog.Debug("no job event function set for job status", slog.String("jobID", job.ID.String()), slog.String("status", string(job.Status)))
		return nil
	}
}

// sendAndUpdateEvent sends a job event notification using the provided event function.
// If the event function is nil, it returns immediately. Otherwise, it invokes
// the event function callback and logs any error encountered. It then updates
// the event's notification status in the repository based on whether the
// callback succeeded. Returns an error if updating the event fails.
func (m *Manager) sendAndUpdateEvent(ctx context.Context, repo Repository, jobEventFunc JobTerminatedEventFunc, job Job, event JobEvent) error {
	if jobEventFunc == nil {
		return nil
	}
	err := jobEventFunc(ctx, job)
	if err != nil {
		slog.Error("failed to send job event",
			slog.String("jobID", job.ID.String()),
			slog.String("status", string(job.Status)),
			slog.Any("error", err))
	}
	event.IsNotified = err == nil
	return repo.updateJobEvent(ctx, event)
}
