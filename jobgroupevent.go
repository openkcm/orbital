package orbital

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	slogctx "github.com/veqryn/slog-context"
)

// JobGroupEvent tracks a pending notification for a job group that reached a terminal state.
type JobGroupEvent struct {
	ID         uuid.UUID
	IsNotified bool
	UpdatedAt  int64
	CreatedAt  int64
}

// GroupTerminatedEventFunc is a callback invoked when a job group reaches a terminal state.
type GroupTerminatedEventFunc func(ctx context.Context, group JobGroup) error

// recordGroupTerminatedEvent creates or resets a group event record so that
// sendGroupTerminatedEvent will pick it up for notification.
// Returns nil immediately if no callback is registered for the group's status.
func (m *Manager) recordGroupTerminatedEvent(ctx context.Context, repo Repository, group JobGroup) error {
	eventFunc := m.groupEventFunc(group)
	if eventFunc == nil {
		slogctx.Debug(ctx, "no group event function set, skipping event recording")
		return nil
	}
	event, ok, err := repo.getJobGroupEvent(ctx, ListJobGroupEventQuery{ID: group.ID})
	if err != nil {
		return err
	}
	if !ok {
		_, err = repo.createJobGroupEvent(ctx, JobGroupEvent{ID: group.ID})
		return err
	}
	if event.IsNotified {
		event.IsNotified = false
		return repo.updateJobGroupEvent(ctx, event)
	}
	return nil
}

// sendGroupTerminatedEvent is a background worker that picks up unnotified group events,
// invokes the registered callback, and marks them as notified on success.
func (m *Manager) sendGroupTerminatedEvent(ctx context.Context) error {
	return m.repo.transaction(ctx, func(ctx context.Context, repo Repository) error {
		isNotified := false
		event, ok, err := repo.getJobGroupEvent(ctx, ListJobGroupEventQuery{
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

		group, ok, err := repo.getJobGroup(ctx, event.ID)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("job group %s: %w", event.ID, ErrJobGroupNotFound)
		}

		ctx = slogctx.With(ctx, "groupId", group.ID, "status", group.Status)

		eventFunc := m.groupEventFunc(group)
		if eventFunc == nil {
			slogctx.Debug(ctx, "no group event function set for group status")
			return repo.updateJobGroupEvent(ctx, event)
		}

		err = eventFunc(ctx, group)
		if err != nil {
			slogctx.Error(ctx, "failed to send group event", slog.Any("error", err))
		}
		event.IsNotified = err == nil
		return repo.updateJobGroupEvent(ctx, event)
	})
}

// groupEventFunc selects the callback for the group's terminal status, or nil if none is registered.
//
//nolint:exhaustive
func (m *Manager) groupEventFunc(group JobGroup) GroupTerminatedEventFunc {
	switch group.Status {
	case GroupStatusDone:
		return m.groupDoneEventFunc
	case GroupStatusCanceled:
		return m.groupCanceledEventFunc
	case GroupStatusFailed:
		return m.groupFailedEventFunc
	default:
		return nil
	}
}
