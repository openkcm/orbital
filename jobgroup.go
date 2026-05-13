package orbital

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

// Possible group statuses.
const (
	GroupStatusCreated    GroupStatus = "CREATED"
	GroupStatusProcessing GroupStatus = "PROCESSING"
	GroupStatusDone       GroupStatus = "DONE"
	GroupStatusFailed     GroupStatus = "FAILED"
	GroupStatusCanceled   GroupStatus = "CANCELED"
)

type (
	// JobGroup represents a collection of jobs to be executed sequentially.
	JobGroup struct {
		ID           uuid.UUID
		Type         string
		Jobs         []Job // Jobs in the group, ordered by their position
		CreatedAt    int64
		UpdatedAt    int64
		Status       GroupStatus
		Labels       Labels
		ErrorMessage string
	}

	// GroupStatus represents the possible statuses of a JobGroup.
	GroupStatus string
)

// NewJobGroup creates a new JobGroup with the provided type and jobs.
// The order of jobs is preserved in the Jobs slice.
func NewJobGroup(groupType string, jobs ...Job) JobGroup {
	return JobGroup{
		Type: groupType,
		Jobs: jobs,
	}
}

// WithLabels sets labels on the JobGroup and returns it for method chaining.
func (jg JobGroup) WithLabels(labels Labels) JobGroup {
	jg.Labels = labels
	return jg
}

// sortJobsByGroupOrder sorts jobs in-place by their group order key (ascending).
// Returns an error if any job has an invalid or missing order key.
func sortJobsByGroupOrder(jobs []Job) error {
	for _, job := range jobs {
		if _, err := strconv.Atoi(job.Labels[LabelKeyGroupOrderKey]); err != nil {
			return fmt.Errorf("job %s has invalid or missing group order key: %w", job.ID, err)
		}
	}

	sort.Slice(jobs, func(i, j int) bool {
		orderI, _ := strconv.Atoi(jobs[i].Labels[LabelKeyGroupOrderKey])
		orderJ, _ := strconv.Atoi(jobs[j].Labels[LabelKeyGroupOrderKey])
		return orderI < orderJ
	})

	return nil
}

// groupResult represents the outcome of evaluating the jobs in a group.
// Each implementation knows how to apply itself to transition the group state.
type groupResult interface {
	apply(ctx context.Context, repo Repository, group *JobGroup) error
}

type groupWaitResult struct{}

func (groupWaitResult) apply(_ context.Context, _ Repository, _ *JobGroup) error {
	return nil
}

type groupCompletedResult struct{}

func (groupCompletedResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	group.Status = GroupStatusDone
	return repo.updateJobGroup(ctx, *group)
}

type groupFailedResult struct {
	reason string
}

func (r groupFailedResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	group.Status = GroupStatusFailed
	group.ErrorMessage = r.reason
	return repo.updateJobGroup(ctx, *group)
}

type groupPromoteResult struct {
	job *Job
}

func (r groupPromoteResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	r.job.Status = JobStatusCreated
	if err := repo.updateJob(ctx, *r.job); err != nil {
		return err
	}
	group.Status = GroupStatusProcessing
	return repo.updateJobGroup(ctx, *group)
}

// evaluateJobs inspects the jobs in order and returns a groupResult
// representing the next action for the group's scheduling lifecycle.
func evaluateJobs(jobs []Job) groupResult {
	for i := range jobs {
		switch {
		case jobs[i].Status == JobStatusDone:
			continue
		case isUnsuccessful(jobs[i].Status):
			return groupFailedResult{reason: fmt.Sprintf("job %s failed or is canceled", jobs[i].ID)}
		case jobs[i].Status == JobStatusScheduled:
			return groupPromoteResult{job: &jobs[i]}
		default:
			return groupWaitResult{}
		}
	}
	return groupCompletedResult{}
}

func isUnsuccessful(status JobStatus) bool {
	_, ok := unsuccessfulStatuses[status]
	return ok
}
