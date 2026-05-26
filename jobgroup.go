package orbital

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

// Possible job group statuses.
const (
	JobGroupStatusCreated    JobGroupStatus = "CREATED"
	JobGroupStatusProcessing JobGroupStatus = "PROCESSING"
	JobGroupStatusDone       JobGroupStatus = "DONE"
	JobGroupStatusFailed     JobGroupStatus = "FAILED"
	JobGroupStatusCanceled   JobGroupStatus = "CANCELED"
)

type (
	// JobGroup represents a collection of jobs to be executed sequentially.
	JobGroup struct {
		ID           uuid.UUID
		Type         string
		Jobs         []Job // Jobs in the group, ordered by their position
		CreatedAt    int64
		UpdatedAt    int64
		Status       JobGroupStatus
		Labels       Labels
		ErrorMessage string
	}

	// JobGroupStatus represents the possible statuses of a JobGroup.
	JobGroupStatus string
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

// isCancelable reports whether the job group can be canceled based on its current status.
func (jg JobGroup) isCancelable() bool {
	return jg.Status == JobGroupStatusCreated || jg.Status == JobGroupStatusProcessing
}

// hasTerminalState reports whether the job group has reached a terminal state.
func (jg JobGroup) hasTerminalState() bool {
	return jg.Status == JobGroupStatusDone || jg.Status == JobGroupStatusFailed || jg.Status == JobGroupStatusCanceled
}

// sortJobsByGroupOrder sorts jobs in-place by their job group order key (ascending).
// Returns an error if any job has an invalid or missing order key.
func sortJobsByGroupOrder(jobs []Job) error {
	for _, job := range jobs {
		if _, err := strconv.Atoi(job.Labels[LabelKeyJobGroupOrderKey]); err != nil {
			return fmt.Errorf("job %s has invalid or missing job group order key: %w", job.ID, err)
		}
	}

	sort.Slice(jobs, func(i, j int) bool {
		orderI, _ := strconv.Atoi(jobs[i].Labels[LabelKeyJobGroupOrderKey])
		orderJ, _ := strconv.Atoi(jobs[j].Labels[LabelKeyJobGroupOrderKey])
		return orderI < orderJ
	})

	return nil
}

// jobGroupResult represents the outcome of evaluating the jobs in a job group.
// Each implementation knows how to apply itself to transition the job group state.
type jobGroupResult interface {
	apply(ctx context.Context, repo Repository, group *JobGroup) error
}

type jobGroupWaitResult struct{}

func (jobGroupWaitResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	return repo.updateJobGroup(ctx, *group)
}

type jobGroupCompletedResult struct{}

func (jobGroupCompletedResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	group.Status = JobGroupStatusDone
	return repo.updateJobGroup(ctx, *group)
}

type jobGroupFailedResult struct {
	reason string
}

func (r jobGroupFailedResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	group.Status = JobGroupStatusFailed
	group.ErrorMessage = r.reason
	return repo.updateJobGroup(ctx, *group)
}

type jobGroupPromoteResult struct {
	job *Job
}

func (r jobGroupPromoteResult) apply(ctx context.Context, repo Repository, group *JobGroup) error {
	r.job.Status = JobStatusCreated
	if err := repo.updateJob(ctx, *r.job); err != nil {
		return err
	}
	group.Status = JobGroupStatusProcessing
	return repo.updateJobGroup(ctx, *group)
}

// evaluateJobs inspects the jobs in order and returns a jobGroupResult
// representing the next action for the job group's scheduling lifecycle.
func evaluateJobs(jobs []Job) jobGroupResult {
	for i := range jobs {
		switch {
		case jobs[i].Status == JobStatusDone:
			continue
		case isUnsuccessful(jobs[i].Status):
			return jobGroupFailedResult{reason: fmt.Sprintf("job %s failed or is canceled", jobs[i].ID)}
		case jobs[i].Status == JobStatusScheduled:
			return jobGroupPromoteResult{job: &jobs[i]}
		default:
			return jobGroupWaitResult{}
		}
	}
	return jobGroupCompletedResult{}
}

func isUnsuccessful(status JobStatus) bool {
	_, ok := unsuccessfulStatuses[status]
	return ok
}
