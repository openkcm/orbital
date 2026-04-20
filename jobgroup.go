package orbital

import (
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
