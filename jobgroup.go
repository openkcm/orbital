package orbital

import "github.com/google/uuid"

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
		Jobs         []Job // Ordered list of jobs
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
