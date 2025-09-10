package orbital

import (
	"github.com/google/uuid"
)

// Possible job statuses.
const (
	JobStatusCreated         JobStatus = "CREATED"
	JobStatusConfirming      JobStatus = "CONFIRMING"
	JobStatusConfirmed       JobStatus = "CONFIRMED"
	JobStatusResolving       JobStatus = "RESOLVING"
	JobStatusReady           JobStatus = "READY"
	JobStatusProcessing      JobStatus = "PROCESSING"
	JobStatusDone            JobStatus = "DONE"
	JobStatusFailed          JobStatus = "FAILED"
	JobStatusResolveCanceled JobStatus = "RESOLVE_CANCELED"
	JobStatusConfirmCanceled JobStatus = "CONFIRM_CANCELED"
	JobStatusUserCanceled    JobStatus = "USER_CANCELED"
)

type (
	// Job is the translated domain object representing an event.
	Job struct {
		ID           uuid.UUID
		ExternalID   string
		Data         []byte
		Type         string
		Status       JobStatus
		ErrorMessage string
		UpdatedAt    int64
		CreatedAt    int64
	}

	// JobStatus represents the possible statuses of a Job.
	JobStatus string
)

// NewJob creates a new Job instance with the given parameters.
func NewJob(jobType string, data []byte) Job {
	return Job{
		Data: data,
		Type: jobType,
	}
}

// WithExternalID allows to set an external identifier for the job.
func (j Job) WithExternalID(id string) Job {
	j.ExternalID = id
	return j
}

// isCancelable checks if the job can be canceled based on its current status.
func (j Job) isCancelable() bool {
	return j.Status == JobStatusCreated || j.Status == JobStatusConfirmed || j.Status == JobStatusResolving ||
		j.Status == JobStatusReady || j.Status == JobStatusProcessing
}

// JobStatuses is a slice of JobStatus values.
type JobStatuses []JobStatus

// StringSlice converts the JobStatuses to a slice of strings.
func (js JobStatuses) StringSlice() []string {
	result := make([]string, len(js))
	for i, state := range js {
		result[i] = string(state)
	}
	return result
}

// TransientStatuses returns the list of job statuses that are considered transient.
func TransientStatuses() JobStatuses {
	return []JobStatus{
		JobStatusCreated,
		JobStatusConfirming,
		JobStatusConfirmed,
		JobStatusResolving,
		JobStatusReady,
		JobStatusProcessing,
	}
}

// TerminalStatuses returns the list of job statuses that are considered terminal.
func TerminalStatuses() []JobStatus {
	return []JobStatus{
		JobStatusDone,
		JobStatusFailed,
		JobStatusResolveCanceled,
		JobStatusConfirmCanceled,
		JobStatusUserCanceled,
	}
}
