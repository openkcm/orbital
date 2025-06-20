package orbital

import (
	"github.com/google/uuid"
)

// Possible job states.
const (
	JobStatusCreated         JobStatus = "CREATED"
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
		Data         []byte
		Type         string
		Status       JobStatus
		ErrorMessage string
		UpdatedAt    int64
		CreatedAt    int64
	}

	// JobStatus represents the possible states of a Job.
	JobStatus string
)

// NewJob creates a new Job instance with the given parameters.
func NewJob(jobType string, data []byte) Job {
	return Job{
		Data: data,
		Type: jobType,
	}
}

// isCancelable checks if the job can be canceled based on its current status.
func (j *Job) isCancelable() bool {
	return j.Status == JobStatusCreated || j.Status == JobStatusConfirmed || j.Status == JobStatusResolving ||
		j.Status == JobStatusReady || j.Status == JobStatusProcessing
}
