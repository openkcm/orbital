package orbital

import (
	"github.com/google/uuid"
)

// Possible Task status.
const (
	TaskStatusCreated    TaskStatus = "CREATED"
	TaskStatusProcessing TaskStatus = "PROCESSING"
	TaskStatusDone       TaskStatus = "DONE"
	TaskStatusFailed     TaskStatus = "FAILED"
)

// TaskStatus represents the status of the Task.
type TaskStatus string

// Task is a trackable unit derived from the Job.
type Task struct {
	ID                 uuid.UUID
	JobID              uuid.UUID
	Type               string
	Data               []byte
	WorkingState       []byte
	LastReconciledAt   int64 // The last time the task was reconciled.
	ReconcileCount     int64 // The number of times the task has been reconciled.
	ReconcileAfterSec  int64 // The number of seconds after which the task should be reconciled.
	TotalSentCount     int64
	TotalReceivedCount int64
	ETag               string
	Status             TaskStatus
	Target             string
	ErrorMessage       string
	UpdatedAt          int64
	CreatedAt          int64
}

// TaskInfo represents the result of resolving a task.
type TaskInfo struct {
	// Data contains the byte data that needs to be sent.
	Data []byte
	// Type specifies the type of the data.
	Type string
	// Targets lists the target identifiers associated with job.
	Target string
}

// newTasks creates a slice of Task instances for the given job ID and task configurations.
// It returns a slice of Task, each initialized with the provided job ID and TaskInfo.
func newTasks(jobID uuid.UUID, infos []TaskInfo) []Task {
	tasks := make([]Task, 0, len(infos))
	for _, info := range infos {
		tasks = append(tasks, newTask(jobID, info))
	}
	return tasks
}

// newTask creates and returns a new Task instance with the provided jobID and
// TaskInfo. It initializes the WorkingState as an empty byte slice, sets the
// ETag to a new UUID string, and assigns the TaskStatusCreated status.
func newTask(jobID uuid.UUID, info TaskInfo) Task {
	return Task{
		JobID:        jobID,
		Type:         info.Type,
		Data:         info.Data,
		WorkingState: make([]byte, 0),
		ETag:         uuid.NewString(),
		Status:       TaskStatusCreated,
		Target:       info.Target,
	}
}
