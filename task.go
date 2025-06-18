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

// Default maximum number of times a task can be sent.
const DefTaskMaxSentCount int64 = 10

// TaskStatus represents the status of the Task.
type TaskStatus string

// Task is a trackable unit derived from the Job.
type Task struct {
	ID                uuid.UUID
	JobID             uuid.UUID
	Type              string
	Data              []byte
	WorkingState      []byte
	LastSentAt        int64
	SentCount         int64
	MaxSentCount      int64
	ReconcileAfterSec int64
	ETag              string
	Status            TaskStatus
	Target            string
	ErrorMessage      string
	UpdatedAt         int64
	CreatedAt         int64
}

// TaskInfo represents the result of resolving a task.
type TaskInfo struct {
	// Data contains the byte data that needs to be sent.
	Data []byte
	// Type specifies the type of the data.
	Type string
	// Targets lists the target identifiers associated with job.
	Target string
	// MaxSentCount is the maximum number of times the task can be sent.
	MaxSentCount int64
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
	if info.MaxSentCount <= 0 {
		info.MaxSentCount = DefTaskMaxSentCount
	}
	return Task{
		JobID:        jobID,
		Type:         info.Type,
		Data:         info.Data,
		WorkingState: make([]byte, 0),
		MaxSentCount: info.MaxSentCount,
		ETag:         uuid.NewString(),
		Status:       TaskStatusCreated,
		Target:       info.Target,
	}
}
