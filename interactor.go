package orbital

import (
	"context"

	"github.com/google/uuid"
)

// TaskRequest is the request object that will be sent to the operator.
type TaskRequest struct {
	TaskID       uuid.UUID `json:"taskId"`       // TaskID is used to identify the task.
	Type         string    `json:"type"`         // Type is the type of the task.
	ExternalID   string    `json:"externalId"`   // External ID serves as an identifier for a Job
	Data         []byte    `json:"data"`         // Data is the static context for the task.
	WorkingState []byte    `json:"workingState"` // WorkingState is the current state of the task that the operator works upon.
	ETag         string    `json:"eTag"`         // ETag is used to identify the version of the TaskRequest.
}

// TaskResponse is the response object received from the operator.
type TaskResponse struct {
	TaskID            uuid.UUID `json:"taskId"`                 // TaskID is used to identify the task.
	Type              string    `json:"type"`                   // Type is the type of the task.
	ExternalID        string    `json:"externalId"`             // External ID serves as an identifier for a Job
	WorkingState      []byte    `json:"workingState"`           // WorkingState is the state of the task that the operator updates.
	ETag              string    `json:"eTag"`                   // ETag is used to correlate the TaskResponse with the TaskRequest.
	Status            string    `json:"status"`                 // Status is the status of the task.
	ErrorMessage      string    `json:"errorMessage,omitempty"` // ErrorMessage contains the error message if the task fails.
	ReconcileAfterSec int64     `json:"reconcileAfterSec"`      // ReconcileAfterSec is the time in seconds after which the next TaskRequest should be queued again.
}

// Initiator defines the methods for sending task requests and receiving task responses.
type Initiator interface {
	SendTaskRequest(ctx context.Context, request TaskRequest) error
	ReceiveTaskResponse(ctx context.Context) (TaskResponse, error)
}

// Responder defines the methods for receiving task requests and sending task responses.
type Responder interface {
	ReceiveTaskRequest(ctx context.Context) (TaskRequest, error)
	SendTaskResponse(ctx context.Context, response TaskResponse) error
}

// Codec defines the methods for encoding and decoding task requests and responses.
type Codec interface {
	EncodeTaskRequest(request TaskRequest) ([]byte, error)
	DecodeTaskRequest(bytes []byte) (TaskRequest, error)
	EncodeTaskResponse(response TaskResponse) ([]byte, error)
	DecodeTaskResponse(bytes []byte) (TaskResponse, error)
}
