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
	MetaData     MetaData  `json:"metaData"`
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
	MetaData          MetaData  `json:"metaData"`
}

type MetaData struct {
	Signature   Signature `json:"signature"`
	IsEncrypted bool      `json:"isEncrypted,omitempty"`
}

// Signature represents a cryptographic signature with its value and type.
// Value contains the signature string, and Type specifies the signature type.
type Signature struct {
	Value string `json:"value"`
	Type  string `json:"type"`
}

// Initiator holds the client and cryptographic implementation for initiating
// tasks. It provides access to the InitiatorClient for communication and
// InitiatorCrypto for cryptographic operations.
type Initiator struct {
	Client InitiatorClient
	Crypto InitiatorCrypto
}

// InitiatorClient defines the methods for sending task requests and receiving task responses.
type InitiatorClient interface {
	SendTaskRequest(ctx context.Context, request TaskRequest) error
	ReceiveTaskResponse(ctx context.Context) (TaskResponse, error)
}

// InitiatorCrypto defines cryptographic operations for initiators.
type InitiatorCrypto interface {
	// SignTaskRequest signs the given TaskRequest and returns a Signature.
	SignTaskRequest(ctx context.Context, request TaskRequest) (Signature, error)
	// VerifyTaskResponse verifies the authenticity of the given TaskResponse.
	VerifyTaskResponse(ctx context.Context, response TaskResponse) error
}

// Responder holds the client and cryptographic implementation for responding
// to tasks. It provides access to the ResponderClient for communication and
// ResponderCrypto for cryptographic operations.
type Responder struct {
	Client ResponderClient
	Crypto ResponderCrypto
}

// ResponderClient defines the methods for receiving task requests and sending task responses.
type ResponderClient interface {
	ReceiveTaskRequest(ctx context.Context) (TaskRequest, error)
	SendTaskResponse(ctx context.Context, response TaskResponse) error
}

// ResponderCrypto defines cryptographic operations for responders.
type ResponderCrypto interface {
	// SignTaskResponse signs the given TaskResponse and returns a Signature.
	SignTaskResponse(ctx context.Context, response TaskResponse) (Signature, error)
	// VerifyTaskRequest verifies the authenticity of the given TaskRequest.
	VerifyTaskRequest(ctx context.Context, request TaskRequest) error
}

// Codec defines the methods for encoding and decoding task requests and responses.
type Codec interface {
	EncodeTaskRequest(request TaskRequest) ([]byte, error)
	DecodeTaskRequest(bytes []byte) (TaskRequest, error)
	EncodeTaskResponse(response TaskResponse) ([]byte, error)
	DecodeTaskResponse(bytes []byte) (TaskResponse, error)
}
