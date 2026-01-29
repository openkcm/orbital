package orbital

import (
	"context"
	"maps"

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
	MetaData     MetaData  `json:"metaData"`     // MetaData contains additional information about the TaskRequest.
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
	ReconcileAfterSec uint64    `json:"reconcileAfterSec"`      // ReconcileAfterSec is the time in seconds after which the next TaskRequest should be queued again.
	MetaData          MetaData  `json:"metaData"`               // MetaData contains additional information about the TaskResponse.
}

// MetaData represents a set of key-value pairs containing metadata
// information. It is commonly used for storing additional attributes
// related to requests, responses, or signatures.
type MetaData map[string]string

// TargetManager holds the client and cryptographic implementation for initiating
// tasks. It provides access to the Initiator for communication,
// Signer and Verifier for signing and verification operations.
type TargetManager struct {
	Client             Initiator
	Signer             TaskRequestSigner
	Verifier           TaskResponseVerifier
	MustCheckSignature bool
}

// Initiator defines the methods for sending task requests and receiving task responses.
type Initiator interface {
	SendTaskRequest(ctx context.Context, request TaskRequest) error
	ReceiveTaskResponse(ctx context.Context) (TaskResponse, error)
	Close(ctx context.Context) error
}

// TargetOperator holds the client and cryptographic implementation for responding
// to tasks. It provides access to the Responder for communication,
// Signer and Verifier for signing and verification operations.
type TargetOperator struct {
	Client             Responder
	Verifier           TaskRequestVerifier
	Signer             TaskResponseSigner
	MustCheckSignature bool
}

// Responder defines the methods for receiving task requests and sending task responses.
type Responder interface {
	ReceiveTaskRequest(ctx context.Context) (TaskRequest, error)
	SendTaskResponse(ctx context.Context, response TaskResponse) error
	Close(ctx context.Context) error
}

// Signature represents the metadata used for signing and verifying requests.
// It typically contains cryptographic information such as signatures or tokens.
type Signature MetaData

// TaskRequestSigner defines an interface for signing TaskRequest objects.
// Implementations of this interface are responsible for generating a Signature
// for a given TaskRequest, typically for authentication or verification purposes.
type TaskRequestSigner interface {
	// Sign signs the given TaskRequest and returns a Signature.
	Sign(ctx context.Context, request TaskRequest) (Signature, error)
}

// TaskRequestVerifier defines an interface for verifying the authenticity of TaskRequest objects.
// Implementations of this interface are responsible for checking the validity and integrity
// of a given TaskRequest, typically using cryptographic methods.
type TaskRequestVerifier interface {
	// Verify verifies the authenticity of the given TaskRequest.
	Verify(ctx context.Context, request TaskRequest) error
}

// TaskResponseSigner defines an interface for signing TaskResponse objects.
// Implementations of this interface are responsible for generating a Signature
// for a given TaskResponse, typically for authentication or verification purposes.
type TaskResponseSigner interface {
	// Sign signs the given TaskResponse and returns a Signature.
	Sign(ctx context.Context, response TaskResponse) (Signature, error)
}

// TaskResponseVerifier defines an interface for verifying the authenticity of TaskResponse objects.
// Implementations of this interface are responsible for checking the validity and integrity
// of a given TaskResponse, typically using cryptographic methods.
type TaskResponseVerifier interface {
	// Verify verifies the authenticity of the given TaskResponse.
	Verify(ctx context.Context, response TaskResponse) error
}

// Codec defines the methods for encoding and decoding task requests and responses.
type Codec interface {
	EncodeTaskRequest(request TaskRequest) ([]byte, error)
	DecodeTaskRequest(bytes []byte) (TaskRequest, error)
	EncodeTaskResponse(response TaskResponse) ([]byte, error)
	DecodeTaskResponse(bytes []byte) (TaskResponse, error)
}

func (req *TaskRequest) addMeta(m map[string]string) {
	if req.MetaData == nil {
		req.MetaData = make(MetaData)
	}
	maps.Copy(req.MetaData, m)
}

func (res *TaskResponse) addMeta(m map[string]string) {
	if res.MetaData == nil {
		res.MetaData = make(MetaData)
	}
	maps.Copy(res.MetaData, m)
}
