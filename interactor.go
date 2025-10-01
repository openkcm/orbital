package orbital

import (
	"context"

	"github.com/google/uuid"
)

// HeaderMessageSignature is the header key used to store the message signature for hash verification.
const HeaderMessageSignature = "X-Message-Signature"

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

// Crypto defines cryptographic operations.
type Crypto interface {
	// Signature generates a signature the given message.
	// For eg:
	// Returns the signed token as a string, or an error if signing fails.
	// 1. create a hash of the message content
	// 2. create a JSON Web Token (JWT) as below.
	// Header:
	// 		alg: S256
	// 		typ: JWT
	// Payload:
	// 		iss: clusterUrl of the issuer that exposes the .well-known endpoint.
	// 		kid: key id of the signing key
	// 		hash: the calculated hash value
	//    hash-alg: SHA256
	// JWS Serialization Mode: [Compact]: https://datatracker.ietf.org/doc/html/rfc7515#section-3.1
	Signature(message []byte) (string, error)

	// VerifySignature verifies a signature.
	// For eg:
	// 1. validate the token
	// 		* check if issuer (iss) is trusted
	// 		* read kid (signing key id)
	// 		* get public key of the signing key from issuer's .well-known endpoint (cache key by kid)
	// 		* verify signature
	// 		* read hash-alg and hash
	// 2. hash the received message
	// 3. compare message hash value with extracted hash value
	VerifySignature(signature string, message []byte) error
}
