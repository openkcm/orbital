package orbital

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/openkcm/common-sdk/pkg/jwtsigning"
)

// MessageSignatureKey is the key used to store the message signature.
const MessageSignatureKey = "X-Message-Signature"

type (
	// InitiatorSignatureHandler manages signing and verification operations for initiators.
	// It holds references to a signer for generating signatures and a verifier for validating them.
	InitiatorSignatureHandler struct {
		signer   *jwtsigning.Signer
		verifier *jwtsigning.Verifier
	}

	// ResponderSignatureHandler manages signing and verification operations for responders.
	// It holds references to a signer for generating signatures and a verifier for validating them.
	ResponderSignatureHandler struct {
		signer   *jwtsigning.Signer
		verifier *jwtsigning.Verifier
	}
)

var (
	_ TaskRequestSigner    = &InitiatorSignatureHandler{}
	_ TaskResponseVerifier = &InitiatorSignatureHandler{}
	_ TaskRequestVerifier  = &ResponderSignatureHandler{}
	_ TaskResponseSigner   = &ResponderSignatureHandler{}
)

var (
	ErrMissingMessageSignature = errors.New("signer missing message signature")
	ErrUnsupportedDataType     = errors.New("signer unsupported data type")
	ErrSignerNil               = errors.New("signer cannot be nil")
	ErrVerifierNil             = errors.New("verifier cannot be nil")
)

// NewInitiatorSignatureHandler creates a new InitiatorSignatureHandler using the provided signer and verifier.
// Returns an error if either signer or verifier is nil.
func NewInitiatorSignatureHandler(signer *jwtsigning.Signer, verifier *jwtsigning.Verifier) (*InitiatorSignatureHandler, error) {
	if signer == nil {
		return nil, ErrSignerNil
	}
	if verifier == nil {
		return nil, ErrVerifierNil
	}
	return &InitiatorSignatureHandler{
		signer:   signer,
		verifier: verifier,
	}, nil
}

// NewResponderSignatureHandler creates a new ResponderSignatureHandler using the provided signer and verifier.
// Returns an error if either signer or verifier is nil.
func NewResponderSignatureHandler(signer *jwtsigning.Signer, verifier *jwtsigning.Verifier) (*ResponderSignatureHandler, error) {
	if signer == nil {
		return nil, ErrSignerNil
	}
	if verifier == nil {
		return nil, ErrVerifierNil
	}
	return &ResponderSignatureHandler{
		signer:   signer,
		verifier: verifier,
	}, nil
}

// Verify checks the signature of the provided TaskResponse using the InitiatorSignatureHandler's verifier.
// Returns an error if the signature is invalid.
func (m *InitiatorSignatureHandler) Verify(ctx context.Context, response TaskResponse) error {
	return verifySignature(ctx, m.verifier, response)
}

// Sign generates a signature for the provided TaskRequest using the InitiatorSignatureHandler's signer.
// Returns the generated Signature and any error encountered during signing.
func (m *InitiatorSignatureHandler) Sign(ctx context.Context, request TaskRequest) (Signature, error) {
	return signSignature(ctx, m.signer, request)
}

// Sign generates a signature for the provided TaskResponse using the ResponderSignatureHandler's signer.
// Returns the generated Signature and any error encountered during signing.
func (o *ResponderSignatureHandler) Sign(ctx context.Context, response TaskResponse) (Signature, error) {
	return signSignature(ctx, o.signer, response)
}

// Verify checks the signature of the provided TaskRequest using the ResponderSignatureHandler's verifier.
// Returns an error if the signature is invalid.
func (o *ResponderSignatureHandler) Verify(ctx context.Context, request TaskRequest) error {
	return verifySignature(ctx, o.verifier, request)
}

func signSignature[T TaskRequest | TaskResponse](ctx context.Context, signer *jwtsigning.Signer, in T) (Signature, error) {
	b, err := toCanonicalData(in)
	if err != nil {
		return nil, err
	}

	sig, err := signer.Sign(ctx, b)
	if err != nil {
		return nil, err
	}

	return Signature{
		MessageSignatureKey: sig,
	}, nil
}

func verifySignature[T TaskRequest | TaskResponse](ctx context.Context, verifier *jwtsigning.Verifier, in T) error {
	token, err := tokenFromMetaData(in)
	if err != nil {
		return err
	}

	b, err := toCanonicalData(in)
	if err != nil {
		return err
	}
	return verifier.Verify(ctx, token, b)
}

func tokenFromMetaData[T TaskRequest | TaskResponse](in T) (string, error) {
	switch v := any(in).(type) {
	case TaskRequest:
		t, ok := v.MetaData[MessageSignatureKey]
		if !ok {
			return "", ErrMissingMessageSignature
		}
		return t, nil
	case TaskResponse:
		t, ok := v.MetaData[MessageSignatureKey]
		if !ok {
			return "", ErrMissingMessageSignature
		}
		return t, nil
	default:
		return "", ErrUnsupportedDataType
	}
}

// toCanonicalData serializes a TaskRequest or TaskResponse into a canonical byte slice representation.
// The output is used for signing and verification purposes. Returns an error if the input type is unsupported.
// Example serialization format
// For TaskRequest:
// taskId:string,type:string,externalId:string,data:base64EnodedString,workingState:base64EnodedString,eTag:string
// For TaskResponse:
// taskId:string,type:string,externalId:string,workingState:base64EnodedString,eTag:string,status:string,errorMessage:string,reconcileAfterSec:string .
func toCanonicalData[T TaskRequest | TaskResponse](in T) ([]byte, error) {
	enc := base64.StdEncoding

	switch v := any(in).(type) {
	case TaskRequest:
		taskID := v.TaskID.String()
		if v.TaskID == uuid.Nil {
			taskID = ""
		}
		return concatStringToBytes(
			"taskId:", taskID,
			",type:", v.Type,
			",externalId:", v.ExternalID,
			",data:", enc.EncodeToString(v.Data),
			",workingState:", enc.EncodeToString(v.WorkingState),
			",eTag:", v.ETag,
		)
	case TaskResponse:
		taskID := v.TaskID.String()
		if v.TaskID == uuid.Nil {
			taskID = ""
		}
		return concatStringToBytes(
			"taskId:", taskID,
			",type:", v.Type,
			",externalId:", v.ExternalID,
			",workingState:", enc.EncodeToString(v.WorkingState),
			",eTag:", v.ETag,
			",status:", v.Status,
			",errorMessage:", v.ErrorMessage,
			",reconcileAfterSec:", strconv.FormatInt(v.ReconcileAfterSec, 10),
		)
	default:
		return nil, ErrUnsupportedDataType
	}
}

func concatStringToBytes(values ...string) ([]byte, error) {
	var sb strings.Builder
	for _, value := range values {
		_, err := sb.WriteString(value)
		if err != nil {
			return nil, err
		}
	}
	return []byte(sb.String()), nil
}
