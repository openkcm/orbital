package orbital

import (
	"context"
	"errors"
	"fmt"
	"sync"

	slogctx "github.com/veqryn/slog-context"
)

type (
	// Runner drives a transport: it decides where task requests come from
	// and where task responses go. Run is fire-and-forget: implementations
	// spawn their own goroutines and return; ctx cancellation signals
	// shutdown.
	Runner interface {
		Run(ctx context.Context, process ProcessFunc)
	}

	// ProcessFunc is the shared pipeline handed to a Runner. It performs
	// signature verification, handler dispatch, and response signing.
	// Sentinel errors (ErrSignatureInvalid, ErrResponseSigning) let sync
	// transports map to transport-level failures; unknown task types stay
	// in-band as a FAILED TaskResponse with a nil error.
	ProcessFunc func(ctx context.Context, req TaskRequest) (TaskResponse, error)

	// Operator handles task requests and responses.
	Operator struct {
		target          TargetOperator
		handlerRegistry handlerRegistry
	}

	handlerRegistry struct {
		mu sync.RWMutex
		r  map[string]HandlerFunc
	}
)

var (
	ErrOperatorInvalidConfig = errors.New("invalid operator configuration")
	ErrHandlerNil            = errors.New("handler cannot be nil")
	ErrRunnerNil             = errors.New("runner cannot be nil")
	ErrUnknownTaskType       = errors.New("unknown task type")

	// ErrSignatureInvalid is returned by Process when signature verification
	// fails. Sync transports should translate this to a transport-level
	// auth failure (e.g. gRPC codes.Unauthenticated).
	ErrSignatureInvalid = errors.New("task request signature invalid")

	// ErrResponseSigning is returned by Process when the response signer
	// fails. Sync transports should translate this to a transport-level
	// internal failure (e.g. gRPC codes.Internal).
	ErrResponseSigning = errors.New("failed to sign task response")
)

// NewOperator creates a new Operator from a TargetOperator configuration.
// The Runner is required; a nil Runner yields ErrOperatorInvalidConfig.
// If MustCheckSignature is set, Verifier must be non-nil.
func NewOperator(target TargetOperator) (*Operator, error) {
	if target.Runner == nil {
		return nil, fmt.Errorf("%w: %w", ErrOperatorInvalidConfig, ErrRunnerNil)
	}
	if target.MustCheckSignature && target.Verifier == nil {
		return nil, ErrOperatorInvalidConfig
	}

	return &Operator{
		target:          target,
		handlerRegistry: handlerRegistry{r: make(map[string]HandlerFunc)},
	}, nil
}

// RegisterHandler registers a handler for a specific task type.
// It returns an error if the handler is nil.
func (o *Operator) RegisterHandler(taskType string, h HandlerFunc) error {
	if h == nil {
		return ErrHandlerNil
	}
	o.handlerRegistry.mu.Lock()
	defer o.handlerRegistry.mu.Unlock()
	o.handlerRegistry.r[taskType] = h
	return nil
}

// ListenAndRespond starts the configured Runner. It is fire-and-forget: the
// Runner spawns its own goroutines and this call returns. Shutdown is
// signalled via ctx cancellation.
func (o *Operator) ListenAndRespond(ctx context.Context) {
	o.target.Runner.Run(ctx, o.Process)
}

// Process is the shared pipeline: signature verify → handler dispatch →
// response signing. It is exposed so that custom Runner implementations
// (sync or async) can drive the Operator directly.
//
// Return semantics:
//   - A TaskResponse with Status=FAILED (unknown task type or handler-reported
//     failure) is a *successful* pipeline run and is returned with nil error.
//     Transports should deliver it normally.
//   - A non-nil error (ErrSignatureInvalid, ErrResponseSigning) is a pipeline
//     failure. Async transports typically drop; sync transports map to a
//     transport-level error.
func (o *Operator) Process(ctx context.Context, req TaskRequest) (TaskResponse, error) {
	logCtx := slogctx.With(ctx,
		"externalId", req.ExternalID,
		"taskId", req.TaskID,
		"etag", req.ETag,
		"taskType", req.Type,
	)
	slogctx.Debug(logCtx, "received task request")

	if !o.isValidSignature(logCtx, req) {
		return TaskResponse{}, ErrSignatureInvalid
	}

	o.handlerRegistry.mu.RLock()
	h, ok := o.handlerRegistry.r[req.Type]
	o.handlerRegistry.mu.RUnlock()

	var resp TaskResponse
	if !ok {
		slogctx.Error(logCtx, "no handler registered for task type", "taskType", req.Type)
		resp = req.prepareResponse()
		resp.Status = string(TaskStatusFailed)
		resp.ErrorMessage = "no handler registered for task type " + req.Type
	} else {
		resp = ExecuteHandler(logCtx, h, req)
	}

	signature, err := o.createSignature(logCtx, resp)
	if err != nil {
		return TaskResponse{}, fmt.Errorf("%w: %w", ErrResponseSigning, err)
	}
	resp.addMeta(signature)

	return resp, nil
}

func (o *Operator) isValidSignature(ctx context.Context, req TaskRequest) bool {
	if !o.target.MustCheckSignature {
		return true
	}
	verifier := o.target.Verifier
	if verifier == nil {
		slogctx.Error(ctx, "signature verification is enabled but no signature verifier is set")
		return false
	}
	err := verifier.Verify(ctx, req)
	if err != nil {
		slogctx.Error(ctx, "failed while verifying task request signature", "error", err)
		return false
	}
	return true
}

func (o *Operator) createSignature(ctx context.Context, resp TaskResponse) (Signature, error) {
	signer := o.target.Signer
	if signer != nil {
		signature, err := signer.Sign(ctx, resp)
		if err != nil {
			slogctx.Error(ctx, "signing task response", "error", err)
		}
		return signature, err
	}
	return Signature{}, nil
}
