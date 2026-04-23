package orbital

import (
	"context"
	"errors"
	"fmt"
	"sync"

	slogctx "github.com/veqryn/slog-context"
)

type (
	// ProcessorConfig holds the cryptographic configuration for request
	// processing. Verifier and Signer are optional unless MustCheckSignature
	// is set, in which case Verifier must be non-nil.
	ProcessorConfig struct {
		Verifier           TaskRequestVerifier
		Signer             TaskResponseSigner
		MustCheckSignature bool
	}

	// Processor handles signature verification, handler dispatch, and
	// response signing. It is transport-agnostic: a Runner or server
	// calls Processor.Process for each inbound TaskRequest.
	Processor struct {
		config          ProcessorConfig
		handlerRegistry handlerRegistry
	}

	handlerRegistry struct {
		mu sync.RWMutex
		r  map[string]HandlerFunc
	}
)

var (
	ErrProcessorInvalidConfig = errors.New("invalid processor configuration")
	ErrHandlerNil             = errors.New("handler cannot be nil")
	ErrUnknownTaskType        = errors.New("unknown task type")

	// ErrSignatureInvalid is returned by Process when signature verification
	// fails. Sync transports should translate this to a transport-level
	// auth failure (e.g. gRPC codes.Unauthenticated).
	ErrSignatureInvalid = errors.New("task request signature invalid")

	// ErrResponseSigning is returned by Process when the response signer
	// fails. Sync transports should translate this to a transport-level
	// internal failure (e.g. gRPC codes.Internal).
	ErrResponseSigning = errors.New("failed to sign task response")
)

// NewProcessor creates a Processor from the given configuration.
// If MustCheckSignature is set, Verifier must be non-nil.
func NewProcessor(config ProcessorConfig) (*Processor, error) {
	if config.MustCheckSignature && config.Verifier == nil {
		return nil, ErrProcessorInvalidConfig
	}

	return &Processor{
		config:          config,
		handlerRegistry: handlerRegistry{r: make(map[string]HandlerFunc)},
	}, nil
}

// RegisterHandler registers a handler for a specific task type.
// It returns an error if the handler is nil.
func (p *Processor) RegisterHandler(taskType string, h HandlerFunc) error {
	if h == nil {
		return ErrHandlerNil
	}
	p.handlerRegistry.mu.Lock()
	defer p.handlerRegistry.mu.Unlock()
	p.handlerRegistry.r[taskType] = h
	return nil
}

// Process is the shared pipeline: signature verify → handler dispatch →
// response signing.
//
// Return semantics:
//   - A TaskResponse with Status=FAILED (unknown task type or handler-reported
//     failure) is a *successful* pipeline run and is returned with nil error.
//     Transports should deliver it normally.
//   - A non-nil error (ErrSignatureInvalid, ErrResponseSigning) is a pipeline
//     failure. Async transports typically drop; sync transports map to a
//     transport-level error.
func (p *Processor) Process(ctx context.Context, req TaskRequest) (TaskResponse, error) {
	logCtx := slogctx.With(ctx,
		"externalId", req.ExternalID,
		"taskId", req.TaskID,
		"etag", req.ETag,
		"taskType", req.Type,
	)
	slogctx.Debug(logCtx, "received task request")

	if !p.isValidSignature(logCtx, req) {
		return TaskResponse{}, ErrSignatureInvalid
	}

	p.handlerRegistry.mu.RLock()
	h, ok := p.handlerRegistry.r[req.Type]
	p.handlerRegistry.mu.RUnlock()

	var resp TaskResponse
	if !ok {
		slogctx.Error(logCtx, "no handler registered for task type", "taskType", req.Type)
		resp = req.prepareResponse()
		resp.Status = string(TaskStatusFailed)
		resp.ErrorMessage = "no handler registered for task type " + req.Type
	} else {
		resp = ExecuteHandler(logCtx, h, req)
	}

	signature, err := p.createSignature(logCtx, resp)
	if err != nil {
		return TaskResponse{}, fmt.Errorf("%w: %w", ErrResponseSigning, err)
	}
	resp.addMeta(signature)

	return resp, nil
}

func (p *Processor) isValidSignature(ctx context.Context, req TaskRequest) bool {
	if !p.config.MustCheckSignature {
		return true
	}
	verifier := p.config.Verifier
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

func (p *Processor) createSignature(ctx context.Context, resp TaskResponse) (Signature, error) {
	signer := p.config.Signer
	if signer != nil {
		signature, err := signer.Sign(ctx, resp)
		if err != nil {
			slogctx.Error(ctx, "signing task response", "error", err)
		}
		return signature, err
	}
	return Signature{}, nil
}
