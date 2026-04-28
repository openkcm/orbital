package orbital

import (
	"context"
	"errors"
	"fmt"
	"sync"

	slogctx "github.com/veqryn/slog-context"
)

type (
	// TaskRequestHandler processes a single inbound TaskRequest and returns a TaskResponse.
	// It performs signature verification, handler dispatch, and response signing.
	TaskRequestHandler func(ctx context.Context, req TaskRequest) (TaskResponse, error)

	// Option is a function that modifies the config parameter of the Operator.
	Option func(*config) error
	config struct {
		bufferSize      int
		numberOfWorkers int
	}

	handlerRegistry struct {
		mu sync.RWMutex
		r  map[string]HandlerFunc
	}

	// Operator handles task requests and responses.
	Operator struct {
		target          TargetOperator
		handlerRegistry handlerRegistry
		requests        chan TaskRequest
		numberOfWorkers int
	}
)

var (
	ErrOperatorInvalidConfig      = errors.New("invalid operator configuration")
	ErrHandlerNil                 = errors.New("handler cannot be nil")
	ErrBufferSizeNegative         = errors.New("buffer size cannot be negative")
	ErrNumberOfWorkersNotPositive = errors.New("number of workers must be greater than 0")
	ErrUnknownTaskType            = errors.New("unknown task type")
	ErrUnsupportedResponder       = errors.New("unsupported responder type")
	ErrSignatureInvalid           = errors.New("task request signature invalid")
	ErrResponseSigning            = errors.New("failed to sign task response")
)

// NewOperator creates a new Operator instance with the given TargetOperator and options.
func NewOperator(target TargetOperator, opts ...Option) (*Operator, error) {
	if target.MustCheckSignature && target.Verifier == nil {
		return nil, ErrOperatorInvalidConfig
	}

	c := config{
		bufferSize:      100,
		numberOfWorkers: 10,
	}

	for _, opt := range opts {
		err := opt(&c)
		if err != nil {
			return nil, err
		}
	}

	return &Operator{
		target:          target,
		handlerRegistry: handlerRegistry{r: make(map[string]HandlerFunc)},
		requests:        make(chan TaskRequest, c.bufferSize),
		numberOfWorkers: c.numberOfWorkers,
	}, nil
}

// WithBufferSize sets the buffer size for the requests channel.
// It returns an error if the size is negative.
func WithBufferSize(size int) Option {
	return func(c *config) error {
		if size < 0 {
			return ErrBufferSizeNegative
		}
		c.bufferSize = size
		return nil
	}
}

// WithNumberOfWorkers sets the number of workers for processing requests.
// It returns an error if the number is not positive.
func WithNumberOfWorkers(num int) Option {
	return func(c *config) error {
		if num <= 0 {
			return ErrNumberOfWorkersNotPositive
		}
		c.numberOfWorkers = num
		return nil
	}
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

// ListenAndRespond starts listening for task requests and responding to them.
// It blocks until ctx is cancelled or the transport exits.
func (o *Operator) ListenAndRespond(ctx context.Context) error {
	switch r := o.target.Client.(type) {
	case SyncResponder:
		return r.Run(ctx, o.process)
	case AsyncResponder:
		o.startWorkers(ctx, r)
		o.startListening(ctx, r)

		return ctx.Err()
	default:
		return ErrUnsupportedResponder
	}
}

func (o *Operator) process(ctx context.Context, req TaskRequest) (TaskResponse, error) {
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

func (o *Operator) startListening(ctx context.Context, r AsyncResponder) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		req, err := r.ReceiveTaskRequest(ctx)
		if err != nil {
			slogctx.Error(ctx, "failed to receive task request", "error", err)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case o.requests <- req:
		}
	}
}

func (o *Operator) startWorkers(ctx context.Context, r AsyncResponder) {
	for range o.numberOfWorkers {
		go o.worker(ctx, r)
	}
}

func (o *Operator) worker(ctx context.Context, r AsyncResponder) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-o.requests:
			resp, err := o.process(ctx, req)
			if err != nil {
				slogctx.Error(ctx, "failed to process task request", "error", err, "taskId", req.TaskID, "etag", req.ETag)
				continue
			}

			err = r.SendTaskResponse(ctx, resp)
			if err != nil {
				slogctx.Error(ctx, "failed to send task response",
					"error", err, "taskId", resp.TaskID, "etag", resp.ETag)
				continue
			}
			slogctx.Debug(ctx, "sent task response",
				"taskId", resp.TaskID, "etag", resp.ETag)
		}
	}
}
