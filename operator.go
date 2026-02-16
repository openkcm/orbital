package orbital

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	slogctx "github.com/veqryn/slog-context"
)

type (
	// Operator handles task requests and responses.
	Operator struct {
		target          TargetOperator
		handlerRegistry handlerRegistry
		requests        chan TaskRequest
		numberOfWorkers int
	}

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
)

var (
	ErrOperatorInvalidConfig      = errors.New("invalid operator configuration")
	ErrHandlerNil                 = errors.New("handler cannot be nil")
	ErrBufferSizeNegative         = errors.New("buffer size cannot be negative")
	ErrNumberOfWorkersNotPositive = errors.New("number of workers must be greater than 0")
	ErrUnknownTaskType            = errors.New("unknown task type")
)

// NewOperator creates a new Operator instance with the given Responder and options.
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
func (o *Operator) ListenAndRespond(ctx context.Context) {
	go o.startListening(ctx)
	o.startResponding(ctx)
}

func (o *Operator) startListening(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			req, err := o.target.Client.ReceiveTaskRequest(ctx)
			if err != nil {
				slogctx.Error(ctx, "failed to receive task request", "error", err)
				continue
			}
			o.requests <- req
		}
	}
}

func (o *Operator) startResponding(ctx context.Context) {
	for range o.numberOfWorkers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case req := <-o.requests:
					logCtx := slogctx.With(ctx, "externalID", req.ExternalID, "taskID", req.TaskID, "etag", req.ETag, "taskType", req.Type)
					slogctx.Debug(logCtx, "received task request")

					isValid := o.isValidSignature(logCtx, req)
					if !isValid {
						continue
					}

					var resp TaskResponse

					h, ok := o.handlerRegistry.r[req.Type]
					if !ok {
						slogctx.Error(logCtx, "no handler registered for task type", "taskType", req.Type)
						resp = req.prepareResponse()
						resp.Status = string(TaskStatusFailed)
						resp.ErrorMessage = "no handler registered for task type " + req.Type
					} else {
						resp = executeHandler(logCtx, h, req)
					}

					signature, err := o.createSignature(logCtx, resp)
					if err != nil {
						continue
					}
					resp.addMeta(signature)

					logCtx = slogctx.With(logCtx, slog.Any("response", resp))
					err = o.target.Client.SendTaskResponse(logCtx, resp)
					if err != nil {
						slogctx.Error(logCtx, "failed to send task response", "error", err)
						continue
					}
					slogctx.Debug(logCtx, "sent task response")
				}
			}
		}()
	}
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
