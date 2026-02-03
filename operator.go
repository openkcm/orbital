package orbital

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	slogctx "github.com/veqryn/slog-context"
)

const (
	ResultFailed     Result = "FAILED"
	ResultProcessing Result = "PROCESSING"
	ResultDone       Result = "DONE"
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
		r  map[string]Handler
	}
)

type (
	// Handler processes task requests and populates the response.
	// It returns an error if the task cannot be processed successfully.
	Handler func(ctx context.Context, request HandlerRequest, response *HandlerResponse) error

	// HandlerRequest contains information extracted from orbital.TaskRequest
	// that are relevant for the operator's processing.
	HandlerRequest struct {
		TaskID uuid.UUID
		Type   string
		Data   []byte
	}

	// HandlerResponse contains information that can be modified by the operator
	// and will be populated to the orbital.TaskResponse.
	HandlerResponse struct {
		RawWorkingState   []byte
		Result            Result
		ReconcileAfterSec uint64

		workingState *WorkingState
	}

	// Result represents the result of the operator's processing.
	Result string
)

// WorkingState returns the WorkingState from the HandlerResponse.
// It returns an error if the decoding of the RawWorkingState fails.
//
// The WorkingState is automatically encoded back into the orbital.TaskResponse.
// If the working state is not decoded,
// if the changes are discarded,
// or if there is an error during encoding,
// the RawWorkingState field will take precedence.
func (r *HandlerResponse) WorkingState() (*WorkingState, error) {
	if r.workingState != nil {
		return r.workingState, nil
	}

	if len(r.RawWorkingState) == 0 {
		workingState := &WorkingState{
			s: make(map[string]any),
		}
		r.workingState = workingState
		return workingState, nil
	}

	workingState, err := decodeWorkingState(r.RawWorkingState)
	if err != nil {
		return nil, err
	}
	r.workingState = workingState

	return workingState, nil
}

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
		handlerRegistry: handlerRegistry{r: make(map[string]Handler)},
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
func (o *Operator) RegisterHandler(taskType string, h Handler) error {
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
				log.Printf("ERROR: receiving task request, %v", err)
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

					resp, err := o.handleRequest(logCtx, req)
					if err != nil {
						o.sendErrorResponse(logCtx, resp, err)
						continue
					}

					signature, err := o.createSignature(logCtx, resp)
					if err != nil {
						continue
					}
					resp.addMeta(signature)

					slogctx.Debug(logCtx, "sending task response", slog.Any("response", resp))
					err = o.target.Client.SendTaskResponse(logCtx, resp)
					handleError(logCtx, "sending task response", err)
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

func (o *Operator) handleRequest(ctx context.Context, req TaskRequest) (TaskResponse, error) {
	resp := TaskResponse{
		TaskID:     req.TaskID,
		Type:       req.Type,
		ExternalID: req.ExternalID,
		ETag:       req.ETag,
	}

	h, ok := o.handlerRegistry.r[req.Type]
	if !ok {
		return resp, fmt.Errorf("%w: %s", ErrUnknownTaskType, req.Type)
	}

	hReq := HandlerRequest{
		TaskID: req.TaskID,
		Type:   req.Type,
		Data:   req.Data,
	}
	hResp := &HandlerResponse{
		RawWorkingState: req.WorkingState,
		Result:          ResultProcessing,
	}
	start := time.Now()
	err := h(ctx, hReq, hResp)
	slogctx.Debug(ctx, "task handler finished", "processingTime", time.Since(start))
	if err != nil {
		return resp, err
	}

	resp.Status = string(hResp.Result)
	resp.ReconcileAfterSec = hResp.ReconcileAfterSec
	resp.WorkingState = hResp.RawWorkingState
	if hResp.workingState != nil && hResp.workingState.s != nil {
		encodedState, err := hResp.workingState.encode()
		if err != nil {
			slogctx.Warn(ctx, "encoding working state", "error", err)
			return resp, nil
		}
		resp.WorkingState = encodedState
	}

	return resp, nil
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

func (o *Operator) sendErrorResponse(ctx context.Context, resp TaskResponse, err error) {
	slogctx.Error(ctx, "handling task request", "error", err)
	resp.Status = string(ResultFailed)
	resp.ErrorMessage = err.Error()
	err = o.target.Client.SendTaskResponse(ctx, resp)
	handleError(ctx, "sending task response", err)
}

func handleError(ctx context.Context, msg string, err error) {
	if err != nil {
		slogctx.Error(ctx, msg, "error", err)
		return
	}
	slogctx.Debug(ctx, msg)
}
