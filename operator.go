package orbital

import (
	"context"
	"encoding/json"
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
		target          OperatorTarget
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
	// Handler is a function that takes a HandlerRequest,
	// and returns a HandlerResponse and an error.
	Handler func(ctx context.Context, request HandlerRequest) (HandlerResponse, error)

	// HandlerRequest contains the fields extracted from orbital.TaskRequest
	// that are relevant for the operator's processing.
	HandlerRequest struct {
		TaskID       uuid.UUID
		Type         string
		Data         []byte
		WorkingState *WorkingState
	}

	// HandlerResponse contains the fields extracted from orbital.TaskResponse
	// that can be modified by the operator during processing.
	HandlerResponse struct {
		Result            Result
		ReconcileAfterSec int64
	}

	// WorkingState represents the working state of a task.
	// It provides methods for storing arbitrary key-value pairs
	// and convenience methods for tracking integer metrics.
	WorkingState struct {
		s  map[string]any
		mu sync.RWMutex
	}

	// Result represents the result of the operator's processing.
	Result string
)

var (
	ErrHandlerNil                 = errors.New("handler cannot be nil")
	ErrBufferSizeNegative         = errors.New("buffer size cannot be negative")
	ErrNumberOfWorkersNotPositive = errors.New("number of workers must be greater than 0")
	ErrWorkingStateInvalid        = errors.New("invalid working state")
	ErrUnknownTaskType            = errors.New("unknown task type")
)

// decodeWorkingState decodes a byte slice into a WorkingState.
func decodeWorkingState(data []byte) (*WorkingState, error) {
	if len(data) == 0 {
		return &WorkingState{
			s: make(map[string]any),
		}, nil
	}
	var state map[string]any
	err := json.Unmarshal(data, &state)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWorkingStateInvalid, err)
	}
	return &WorkingState{
		s: state,
	}, nil
}

// Set sets a key-value pair in the WorkingState.
func (w *WorkingState) Set(key string, value any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.s == nil {
		w.s = make(map[string]any)
	}
	w.s[key] = value
}

// Value gets the value for a key from the WorkingState.
func (w *WorkingState) Value(key string) (any, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.s == nil {
		return nil, false
	}
	val, ok := w.s[key]
	return val, ok
}

// Inc increments the value of a key and returns the new value.
func (w *WorkingState) Inc(key string) int {
	return w.add(key, 1)
}

// Dec decrements a key and returns the new value.
func (w *WorkingState) Dec(key string) int {
	return w.add(key, -1)
}

// Add adds the specidied amount to a key and returns the new value.
func (w *WorkingState) Add(key string, amount int) int {
	return w.add(key, amount)
}

// Sub subtracts the specidied amount from a key and returns the new value.
func (w *WorkingState) Sub(key string, amount int) int {
	return w.add(key, -amount)
}

func (w *WorkingState) add(key string, amount int) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.s == nil {
		w.s = make(map[string]any)
	}
	val, ok := w.s[key]
	if !ok {
		w.s[key] = amount
		return amount
	}
	var num int
	switch v := val.(type) {
	case int:
		num = v
	case float64: // JSON numbers are decoded as float64
		num = int(v)
	default:
		num = 0
	}
	num += amount
	w.s[key] = num
	return num
}

// encode encodes the WorkingState to a byte slice.
func (w *WorkingState) encode() ([]byte, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.s == nil {
		return []byte{}, nil
	}
	bytes, err := json.Marshal(w.s)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWorkingStateInvalid, err)
	}
	return bytes, nil
}

// NewOperator creates a new Operator instance with the given Responder and options.
func NewOperator(target OperatorTarget, opts ...Option) (*Operator, error) {
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

					err := o.verifySignature(logCtx, req)
					if err != nil {
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

func (o *Operator) verifySignature(ctx context.Context, req TaskRequest) error {
	verifier := o.target.Verifier
	if verifier != nil {
		err := verifier.Verify(ctx, req)
		if err != nil {
			slogctx.Error(ctx, "error while verifying task request signature", "error", err)
		}
		return err
	}
	return nil
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

	workingState, err := decodeWorkingState(req.WorkingState)
	if err != nil {
		return resp, fmt.Errorf("%w: %w", ErrWorkingStateInvalid, err)
	}

	start := time.Now()
	hResp, err := h(ctx, HandlerRequest{
		TaskID:       req.TaskID,
		Type:         req.Type,
		Data:         req.Data,
		WorkingState: workingState,
	})
	slogctx.Debug(ctx, "task handler finished", "status", hResp.Result, "processingTime", time.Since(start))
	if err != nil {
		return resp, err
	}

	bytes, err := workingState.encode()
	if err != nil {
		return resp, fmt.Errorf("%w: %w", ErrWorkingStateInvalid, err)
	}

	resp.WorkingState = bytes
	resp.Status = string(hResp.Result)
	resp.ReconcileAfterSec = hResp.ReconcileAfterSec

	return resp, nil
}

func (o *Operator) createSignature(ctx context.Context, resp TaskResponse) (Signature, error) {
	signer := o.target.Signer
	if signer != nil {
		signature, err := signer.Sign(ctx, resp)
		if err != nil {
			slogctx.Error(ctx, "ERROR: signing task response, %v", err)
		}
		return signature, err
	}
	return Signature{}, nil
}

func (o *Operator) sendErrorResponse(ctx context.Context, resp TaskResponse, err error) {
	slogctx.Error(ctx, "ERROR: handling task request, %v", err)
	resp.Status = string(ResultFailed)
	resp.ErrorMessage = err.Error()
	err = o.target.Client.SendTaskResponse(ctx, resp)
	handleError(ctx, "sending task response", err)
}

func handleError(ctx context.Context, msg string, err error) {
	if err != nil {
		slogctx.Error(ctx, "ERROR: %s, %v", msg, err)
		return
	}
	slogctx.Debug(ctx, msg)
}
