package orbital

import (
	"context"
	"errors"
	"log"
	"sync"

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
		responder       Responder
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
		WorkingState []byte
	}

	// HandlerResponse contains the fields extracted from orbital.TaskResponse
	// that can be modified by the operator during processing.
	HandlerResponse struct {
		WorkingState      []byte
		Result            Result
		ReconcileAfterSec int64
	}

	// Result represents the result of the operator's processing.
	Result string
)

var (
	ErrHandlerNil                 = errors.New("handler cannot be nil")
	ErrBufferSizeNegative         = errors.New("buffer size cannot be negative")
	ErrNumberOfWorkersNotPositive = errors.New("number of workers must be greater than 0")
)

var ErrMsgUnknownTaskType = "unknown task type"

// NewOperator creates a new Operator instance with the given Responder and options.
func NewOperator(r Responder, opts ...Option) (*Operator, error) {
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
		responder:       r,
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
//
// NOTE: Handlers must be registered before calling ListenAndRespond.
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
			req, err := o.responder.ReceiveTaskRequest(ctx)
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
					logCtx := slogctx.With(ctx, "externalID", req.ExternalID, "taskID", req.TaskID, "etag", req.ETag)
					slogctx.Debug(logCtx, "received task request")
					resp := TaskResponse{
						TaskID:     req.TaskID,
						Type:       req.Type,
						ExternalID: req.ExternalID,
						ETag:       req.ETag,
					}

					h, ok := o.handlerRegistry.r[req.Type]
					if !ok {
						o.sendErrorResponse(logCtx, resp, ErrMsgUnknownTaskType)
						continue
					}

					hResp, err := h(logCtx, HandlerRequest{
						TaskID:       req.TaskID,
						Type:         req.Type,
						Data:         req.Data,
						WorkingState: req.WorkingState,
					})
					if err != nil {
						o.sendErrorResponse(logCtx, resp, err.Error())
						continue
					}

					resp.WorkingState = hResp.WorkingState
					resp.Status = string(hResp.Result)
					resp.ReconcileAfterSec = hResp.ReconcileAfterSec
					err = o.responder.SendTaskResponse(logCtx, resp)
					handleError(logCtx, "sending task response", err)
				}
			}
		}()
	}
}

func (o *Operator) sendErrorResponse(ctx context.Context, resp TaskResponse, errMsg string) {
	resp.Status = string(ResultFailed)
	resp.ErrorMessage = errMsg
	err := o.responder.SendTaskResponse(ctx, resp)
	handleError(ctx, "sending task response", err)
}

func handleError(ctx context.Context, msg string, err error) {
	if err != nil {
		slogctx.Error(ctx, "ERROR: %s, %v", msg, err)
		return
	}
	slogctx.Debug(ctx, msg)
}
