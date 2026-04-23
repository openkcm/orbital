package async

import (
	"context"
	"errors"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital"
)

// Runner implements orbital.Runner on top of a half-duplex Responder
// (e.g. AMQP). It pulls requests from the Responder, fans out to a bounded
// worker pool, and pushes responses back on the same Responder.
type Runner struct {
	client          orbital.Responder
	process         orbital.ProcessFunc
	requests        chan orbital.TaskRequest
	numberOfWorkers int
}

type (
	Option func(*config) error
	config struct {
		bufferSize      int
		numberOfWorkers int
	}
)

var (
	ErrBufferSizeNegative         = errors.New("buffer size cannot be negative")
	ErrNumberOfWorkersNotPositive = errors.New("number of workers must be greater than 0")
	ErrResponderNil               = errors.New("responder cannot be nil")
	ErrProcessFuncNil             = errors.New("process func cannot be nil")
)

var _ orbital.Runner = (*Runner)(nil)

// New creates a Runner backed by the given Responder and ProcessFunc.
// Defaults: buffer size 100, 10 workers.
func New(client orbital.Responder, process orbital.ProcessFunc, opts ...Option) (*Runner, error) {
	if client == nil {
		return nil, ErrResponderNil
	}
	if process == nil {
		return nil, ErrProcessFuncNil
	}

	c := config{
		bufferSize:      100,
		numberOfWorkers: 10,
	}
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	return &Runner{
		client:          client,
		process:         process,
		requests:        make(chan orbital.TaskRequest, c.bufferSize),
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

// Run spawns one listener goroutine and numberOfWorkers worker goroutines,
// then blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	go r.startListening(ctx)
	for range r.numberOfWorkers {
		go r.worker(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (r *Runner) startListening(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		req, err := r.client.ReceiveTaskRequest(ctx)
		if err != nil {
			slogctx.Error(ctx, "failed to receive task request", "error", err)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case r.requests <- req:
		}
	}
}

func (r *Runner) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-r.requests:
			resp, err := r.process(ctx, req)
			if err != nil {
				slogctx.Error(ctx, "failed to process task request", "error", err, "taskId", req.TaskID, "etag", req.ETag)
				continue
			}

			err = r.client.SendTaskResponse(ctx, resp)
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
