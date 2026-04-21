package orbital

import (
	"context"
	"errors"

	slogctx "github.com/veqryn/slog-context"
)

// AsyncRunner implements Runner on top of a half-duplex Responder
// (e.g. AMQP). It pulls requests from the Responder, fans out to a bounded
// worker pool, and pushes responses back on the same Responder.
type AsyncRunner struct {
	client          Responder
	requests        chan TaskRequest
	numberOfWorkers int
}

type (
	// AsyncOption configures an AsyncRunner.
	AsyncOption func(*asyncConfig) error
	asyncConfig struct {
		bufferSize      int
		numberOfWorkers int
	}
)

var (
	ErrBufferSizeNegative         = errors.New("buffer size cannot be negative")
	ErrNumberOfWorkersNotPositive = errors.New("number of workers must be greater than 0")
	ErrResponderNil               = errors.New("responder cannot be nil")
)

// NewAsyncRunner creates an AsyncRunner backed by the given Responder.
// Defaults: buffer size 100, 10 workers.
func NewAsyncRunner(client Responder, opts ...AsyncOption) (*AsyncRunner, error) {
	if client == nil {
		return nil, ErrResponderNil
	}

	c := asyncConfig{
		bufferSize:      100,
		numberOfWorkers: 10,
	}
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	return &AsyncRunner{
		client:          client,
		requests:        make(chan TaskRequest, c.bufferSize),
		numberOfWorkers: c.numberOfWorkers,
	}, nil
}

// WithBufferSize sets the buffer size for the requests channel.
// It returns an error if the size is negative.
func WithBufferSize(size int) AsyncOption {
	return func(c *asyncConfig) error {
		if size < 0 {
			return ErrBufferSizeNegative
		}
		c.bufferSize = size
		return nil
	}
}

// WithNumberOfWorkers sets the number of workers for processing requests.
// It returns an error if the number is not positive.
func WithNumberOfWorkers(num int) AsyncOption {
	return func(c *asyncConfig) error {
		if num <= 0 {
			return ErrNumberOfWorkersNotPositive
		}
		c.numberOfWorkers = num
		return nil
	}
}

// Run spawns one listener goroutine and numberOfWorkers worker goroutines,
// then returns. Cancellation of ctx stops all of them.
func (r *AsyncRunner) Run(ctx context.Context, process ProcessFunc) {
	go r.startListening(ctx)
	for range r.numberOfWorkers {
		go r.worker(ctx, process)
	}
}

func (r *AsyncRunner) startListening(ctx context.Context) {
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

		// Hand off to a worker, but respect shutdown if the buffer is full.
		select {
		case <-ctx.Done():
			return
		case r.requests <- req:
		}
	}
}

func (r *AsyncRunner) worker(ctx context.Context, process ProcessFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-r.requests:
			resp, err := process(ctx, req)
			if err != nil {
				// Pipeline errors (bad signature, signer failure) are already
				// logged by Process. Async semantics: drop and move on,
				// matching the previous inline behavior.
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
