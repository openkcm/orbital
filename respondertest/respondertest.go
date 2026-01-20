package respondertest

import (
	"context"

	"github.com/openkcm/orbital"
)

type (
	// Responder is a mock implementation of the operator.Responder interface.
	Responder struct {
		input  chan orbital.TaskRequest
		output chan orbital.TaskResponse
	}

	// Option is a function that modifies the config parameter of the Responder.
	Option func(*config)
	config struct {
		inputBufferSize  int
		outputBufferSize int
	}
)

var _ orbital.Responder = &Responder{}

// NewResponder creates a new Responder instance.
// It allows options to configure the buffer size of the input and output channel.
func NewResponder(opts ...Option) *Responder {
	c := config{
		inputBufferSize:  10,
		outputBufferSize: 10,
	}

	for _, opt := range opts {
		opt(&c)
	}

	return &Responder{
		input:  make(chan orbital.TaskRequest, c.inputBufferSize),
		output: make(chan orbital.TaskResponse, c.outputBufferSize),
	}
}

// WithInputBufferSize sets the input buffer size for the Responder.
func WithInputBufferSize(size int) Option {
	return func(c *config) {
		c.inputBufferSize = size
	}
}

// WithOutputBufferSize sets the output buffer size for the Responder.
func WithOutputBufferSize(size int) Option {
	return func(c *config) {
		c.outputBufferSize = size
	}
}

// NewRequest sends the task request to the input channel.
func (r *Responder) NewRequest(req orbital.TaskRequest) {
	r.input <- req
}

// NewResponse receives the task response from the output channel.
func (r *Responder) NewResponse() orbital.TaskResponse {
	return <-r.output
}

// ReceiveTaskRequest receives a task request from the input channel.
func (r *Responder) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	select {
	case <-ctx.Done():
		return orbital.TaskRequest{}, ctx.Err()
	case req := <-r.input:
		return req, nil
	}
}

// SendTaskResponse sends a task response to the output channel.
func (r *Responder) SendTaskResponse(ctx context.Context, resp orbital.TaskResponse) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.output <- resp:
		return nil
	}
}

func (r *Responder) Close(_ context.Context) error {
	return nil
}
