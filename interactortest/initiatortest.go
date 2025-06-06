package interactortest

import (
	"context"
	"errors"

	"github.com/openkcm/orbital"
)

type (
	// Initiator is a mock implementation of the orbital.Initiator interface.
	Initiator struct {
		operatorFunc OperatorFunc
		link         chan orbital.TaskResponse
	}

	// OperatorFunc is a function type that simulates the behavior of an operator.
	OperatorFunc func(context.Context, orbital.TaskRequest) (orbital.TaskResponse, error)

	// Opts contains options to configure the Initiator.
	Opts struct {
		BufferSize int
	}
)

var _ orbital.Initiator = &Initiator{}

var ErrMissingOperatorFunc = errors.New("missing operator function")

var defaultBufferSize = 10

// NewInitiator creates a new Initiator instance.
// It expects an OperatorFunc which is used to simulate the behavior of an operator.
// It allows options to configure the buffer size of the link channel.
func NewInitiator(f OperatorFunc, opts *Opts) (*Initiator, error) {
	if f == nil {
		return nil, ErrMissingOperatorFunc
	}

	if opts == nil {
		opts = &Opts{
			BufferSize: defaultBufferSize,
		}
	}

	return &Initiator{
		operatorFunc: f,
		link:         make(chan orbital.TaskResponse, opts.BufferSize),
	}, nil
}

// SendTaskRequest passes a task request to the operator function and sends the response to the link channel.
func (r *Initiator) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	resp, err := r.operatorFunc(ctx, request)
	if err != nil {
		return err
	}
	r.link <- resp
	return nil
}

// ReceiveTaskResponse waits for a task response from the link channel and returns it.
func (r *Initiator) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	select {
	case resp := <-r.link:
		return resp, nil
	case <-ctx.Done():
		return orbital.TaskResponse{}, ctx.Err()
	}
}

// Close closes the link channel.
func (r *Initiator) Close() {
	close(r.link)
}
