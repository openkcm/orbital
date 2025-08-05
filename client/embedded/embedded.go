package embedded

import (
	"context"
	"errors"

	"github.com/openkcm/orbital"
)

type (
	// Client is a implementation of the orbital.Initiator interface.
	Client struct {
		operatorFunc OperatorFunc
		link         chan orbital.TaskRequest
	}

	// OperatorFunc is a function type that processes a TaskRequest and returns a TaskResponse.
	OperatorFunc func(context.Context, orbital.TaskRequest) (orbital.TaskResponse, error)

	// Option is a function type that modifies the configuration of the Client.
	Option func(*config)
	config struct {
		BufferSize int
	}
)

var _ orbital.Initiator = &Client{}

var ErrMissingOperatorFunc = errors.New("missing operator function")

var defaultBufferSize = 10

// NewClient creates a new embedded Client instance.
// It expects an OperatorFunc which is used to process the task requests.
// It allows options to configure the buffer size of the link channel.
func NewClient(f OperatorFunc, opts ...Option) (*Client, error) {
	if f == nil {
		return nil, ErrMissingOperatorFunc
	}

	config := config{
		BufferSize: defaultBufferSize,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return &Client{
		operatorFunc: f,
		link:         make(chan orbital.TaskRequest, config.BufferSize),
	}, nil
}

// SendTaskRequest sends the task request to the link channel.
func (c *Client) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		c.link <- request
		return nil
	}
}

// ReceiveTaskResponse waits for a task request from the link channel,
// passes the task request to the operator function,
// and returns the task response.
func (c *Client) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	select {
	case <-ctx.Done():
		return orbital.TaskResponse{}, ctx.Err()
	case req := <-c.link:
		resp, err := c.operatorFunc(ctx, req)
		resp.TaskID = req.TaskID
		resp.ETag = req.ETag
		resp.Type = req.Type
		return resp, err
	}
}

// Close closes the link channel.
func (c *Client) Close() {
	close(c.link)
}
