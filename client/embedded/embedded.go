package embedded

import (
	"context"
	"errors"
	"sync"

	"github.com/openkcm/orbital"
)

type (
	// Client is a implementation of the orbital.Initiator interface.
	Client struct {
		operatorFunc OperatorFunc
		link         chan orbital.TaskResponse
		close        chan struct{}
		closeOnce    sync.Once
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

var (
	ErrMissingOperatorFunc = errors.New("missing operator function")
	ErrClientClosed        = errors.New("client closed")
)

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
		link:         make(chan orbital.TaskResponse, config.BufferSize),
		close:        make(chan struct{}),
	}, nil
}

// SendTaskRequest passes the task request to the operator function and sends the task response to the link channel.
func (c *Client) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	errCh := make(chan error, 1)
	respCh := make(chan orbital.TaskResponse, 1)
	go func() {
		resp, err := c.operatorFunc(ctx, request)
		if err != nil {
			errCh <- err
			return
		}
		resp.TaskID = request.TaskID
		resp.ETag = request.ETag
		resp.Type = request.Type
		resp.ExternalID = request.ExternalID
		respCh <- resp
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.close:
		return ErrClientClosed
	case err := <-errCh:
		return err
	case resp := <-respCh:
		c.link <- resp
		return nil
	}
}

// ReceiveTaskResponse waits for a task response from the link channel and returns it.
func (c *Client) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	select {
	case <-ctx.Done():
		return orbital.TaskResponse{}, ctx.Err()
	case <-c.close:
		return orbital.TaskResponse{}, ErrClientClosed
	case resp := <-c.link:
		return resp, nil
	}
}

// Close closes the link channel.
func (c *Client) Close(_ context.Context) error {
	c.closeOnce.Do(func() {
		close(c.close)
	})
	return nil
}
