package embedded

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/openkcm/orbital"
)

type (
	// Client is a implementation of the orbital.Initiator interface.
	Client struct {
		handler   orbital.HandlerFunc
		link      chan orbital.TaskResponse
		config    config
		close     chan struct{}
		closeOnce sync.Once
	}

	// Option is a function type that modifies the configuration of the Client.
	Option func(*config) error
	config struct {
		bufferSize     int
		handlerTimeout time.Duration
	}
)

var _ orbital.Initiator = &Client{}

var (
	ErrMissingHandler            = errors.New("missing handler")
	ErrClientClosed              = errors.New("client closed")
	ErrNegativeBufferSize        = errors.New("buffer size cannot be negative")
	ErrNonPositiveHandlerTimeout = errors.New("handler timeout must be greater than 0")
)

const (
	defaultBufferSize     = 10
	defaultHandlerTimeout = 10 * time.Second
)

// NewClient creates a new embedded Client instance.
// It expects a handler which is used to process task requests and generate task responses.
// It allows options to configure the buffer size of the link channel and the timeout for the handler.
func NewClient(h orbital.HandlerFunc, opts ...Option) (*Client, error) {
	if h == nil {
		return nil, ErrMissingHandler
	}

	cfg := config{
		bufferSize:     defaultBufferSize,
		handlerTimeout: defaultHandlerTimeout,
	}

	for _, opt := range opts {
		err := opt(&cfg)
		if err != nil {
			return nil, err
		}
	}

	return &Client{
		handler: h,
		link:    make(chan orbital.TaskResponse, cfg.bufferSize),
		config:  cfg,
		close:   make(chan struct{}),
	}, nil
}

// WithBufferSize sets the buffer size of the link channel.
func WithBufferSize(size int) Option {
	return func(c *config) error {
		if size < 0 {
			return ErrNegativeBufferSize
		}
		c.bufferSize = size
		return nil
	}
}

// WithHandlerTimeout sets the timeout for the handler.
func WithHandlerTimeout(timeout time.Duration) Option {
	return func(c *config) error {
		if timeout <= 0 {
			return ErrNonPositiveHandlerTimeout
		}
		c.handlerTimeout = timeout
		return nil
	}
}

// SendTaskRequest passes the task request to the handler which processes the request asynchronously.
func (c *Client) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.close:
		return ErrClientClosed
	default:
	}

	// process the request asynchronously with its own context
	//nolint:contextcheck
	go func() {
		ctxTimeout, cancel := context.WithTimeout(context.Background(), c.config.handlerTimeout)
		defer cancel()

		resp := orbital.ExecuteHandler(ctxTimeout, c.handler, request)
		select {
		case <-c.close:
			return
		default:
			c.link <- resp
		}
	}()

	return nil
}

// ReceiveTaskResponse waits for a task response.
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
