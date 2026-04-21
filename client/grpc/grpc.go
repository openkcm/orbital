package grpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/codec"
	orbitalv1 "github.com/openkcm/orbital/proto/orbital/v1"
)

var (
	// ErrClientClosed is returned when a method is called on a closed Initiator.
	ErrClientClosed = errors.New("grpc initiator: closed")
	// ErrNilConn is returned by NewClient when given a nil grpc.ClientConn.
	ErrNilConn = errors.New("grpc initiator: nil connection")
	// ErrInvalidBufferSize is returned for negative buffer sizes.
	ErrInvalidBufferSize = errors.New("grpc initiator: buffer size cannot be negative")
	// ErrInvalidCallTimeout is returned for non-positive call timeouts.
	ErrInvalidCallTimeout = errors.New("grpc initiator: call timeout must be positive")
)

// Default configuration values.
const (
	defaultBufferSize  = 100
	defaultCallTimeout = 30 * time.Second
)

var _ orbital.Initiator = &Client{}

type (
	ClientOption func(*config) error
	config       struct {
		bufferSize  int
		callTimeout time.Duration
	}

	Client struct {
		stub      orbitalv1.TaskServiceClient
		responses chan rpcResp
		closeCh   chan struct{}
		closeOnce sync.Once
		config    config
	}

	rpcResp struct {
		taskRespose orbital.TaskResponse
		err         error
	}
)

// NewClient creates a new gRPC-backed Initiator. The caller retains ownership
// of conn and is responsible for closing it after the Client is closed.
func NewClient(conn *grpc.ClientConn, opts ...ClientOption) (*Client, error) {
	if conn == nil {
		return nil, ErrNilConn
	}

	cfg := config{
		bufferSize:  defaultBufferSize,
		callTimeout: defaultCallTimeout,
	}

	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	return &Client{
		stub:      orbitalv1.NewTaskServiceClient(conn),
		responses: make(chan rpcResp, cfg.bufferSize),
		closeCh:   make(chan struct{}),
		config:    cfg,
	}, nil
}

// WithBufferSize sets the buffer of the internal responses channel.
// Defaults to 100. A larger buffer tolerates bursts of responses without
// blocking RPC goroutines at the send-to-channel step.
func WithBufferSize(n int) ClientOption {
	return func(c *config) error {
		if n < 0 {
			return ErrInvalidBufferSize
		}
		c.bufferSize = n
		return nil
	}
}

// WithCallTimeout sets the per-RPC deadline used for Send calls.
// Defaults to 30 seconds. This is derived from context.Background, NOT the
// caller's ctx, because the reconcile transaction ctx handed to
// SendTaskRequest is too short-lived for the outbound RPC.
func WithCallTimeout(d time.Duration) ClientOption {
	return func(c *config) error {
		if d <= 0 {
			return ErrInvalidCallTimeout
		}
		c.callTimeout = d
		return nil
	}
}

// SendTaskRequest encodes the request and dispatches the gRPC call
// asynchronously. The resulting TaskResponse is placed on an internal channel
// for later retrieval via ReceiveTaskResponse.
func (c *Client) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closeCh:
		return ErrClientClosed
	default:
	}

	//nolint:contextcheck
	go c.dispatchRPC(request)

	return nil
}

// ReceiveTaskResponse blocks until a task response is available, the context
// is cancelled, or the client is closed.
func (c *Client) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	select {
	case <-ctx.Done():
		return orbital.TaskResponse{}, ctx.Err()
	case <-c.closeCh:
		return orbital.TaskResponse{}, ErrClientClosed
	case resp := <-c.responses:
		return resp.taskRespose, resp.err
	}
}

// Close signals all in-flight goroutines to stop delivering responses.
// It does not close the underlying grpc.ClientConn.
func (c *Client) Close(_ context.Context) error {
	c.closeOnce.Do(func() {
		close(c.closeCh)
	})
	return nil
}

// dispatchRPC performs the gRPC call with its own timeout context and delivers
// the result to the responses channel. On transport or decoding failures it
// synthesizes a FAILED TaskResponse preserving the original request identity.
func (c *Client) dispatchRPC(request orbital.TaskRequest) {
	protoReq := codec.FromTaskRequestToProto(request)

	callCtx, cancel := context.WithTimeout(context.Background(), c.config.callTimeout)
	defer cancel()

	protoResp, err := c.stub.SendTaskRequest(callCtx, protoReq)
	var resp rpcResp

	if err != nil {
		resp.err = err
	} else {
		taskResp, convErr := codec.FromProtoToTaskResponse(protoResp)
		resp.err = convErr
		resp.taskRespose = taskResp
	}

	select {
	case <-c.closeCh:
	default:
		c.responses <- resp
	}
}
