package sync

import (
	"context"
	"errors"
	"net"

	"github.com/openkcm/orbital"
	grpcserver "github.com/openkcm/orbital/client/grpc"
)

var (
	ErrServerNil   = errors.New("server cannot be nil")
	ErrListenerNil = errors.New("listener cannot be nil")
)

var _ orbital.Runner = (*Runner)(nil)

// Runner implements orbital.Runner by delegating to a gRPC Server.
type Runner struct {
	server *grpcserver.Server
	lis    net.Listener
}

// New creates a sync Runner backed by the given gRPC Server and listener.
func New(server *grpcserver.Server, lis net.Listener) (*Runner, error) {
	if server == nil {
		return nil, ErrServerNil
	}
	if lis == nil {
		return nil, ErrListenerNil
	}
	return &Runner{server: server, lis: lis}, nil
}

// Run starts the gRPC server and blocks until it exits.
func (r *Runner) Run(ctx context.Context) error {
	return r.server.Run(ctx, r.lis)
}
