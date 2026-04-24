package rpc

import (
	"context"
	"errors"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/codec"
	orbitalv1 "github.com/openkcm/orbital/proto/orbital/v1"
)

var _ orbital.SyncResponder = (*Server)(nil)

var (
	// ErrNilListener is returned by NewServer when given a nil net.Listener.
	ErrNilListener = errors.New("grpc server: listener cannot be nil")
)

type (
	// ServerOption configures a Server.
	ServerOption func(*serverConfig) error

	serverConfig struct {
		grpcServerOpts []grpc.ServerOption
	}

	// Server implements the orbital TaskService gRPC server and the
	// SyncResponder interface. It receives task requests over gRPC,
	// processes them via the TaskRequestHandler provided to Run,
	// and returns responses synchronously.
	Server struct {
		orbitalv1.UnimplementedTaskServiceServer

		config     serverConfig
		lis        net.Listener
		grpcServer *grpc.Server
		handler    orbital.TaskRequestHandler
		stopOnce   sync.Once
	}
)

// NewServer creates a gRPC Server bound to the given listener.
func NewServer(lis net.Listener, opts ...ServerOption) (*Server, error) {
	if lis == nil {
		return nil, ErrNilListener
	}

	cfg := serverConfig{}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	return &Server{
		config: cfg,
		lis:    lis,
	}, nil
}

// WithServerOptions appends gRPC server options (interceptors, TLS
// credentials, etc.) used when the underlying grpc.Server is created.
func WithServerOptions(opts ...grpc.ServerOption) ServerOption {
	return func(cfg *serverConfig) error {
		cfg.grpcServerOpts = append(cfg.grpcServerOpts, opts...)
		return nil
	}
}

// Run registers the TaskService, starts serving, and blocks until
// Close is called or ctx is cancelled. A new grpc.Server is created on
// each call.
func (s *Server) Run(ctx context.Context, handler orbital.TaskRequestHandler) error {
	s.handler = handler
	s.grpcServer = grpc.NewServer(s.config.grpcServerOpts...)
	orbitalv1.RegisterTaskServiceServer(s.grpcServer, s)

	go func() {
		<-ctx.Done()
		s.stop()
	}()

	slogctx.Info(ctx, "grpc server starting", "address", s.lis.Addr().String())
	return s.grpcServer.Serve(s.lis)
}

// Close gracefully stops the gRPC server. If ctx expires before graceful
// shutdown completes, it force-stops.
func (s *Server) Close(ctx context.Context) error {
	if s.grpcServer == nil {
		return nil
	}

	stopped := make(chan struct{})
	go func() {
		s.stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-ctx.Done():
		s.grpcServer.Stop()
	}
	return nil
}

// SendTaskRequest implements orbitalv1.TaskServiceServer.
func (s *Server) SendTaskRequest(ctx context.Context, pReq *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
	req, err := codec.FromProtoToTaskRequest(pReq)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task request: %v", err)
	}

	resp, err := s.handler(ctx, req)
	if err != nil {
		return nil, mapProcessError(err)
	}

	return codec.FromTaskResponseToProto(resp), nil
}

func mapProcessError(err error) error {
	switch {
	case errors.Is(err, orbital.ErrSignatureInvalid):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, orbital.ErrResponseSigning):
		return status.Error(codes.Internal, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func (s *Server) stop() {
	s.stopOnce.Do(func() {
		if s.grpcServer != nil {
			s.grpcServer.GracefulStop()
		}
	})
}
