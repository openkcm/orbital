package grpc

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

var (
	// ErrProcessFuncNil is returned by NewServer when given a nil ProcessFunc.
	ErrProcessFuncNil = errors.New("grpc server: process func cannot be nil")
	// ErrListenerNil is returned by Run when given a nil net.Listener.
	ErrListenerNil = errors.New("grpc server: listener cannot be nil")
)

type (
	// ServerOption configures a Server.
	ServerOption func(*serverConfig) error

	serverConfig struct {
		grpcServerOpts []grpc.ServerOption
	}

	// Server implements the orbital TaskService gRPC server. It receives
	// task requests over gRPC, processes them via the provided ProcessFunc,
	// and returns responses synchronously.
	Server struct {
		orbitalv1.UnimplementedTaskServiceServer

		config     serverConfig
		grpcServer *grpc.Server
		process    orbital.ProcessFunc
		stopOnce   sync.Once
	}
)

// NewServer creates a gRPC Server that delegates request processing to
// the given ProcessFunc. The ProcessFunc must be non-nil.
func NewServer(process orbital.ProcessFunc, opts ...ServerOption) (*Server, error) {
	if process == nil {
		return nil, ErrProcessFuncNil
	}

	cfg := serverConfig{}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			return nil, err
		}
	}

	return &Server{
		config:  cfg,
		process: process,
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

// Run registers the TaskService, starts serving on lis, and blocks until
// Stop is called or ctx is cancelled. A new grpc.Server is created on
// each call. Once stopped the Server cannot be restarted.
func (s *Server) Run(ctx context.Context, lis net.Listener) error {
	if lis == nil {
		return ErrListenerNil
	}

	s.grpcServer = grpc.NewServer(s.config.grpcServerOpts...)
	orbitalv1.RegisterTaskServiceServer(s.grpcServer, s)

	go func() {
		<-ctx.Done()
		s.stop()
	}()

	slogctx.Info(ctx, "grpc server starting", "address", lis.Addr().String())
	return s.grpcServer.Serve(lis)
}

// Stop gracefully stops the gRPC server. If ctx expires before graceful
// shutdown completes, it force-stops.
func (s *Server) Stop(ctx context.Context) {
	if s.grpcServer == nil {
		return
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
}

// SendTaskRequest implements orbitalv1.TaskServiceServer.
func (s *Server) SendTaskRequest(ctx context.Context, pReq *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
	req, err := codec.FromProtoToTaskRequest(pReq)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task request: %v", err)
	}

	resp, err := s.process(ctx, req)
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
