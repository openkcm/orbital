package grpc_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/openkcm/orbital"
	grpcclient "github.com/openkcm/orbital/client/grpc"
	orbitalv1 "github.com/openkcm/orbital/proto/orbital/v1"
)

const bufSize = 1024 * 1024

type stubServer struct {
	orbitalv1.UnimplementedTaskServiceServer

	handler func(context.Context, *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error)
}

func (s *stubServer) SendTaskRequest(ctx context.Context, req *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
	return s.handler(ctx, req)
}

func startServer(t *testing.T, handler func(context.Context, *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error)) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	orbitalv1.RegisterTaskServiceServer(srv, &stubServer{handler: handler})

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("server exited: %v", err)
		}
	}()
	t.Cleanup(srv.GracefulStop)

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return conn
}

func noopHandler(_ context.Context, _ *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
	return &orbitalv1.TaskResponse{}, nil
}

func TestNewClient(t *testing.T) {
	conn := startServer(t, noopHandler)

	tests := []struct {
		name    string
		conn    *grpc.ClientConn
		opts    []grpcclient.ClientOption
		wantErr error
	}{
		{
			name:    "NilConn",
			conn:    nil,
			wantErr: grpcclient.ErrNilConn,
		},
		{
			name:    "InvalidBufferSize",
			conn:    conn,
			opts:    []grpcclient.ClientOption{grpcclient.WithBufferSize(-1)},
			wantErr: grpcclient.ErrInvalidBufferSize,
		},
		{
			name:    "InvalidCallTimeout/zero",
			conn:    conn,
			opts:    []grpcclient.ClientOption{grpcclient.WithCallTimeout(0)},
			wantErr: grpcclient.ErrInvalidCallTimeout,
		},
		{
			name:    "InvalidCallTimeout/negative",
			conn:    conn,
			opts:    []grpcclient.ClientOption{grpcclient.WithCallTimeout(-1 * time.Second)},
			wantErr: grpcclient.ErrInvalidCallTimeout,
		},
		{
			name: "ValidWithZeroBuffer",
			conn: conn,
			opts: []grpcclient.ClientOption{grpcclient.WithBufferSize(0)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := grpcclient.NewClient(tt.conn, tt.opts...)
			if tt.wantErr != nil {
				assert.Nil(t, c)
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, c)
				defer c.Close(t.Context())
			}
		})
	}
}

func TestSendTaskRequest(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		taskID := uuid.New()
		etag := uuid.New().String()

		conn := startServer(t, func(_ context.Context, req *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
			return &orbitalv1.TaskResponse{
				TaskId:            req.TaskId,
				Type:              req.Type,
				ExternalId:        req.ExternalId,
				WorkingState:      []byte(`{"progress":50}`),
				Etag:              req.Etag,
				Status:            orbitalv1.TaskStatus_DONE,
				ReconcileAfterSec: 120,
				MetaData:          map[string]string{"key": "value"},
			}, nil
		})

		client, err := grpcclient.NewClient(conn, grpcclient.WithCallTimeout(5*time.Second))
		require.NoError(t, err)
		defer client.Close(t.Context())

		req := orbital.TaskRequest{
			TaskID:               taskID,
			Type:                 "test-type",
			ExternalID:           "ext-123",
			Data:                 []byte(`{"input":"data"}`),
			WorkingState:         []byte(`{"state":"initial"}`),
			ETag:                 etag,
			MetaData:             orbital.MetaData{"reqKey": "reqVal"},
			TaskCreatedAt:        time.Now().Unix(),
			TaskLastReconciledAt: time.Now().Unix(),
		}

		err = client.SendTaskRequest(t.Context(), req)
		require.NoError(t, err)

		resp, err := client.ReceiveTaskResponse(t.Context())
		require.NoError(t, err)

		assert.Equal(t, taskID, resp.TaskID)
		assert.Equal(t, "test-type", resp.Type)
		assert.Equal(t, "ext-123", resp.ExternalID)
		assert.Equal(t, etag, resp.ETag)
		assert.Equal(t, "DONE", resp.Status)
		assert.Equal(t, []byte(`{"progress":50}`), resp.WorkingState)
		assert.Equal(t, uint64(120), resp.ReconcileAfterSec)
		assert.Equal(t, orbital.MetaData{"key": "value"}, resp.MetaData)
	})

	t.Run("GRPCError", func(t *testing.T) {
		taskID := uuid.New()

		conn := startServer(t, func(context.Context, *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
			return nil, status.Error(codes.Internal, "something broke")
		})

		client, err := grpcclient.NewClient(conn, grpcclient.WithCallTimeout(5*time.Second))
		require.NoError(t, err)
		defer client.Close(t.Context())

		req := orbital.TaskRequest{
			TaskID:     taskID,
			Type:       "test",
			ExternalID: "ext-1",
			ETag:       "etag-1",
		}

		err = client.SendTaskRequest(t.Context(), req)
		require.NoError(t, err)

		resp, err := client.ReceiveTaskResponse(t.Context())
		require.NoError(t, err)

		assert.Equal(t, taskID, resp.TaskID)
		assert.Equal(t, "test", resp.Type)
		assert.Equal(t, "ext-1", resp.ExternalID)
		assert.Equal(t, "etag-1", resp.ETag)
		assert.Equal(t, "FAILED", resp.Status)
		assert.Contains(t, resp.ErrorMessage, "something broke")
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		conn := startServer(t, noopHandler)

		client, err := grpcclient.NewClient(conn)
		require.NoError(t, err)
		defer client.Close(t.Context())

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err = client.SendTaskRequest(ctx, orbital.TaskRequest{})
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("MultipleConcurrent", func(t *testing.T) {
		conn := startServer(t, func(_ context.Context, req *orbitalv1.TaskRequest) (*orbitalv1.TaskResponse, error) {
			return &orbitalv1.TaskResponse{
				TaskId: req.TaskId,
				Type:   req.Type,
				Etag:   req.Etag,
				Status: orbitalv1.TaskStatus_DONE,
			}, nil
		})

		client, err := grpcclient.NewClient(conn, grpcclient.WithCallTimeout(5*time.Second))
		require.NoError(t, err)
		defer client.Close(t.Context())

		const n = 20
		ids := make(map[uuid.UUID]struct{}, n)

		for range n {
			id := uuid.New()
			ids[id] = struct{}{}
			err := client.SendTaskRequest(t.Context(), orbital.TaskRequest{
				TaskID: id,
				Type:   "concurrent",
				ETag:   id.String(),
			})
			require.NoError(t, err)
		}

		received := make(map[uuid.UUID]struct{}, n)
		for range n {
			resp, err := client.ReceiveTaskResponse(t.Context())
			require.NoError(t, err)
			received[resp.TaskID] = struct{}{}
		}

		assert.Equal(t, ids, received)
	})
}

func TestReceiveTaskResponse(t *testing.T) {
	t.Run("ContextCancelled", func(t *testing.T) {
		conn := startServer(t, noopHandler)

		client, err := grpcclient.NewClient(conn)
		require.NoError(t, err)
		defer client.Close(t.Context())

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		resp, err := client.ReceiveTaskResponse(ctx)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, orbital.TaskResponse{}, resp)
	})
}

func TestClose(t *testing.T) {
	t.Run("ThenSendAndReceive", func(t *testing.T) {
		conn := startServer(t, noopHandler)

		client, err := grpcclient.NewClient(conn)
		require.NoError(t, err)

		err = client.Close(t.Context())
		require.NoError(t, err)

		err = client.SendTaskRequest(t.Context(), orbital.TaskRequest{})
		assert.ErrorIs(t, err, grpcclient.ErrClientClosed)

		resp, err := client.ReceiveTaskResponse(t.Context())
		assert.ErrorIs(t, err, grpcclient.ErrClientClosed)
		assert.Equal(t, orbital.TaskResponse{}, resp)
	})

	t.Run("Idempotent", func(t *testing.T) {
		conn := startServer(t, noopHandler)

		client, err := grpcclient.NewClient(conn)
		require.NoError(t, err)

		assert.NotPanics(t, func() {
			_ = client.Close(t.Context())
			_ = client.Close(t.Context())
			_ = client.Close(t.Context())
		})
	})
}
