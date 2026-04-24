package rpc_test

import (
	"context"
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
	"github.com/openkcm/orbital/client/rpc"
	"github.com/openkcm/orbital/codec"
	orbitalv1 "github.com/openkcm/orbital/proto/orbital/v1"
)

const serverBufSize = 1024 * 1024

func startTestServer(t *testing.T, handler orbital.TaskRequestHandler) (orbitalv1.TaskServiceClient, *rpc.Server) {
	t.Helper()

	lis := bufconn.Listen(serverBufSize)

	srv, err := rpc.NewServer(lis)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run(ctx, handler)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return orbitalv1.NewTaskServiceClient(conn), srv
}

func TestNewServer(t *testing.T) {
	lis := bufconn.Listen(serverBufSize)
	defer lis.Close()

	t.Run("NilListener", func(t *testing.T) {
		srv, err := rpc.NewServer(nil)
		assert.Nil(t, srv)
		assert.ErrorIs(t, err, rpc.ErrNilListener)
	})

	t.Run("Defaults", func(t *testing.T) {
		srv, err := rpc.NewServer(lis)
		require.NoError(t, err)
		assert.NotNil(t, srv)
	})

	t.Run("WithServerOptions", func(t *testing.T) {
		srv, err := rpc.NewServer(lis, rpc.WithServerOptions(grpc.MaxRecvMsgSize(1024)))
		require.NoError(t, err)
		assert.NotNil(t, srv)
	})
}

func TestServer_SendTaskRequest(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		taskID := uuid.New()
		etag := uuid.New().String()

		handler := func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{
				TaskID:            req.TaskID,
				Type:              req.Type,
				ExternalID:        req.ExternalID,
				WorkingState:      []byte(`{"progress":50}`),
				ETag:              req.ETag,
				Status:            string(orbital.TaskStatusDone),
				ReconcileAfterSec: 120,
				MetaData:          orbital.MetaData{"key": "value"},
			}, nil
		}

		stub, _ := startTestServer(t, handler)

		protoReq := codec.FromTaskRequestToProto(orbital.TaskRequest{
			TaskID:               taskID,
			Type:                 "test-type",
			ExternalID:           "ext-123",
			Data:                 []byte(`{"input":"data"}`),
			WorkingState:         []byte(`{"state":"initial"}`),
			ETag:                 etag,
			MetaData:             orbital.MetaData{"reqKey": "reqVal"},
			TaskCreatedAt:        time.Now().Unix(),
			TaskLastReconciledAt: time.Now().Unix(),
		})

		resp, err := stub.SendTaskRequest(t.Context(), protoReq)
		require.NoError(t, err)

		assert.Equal(t, taskID.String(), resp.GetTaskId())
		assert.Equal(t, "test-type", resp.GetType())
		assert.Equal(t, "ext-123", resp.GetExternalId())
		assert.Equal(t, etag, resp.GetEtag())
		assert.Equal(t, orbitalv1.TaskStatus_DONE, resp.GetStatus())
		assert.Equal(t, []byte(`{"progress":50}`), resp.GetWorkingState())
		assert.Equal(t, uint64(120), resp.GetReconcileAfterSec())
		assert.Equal(t, map[string]string{"key": "value"}, resp.GetMetaData())
	})

	t.Run("InvalidTaskID", func(t *testing.T) {
		handler := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{}, nil
		}

		stub, _ := startTestServer(t, handler)

		resp, err := stub.SendTaskRequest(t.Context(), &orbitalv1.TaskRequest{
			TaskId: "not-a-uuid",
			Type:   "test",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ErrSignatureInvalid", func(t *testing.T) {
		handler := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{}, orbital.ErrSignatureInvalid
		}

		stub, _ := startTestServer(t, handler)

		protoReq := codec.FromTaskRequestToProto(orbital.TaskRequest{
			TaskID: uuid.New(),
			Type:   "test",
			ETag:   "etag-1",
		})

		resp, err := stub.SendTaskRequest(t.Context(), protoReq)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("ErrResponseSigning", func(t *testing.T) {
		handler := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{}, orbital.ErrResponseSigning
		}

		stub, _ := startTestServer(t, handler)

		protoReq := codec.FromTaskRequestToProto(orbital.TaskRequest{
			TaskID: uuid.New(),
			Type:   "test",
			ETag:   "etag-1",
		})

		resp, err := stub.SendTaskRequest(t.Context(), protoReq)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("FailedStatusIsNormalResponse", func(t *testing.T) {
		handler := func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{
				TaskID:       req.TaskID,
				Status:       string(orbital.TaskStatusFailed),
				ErrorMessage: "something went wrong",
				ETag:         req.ETag,
			}, nil
		}

		stub, _ := startTestServer(t, handler)

		protoReq := codec.FromTaskRequestToProto(orbital.TaskRequest{
			TaskID: uuid.New(),
			Type:   "test",
			ETag:   "etag-1",
		})

		resp, err := stub.SendTaskRequest(t.Context(), protoReq)
		require.NoError(t, err)
		assert.Equal(t, orbitalv1.TaskStatus_FAILED, resp.GetStatus())
		assert.Equal(t, "something went wrong", resp.GetErrorMessage())
	})
}

func TestServer_Close(t *testing.T) {
	handler := func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{TaskID: req.TaskID}, nil
	}

	t.Run("GracefulClose", func(t *testing.T) {
		stub, srv := startTestServer(t, handler)

		protoReq := codec.FromTaskRequestToProto(orbital.TaskRequest{
			TaskID: uuid.New(),
			Type:   "test",
			ETag:   "etag-1",
		})
		_, err := stub.SendTaskRequest(t.Context(), protoReq)
		require.NoError(t, err)

		require.NoError(t, srv.Close(t.Context()))

		_, err = stub.SendTaskRequest(t.Context(), protoReq)
		assert.Error(t, err)
	})

	t.Run("CloseBeforeRun", func(t *testing.T) {
		lis := bufconn.Listen(serverBufSize)
		defer lis.Close()

		srv, err := rpc.NewServer(lis)
		require.NoError(t, err)

		assert.NotPanics(t, func() {
			require.NoError(t, srv.Close(t.Context()))
		})
	})

	t.Run("ForceCloseOnContextExpiry", func(t *testing.T) {
		_, srv := startTestServer(t, handler)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		assert.NotPanics(t, func() {
			require.NoError(t, srv.Close(ctx))
		})
	})

	t.Run("ContextCancellationStopsServer", func(t *testing.T) {
		lis := bufconn.Listen(serverBufSize)

		srv, err := rpc.NewServer(lis)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())

		errCh := make(chan error, 1)
		go func() {
			errCh <- srv.Run(ctx, handler)
		}()

		cancel()

		select {
		case <-errCh:
		case <-time.After(5 * time.Second):
			t.Fatal("server did not stop after context cancellation")
		}
	})
}
