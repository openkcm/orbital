package sync_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/openkcm/orbital"
	grpcpkg "github.com/openkcm/orbital/client/grpc"
	"github.com/openkcm/orbital/codec"
	orbitalv1 "github.com/openkcm/orbital/proto/orbital/v1"
	syncrunner "github.com/openkcm/orbital/runner/sync"
)

const bufSize = 1024 * 1024

func TestNew(t *testing.T) {
	process := func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{TaskID: req.TaskID}, nil
	}
	srv, err := grpcpkg.NewServer(process)
	require.NoError(t, err)

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	t.Run("NilServer", func(t *testing.T) {
		r, err := syncrunner.New(nil, lis)
		assert.Nil(t, r)
		assert.ErrorIs(t, err, syncrunner.ErrServerNil)
	})

	t.Run("NilListener", func(t *testing.T) {
		r, err := syncrunner.New(srv, nil)
		assert.Nil(t, r)
		assert.ErrorIs(t, err, syncrunner.ErrListenerNil)
	})

	t.Run("Valid", func(t *testing.T) {
		r, err := syncrunner.New(srv, lis)
		require.NoError(t, err)
		assert.NotNil(t, r)
	})
}

func TestRunner_Run(t *testing.T) {
	t.Run("RoundTrip", func(t *testing.T) {
		taskID := uuid.New()

		process := func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{
				TaskID: req.TaskID,
				Type:   req.Type,
				ETag:   req.ETag,
				Status: string(orbital.TaskStatusDone),
			}, nil
		}

		lis := bufconn.Listen(bufSize)

		srv, err := grpcpkg.NewServer(process)
		require.NoError(t, err)

		runner, err := syncrunner.New(srv, lis)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		go func() {
			err := runner.Run(ctx)
			if err != nil {
				assert.ErrorIs(t, err, grpc.ErrServerStopped)
			}
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

		stub := orbitalv1.NewTaskServiceClient(conn)

		protoReq := codec.FromTaskRequestToProto(orbital.TaskRequest{
			TaskID: taskID,
			Type:   "test",
			ETag:   "etag-1",
		})

		resp, err := stub.SendTaskRequest(ctx, protoReq)
		require.NoError(t, err)
		assert.Equal(t, taskID.String(), resp.GetTaskId())
		assert.Equal(t, orbitalv1.TaskStatus_DONE, resp.GetStatus())
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		process := func(_ context.Context, req orbital.TaskRequest) (orbital.TaskResponse, error) {
			return orbital.TaskResponse{TaskID: req.TaskID}, nil
		}

		lis := bufconn.Listen(bufSize)

		srv, err := grpcpkg.NewServer(process)
		require.NoError(t, err)

		runner, err := syncrunner.New(srv, lis)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())

		errCh := make(chan error, 1)
		go func() {
			errCh <- runner.Run(ctx)
		}()

		cancel()

		select {
		case <-errCh:
			// Server exited after context cancellation — success.
		case <-time.After(5 * time.Second):
			t.Fatal("runner did not stop after context cancellation")
		}
	})
}
