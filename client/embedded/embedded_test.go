package embedded_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
)

func TestNewclient_MissingHandler(t *testing.T) {
	client, err := embedded.NewClient(nil)

	assert.Error(t, err)
	assert.Equal(t, embedded.ErrMissingHandler, err)
	assert.Nil(t, client)
}

func TestNewclient_NilOptions(t *testing.T) {
	h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}

	client, err := embedded.NewClient(h)

	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestHandlerTimeoutOption(t *testing.T) {
	t.Run("Non positive handler timeout returns error", func(t *testing.T) {
		h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}

		client, err := embedded.NewClient(h, embedded.WithHandlerTimeout(0))
		assert.Error(t, err)
		assert.Equal(t, embedded.ErrNonPositiveHandlerTimeout, err)
		assert.Nil(t, client)
	})

	expectedTimeout := 5 * time.Second
	done := make(chan struct{})
	h := func(ctx context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {
		deadline, ok := ctx.Deadline()
		assert.True(t, ok)
		assert.WithinDuration(t, time.Now().Add(expectedTimeout), deadline, time.Second)
		close(done)
	}

	client, err := embedded.NewClient(h, embedded.WithHandlerTimeout(expectedTimeout))
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	err = client.SendTaskRequest(context.Background(), orbital.TaskRequest{})
	assert.NoError(t, err)
	<-done
}

func TestBufferSizeOption(t *testing.T) {
	t.Run("Negative buffer size returns error", func(t *testing.T) {
		h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}

		client, err := embedded.NewClient(h, embedded.WithBufferSize(-1))
		assert.Error(t, err)
		assert.Equal(t, embedded.ErrNegativeBufferSize, err)
		assert.Nil(t, client)
	})

	h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}

	client, err := embedded.NewClient(h, embedded.WithBufferSize(0))
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())
}

func TestClose(t *testing.T) {
	h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}

	client, err := embedded.NewClient(h)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	err = client.Close(t.Context())
	assert.NoError(t, err)

	err = client.SendTaskRequest(t.Context(), orbital.TaskRequest{})
	assert.Error(t, err)
	assert.Equal(t, embedded.ErrClientClosed, err)

	resp, err := client.ReceiveTaskResponse(t.Context())
	assert.Error(t, err)
	assert.Equal(t, embedded.ErrClientClosed, err)
	assert.Equal(t, orbital.TaskResponse{}, resp)
}

func TestSendTask_ReceivedTaskResponse(t *testing.T) {
	ctx := t.Context()
	req := orbital.TaskRequest{
		TaskID:               uuid.New(),
		ETag:                 uuid.New().String(),
		ExternalID:           uuid.New().String(),
		Type:                 "test",
		Data:                 []byte(`{"key":"value"}`),
		WorkingState:         []byte(`{"key":"value"}`),
		TaskCreatedAt:        time.Now().UnixNano(),
		TaskLastReconciledAt: time.Now().UnixNano(),
	}

	noOfCalled := 0
	h := func(_ context.Context, hReq orbital.HandlerRequest, _ *orbital.HandlerResponse) {
		noOfCalled++
		assert.Equal(t, req.TaskID, hReq.TaskID)
		assert.Equal(t, req.Type, hReq.TaskType)
		assert.Equal(t, req.Data, hReq.TaskData)
		assert.Equal(t, req.WorkingState, hReq.TaskRawWorkingState)
		assert.WithinDuration(t, time.Unix(0, req.TaskCreatedAt), time.Now(), time.Second)
		assert.WithinDuration(t, time.Unix(0, req.TaskLastReconciledAt), time.Now(), time.Second)
	}
	client, err := embedded.NewClient(h)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	err = client.SendTaskRequest(ctx, req)
	assert.NoError(t, err)

	resp, err := client.ReceiveTaskResponse(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 1, noOfCalled)
	assert.Equal(t, req.TaskID, resp.TaskID)
	assert.Equal(t, req.ETag, resp.ETag)
	assert.Equal(t, req.Type, resp.Type)
	assert.Equal(t, req.ExternalID, resp.ExternalID)
	assert.Equal(t, string(orbital.TaskStatusProcessing), resp.Status)
	assert.Equal(t, req.WorkingState, resp.WorkingState)
	assert.Empty(t, resp.ErrorMessage)
	assert.Equal(t, uint64(0), resp.ReconcileAfterSec)
}

func TestContextCancelled(t *testing.T) {
	h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}
	client, err := embedded.NewClient(h)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = client.SendTaskRequest(ctx, orbital.TaskRequest{})
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	resp, err := client.ReceiveTaskResponse(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, orbital.TaskResponse{}, resp)
}
