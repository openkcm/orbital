package embedded_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/embedded"
)

func TestNewclient_MissingOperatorFunc(t *testing.T) {
	client, err := embedded.NewClient(nil)

	assert.Error(t, err)
	assert.Equal(t, embedded.ErrMissingOperatorFunc, err)
	assert.Nil(t, client)
}

func TestNewclient_NilOptions(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}

	client, err := embedded.NewClient(f)

	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestClose(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}

	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	client.Close()

	assert.Panics(t, func() {
		client.SendTaskRequest(t.Context(), orbital.TaskRequest{}) //nolint:errcheck
	})
}

func TestSendTask_ReceivedTaskResponse(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	ctx := t.Context()
	req := orbital.TaskRequest{
		TaskID: uuid.New(),
		ETag:   uuid.New().String(),
	}

	err = client.SendTaskRequest(ctx, req)
	assert.NoError(t, err)

	resp, err := client.ReceiveTaskResponse(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, req.TaskID, resp.TaskID)
	assert.Equal(t, req.ETag, resp.ETag)
	assert.Equal(t, req.Type, resp.Type)
}

func TestOperatorFuncError(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, assert.AnError
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

	ctx := t.Context()

	err = client.SendTaskRequest(ctx, orbital.TaskRequest{})
	assert.NoError(t, err)

	resp, err := client.ReceiveTaskResponse(ctx)
	assert.Equal(t, assert.AnError, err)
	assert.Equal(t, orbital.TaskResponse{}, resp)
}

func TestContextCancelled(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close()

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
