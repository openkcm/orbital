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

	err = client.Close(t.Context())
	assert.NoError(t, err)

	resp, err := client.ReceiveTaskResponse(t.Context())
	assert.Error(t, err)
	assert.Equal(t, embedded.ErrClientClosed, err)
	assert.Equal(t, orbital.TaskResponse{}, resp)
}

func TestSendTask_ReceivedTaskResponse(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	ctx := t.Context()
	req := orbital.TaskRequest{
		TaskID:     uuid.New(),
		ETag:       uuid.New().String(),
		ExternalID: uuid.New().String(),
	}

	err = client.SendTaskRequest(ctx, req)
	assert.NoError(t, err)

	resp, err := client.ReceiveTaskResponse(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, req.TaskID, resp.TaskID)
	assert.Equal(t, req.ETag, resp.ETag)
	assert.Equal(t, req.Type, resp.Type)
	assert.Equal(t, req.ExternalID, resp.ExternalID)
}

func TestOperatorFuncError(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, assert.AnError
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	ctx := t.Context()

	err = client.SendTaskRequest(ctx, orbital.TaskRequest{})
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestReceiveTaskResponse_ContextCancelled(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	done := make(chan struct{})

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		resp, err := client.ReceiveTaskResponse(ctx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Equal(t, orbital.TaskResponse{}, resp)
		close(done)
	}()
	cancel()
	<-done
}

func TestSendTaskRequest_ContextCancelled(t *testing.T) {
	f := func(ctx context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		<-ctx.Done()
		return orbital.TaskResponse{}, ctx.Err()
	}
	client, err := embedded.NewClient(f)
	assert.NoError(t, err)
	assert.NotNil(t, client)
	defer client.Close(t.Context())

	done := make(chan struct{})

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := client.SendTaskRequest(ctx, orbital.TaskRequest{})
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		close(done)
	}()
	cancel()
	<-done
}
