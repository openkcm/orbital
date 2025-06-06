package interactortest_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/interactortest"
)

func TestNewResponder_MissingOperatorFunc(t *testing.T) {
	responder, err := interactortest.NewInitiator(nil, nil)

	assert.Error(t, err)
	assert.Equal(t, interactortest.ErrMissingOperatorFunc, err)
	assert.Nil(t, responder)
}

func TestNewResponder_NilOptions(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}

	responder, err := interactortest.NewInitiator(f, nil)

	assert.NoError(t, err)
	assert.NotNil(t, responder)
}

func TestClose(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, nil
	}

	responder, err := interactortest.NewInitiator(f, nil)
	assert.NoError(t, err)
	assert.NotNil(t, responder)

	responder.Close()

	assert.Panics(t, func() {
		responder.SendTaskRequest(t.Context(), orbital.TaskRequest{}) //nolint:errcheck
	})
}

func TestSendTask_OperatorFuncError(t *testing.T) {
	f := func(_ context.Context, _ orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{}, assert.AnError
	}
	responder, err := interactortest.NewInitiator(f, nil)
	assert.NoError(t, err)
	assert.NotNil(t, responder)
	defer responder.Close()

	err = responder.SendTaskRequest(t.Context(), orbital.TaskRequest{})
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

func TestSendTask_ReceivedTaskResponse(t *testing.T) {
	f := func(_ context.Context, request orbital.TaskRequest) (orbital.TaskResponse, error) {
		return orbital.TaskResponse{TaskID: request.TaskID}, nil
	}
	responder, err := interactortest.NewInitiator(f, nil)
	assert.NoError(t, err)
	assert.NotNil(t, responder)
	defer responder.Close()

	ctx := t.Context()
	request := orbital.TaskRequest{
		TaskID: uuid.New(),
	}

	err = responder.SendTaskRequest(ctx, request)
	assert.NoError(t, err)

	resp, err := responder.ReceiveTaskResponse(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, request.TaskID, resp.TaskID)
}
