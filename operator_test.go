package orbital_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/interactortest"
)

func TestNew(t *testing.T) {
	responder := interactortest.NewResponder()

	tests := []struct {
		name   string
		opts   []orbital.Option
		expErr error
	}{
		{
			name:   "negative buffer size",
			opts:   []orbital.Option{orbital.WithBufferSize(-1)},
			expErr: orbital.ErrBufferSizeNegative,
		},
		{
			name:   "zero number of workers",
			opts:   []orbital.Option{orbital.WithNumberOfWorkers(0)},
			expErr: orbital.ErrNumberOfWorkersNotPositive,
		},
		{
			name: "without options",
			opts: []orbital.Option{},
		},
		{
			name: "with options",
			opts: []orbital.Option{
				orbital.WithBufferSize(0),
				orbital.WithNumberOfWorkers(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := orbital.NewOperator(responder, tt.opts...)
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, o)
		})
	}
}

func TestRegisterHandler(t *testing.T) {
	responder := interactortest.NewResponder()

	o, err := orbital.NewOperator(responder)
	assert.NoError(t, err)
	assert.NotNil(t, o)

	h := func(_ context.Context, _ orbital.HandlerRequest) (orbital.HandlerResponse, error) {
		return orbital.HandlerResponse{}, nil
	}

	tests := []struct {
		name     string
		taskType string
		handler  orbital.Handler
		expErr   error
	}{
		{
			name:     "nil handler",
			taskType: "test",
			expErr:   orbital.ErrHandlerNil,
		},
		{
			name:    "empty task type",
			handler: h,
		},
		{
			name:     "valid handler",
			taskType: "test",
			handler:  h,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := o.RegisterHandler(tt.taskType, tt.handler)
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestListenAndRespond_ErrorResponse(t *testing.T) {
	responder := interactortest.NewResponder()

	o, err := orbital.NewOperator(responder)
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	tests := []struct {
		name      string
		taskType  string
		handler   orbital.Handler
		expErrMsg string
	}{
		{
			name:      "unknown task type",
			expErrMsg: orbital.ErrMsgUnknownTaskType,
		},
		{
			name:     "handler error",
			taskType: "error",
			handler: func(_ context.Context, _ orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				return orbital.HandlerResponse{}, assert.AnError
			},
			expErrMsg: assert.AnError.Error(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.taskType != "" {
				err := o.RegisterHandler(tt.taskType, tt.handler)
				assert.NoError(t, err)
			}
			req := orbital.TaskRequest{
				Type: tt.taskType,
			}
			responder.NewRequest(req)
			resp := responder.NewResponse()

			assert.Equal(t, string(orbital.ResultFailed), resp.Status)
			assert.Equal(t, tt.expErrMsg, resp.ErrorMessage)
		})
	}
}

func TestListenAndRespond(t *testing.T) {
	responder := interactortest.NewResponder()

	o, err := orbital.NewOperator(responder)
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	taskReq := orbital.TaskRequest{
		TaskID:       uuid.New(),
		Type:         "success",
		ExternalID:   "external-id",
		ETag:         "etag",
		Data:         []byte("test data"),
		WorkingState: []byte("prev working state"),
	}

	expWorkingState := []byte("after working state")
	expState := orbital.ResultDone
	expReconcileAfterSec := int64(10)

	h := func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
		assert.Equal(t, taskReq.TaskID, req.TaskID)
		assert.Equal(t, taskReq.Type, req.Type)
		assert.Equal(t, taskReq.Data, req.Data)
		assert.Equal(t, taskReq.WorkingState, req.WorkingState)

		return orbital.HandlerResponse{
			WorkingState:      expWorkingState,
			Result:            expState,
			ReconcileAfterSec: expReconcileAfterSec,
		}, nil
	}

	err = o.RegisterHandler(taskReq.Type, h)
	assert.NoError(t, err)

	responder.NewRequest(taskReq)
	resp := responder.NewResponse()

	assert.Equal(t, taskReq.TaskID, resp.TaskID)
	assert.Equal(t, taskReq.Type, resp.Type)
	assert.Equal(t, taskReq.ExternalID, resp.ExternalID)
	assert.Equal(t, taskReq.ETag, resp.ETag)
	assert.Equal(t, expWorkingState, resp.WorkingState)
	assert.Equal(t, string(expState), resp.Status)
	assert.Equal(t, expReconcileAfterSec, resp.ReconcileAfterSec)
	assert.Empty(t, resp.ErrorMessage)
}
