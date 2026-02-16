package orbital_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
)

func TestOperator_NewOperator(t *testing.T) {
	t.Run("should return error if signature checking is enabled and the verifier is nil set for the target", func(t *testing.T) {
		invalidTarget := orbital.TargetOperator{
			Client:             respondertest.NewResponder(),
			Verifier:           nil,
			MustCheckSignature: true,
		}

		actResult, actErr := orbital.NewOperator(invalidTarget)
		assert.Nil(t, actResult)
		assert.ErrorIs(t, actErr, orbital.ErrOperatorInvalidConfig)
	})
}

func TestNew(t *testing.T) {
	client := respondertest.NewResponder()

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
			o, err := orbital.NewOperator(orbital.TargetOperator{Client: client}, tt.opts...)
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
	client := respondertest.NewResponder()

	o, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) {}

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

func TestListenAndRespond_UnknownTaskType(t *testing.T) {
	client := respondertest.NewResponder()

	o, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	taskType := "unknown-task-type"

	req := orbital.TaskRequest{
		Type: taskType,
	}
	client.NewRequest(req)
	resp := client.NewResponse()

	assert.Equal(t, string(orbital.TaskStatusFailed), resp.Status)
	assert.Equal(t, "no handler registered for task type unknown-task-type", resp.ErrorMessage)
}

func TestListenAndRespond(t *testing.T) {
	taskReq := orbital.TaskRequest{
		TaskID:               uuid.New(),
		Type:                 "any",
		ExternalID:           "external-id",
		ETag:                 "etag",
		Data:                 []byte("test data"),
		WorkingState:         []byte("prev state"),
		TaskCreatedAt:        123,
		TaskLastReconciledAt: 456,
	}

	tests := []struct {
		name              string
		expStatus         orbital.TaskStatus
		expErrMsg         string
		expReconcileAfter time.Duration
	}{
		{
			name:      "done",
			expStatus: orbital.TaskStatusDone,
		},
		{
			name:      "failed",
			expStatus: orbital.TaskStatusFailed,
			expErrMsg: "error message",
		},
		{
			name:              "continue with reconcile after",
			expStatus:         orbital.TaskStatusProcessing,
			expReconcileAfter: 30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := respondertest.NewResponder()

			o, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
			assert.NoError(t, err)
			assert.NotNil(t, o)

			o.ListenAndRespond(t.Context())

			h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
				assert.Equal(t, taskReq.TaskID, req.TaskID)
				assert.Equal(t, taskReq.Type, req.TaskType)
				assert.Equal(t, taskReq.Data, req.TaskData)
				assert.Equal(t, taskReq.TaskCreatedAt, req.TaskCreatedAt.UnixNano())
				assert.Equal(t, taskReq.TaskLastReconciledAt, req.TaskLastReconciledAt.UnixNano())
				assert.Equal(t, taskReq.WorkingState, req.TaskRawWorkingState)

				switch tt.expStatus {
				case orbital.TaskStatusDone:
					resp.Complete()
				case orbital.TaskStatusFailed:
					resp.Fail(tt.expErrMsg)
				case orbital.TaskStatusProcessing:
					resp.ContinueAndWaitFor(tt.expReconcileAfter)
				case orbital.TaskStatusCreated:
					assert.Fail(t, "unexpected TaskStatusCreated result from handler")
				}
			}

			err = o.RegisterHandler(taskReq.Type, h)
			assert.NoError(t, err)

			client.NewRequest(taskReq)
			resp := client.NewResponse()

			assert.Equal(t, taskReq.TaskID, resp.TaskID)
			assert.Equal(t, taskReq.Type, resp.Type)
			assert.Equal(t, taskReq.ExternalID, resp.ExternalID)
			assert.Equal(t, taskReq.ETag, resp.ETag)
			assert.Equal(t, string(tt.expStatus), resp.Status)
			assert.InDelta(t, tt.expReconcileAfter.Seconds(), float64(resp.ReconcileAfterSec), 0.001)
			assert.Equal(t, tt.expErrMsg, resp.ErrorMessage)
		})
	}
}

func TestListenAndRespond_WorkingState(t *testing.T) {
	client := respondertest.NewResponder()

	o, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	tests := []struct {
		name                string
		rawWorkingState     []byte
		customWorkingState  []byte
		mutateWorkingState  func(ws *orbital.WorkingState)
		discardWorkingState bool
		expErrDecode        error
		expRawWorkingState  []byte
	}{
		{
			name:               "nil working state",
			rawWorkingState:    nil,
			expRawWorkingState: []byte("{}"),
		},
		{
			name:               "empty working state",
			rawWorkingState:    []byte("{}"),
			expRawWorkingState: []byte("{}"),
		},
		{
			name:               "invalid working state in request",
			rawWorkingState:    []byte("{invalid json}"),
			expErrDecode:       orbital.ErrWorkingStateInvalid,
			expRawWorkingState: []byte("{invalid json}"),
		},
		{
			name:            "invalid working state modified in handler",
			rawWorkingState: []byte(`{"key":"value"}`),
			mutateWorkingState: func(ws *orbital.WorkingState) {
				ws.Set("key", func() {})
			},
			expRawWorkingState: []byte(`{"key":"value"}`),
		},
		{
			name:               "valid working state",
			rawWorkingState:    []byte(`{"key":"value"}`),
			expRawWorkingState: []byte(`{"key":"value"}`),
		},
		{
			name:            "modified working state",
			rawWorkingState: []byte(`{"key":"value"}`),
			mutateWorkingState: func(ws *orbital.WorkingState) {
				ws.Set("key", "newValue")
				ws.Set("newKey", "newValue2")
			},
			expRawWorkingState: []byte(`{"key":"newValue","newKey":"newValue2"}`),
		},
		{
			name:               "custom working state",
			customWorkingState: []byte("custom working state"),
			expRawWorkingState: []byte("custom working state"),
		},
		{
			name:            "discard working state",
			rawWorkingState: []byte(`{"key":"value"}`),
			mutateWorkingState: func(ws *orbital.WorkingState) {
				ws.Set("key", "newValue")
			},
			discardWorkingState: true,
			expRawWorkingState:  []byte(`{"key":"value"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskReq := orbital.TaskRequest{
				TaskID:       uuid.New(),
				ETag:         uuid.NewString(),
				Type:         tt.name,
				WorkingState: tt.rawWorkingState,
			}

			h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
				assert.Equal(t, taskReq.TaskID, req.TaskID)
				assert.Equal(t, taskReq.Type, req.TaskType)

				if tt.customWorkingState != nil {
					resp.UseRawWorkingState(tt.customWorkingState)
					resp.Complete()
					return
				}

				workingState, err := resp.WorkingState()
				if tt.expErrDecode != nil {
					assert.ErrorIs(t, err, tt.expErrDecode)
					resp.Fail(err.Error())
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, workingState)

				if tt.mutateWorkingState != nil {
					tt.mutateWorkingState(workingState)
				}

				if tt.discardWorkingState {
					workingState.DiscardChanges()
				}

				resp.Complete()
			}

			err = o.RegisterHandler(taskReq.Type, h)
			assert.NoError(t, err)

			client.NewRequest(taskReq)
			resp := client.NewResponse()

			assert.Equal(t, taskReq.TaskID, resp.TaskID)
			assert.Equal(t, taskReq.Type, resp.Type)
			assert.Equal(t, tt.expRawWorkingState, resp.WorkingState)
		})
	}
}

type mockResponder struct {
	FnReceiveTaskRequest func(ctx context.Context) (orbital.TaskRequest, error)
	FnSendTaskResponse   func(ctx context.Context, response orbital.TaskResponse) error
	FnClose              func(ctx context.Context) error
}

var _ orbital.Responder = &mockResponder{}

// ReceiveTaskRequest implements orbital.Responder.
func (m *mockResponder) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	return m.FnReceiveTaskRequest(ctx)
}

// SendTaskResponse implements orbital.Responder.
func (m *mockResponder) SendTaskResponse(ctx context.Context, response orbital.TaskResponse) error {
	return m.FnSendTaskResponse(ctx, response)
}

func (m *mockResponder) Close(ctx context.Context) error {
	if m.FnClose == nil {
		return nil
	}

	return m.FnClose(ctx)
}
