package orbital_test

import (
	"context"
	"strings"
	"testing"

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

func TestHandlerResponse_WorkingState(t *testing.T) {
	// given
	tests := []struct {
		name            string
		pair            map[string]string
		rawWorkingState func(pair map[string]string) []byte
		expWorkingState func(pair map[string]string) *orbital.WorkingState
		expErr          error
	}{
		{
			name: "nil raw working state",
			rawWorkingState: func(_ map[string]string) []byte {
				return nil
			},
			expWorkingState: func(_ map[string]string) *orbital.WorkingState {
				return &orbital.WorkingState{}
			},
		},
		{
			name: "empty raw working state",
			rawWorkingState: func(_ map[string]string) []byte {
				return []byte("{}")
			},
			expWorkingState: func(_ map[string]string) *orbital.WorkingState {
				return &orbital.WorkingState{}
			},
		},
		{
			name: "invalid raw working state",
			rawWorkingState: func(_ map[string]string) []byte {
				return []byte("{invalid json}")
			},
			expWorkingState: func(_ map[string]string) *orbital.WorkingState {
				return nil
			},
			expErr: orbital.ErrWorkingStateInvalid,
		},
		{
			name: "valid raw working state",
			pair: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			rawWorkingState: func(pair map[string]string) []byte {
				var sb strings.Builder
				for k, v := range pair {
					if sb.Len() > 0 {
						sb.WriteString(",")
					}
					sb.WriteString(`"` + k + `":"` + v + `"`)
				}
				return []byte(`{` + sb.String() + `}`)
			},
			expWorkingState: func(pair map[string]string) *orbital.WorkingState {
				ws := &orbital.WorkingState{}
				for k, v := range pair {
					ws.Set(k, v)
				}
				return ws
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := orbital.HandlerResponse{
				RawWorkingState: tt.rawWorkingState(tt.pair),
			}

			// when
			ws, err := resp.WorkingState()

			// then
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				assert.Nil(t, ws)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, ws)

			expWorkingState := tt.expWorkingState(tt.pair)
			for k := range tt.pair {
				expVal, ok := expWorkingState.Value(k)
				assert.True(t, ok)
				actVal, ok := ws.Value(k)
				assert.True(t, ok)
				assert.Equal(t, expVal, actVal)
			}

			// when called again, should return the same working state
			ws2, err := resp.WorkingState()

			// then
			assert.NoError(t, err)
			assert.Equal(t, ws, ws2)
		})
	}
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

	h := func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) error {
		return nil
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
	client := respondertest.NewResponder()

	o, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
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
			expErrMsg: orbital.ErrUnknownTaskType.Error(),
		},
		{
			name:     "handler error",
			taskType: "handler error",
			handler: func(_ context.Context, _ orbital.HandlerRequest, _ *orbital.HandlerResponse) error {
				return assert.AnError
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
			client.NewRequest(req)
			resp := client.NewResponse()

			assert.Equal(t, string(orbital.ResultFailed), resp.Status)
			assert.Contains(t, resp.ErrorMessage, tt.expErrMsg)
		})
	}
}

func TestListenAndRespond(t *testing.T) {
	client := respondertest.NewResponder()

	o, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	taskReq := orbital.TaskRequest{
		TaskID:               uuid.New(),
		Type:                 "success",
		ExternalID:           "external-id",
		ETag:                 "etag",
		Data:                 []byte("test data"),
		TaskCreatedAt:        123,
		TaskLastReconciledAt: 456,
	}

	expState := orbital.ResultDone
	expReconcileAfterSec := uint64(10)

	h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
		assert.Equal(t, taskReq.TaskID, req.TaskID)
		assert.Equal(t, taskReq.Type, req.TaskType)
		assert.Equal(t, taskReq.Data, req.TaskData)
		assert.Equal(t, taskReq.TaskCreatedAt, req.TaskCreatedAt.UnixNano())
		assert.Equal(t, taskReq.TaskLastReconciledAt, req.TaskLastReconciledAt.UnixNano())

		resp.Result = expState
		resp.ReconcileAfterSec = expReconcileAfterSec
		return nil
	}

	err = o.RegisterHandler(taskReq.Type, h)
	assert.NoError(t, err)

	client.NewRequest(taskReq)
	resp := client.NewResponse()

	assert.Equal(t, taskReq.TaskID, resp.TaskID)
	assert.Equal(t, taskReq.Type, resp.Type)
	assert.Equal(t, taskReq.ExternalID, resp.ExternalID)
	assert.Equal(t, taskReq.ETag, resp.ETag)
	assert.Equal(t, string(expState), resp.Status)
	assert.Equal(t, expReconcileAfterSec, resp.ReconcileAfterSec)
	assert.Empty(t, resp.ErrorMessage)
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

			h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
				assert.Equal(t, taskReq.TaskID, req.TaskID)
				assert.Equal(t, taskReq.Type, req.TaskType)

				if tt.customWorkingState != nil {
					resp.RawWorkingState = tt.customWorkingState
					resp.Result = orbital.ResultDone
					return nil
				}

				workingState, err := resp.WorkingState()
				if tt.expErrDecode != nil {
					assert.ErrorIs(t, err, tt.expErrDecode)
					resp.Result = orbital.ResultDone
					return nil
				}
				assert.NoError(t, err)
				assert.NotNil(t, workingState)

				if tt.mutateWorkingState != nil {
					tt.mutateWorkingState(workingState)
				}

				if tt.discardWorkingState {
					workingState.DiscardChanges()
				}

				resp.Result = orbital.ResultDone
				return nil
			}

			err = o.RegisterHandler(taskReq.Type, h)
			assert.NoError(t, err)

			client.NewRequest(taskReq)
			resp := client.NewResponse()

			assert.Equal(t, taskReq.TaskID, resp.TaskID)
			assert.Equal(t, taskReq.Type, resp.Type)
			assert.Equal(t, string(orbital.ResultDone), resp.Status)
			assert.Equal(t, tt.expRawWorkingState, resp.WorkingState)
			assert.Empty(t, resp.ErrorMessage)
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
