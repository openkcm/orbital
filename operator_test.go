package orbital_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
)

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
			o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client}, tt.opts...)
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

	o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
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

	o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
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

	o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	taskReq := orbital.TaskRequest{
		TaskID:     uuid.New(),
		Type:       "success",
		ExternalID: "external-id",
		ETag:       "etag",
		Data:       []byte("test data"),
	}

	expState := orbital.ResultDone
	expReconcileAfterSec := int64(10)

	h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
		assert.Equal(t, taskReq.TaskID, req.TaskID)
		assert.Equal(t, taskReq.Type, req.Type)
		assert.Equal(t, taskReq.Data, req.Data)

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

	o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
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
				assert.Equal(t, taskReq.Type, req.Type)

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

func TestOperatorCrypto(t *testing.T) {
	t.Run("VerifyTaskRequest", func(t *testing.T) {
		taskReq := orbital.TaskRequest{
			TaskID:       uuid.New(),
			Type:         "success",
			ExternalID:   "external-id",
			ETag:         "etag",
			Data:         []byte("test data"),
			WorkingState: []byte("{}"),
		}

		var actVerifyTaskRequestCalls atomic.Int32

		respSigner := &mockResponseSigner{
			FnSign: func(_ context.Context, _ orbital.TaskResponse) (orbital.Signature, error) {
				return orbital.Signature{}, nil
			},
		}

		tts := []struct {
			name                      string
			reqVerifier               orbital.TaskRequestVerifier
			expVerifyTaskRequestCalls int32
			expHandlerCalls           int32
		}{
			{
				name: "should call verifyTaskRequest and handler if the crypto is not nil",
				reqVerifier: &mockRequestVerifier{
					FnVerify: func(_ context.Context, request orbital.TaskRequest) error {
						actVerifyTaskRequestCalls.Add(1)
						assert.Equal(t, taskReq, request)
						return nil
					},
				},
				expVerifyTaskRequestCalls: 1,
				expHandlerCalls:           1,
			},
			{
				name:                      "should call handler even if the crypto is nil",
				reqVerifier:               nil,
				expVerifyTaskRequestCalls: 0,
				expHandlerCalls:           1,
			},
			{
				name: "should not call the handler if the verifyTaskRequest returns an error",
				reqVerifier: &mockRequestVerifier{
					FnVerify: func(_ context.Context, request orbital.TaskRequest) error {
						actVerifyTaskRequestCalls.Add(1)
						assert.Equal(t, taskReq, request)
						return assert.AnError
					},
				},
				expVerifyTaskRequestCalls: 1,
				expHandlerCalls:           0,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				actVerifyTaskRequestCalls.Store(0)

				client := respondertest.NewResponder()
				o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client, Signer: respSigner, Verifier: tt.reqVerifier})
				assert.NoError(t, err)
				assert.NotNil(t, o)

				ctx := t.Context()
				o.ListenAndRespond(ctx)

				var actHandlerCalls atomic.Int32
				actHandlerCallChan := make(chan struct{})
				h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
					assert.Equal(t, taskReq.TaskID, req.TaskID)
					actHandlerCalls.Add(1)
					actHandlerCallChan <- struct{}{}

					resp.Result = orbital.ResultDone
					resp.ReconcileAfterSec = int64(10)
					return nil
				}

				err = o.RegisterHandler(taskReq.Type, h)
				assert.NoError(t, err)

				// when
				client.NewRequest(taskReq)

				ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*2)
				defer cancel()
				select {
				case <-ctxTimeout.Done():
				case <-actHandlerCallChan:
				}

				// then
				assert.Equal(t, tt.expVerifyTaskRequestCalls, actVerifyTaskRequestCalls.Load())
				assert.Equal(t, tt.expHandlerCalls, actHandlerCalls.Load())
			})
		}
	})
	t.Run("SignTaskResponse", func(t *testing.T) {
		// given
		mockVerifier := &mockRequestVerifier{
			FnVerify: func(_ context.Context, _ orbital.TaskRequest) error {
				return nil
			},
		}
		expSignature := orbital.Signature{
			"value": "signature",
			"type":  "jwt",
		}

		expStatus := string(orbital.ResultDone)
		expReconcileAfterSec := int64(19)
		expWorkingState := []byte("{}")

		taskReq := orbital.TaskRequest{
			TaskID:       uuid.New(),
			Type:         "success",
			ExternalID:   "external-id",
			ETag:         "etag",
			Data:         []byte("test data"),
			WorkingState: []byte("{}"),
		}
		expResponse := orbital.TaskResponse{
			TaskID:            taskReq.TaskID,
			Type:              taskReq.Type,
			ExternalID:        taskReq.ExternalID,
			ETag:              taskReq.ETag,
			WorkingState:      expWorkingState,
			Status:            expStatus,
			ReconcileAfterSec: expReconcileAfterSec,
		}

		var actSignTaskResponseCalls atomic.Int32
		tts := []struct {
			name                     string
			respSigner               orbital.TaskResponseSigner
			expSendTaskResponseCalls int32
			expSignCalls             int32
			expSignature             orbital.Signature
		}{
			{
				name: "should call signTaskResponse and client sendTaskResponse if crypto is not nil",
				respSigner: &mockResponseSigner{
					FnSign: func(_ context.Context, response orbital.TaskResponse) (orbital.Signature, error) {
						actSignTaskResponseCalls.Add(1)
						assert.Equal(t, expResponse, response)
						return expSignature, nil
					},
				},
				expSignCalls:             1,
				expSendTaskResponseCalls: 1,
				expSignature:             expSignature,
			},
			{
				name:                     "should call client sendTaskResponse even if the crypto is nil",
				respSigner:               nil,
				expSignCalls:             0,
				expSendTaskResponseCalls: 1,
			},
			{
				name: "should not call the client sendTaskResponse if the sign task response returns an error",
				respSigner: &mockResponseSigner{
					FnSign: func(_ context.Context, response orbital.TaskResponse) (orbital.Signature, error) {
						actSignTaskResponseCalls.Add(1)
						assert.Equal(t, expResponse, response)
						return orbital.Signature{}, assert.AnError
					},
				},
				expSignCalls:             1,
				expSendTaskResponseCalls: 0,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				actSignTaskResponseCalls.Store(0)

				client := &mockResponder{}
				taskRequestChan := make(chan orbital.TaskRequest)
				client.FnReceiveTaskRequest = func(_ context.Context) (orbital.TaskRequest, error) {
					return <-taskRequestChan, nil
				}

				var actSendTaskResponseCalls atomic.Int32
				actSendTaskResponseCallChan := make(chan struct{})
				client.FnSendTaskResponse = func(_ context.Context, response orbital.TaskResponse) error {
					actSendTaskResponseCalls.Add(1)
					actSendTaskResponseCallChan <- struct{}{}
					assertMapContains(t, response.MetaData, tt.expSignature)
					return nil
				}

				o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client, Verifier: mockVerifier, Signer: tt.respSigner})
				assert.NoError(t, err)
				assert.NotNil(t, o)

				ctx := t.Context()
				o.ListenAndRespond(ctx)

				h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
					assert.Equal(t, taskReq.TaskID, req.TaskID)
					resp.Result = orbital.Result(expStatus)
					resp.ReconcileAfterSec = expReconcileAfterSec
					return nil
				}

				err = o.RegisterHandler(taskReq.Type, h)
				assert.NoError(t, err)

				// when
				taskRequestChan <- taskReq

				ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*2)
				defer cancel()
				select {
				case <-ctxTimeout.Done():
				case <-actSendTaskResponseCallChan:
				}

				// then
				assert.Equal(t, tt.expSignCalls, actSignTaskResponseCalls.Load())
				assert.Equal(t, tt.expSendTaskResponseCalls, actSendTaskResponseCalls.Load())
			})
		}
	})
}

type mockRequestVerifier struct {
	FnVerify func(ctx context.Context, request orbital.TaskRequest) error
}

var _ orbital.TaskRequestVerifier = &mockRequestVerifier{}

func (m *mockRequestVerifier) Verify(ctx context.Context, request orbital.TaskRequest) error {
	return m.FnVerify(ctx, request)
}

type mockResponseSigner struct {
	FnSign func(ctx context.Context, response orbital.TaskResponse) (orbital.Signature, error)
}

var _ orbital.TaskResponseSigner = &mockResponseSigner{}

func (m *mockResponseSigner) Sign(ctx context.Context, response orbital.TaskResponse) (orbital.Signature, error) {
	return m.FnSign(ctx, response)
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
