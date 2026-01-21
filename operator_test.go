package orbital_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
)

func TestWorkingState_Codec(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		expBytes []byte
		expErr   error
	}{
		{
			name:     "nil working state",
			bytes:    nil,
			expBytes: []byte("{}"),
		},
		{
			name:  "empty working state",
			bytes: []byte("{}"),
		},
		{
			name:  "valid working state bytes",
			bytes: []byte(`{"key":"value","number":42}`),
		},
		{
			name:   "invalid working state bytes",
			bytes:  []byte("invalid"),
			expErr: orbital.ErrWorkingStateInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws, err := orbital.DecodeWorkingState(tt.bytes)
			if tt.expErr != nil {
				assert.ErrorIs(t, err, tt.expErr)
				return
			}
			assert.NoError(t, err)

			bytes, err := ws.Encode()
			assert.NoError(t, err)
			if tt.expBytes != nil {
				assert.Equal(t, tt.expBytes, bytes)
				return
			}
			assert.Equal(t, tt.bytes, bytes)
		})
	}
}

func TestWorkingState_SetAndValue(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    any
		expValue any
	}{
		{
			name:     "set and get string value",
			key:      "key1",
			value:    "value1",
			expValue: "value1",
		},
		{
			name:     "set and get integer value",
			key:      "key2",
			value:    42,
			expValue: 42,
		},
		{
			name:     "set and get struct value",
			key:      "key3",
			value:    struct{ Field string }{Field: "data"},
			expValue: struct{ Field string }{Field: "data"},
		},
		{
			name:     "set and get nil value",
			key:      "key4",
			value:    nil,
			expValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			ws.Set(tt.key, tt.value)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.expValue, val)
		})
	}
}

func TestWorkingState_GaugeMethods(t *testing.T) {
	incTests := []struct {
		name      string
		key       string
		initValue int
		expValue  int
	}{
		{
			name:     "increment key",
			key:      "gauge1",
			expValue: 1,
		},
		{
			name:      "increment key with existing value",
			key:       "gauge2",
			initValue: 2,
			expValue:  3,
		},
	}

	for _, tt := range incTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Inc(tt.key)
			assert.Equal(t, tt.expValue, value)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.expValue, val)
		})
	}

	decTests := []struct {
		name      string
		key       string
		initValue int
		expValue  int
	}{
		{
			name:     "decrement key",
			key:      "gauge1",
			expValue: -1,
		},
		{
			name:      "decrement key with existing value",
			key:       "gauge2",
			initValue: 5,
			expValue:  4,
		},
	}

	for _, tt := range decTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Dec(tt.key)
			assert.Equal(t, tt.expValue, value)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.expValue, val)
		})
	}

	addTests := []struct {
		name      string
		key       string
		amount    int
		initValue int
		expValue  int
	}{
		{
			name:     "add to key",
			key:      "gauge1",
			amount:   5,
			expValue: 5,
		},
		{
			name:      "add to key with existing value",
			key:       "gauge2",
			amount:    10,
			initValue: 3,
			expValue:  13,
		},
	}

	for _, tt := range addTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Add(tt.key, tt.amount)
			assert.Equal(t, tt.expValue, value)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.expValue, val)
		})
	}

	subTests := []struct {
		name      string
		key       string
		amount    int
		initValue int
		expValue  int
	}{
		{
			name:     "subtract from key",
			key:      "gauge1",
			amount:   4,
			expValue: -4,
		},
		{
			name:      "subtract from key with existing value",
			key:       "gauge2",
			amount:    2,
			initValue: 7,
			expValue:  5,
		},
	}

	for _, tt := range subTests {
		t.Run(tt.name, func(t *testing.T) {
			ws := &orbital.WorkingState{}
			if tt.initValue != 0 {
				ws.Set(tt.key, tt.initValue)
			}
			value := ws.Sub(tt.key, tt.amount)
			assert.Equal(t, tt.expValue, value)

			val, ok := ws.Value(tt.key)
			assert.True(t, ok)
			assert.Equal(t, tt.expValue, val)
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
			client.NewRequest(req)
			resp := client.NewResponse()

			assert.Equal(t, string(orbital.ResultFailed), resp.Status)
			assert.Equal(t, tt.expErrMsg, resp.ErrorMessage)
		})
	}
}

func TestListenAndRespond(t *testing.T) {
	client := respondertest.NewResponder()

	o, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
	assert.NoError(t, err)
	assert.NotNil(t, o)

	o.ListenAndRespond(t.Context())

	workingState := orbital.WorkingState{}
	key := "key"
	workingState.Set(key, "prevValue")
	prevWorkingStateBytes, err := workingState.Encode()
	assert.NoError(t, err)
	taskReq := orbital.TaskRequest{
		TaskID:       uuid.New(),
		Type:         "success",
		ExternalID:   "external-id",
		ETag:         "etag",
		Data:         []byte("test data"),
		WorkingState: prevWorkingStateBytes,
	}

	expState := orbital.ResultDone
	expReconcileAfterSec := int64(10)

	h := func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
		assert.Equal(t, taskReq.TaskID, req.TaskID)
		assert.Equal(t, taskReq.Type, req.Type)
		assert.Equal(t, taskReq.Data, req.Data)

		val, ok := req.WorkingState.Value(key)
		assert.True(t, ok)
		assert.Equal(t, "prevValue", val)
		req.WorkingState.Set(key, "newValue")

		return orbital.HandlerResponse{
			Result:            expState,
			ReconcileAfterSec: expReconcileAfterSec,
		}, nil
	}

	err = o.RegisterHandler(taskReq.Type, h)
	assert.NoError(t, err)

	client.NewRequest(taskReq)
	resp := client.NewResponse()

	assert.Equal(t, taskReq.TaskID, resp.TaskID)
	assert.Equal(t, taskReq.Type, resp.Type)
	assert.Equal(t, taskReq.ExternalID, resp.ExternalID)
	assert.Equal(t, taskReq.ETag, resp.ETag)
	ws, err := orbital.DecodeWorkingState(resp.WorkingState)
	assert.NoError(t, err)
	val, ok := ws.Value(key)
	assert.True(t, ok)
	assert.Equal(t, "newValue", val)
	assert.Equal(t, string(expState), resp.Status)
	assert.Equal(t, expReconcileAfterSec, resp.ReconcileAfterSec)
	assert.Empty(t, resp.ErrorMessage)
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
				h := func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
					assert.Equal(t, taskReq.TaskID, req.TaskID)
					actHandlerCalls.Add(1)
					actHandlerCallChan <- struct{}{}
					return orbital.HandlerResponse{
						Result:            orbital.ResultDone,
						ReconcileAfterSec: int64(10),
					}, nil
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

				h := func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
					assert.Equal(t, taskReq.TaskID, req.TaskID)
					return orbital.HandlerResponse{
						Result:            orbital.Result(expStatus),
						ReconcileAfterSec: expReconcileAfterSec,
					}, nil
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
