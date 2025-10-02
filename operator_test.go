package orbital_test

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/interactortest"
)

func TestNew(t *testing.T) {
	client := interactortest.NewResponder()

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
			o, err := orbital.NewOperator(orbital.Responder{Client: client}, tt.opts...)
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
	client := interactortest.NewResponder()

	o, err := orbital.NewOperator(orbital.Responder{Client: client})
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
	client := interactortest.NewResponder()

	o, err := orbital.NewOperator(orbital.Responder{Client: client})
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
			client.NewRequest(req)
			resp := client.NewResponse()

			assert.Equal(t, string(orbital.ResultFailed), resp.Status)
			assert.Equal(t, tt.expErrMsg, resp.ErrorMessage)
		})
	}
}

func TestListenAndRespond(t *testing.T) {
	client := interactortest.NewResponder()

	o, err := orbital.NewOperator(orbital.Responder{Client: client})
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

	client.NewRequest(taskReq)
	resp := client.NewResponse()

	assert.Equal(t, taskReq.TaskID, resp.TaskID)
	assert.Equal(t, taskReq.Type, resp.Type)
	assert.Equal(t, taskReq.ExternalID, resp.ExternalID)
	assert.Equal(t, taskReq.ETag, resp.ETag)
	assert.Equal(t, expWorkingState, resp.WorkingState)
	assert.Equal(t, string(expState), resp.Status)
	assert.Equal(t, expReconcileAfterSec, resp.ReconcileAfterSec)
	assert.Empty(t, resp.ErrorMessage)
}

func TestOperatorCrypto(t *testing.T) {
	t.Run("VerifyTaskRequest", func(t *testing.T) {
		mockCrypto := &mockResponderCrypto{}
		mockCrypto.FnSignTaskResponse = func(_ context.Context, _ orbital.TaskResponse) (orbital.Signature, error) {
			return orbital.Signature{}, nil
		}

		tts := []struct {
			name                      string
			mockCrypto                *mockResponderCrypto
			fnVerifyTaskRequest       func(request orbital.TaskRequest) error
			expVerifyTaskRequestCalls int
			expHandlerCalls           int32
		}{
			{
				name:       "should call verifyTaskRequest and handler if the crypto is not nil",
				mockCrypto: mockCrypto,
				fnVerifyTaskRequest: func(_ orbital.TaskRequest) error {
					return nil
				},
				expVerifyTaskRequestCalls: 1,
				expHandlerCalls:           1,
			},
			{
				name:                      "should call handler even if the crypto is nil",
				mockCrypto:                nil,
				fnVerifyTaskRequest:       nil,
				expVerifyTaskRequestCalls: 0,
				expHandlerCalls:           1,
			},
			{
				name:       "should not call the handler if the verifyTaskRequest returns an error",
				mockCrypto: mockCrypto,
				fnVerifyTaskRequest: func(_ orbital.TaskRequest) error {
					return assert.AnError
				},
				expVerifyTaskRequestCalls: 1,
				expHandlerCalls:           0,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				// given
				taskReq := orbital.TaskRequest{
					TaskID:       uuid.New(),
					Type:         "success",
					ExternalID:   "external-id",
					ETag:         "etag",
					Data:         []byte("test data"),
					WorkingState: []byte("prev working state"),
					MetaData: orbital.MetaData{
						Signature: orbital.Signature{
							Value: "value",
							Type:  "type",
						},
					},
				}

				var responderCrypto orbital.ResponderCrypto
				actVerifyTaskRequestCalls := 0
				if tt.mockCrypto != nil {
					tt.mockCrypto.FnVerifyTaskRequest = func(_ context.Context, request orbital.TaskRequest) error {
						actVerifyTaskRequestCalls++
						assert.Equal(t, taskReq, request)
						return tt.fnVerifyTaskRequest(request)
					}
					responderCrypto = tt.mockCrypto
				}

				client := interactortest.NewResponder()
				o, err := orbital.NewOperator(orbital.Responder{Client: client, Crypto: responderCrypto})
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
						WorkingState:      []byte("after working state"),
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
					log.Println("timeout test")
				case <-actHandlerCallChan:
				}

				// then
				assert.Equal(t, tt.expVerifyTaskRequestCalls, actVerifyTaskRequestCalls)
				assert.Equal(t, tt.expHandlerCalls, actHandlerCalls.Load())
			})
		}
	})
	t.Run("SignTaskResponse", func(t *testing.T) {
		// given
		mockCrypto := &mockResponderCrypto{}
		mockCrypto.FnVerifyTaskRequest = func(_ context.Context, _ orbital.TaskRequest) error {
			return nil
		}
		expSignature := orbital.Signature{
			Value: "value",
			Type:  "type",
		}
		tts := []struct {
			name                     string
			mockCrypto               *mockResponderCrypto
			fnSignTaskResponse       func(request orbital.TaskResponse) (orbital.Signature, error)
			expSendTaskResponseCalls int32
			expSignTaskResponseCalls int32
			expTaskResponseSignature orbital.Signature
		}{
			{
				name:       "should call signTaskResponse and client sendTaskResponse if crypto is not nil",
				mockCrypto: mockCrypto,
				fnSignTaskResponse: func(_ orbital.TaskResponse) (orbital.Signature, error) {
					return expSignature, nil
				},
				expSignTaskResponseCalls: 1,
				expSendTaskResponseCalls: 1,
				expTaskResponseSignature: expSignature,
			},
			{
				name:                     "should call client sendTaskResponse even if the crypto is nil",
				mockCrypto:               nil,
				fnSignTaskResponse:       nil,
				expSignTaskResponseCalls: 0,
				expSendTaskResponseCalls: 1,
			},
			{
				name:       "should not call the client sendTaskResponse if the sign task response returns an error",
				mockCrypto: mockCrypto,
				fnSignTaskResponse: func(_ orbital.TaskResponse) (orbital.Signature, error) {
					return orbital.Signature{}, assert.AnError
				},
				expSignTaskResponseCalls: 1,
				expSendTaskResponseCalls: 0,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				taskReq := orbital.TaskRequest{
					TaskID:       uuid.New(),
					Type:         "success",
					ExternalID:   "external-id",
					ETag:         "etag",
					Data:         []byte("test data"),
					WorkingState: []byte("prev working state"),
				}

				client := &mockResponderClient{}
				taskRequestChan := make(chan orbital.TaskRequest)
				client.FnReceiveTaskRequest = func(_ context.Context) (orbital.TaskRequest, error) {
					return <-taskRequestChan, nil
				}

				var actSendTaskResponseCalls atomic.Int32
				actSendTaskResponseCallChan := make(chan struct{})
				client.FnSendTaskResponse = func(_ context.Context, response orbital.TaskResponse) error {
					actSendTaskResponseCalls.Add(1)
					actSendTaskResponseCallChan <- struct{}{}
					assert.Equal(t, tt.expTaskResponseSignature, response.MetaData.Signature)
					return nil
				}

				expWorkingState := []byte("after working state")
				expStatus := string(orbital.ResultDone)
				expReconcileAfterSec := int64(19)

				var responderCrypto orbital.ResponderCrypto
				var actSignTaskResponseCalls atomic.Int32
				if tt.mockCrypto != nil {
					tt.mockCrypto.FnSignTaskResponse = func(_ context.Context, response orbital.TaskResponse) (orbital.Signature, error) {
						actSignTaskResponseCalls.Add(1)
						assert.Equal(t, orbital.TaskResponse{
							TaskID:            taskReq.TaskID,
							Type:              taskReq.Type,
							ExternalID:        taskReq.ExternalID,
							ETag:              taskReq.ETag,
							WorkingState:      expWorkingState,
							Status:            expStatus,
							ReconcileAfterSec: expReconcileAfterSec,
						}, response)
						return tt.fnSignTaskResponse(response)
					}
					responderCrypto = tt.mockCrypto
				}

				o, err := orbital.NewOperator(orbital.Responder{Client: client, Crypto: responderCrypto})
				assert.NoError(t, err)
				assert.NotNil(t, o)

				ctx := t.Context()
				o.ListenAndRespond(ctx)

				h := func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
					assert.Equal(t, taskReq.TaskID, req.TaskID)
					return orbital.HandlerResponse{
						WorkingState:      expWorkingState,
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
				assert.Equal(t, tt.expSignTaskResponseCalls, actSignTaskResponseCalls.Load())
				assert.Equal(t, tt.expSendTaskResponseCalls, actSendTaskResponseCalls.Load())
			})
		}
	})
}

type mockResponderCrypto struct {
	FnSignTaskResponse  func(ctx context.Context, response orbital.TaskResponse) (orbital.Signature, error)
	FnVerifyTaskRequest func(ctx context.Context, request orbital.TaskRequest) error
}

var _ orbital.ResponderCrypto = &mockResponderCrypto{}

// SignTaskResponse implements orbital.ResponderCrypto.
func (m *mockResponderCrypto) SignTaskResponse(ctx context.Context, response orbital.TaskResponse) (orbital.Signature, error) {
	return m.FnSignTaskResponse(ctx, response)
}

// VerifyTaskRequest implements orbital.ResponderCrypto.
func (m *mockResponderCrypto) VerifyTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	return m.FnVerifyTaskRequest(ctx, request)
}

type mockResponderClient struct {
	FnReceiveTaskRequest func(ctx context.Context) (orbital.TaskRequest, error)
	FnSendTaskResponse   func(ctx context.Context, response orbital.TaskResponse) error
}

var _ orbital.ResponderClient = &mockResponderClient{}

// ReceiveTaskRequest implements orbital.ResponderClient.
func (m *mockResponderClient) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	return m.FnReceiveTaskRequest(ctx)
}

// SendTaskResponse implements orbital.ResponderClient.
func (m *mockResponderClient) SendTaskResponse(ctx context.Context, response orbital.TaskResponse) error {
	return m.FnSendTaskResponse(ctx, response)
}
