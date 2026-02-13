package orbital_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
)

func TestManager_Signing(t *testing.T) {
	t.Run("SignTaskRequest", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		expSignature := map[string]string{"value": "signature", "type": "jwt"}
		tts := []struct {
			name          string
			reqSigner     orbital.TaskRequestSigner
			expSignature  orbital.Signature
			expTaskStatus orbital.TaskStatus
			expClientCall int
		}{
			{
				name: "should have signature if signer is defined",
				reqSigner: &mockRequestSigner{
					FnSign: func(_ context.Context, _ orbital.TaskRequest) (orbital.Signature, error) {
						return expSignature, nil
					},
				},
				expSignature:  expSignature,
				expTaskStatus: orbital.TaskStatusProcessing,
				expClientCall: 1,
			},
			{
				name:          "should have no signature if signer is not defined",
				reqSigner:     nil,
				expClientCall: 1,
				expTaskStatus: orbital.TaskStatusProcessing,
			},
			{
				name: "should not send task request if signing fails",
				reqSigner: &mockRequestSigner{
					FnSign: func(_ context.Context, _ orbital.TaskRequest) (orbital.Signature, error) {
						return orbital.Signature{}, assert.AnError
					},
				},
				expSignature:  orbital.Signature{},
				expTaskStatus: orbital.TaskStatusFailed,
				expClientCall: 0,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				expTarget := "target-1"
				job := orbital.Job{
					ID:         uuid.New(),
					ExternalID: uuid.NewString(),
				}

				task := orbital.Task{
					JobID:            job.ID,
					Type:             "type",
					Data:             []byte("data"),
					WorkingState:     []byte("working state"),
					ETag:             "etag",
					Status:           orbital.TaskStatusProcessing,
					Target:           expTarget,
					LastReconciledAt: 0,
				}

				ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
					task,
				})

				assert.NoError(t, err)
				assert.Len(t, ids, 1)
				task.ID = ids[0]

				mockClient := &mockInitiator{}
				actClientCall := 0
				mockClient.FnSendTaskRequest = func(_ context.Context, req orbital.TaskRequest) error {
					// when
					assert.Equal(t, ids[0], req.TaskID)
					assert.Equal(t, job.ExternalID, req.ExternalID)
					assertMapContains(t, req.MetaData, tt.expSignature)
					actClientCall++
					return nil
				}

				subj, err := orbital.NewManager(repo,
					mockTaskResolveFunc(),
					orbital.WithTargets(map[string]orbital.TargetManager{
						expTarget: {Client: mockClient, Signer: tt.reqSigner},
					}),
				)
				assert.NoError(t, err)

				wg := &sync.WaitGroup{}
				wg.Add(1)

				// when
				orbital.HandleTask(subj)(ctx, wg, *repo, job, task)

				// then
				assert.Equal(t, tt.expClientCall, actClientCall)
				actTask, ok, err := orbital.GetRepoTask(repo)(ctx, task.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, tt.expTaskStatus, actTask.Status)
			})
		}
	})
}

func TestManager_Verification(t *testing.T) {
	t.Run("VerifyTaskResponse", func(t *testing.T) {
		// given
		ctx := t.Context()
		db, store := createSQLStore(t)
		defer clearTables(t, db)
		repo := orbital.NewRepository(store)

		var actVerifyTaskResponseCalls atomic.Int32
		var actTaskResponse orbital.TaskResponse
		tts := []struct {
			name                       string
			respVerifier               orbital.TaskResponseVerifier
			mustCheckSignature         bool
			expVerifyTaskResponseCalls int32
			expTaskStatus              orbital.TaskStatus
		}{
			{
				name:                       "should not verify the signature if signature checking is disabled and no verifier is set",
				respVerifier:               nil,
				expVerifyTaskResponseCalls: 0,
				expTaskStatus:              orbital.TaskStatusDone,
			},
			{
				name: "should not verify the signature if signature checking is disabled, even if a verifier is set",
				respVerifier: &mockResponseVerifier{
					FnVerify: func(_ context.Context, response orbital.TaskResponse) error {
						actVerifyTaskResponseCalls.Add(1)
						actTaskResponse = response
						return nil
					},
				},
				expVerifyTaskResponseCalls: 0,
				expTaskStatus:              orbital.TaskStatusDone,
			},
			{
				name: "should verify the signature and update the task status if signature checking is enabled and a verifier is set",
				respVerifier: &mockResponseVerifier{
					FnVerify: func(_ context.Context, response orbital.TaskResponse) error {
						actVerifyTaskResponseCalls.Add(1)
						actTaskResponse = response
						return nil
					},
				},
				mustCheckSignature:         true,
				expVerifyTaskResponseCalls: 1,
				expTaskStatus:              orbital.TaskStatusDone,
			},
			{
				name:                       "should not update the task status if signature checking is enabled but no verifier is set",
				respVerifier:               nil,
				mustCheckSignature:         true,
				expVerifyTaskResponseCalls: 0,
				expTaskStatus:              orbital.TaskStatusProcessing,
			},
			{
				name: "should not update the task status if signature checking is enabled and the verifier returns an error",
				respVerifier: &mockResponseVerifier{
					FnVerify: func(_ context.Context, response orbital.TaskResponse) error {
						actVerifyTaskResponseCalls.Add(1)
						actTaskResponse = response
						return assert.AnError
					},
				},
				mustCheckSignature:         true,
				expVerifyTaskResponseCalls: 1,
				expTaskStatus:              orbital.TaskStatusProcessing,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				actVerifyTaskResponseCalls.Store(0)
				actTaskResponse = orbital.TaskResponse{}

				expTarget := "target-1"
				task := orbital.Task{
					JobID:            uuid.New(),
					Type:             "type",
					Data:             []byte("data"),
					WorkingState:     []byte("working state"),
					ETag:             "etag",
					Status:           orbital.TaskStatusProcessing,
					Target:           expTarget,
					LastReconciledAt: 0,
				}

				ids, err := orbital.CreateRepoTasks(repo)(ctx, []orbital.Task{
					task,
				})

				assert.NoError(t, err)
				assert.Len(t, ids, 1)
				task.ID = ids[0]

				mockClient := &mockInitiator{}

				taskResponseChan := make(chan orbital.TaskResponse, 1)
				expTaskResponse := orbital.TaskResponse{
					TaskID:     task.ID,
					ExternalID: uuid.NewString(),
					ETag:       task.ETag,
					Status:     string(orbital.TaskStatusDone),
					MetaData:   orbital.MetaData{"value": "signature", "type": "jwt"},
				}
				taskResponseChan <- expTaskResponse

				taskResponseReceiveChan := make(chan bool)
				mockClient.FnReceiveTaskResponse = func(_ context.Context) (orbital.TaskResponse, error) {
					taskResponseReceiveChan <- true
					return <-taskResponseChan, nil
				}

				initiator := orbital.TargetManager{
					Client:             mockClient,
					Verifier:           tt.respVerifier,
					MustCheckSignature: tt.mustCheckSignature,
				}

				subj, err := orbital.NewManager(repo,
					mockTaskResolveFunc(),
				)
				assert.NoError(t, err)

				// when
				go orbital.HandleResponses(subj)(ctx, initiator, expTarget)

				// making sure we wait for 2 calls
				<-taskResponseReceiveChan
				<-taskResponseReceiveChan

				// then
				assert.Equal(t, tt.expVerifyTaskResponseCalls, actVerifyTaskResponseCalls.Load())
				if actVerifyTaskResponseCalls.Load() > 1 {
					assert.Equal(t, expTaskResponse, actTaskResponse)
				}

				actTask, ok, err := orbital.GetRepoTask(repo)(ctx, task.ID)
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, tt.expTaskStatus, actTask.Status)
			})
		}
	})
}

func TestOperator_Signing(t *testing.T) {
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

		expStatus := string(orbital.TaskStatusProcessing)
		expReconcileAfter := 19 * time.Second
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
			ReconcileAfterSec: uint64(expReconcileAfter.Seconds()),
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
				name: "should call signTaskResponse and client sendTaskResponse if signer is not nil",
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
				name:                     "should call client sendTaskResponse even if the signer is nil",
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

				o, err := orbital.NewOperator(orbital.TargetOperator{Client: client, Verifier: mockVerifier, Signer: tt.respSigner})
				assert.NoError(t, err)
				assert.NotNil(t, o)

				ctx := t.Context()
				o.ListenAndRespond(ctx)

				h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
					assert.Equal(t, taskReq.TaskID, req.TaskID)

					resp.ContinueAndWaitFor(expReconcileAfter)
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

func TestOperator_Verification(t *testing.T) {
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
			expHandlerCalls           int32
			mustCheckSignature        bool
			expVerifyTaskRequestCalls int32
			expOperatorInitError      error
		}{
			{
				name:                      "if signature checking is disabled and no verifier is set, the handler should be called without verifying the signature",
				reqVerifier:               nil,
				expVerifyTaskRequestCalls: 0,
				expHandlerCalls:           1,
			},
			{
				name: "if signature checking is disabled but a verifier is set, the handler should be called without verifying the signature",
				reqVerifier: &mockRequestVerifier{
					FnVerify: func(_ context.Context, request orbital.TaskRequest) error {
						actVerifyTaskRequestCalls.Add(1)
						assert.Equal(t, taskReq, request)
						return nil
					},
				},
				expVerifyTaskRequestCalls: 0,
				expHandlerCalls:           1,
			},
			{
				name:               "if signature checking is enabled and a verifier is set, the signature should be verified before calling the handler",
				mustCheckSignature: true,
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
				name:                 "if signature checking is enabled and no verifier is set, operator initialization should fail",
				mustCheckSignature:   true,
				reqVerifier:          nil,
				expOperatorInitError: orbital.ErrOperatorInvalidConfig,
			},
			{
				name:               "if signature checking is enabled and the verifier returns an error, the handler should not be called",
				mustCheckSignature: true,
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
				o, err := orbital.NewOperator(orbital.TargetOperator{
					Client:             client,
					Signer:             respSigner,
					Verifier:           tt.reqVerifier,
					MustCheckSignature: tt.mustCheckSignature,
				})

				if tt.expOperatorInitError != nil {
					assert.ErrorIs(t, err, tt.expOperatorInitError)
					assert.Nil(t, o)
					return
				}
				assert.NoError(t, err)
				assert.NotNil(t, o)

				ctx := t.Context()
				o.ListenAndRespond(ctx)

				var actHandlerCalls atomic.Int32
				actHandlerCallChan := make(chan struct{})
				h := func(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
					assert.Equal(t, taskReq.TaskID, req.TaskID)
					actHandlerCalls.Add(1)
					actHandlerCallChan <- struct{}{}

					resp.ContinueAndWaitFor(10 * time.Second)
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
