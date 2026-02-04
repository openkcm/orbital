package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/openkcm/common-sdk/pkg/jwtsigning"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openkcm/orbital"
)

const (
	taskType   = "test-task"
	taskTarget = "test-target"
)

var (
	mgrMaxReconcileCount = uint(3)
	validSubject         = pkix.Name{
		CommonName: "CA", Organization: []string{"SE"}, Country: []string{"US"}, Locality: []string{"Canary"},
		OrganizationalUnit: []string{"Clients"},
	}
)

func TestTaskSigningAndVerification(t *testing.T) {
	// given
	ctx := t.Context()
	env, err := setupTestEnvironment(ctx, t)
	require.NoError(t, err)

	t.Run("task request and response signing and verification", func(t *testing.T) {
		// given
		jwksProvider1 := initJWKSProvider(t)
		t.Cleanup(func() {
			jwksProvider1.server.Close()
		})

		jwksSigner1 := jwksProvider1.initSigner(t, nil)

		jwksVerifier1 := jwksProvider1.initVerifier(t, "", nil, "")

		jwksProvider2 := initJWKSProvider(t)
		t.Cleanup(func() {
			jwksProvider2.server.Close()
		})

		jwksSigner2 := jwksProvider2.initSigner(t, nil)

		jwksVerifier2 := jwksProvider2.initVerifier(t, "", nil, "")

		tts := []struct {
			name                    string
			initiatorSigner         *jwtsigning.Signer
			initiatorVerifier       *jwtsigning.Verifier
			responderSigner         *jwtsigning.Signer
			responderVerifier       *jwtsigning.Verifier
			expJobStatus            orbital.JobStatus
			expTaskStatus           orbital.TaskStatus
			expInitiatorSignCalls   uint
			expInitiatorVerifyCalls uint
			expResponderSignCalls   uint
			expResponderVerifyCalls uint
		}{
			{
				name:                    "task request should sign and verify",
				initiatorSigner:         jwksSigner1,
				responderVerifier:       jwksVerifier1,
				expJobStatus:            orbital.JobStatusDone,
				expTaskStatus:           orbital.TaskStatusDone,
				expInitiatorSignCalls:   1,
				expResponderVerifyCalls: 1,
			},
			{
				name:                    "task request verify should fail if wrong token is sent",
				initiatorSigner:         jwksSigner1,
				responderVerifier:       jwksVerifier2,
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expInitiatorSignCalls:   mgrMaxReconcileCount,
				expResponderVerifyCalls: mgrMaxReconcileCount,
			},
			{
				name:                    "task response should sign and verify",
				initiatorVerifier:       jwksVerifier2,
				responderSigner:         jwksSigner2,
				expJobStatus:            orbital.JobStatusDone,
				expTaskStatus:           orbital.TaskStatusDone,
				expInitiatorVerifyCalls: 1,
				expResponderSignCalls:   1,
			},
			{
				name:                    "task response verify should fail if wrong token is sent",
				initiatorVerifier:       jwksVerifier1,
				responderSigner:         jwksSigner2,
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expInitiatorVerifyCalls: mgrMaxReconcileCount,
				expResponderSignCalls:   mgrMaxReconcileCount,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				initiatorHandler, err := orbital.NewInitiatorSignatureHandler(tt.initiatorSigner, tt.initiatorVerifier)
				assert.NoError(t, err)

				responderHandler, err := orbital.NewResponderSignatureHandler(tt.responderSigner, tt.responderVerifier)
				assert.NoError(t, err)

				mockInitiatorSigHandler := &mockInitiatorSigHandler{
					handler: initiatorHandler,
				}
				mockResponderSigHandler := &mockResponderSigHandler{
					handler: responderHandler,
				}

				// when
				actJob, actTasks := execSigningReconciliation(t, env, mockInitiatorSigHandler, mockResponderSigHandler)

				// then
				assert.Equal(t, tt.expJobStatus, actJob.Status)
				assert.Len(t, actTasks, 1)
				task := actTasks[0]
				assert.Equal(t, tt.expTaskStatus, task.Status)

				assert.Equal(t, tt.expInitiatorSignCalls, mockInitiatorSigHandler.actSignCalls)
				assert.Equal(t, tt.expInitiatorVerifyCalls, mockInitiatorSigHandler.actVerifyCalls)
				assert.Equal(t, tt.expResponderSignCalls, mockResponderSigHandler.actSignCalls)
				assert.Equal(t, tt.expResponderVerifyCalls, mockResponderSigHandler.actVerifyCalls)
				assert.Equal(t, mockInitiatorSigHandler.actToken, mockResponderSigHandler.actToken)
			})
		}
	})
	t.Run("task request verification", func(t *testing.T) {
		// given
		wrongRootCA, _, _, _ := generateCertsAndKeys(t)

		tts := []testCase{
			{
				name:                    "should fail if the issuer is wrong",
				issuer:                  "http://localhost:9999/wrong-issuer",
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expInitiatorSignCalls:   mgrMaxReconcileCount,
				expResponderVerifyCalls: mgrMaxReconcileCount,
				expResponderVerifyErr:   jwtsigning.ErrJWTParseFailed,
			},
			{
				name:                    "should fail if the root CA is wrong",
				rootCA:                  wrongRootCA,
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expInitiatorSignCalls:   mgrMaxReconcileCount,
				expResponderVerifyCalls: mgrMaxReconcileCount,
				expResponderVerifyErr:   jwtsigning.ErrKidNoPublicKeyFound,
			},
			{
				name:                    "should fail if the subject is wrong",
				subject:                 "wrong-subject-value",
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expInitiatorSignCalls:   mgrMaxReconcileCount,
				expResponderVerifyCalls: mgrMaxReconcileCount,
				expResponderVerifyErr:   jwtsigning.ErrKidNoPublicKeyFound,
			},
			{
				name:                    "should fail if the hasher is wrong",
				hasher:                  &fakeHasher{},
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expInitiatorSignCalls:   mgrMaxReconcileCount,
				expResponderVerifyCalls: mgrMaxReconcileCount,
				expResponderVerifyErr:   jwtsigning.ErrUnsupportedHashAlgorithm,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				jwksProvider := initJWKSProvider(t)
				t.Cleanup(func() {
					jwksProvider.server.Close()
				})

				jwksSigner := jwksProvider.initSigner(t, tt.hasher)

				jwksVerifier := jwksProvider.initVerifier(t, tt.issuer, tt.rootCA, tt.subject)

				initiatorHandler, err := orbital.NewInitiatorSignatureHandler(jwksSigner, nil)
				assert.NoError(t, err)

				responderHandler, err := orbital.NewResponderSignatureHandler(nil, jwksVerifier)
				assert.NoError(t, err)

				mockInitiatorSigHandler := &mockInitiatorSigHandler{
					handler: initiatorHandler,
				}
				mockResponderSigHandler := &mockResponderSigHandler{
					handler: responderHandler,
				}

				// when
				actJob, actTasks := execSigningReconciliation(t, env, mockInitiatorSigHandler, mockResponderSigHandler)

				// then
				assert.Equal(t, tt.expJobStatus, actJob.Status)
				assert.Len(t, actTasks, 1)
				task := actTasks[0]
				assert.Equal(t, tt.expTaskStatus, task.Status)

				assert.Equal(t, tt.expInitiatorSignCalls, mockInitiatorSigHandler.actSignCalls)
				assert.Equal(t, tt.expInitiatorVerifyCalls, mockInitiatorSigHandler.actVerifyCalls)
				assert.Equal(t, tt.expResponderSignCalls, mockResponderSigHandler.actSignCalls)
				assert.Equal(t, tt.expResponderVerifyCalls, mockResponderSigHandler.actVerifyCalls)
				assert.Equal(t, mockInitiatorSigHandler.actToken, mockResponderSigHandler.actToken)
				assert.ErrorIs(t, mockInitiatorSigHandler.actVerifyErr, tt.expInitiatorVerifyErr)
				assert.ErrorIs(t, mockResponderSigHandler.actVerifyErr, tt.expResponderVerifyErr)
			})
		}
	})
	t.Run("task response verification", func(t *testing.T) {
		// given
		wrongRootCA, _, _, _ := generateCertsAndKeys(t)

		tts := []testCase{
			{
				name:                    "should fail if the issuer is wrong",
				issuer:                  "http://localhost:9999/wrong-issuer",
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expResponderSignCalls:   mgrMaxReconcileCount,
				expInitiatorVerifyCalls: mgrMaxReconcileCount,
				expInitiatorVerifyErr:   jwtsigning.ErrJWTParseFailed,
			},
			{
				name:                    "should fail if the root CA is wrong",
				rootCA:                  wrongRootCA,
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expResponderSignCalls:   mgrMaxReconcileCount,
				expInitiatorVerifyCalls: mgrMaxReconcileCount,
				expInitiatorVerifyErr:   jwtsigning.ErrKidNoPublicKeyFound,
			},
			{
				name:                    "should fail if the subject is wrong",
				subject:                 "wrong-subject-value",
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expResponderSignCalls:   mgrMaxReconcileCount,
				expInitiatorVerifyCalls: mgrMaxReconcileCount,
				expInitiatorVerifyErr:   jwtsigning.ErrKidNoPublicKeyFound,
			},
			{
				name:                    "should fail if the hasher is wrong",
				hasher:                  &fakeHasher{},
				expJobStatus:            orbital.JobStatusFailed,
				expTaskStatus:           orbital.TaskStatusFailed,
				expResponderSignCalls:   mgrMaxReconcileCount,
				expInitiatorVerifyCalls: mgrMaxReconcileCount,
				expInitiatorVerifyErr:   jwtsigning.ErrUnsupportedHashAlgorithm,
			},
		}

		for _, tt := range tts {
			t.Run(tt.name, func(t *testing.T) {
				jwksProvider := initJWKSProvider(t)
				t.Cleanup(func() {
					jwksProvider.server.Close()
				})

				jwksSigner := jwksProvider.initSigner(t, tt.hasher)

				jwksVerifier := jwksProvider.initVerifier(t, tt.issuer, tt.rootCA, tt.subject)

				initiatorHandler, err := orbital.NewInitiatorSignatureHandler(nil, jwksVerifier)
				assert.NoError(t, err)

				responderHandler, err := orbital.NewResponderSignatureHandler(jwksSigner, nil)
				assert.NoError(t, err)

				mockInitiatorSigHandler := &mockInitiatorSigHandler{
					handler: initiatorHandler,
				}
				mockResponderSigHandler := &mockResponderSigHandler{
					handler: responderHandler,
				}

				// when
				actJob, actTasks := execSigningReconciliation(t, env, mockInitiatorSigHandler, mockResponderSigHandler)

				// then
				assert.Equal(t, tt.expJobStatus, actJob.Status)
				assert.Len(t, actTasks, 1)
				task := actTasks[0]
				assert.Equal(t, tt.expTaskStatus, task.Status)

				assert.Equal(t, tt.expInitiatorSignCalls, mockInitiatorSigHandler.actSignCalls)
				assert.Equal(t, tt.expInitiatorVerifyCalls, mockInitiatorSigHandler.actVerifyCalls)
				assert.Equal(t, tt.expResponderSignCalls, mockResponderSigHandler.actSignCalls)
				assert.Equal(t, tt.expResponderVerifyCalls, mockResponderSigHandler.actVerifyCalls)
				assert.Equal(t, mockInitiatorSigHandler.actToken, mockResponderSigHandler.actToken)
				assert.ErrorIs(t, mockInitiatorSigHandler.actVerifyErr, tt.expInitiatorVerifyErr)
				assert.ErrorIs(t, mockResponderSigHandler.actVerifyErr, tt.expResponderVerifyErr)
			})
		}
	})
}

type testCase struct {
	name                    string
	issuer                  string
	rootCA                  *x509.Certificate
	subject                 string
	hasher                  jwtsigning.Hasher
	expJobStatus            orbital.JobStatus
	expTaskStatus           orbital.TaskStatus
	expInitiatorSignCalls   uint
	expInitiatorVerifyCalls uint
	expResponderSignCalls   uint
	expResponderVerifyCalls uint
	expInitiatorVerifyErr   error
	expResponderVerifyErr   error
}

func execSigningReconciliation(t *testing.T, env *testEnvironment, initiatorHandler *mockInitiatorSigHandler, responderHandler *mockResponderSigHandler,
) (orbital.Job, []orbital.Task) {
	t.Helper()

	ctx := t.Context()

	store, db := createStore(ctx, t, env, fmt.Sprintf("orbital_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		db.Close()
	})

	tasksQueue := "tasks-minimal-" + uuid.NewString()
	responsesQueue := "responses-minimal-" + uuid.NewString()

	managerClient, err := createAMQPClient(ctx, env.rabbitMQ.url, tasksQueue, responsesQueue)
	require.NoError(t, err)
	defer closeClient(ctx, t, managerClient)

	operatorClient, err := createAMQPClient(ctx, env.rabbitMQ.url, responsesQueue, tasksQueue)
	require.NoError(t, err)
	defer closeClient(ctx, t, operatorClient)

	terminationDone := make(chan orbital.Job, 1)

	managerConfig := managerConfig{
		taskResolveFunc: func(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			t.Logf("TaskResolver called for job %s", job.ID)
			return orbital.CompleteTaskResolver().
				WithTaskInfo([]orbital.TaskInfo{
					{
						Data:   []byte("task-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				}), nil
		},
		jobConfirmFunc: func(_ context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			t.Logf("JobConfirmFunc called for job %s", job.ID)
			return orbital.JobConfirmResult{Done: true}, nil
		},
		targetManagers: map[string]orbital.TargetManager{
			taskTarget: {
				Client:             managerClient,
				Signer:             initiatorHandler,
				Verifier:           initiatorHandler,
				MustCheckSignature: true,
			},
		},
		jobDoneEventFunc: func(_ context.Context, job orbital.Job) error {
			t.Logf("JobDoneEventFunc called for job %s status: %s", job.ID, job.Status)
			terminationDone <- job
			return nil
		},
		jobCanceledEventFunc: func(_ context.Context, job orbital.Job) error {
			t.Logf("JobCanceledEventFunc called for job %s status: %s", job.ID, job.Status)
			terminationDone <- job
			return nil
		},
		jobFailedEventFunc: func(_ context.Context, job orbital.Job) error {
			t.Logf("JobFailedEventFunc called for job %s status: %s", job.ID, job.Status)
			terminationDone <- job
			return nil
		},
		maxReconcileCount:     uint64(mgrMaxReconcileCount),
		backoffMaxIntervalSec: 1,
	}

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := createAndStartManager(ctxCancel, t, store, managerConfig)
	require.NoError(t, err)

	operatorConfig := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType: func(_ context.Context, request orbital.HandlerRequest, resp *orbital.HandlerResponse) error {
				t.Logf("Handler called for task %s", request.TaskID)
				resp.Result = orbital.ResultDone
				workingState, err := resp.WorkingState()
				assert.NoError(t, err)
				assert.NotNil(t, workingState)
				workingState.Set("progress", "100%")
				return nil
			},
		},
	}

	targetOperator := orbital.TargetOperator{Client: operatorClient, Verifier: responderHandler, Signer: responderHandler, MustCheckSignature: true}
	err = createAndStartOperatorWithTarget(ctxCancel, t, targetOperator, operatorConfig)
	require.NoError(t, err)

	job, err := createTestJob(ctx, t, manager, "test-job", []byte("job-data"))
	require.NoError(t, err)

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for job %s", terminatedJob.ID)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalJob, found, err := manager.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, found)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	assert.NoError(t, err)

	return finalJob, tasks
}

type jwksProvider struct {
	privateKey *rsa.PrivateKey
	jwks       *jwtsigning.JWKS
	server     *httptest.Server
	root       *x509.Certificate
}

// CurrentSigningKey implements [jwtsigning.PrivateKeyProvider].
func (j *jwksProvider) CurrentSigningKey(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
	return j.privateKey, jwtsigning.KeyMetadata{
		Iss: j.server.URL,
		Kid: j.jwks.Keys[0].Kid,
	}, nil
}

type mockInitiatorSigHandler struct {
	handler        *orbital.InitiatorSignatureHandler
	actSignCalls   uint
	actVerifyCalls uint
	actToken       string
	actVerifyErr   error
}

// Sign implements orbital.TaskRequestSigner.
func (h *mockInitiatorSigHandler) Sign(ctx context.Context, response orbital.TaskRequest) (orbital.Signature, error) {
	sig, err := h.handler.Sign(ctx, response)
	token, ok := sig[orbital.MessageSignatureKey]
	if ok {
		h.actSignCalls++
		h.actToken = token
	}
	return sig, err
}

// Verify implements [orbital.TaskResponseVerifier].
func (h *mockInitiatorSigHandler) Verify(ctx context.Context, response orbital.TaskResponse) error {
	token, ok := response.MetaData[orbital.MessageSignatureKey]
	if ok {
		h.actVerifyCalls++
		h.actToken = token
	}

	h.actVerifyErr = h.handler.Verify(ctx, response)
	return h.actVerifyErr
}

type mockResponderSigHandler struct {
	handler        *orbital.ResponderSignatureHandler
	actSignCalls   uint
	actVerifyCalls uint
	actToken       string
	actVerifyErr   error
}

// Sign implements [orbital.TaskResponseSigner].
func (h *mockResponderSigHandler) Sign(ctx context.Context, response orbital.TaskResponse) (orbital.Signature, error) {
	sig, err := h.handler.Sign(ctx, response)
	token, ok := sig[orbital.MessageSignatureKey]
	if ok {
		h.actSignCalls++
		h.actToken = token
	}
	return sig, err
}

// Verify implements orbital.TaskRequestVerifier.
func (h *mockResponderSigHandler) Verify(ctx context.Context, request orbital.TaskRequest) error {
	token, ok := request.MetaData[orbital.MessageSignatureKey]
	if ok {
		h.actVerifyCalls++
		h.actToken = token
	}

	h.actVerifyErr = h.handler.Verify(ctx, request)
	return h.actVerifyErr
}

var (
	_ jwtsigning.PrivateKeyProvider = &jwksProvider{}
	_ orbital.TaskRequestSigner     = &mockInitiatorSigHandler{}
	_ orbital.TaskResponseVerifier  = &mockInitiatorSigHandler{}
	_ orbital.TaskResponseSigner    = &mockResponderSigHandler{}
	_ orbital.TaskRequestVerifier   = &mockResponderSigHandler{}
)

func initJWKSProvider(t *testing.T) *jwksProvider {
	t.Helper()

	// generate certs
	rootCa, intCert, leafCert, privateKey := generateCertsAndKeys(t)

	// generate jwks with jwks encoder and decoder
	jwks := generateJWKS(t, leafCert, intCert)

	result := &jwksProvider{
		privateKey: privateKey,
		jwks:       jwks,
		root:       rootCa,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		b, err := json.Marshal(jwks)
		assert.NoError(t, err)

		_, err = w.Write(b)
		assert.NoError(t, err)
	}))
	result.server = server

	return result
}

func (j *jwksProvider) initSigner(t *testing.T, hasher jwtsigning.Hasher) *jwtsigning.Signer {
	t.Helper()

	signer, err := jwtsigning.NewSigner(j, hasher)
	assert.NoError(t, err)

	return signer
}

func (j *jwksProvider) initVerifier(t *testing.T, issuer string, rootCA *x509.Certificate, subject string) *jwtsigning.Verifier {
	t.Helper()

	endpoint := j.server.URL
	if issuer != "" {
		endpoint = issuer
	}
	root := j.root
	if rootCA != nil {
		root = rootCA
	}
	subjectVal := validSubject.ToRDNSequence().String()
	if subject != "" {
		subjectVal = subject
	}

	validator, err := jwtsigning.NewValidator(root, subjectVal)
	assert.NoError(t, err)

	jwtCli, err := jwtsigning.NewClient(endpoint)
	assert.NoError(t, err)

	publicPrvd := jwtsigning.NewJWKSProvider()
	err = publicPrvd.AddClient(endpoint, jwtCli, validator)
	assert.NoError(t, err)

	verifier, err := jwtsigning.NewVerifier(publicPrvd, nil, map[string]struct{}{endpoint: {}})
	assert.NoError(t, err)

	return verifier
}

type fakeHasher struct{}

var _ jwtsigning.Hasher = &fakeHasher{}

// HashMessage implements [jwtsigning.Hasher].
func (f *fakeHasher) HashMessage(_ []byte) string {
	return "this-is-a-fake-hasher"
}

// ToString implements [jwtsigning.Hasher].
func (f *fakeHasher) ToString() string {
	return "fake-hasher-checksum"
}

func generateJWKS(t *testing.T, leafCert *x509.Certificate, intCert *x509.Certificate) *jwtsigning.JWKS {
	t.Helper()

	jwks, err := jwtsigning.NewJWKS(jwtsigning.Input{
		Kty:    jwtsigning.KeyTypeRSA,
		Alg:    "PS256",
		Use:    "sign",
		KeyOps: []string{"verify"},
		Kid:    uuid.NewString(),
		X509Certs: []x509.Certificate{
			*leafCert,
			*intCert,
		},
	})
	require.NoError(t, err)

	return jwks
}

func generateCertsAndKeys(t *testing.T) (*x509.Certificate, *x509.Certificate, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	validNotBefore := time.Now()
	validNotAfter := validNotBefore.Add(24 * 7 * time.Hour)
	certManager := &certManager{
		notBefore: validNotBefore,
		notAfter:  validNotAfter,
	}

	rootKey, rootDer := certManager.rootCertificate(t)
	rootCa, actErr := x509.ParseCertificate(rootDer)
	require.NoError(t, actErr)

	intKey, intCert := certManager.intermediateCertificate(t, rootDer, rootKey)
	privateKey, leafCert := certManager.leafCertWithParams(t, intCert, intKey, &validSubject)
	return rootCa, intCert, leafCert, privateKey
}

func (c *certManager) rootCertificate(t *testing.T) (*rsa.PrivateKey, []byte) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 3072)
	require.NoError(t, err)

	rootTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Root CA", Organization: []string{"Root Organization"}},
		NotBefore:             c.notBefore,
		NotAfter:              c.notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	rootDer, err := x509.CreateCertificate(
		rand.Reader,
		rootTmpl,
		rootTmpl,
		privateKey.Public(),
		privateKey)
	require.NoError(t, err)

	return privateKey, rootDer
}

func (c *certManager) intermediateCertificate(t *testing.T, rootDer []byte, rootKey *rsa.PrivateKey) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()

	rootCert, err := x509.ParseCertificate(rootDer)
	require.NoError(t, err)

	privateKey, err := rsa.GenerateKey(rand.Reader, 3072)
	require.NoError(t, err)

	intTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "Intermediate CA", Organization: []string{"Intermediate Organization"}},
		NotBefore:             c.notBefore,
		NotAfter:              c.notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	intDer, err := x509.CreateCertificate(rand.Reader,
		intTmpl,
		rootCert,
		privateKey.Public(),
		rootKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(intDer)
	require.NoError(t, err)

	return privateKey, cert
}

type certManager struct {
	notBefore time.Time
	notAfter  time.Time
}

func (c *certManager) leafCertWithParams(t *testing.T, intCert *x509.Certificate, intKey *rsa.PrivateKey, subject *pkix.Name) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 3072)
	require.NoError(t, err)

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		NotBefore:    c.notBefore,
		NotAfter:     c.notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	if subject != nil {
		leafTmpl.Subject = *subject
	}

	leafDer, err := x509.CreateCertificate(rand.Reader,
		leafTmpl,
		intCert,
		privateKey.Public(),
		intKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(leafDer)
	require.NoError(t, err)

	return privateKey, cert
}
