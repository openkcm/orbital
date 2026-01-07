package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openkcm/common-sdk/pkg/jwtsigning"
	"github.com/openkcm/orbital"
	"google.golang.org/protobuf/proto"

	orbitalpb "github.com/openkcm/orbital/proto/orbital/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	taskType   = "test-task"
	taskTarget = "test-target"
)

var errTokenNotFound = errors.New("could not get token header")

func TestSigning(t *testing.T) {
	// given

	ctx := t.Context()
	env, err := setupTestEnvironment(ctx, t)
	assert.NoError(t, err)

	store, db := createStore(ctx, t, env, "orbital_signing")
	t.Cleanup(func() {
		db.Close()
	})

	sigAndVer := initSignerAndVerifier(t)
	t.Cleanup(func() {
		sigAndVer.server.Close()
	})

	tasksQueue := fmt.Sprintf("tasks-minimal-%d", time.Now().UnixNano())
	responsesQueue := fmt.Sprintf("responses-minimal-%d", time.Now().UnixNano())

	managerClient, err := createAMQPClient(ctx, env.rabbitMQ.url, tasksQueue, responsesQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, managerClient)

	operatorClient, err := createAMQPClient(ctx, env.rabbitMQ.url, responsesQueue, tasksQueue)
	assert.NoError(t, err)
	defer closeClient(ctx, t, operatorClient)

	var jobDoneCalls, jobCanceledCalls, jobFailedCalls int32
	terminationDone := make(chan orbital.Job, 1)

	managerConfig := managerConfig{
		taskResolveFunc: func(_ context.Context, job orbital.Job, _ orbital.TaskResolverCursor) (orbital.TaskResolverResult, error) {
			t.Logf("TaskResolver called for job %s", job.ID)
			return orbital.TaskResolverResult{
				TaskInfos: []orbital.TaskInfo{
					{
						Data:   []byte("task-data"),
						Type:   taskType,
						Target: taskTarget,
					},
				},
				Done: true,
			}, nil
		},
		jobConfirmFunc: func(_ context.Context, job orbital.Job) (orbital.JobConfirmResult, error) {
			t.Logf("JobConfirmFunc called for job %s", job.ID)
			return orbital.JobConfirmResult{Done: true}, nil
		},
		managerTargets: map[string]orbital.ManagerTarget{
			taskTarget: {
				Client: managerClient,
				Signer: sigAndVer.signer,
			},
		},
		jobDoneEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobDoneCalls, 1)
			t.Logf("JobDoneEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobCanceledEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobCanceledCalls, 1)
			t.Logf("JobCanceledEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
		jobFailedEventFunc: func(_ context.Context, job orbital.Job) error {
			calls := atomic.AddInt32(&jobFailedCalls, 1)
			t.Logf("JobFailedEventFunc called for job %s (call #%d), status: %s", job.ID, calls, job.Status)
			terminationDone <- job
			return nil
		},
	}

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	manager, err := createAndStartManager(ctxCancel, t, store, managerConfig)
	assert.NoError(t, err)

	operatorDone := make(chan struct{})
	var operatorOnce sync.Once
	operatorConfig := operatorConfig{
		handlers: map[string]orbital.Handler{
			taskType: func(_ context.Context, req orbital.HandlerRequest) (orbital.HandlerResponse, error) {
				operatorOnce.Do(func() {
					close(operatorDone)
				})

				t.Logf("Handler called for task %s", req.TaskID)
				assert.Equal(t, taskType, req.Type)
				assert.Equal(t, []byte("task-data"), req.Data)

				time.Sleep(100 * time.Millisecond)

				return orbital.HandlerResponse{
					WorkingState: []byte("task completed"),
					Result:       orbital.ResultDone,
				}, nil
			},
		},
	}

	ctxCancel, cancel = context.WithCancel(ctx)
	defer cancel()
	err = createAndStartOperatorWithTarget(ctxCancel, t, orbital.OperatorTarget{Client: operatorClient, Verifier: sigAndVer.verifier}, operatorConfig)
	assert.NoError(t, err)

	job, err := createTestJob(ctx, t, manager, "test-job", []byte("job-data"))
	assert.NoError(t, err)

	select {
	case <-operatorDone:
		t.Log("Operator completed successfully")
	case <-time.After(30 * time.Second):
		t.Fatal("Operator was not called within timeout")
	}

	select {
	case terminatedJob := <-terminationDone:
		t.Logf("Termination event function called for job %s", terminatedJob.ID)
		assert.Equal(t, job.ID, terminatedJob.ID)
		assert.Equal(t, orbital.JobStatusDone, terminatedJob.Status)
	case <-time.After(15 * time.Second):
		t.Fatal("Termination event function was not called within timeout")
	}

	finalJob, found, err := manager.GetJob(ctx, job.ID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, orbital.JobStatusDone, finalJob.Status)
	assert.Empty(t, finalJob.ErrorMessage)

	tasks, err := manager.ListTasks(ctx, orbital.ListTasksQuery{
		JobID: job.ID,
		Limit: 10,
	})
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)

	task := tasks[0]
	assert.Equal(t, orbital.TaskStatusDone, task.Status)
	assert.Equal(t, taskType, task.Type)
	assert.Equal(t, taskTarget, task.Target)
	assert.Equal(t, []byte("task-data"), task.Data)
	assert.Equal(t, []byte("task completed"), task.WorkingState)

	assert.Zero(t, task.ReconcileCount, "task reconcile count should be reset to zero")
	assert.Equal(t, int64(1), task.TotalSentCount)
	assert.Equal(t, int64(1), task.TotalReceivedCount)
	assert.Positive(t, task.LastReconciledAt, "last_reconciled_at should be set")
	assert.Equal(t, int64(0), task.ReconcileAfterSec, "reconcile_after_sec should be 0 for completed task")

	assert.Equal(t, int32(1), atomic.LoadInt32(&jobDoneCalls), "job done event function should be called exactly once")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobCanceledCalls), "job canceled event function should not be called")
	assert.Equal(t, int32(0), atomic.LoadInt32(&jobFailedCalls), "job failed event function should not be called")
	// when

	// then
	assert.Equal(t, 1, sigAndVer.signer.calledTimes)
	assert.Equal(t, 1, sigAndVer.verifier.calledTimes)
	assert.Equal(t, sigAndVer.signer.actToken, sigAndVer.verifier.actToken)
}

type orbitalSigner struct {
	calledTimes int
	actToken    string
	signer      *jwtsigning.Signer
}

type orbitalVerifier struct {
	calledTimes int
	actToken    string
	verifier    *jwtsigning.Verifier
}

type privateKeyProvider struct {
	privateKey *rsa.PrivateKey
	jwks       jwtsigning.JWKS
	iss        string
}

type signerAndVerifier struct {
	server   *httptest.Server
	signer   *orbitalSigner
	verifier *orbitalVerifier
}

var (
	_ orbital.TaskRequestSigner     = &orbitalSigner{}
	_ orbital.TaskRequestVerifier   = &orbitalVerifier{}
	_ jwtsigning.PrivateKeyProvider = &privateKeyProvider{}
)

// Sign implements orbital.TaskRequestSigner.
func (s *orbitalSigner) Sign(ctx context.Context, response orbital.TaskRequest) (orbital.Signature, error) {
	s.calledTimes++
	pb := orbitalpb.TaskRequest{
		TaskId:       response.TaskID.String(),
		Type:         response.Type,
		ExternalId:   response.ExternalID,
		Data:         response.Data,
		WorkingState: response.WorkingState,
		Etag:         response.ETag,
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb)
	if err != nil {
		return orbital.Signature{}, err
	}

	token, err := s.signer.Sign(ctx, b)
	if err != nil {
		return orbital.Signature{}, err
	}
	s.actToken = token
	return orbital.Signature{"X-Message-Signature": token}, nil
}

// CurrentSigningKey implements jwtsigning.PrivateKeyProvider.
func (j *privateKeyProvider) CurrentSigningKey(_ context.Context) (*rsa.PrivateKey, jwtsigning.KeyMetadata, error) {
	return j.privateKey, jwtsigning.KeyMetadata{
		Iss: j.iss,
		Kid: j.jwks.Keys[0].Kid,
	}, nil
}

// Verify implements orbital.TaskRequestVerifier.
func (o *orbitalVerifier) Verify(ctx context.Context, request orbital.TaskRequest) error {
	o.calledTimes++
	pb := orbitalpb.TaskRequest{
		TaskId:       request.TaskID.String(),
		Type:         request.Type,
		ExternalId:   request.ExternalID,
		Data:         request.Data,
		WorkingState: request.WorkingState,
		Etag:         request.ETag,
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(&pb)
	if err != nil {
		return err
	}

	token, ok := request.MetaData["X-Message-Signature"]
	if !ok {
		return errTokenNotFound
	}
	o.actToken = token

	return o.verifier.Verify(ctx, token, b)
}

func initSignerAndVerifier(t *testing.T) signerAndVerifier {
	t.Helper()

	validSubject := pkix.Name{
		CommonName: "CA", Organization: []string{"SE"}, Country: []string{"US"}, Locality: []string{"Canary"},
		OrganizationalUnit: []string{"Clients"},
	}

	validNotBefore := time.Now()
	validNotAfter := validNotBefore.Add(24 * 7 * time.Hour)
	rootKey, rootDer := rootCertificate(t, validNotBefore, validNotAfter)
	rootCa, actErr := x509.ParseCertificate(rootDer)
	assert.NoError(t, actErr)

	intKey, intDer := intermediateCertificate(t, rootDer, validNotBefore, validNotAfter, rootKey)
	privateKey, leafDer := leafCertWithParams(t, intDer, validNotBefore, validNotAfter, intKey, validSubject)

	jwks := jwtsigning.JWKS{
		Keys: []jwtsigning.Key{
			{
				Kty:    jwtsigning.KeyTypeRSA,
				Alg:    "PSC253",
				Use:    "signing",
				KeyOps: []string{"signing"},
				Kid:    "kid-1",
				X5c: []string{
					base64.StdEncoding.EncodeToString(leafDer),
					base64.StdEncoding.EncodeToString(intDer),
				},
				N: "",
				E: "",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		b, err := json.Marshal(jwks)
		assert.NoError(t, err)

		_, err = w.Write(b)
		assert.NoError(t, err)
	}))

	provider := privateKeyProvider{
		privateKey: privateKey,
		jwks:       jwks,
		iss:        server.URL,
	}

	signer, err := jwtsigning.NewSigner(&provider, nil)
	assert.NoError(t, err)

	prv := jwtsigning.NewJWKSProvider()

	cli, err := jwtsigning.NewClient(server.URL)
	assert.NoError(t, err)

	validator, err := jwtsigning.NewValidator(rootCa, validSubject.ToRDNSequence().String())
	assert.NoError(t, err)

	err = prv.AddCli(server.URL, jwtsigning.JWKSClientStore{
		Client:    cli,
		Validator: validator,
	})
	assert.NoError(t, err)

	verifier, err := jwtsigning.NewVerifier(prv, nil, map[string]struct{}{server.URL: {}})
	assert.NoError(t, err)

	return signerAndVerifier{
		server: server,
		signer: &orbitalSigner{
			signer: signer,
		},
		verifier: &orbitalVerifier{
			verifier: verifier,
		},
	}
}

func rootCertificate(t *testing.T, notBefore time.Time, notAfter time.Time) (*rsa.PrivateKey, []byte) {
	t.Helper()

	rootKey, err := rsa.GenerateKey(rand.Reader, 3072)
	require.NoError(t, err)

	rootTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Root CA", Organization: []string{"Root Organization"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	rootDer, err := x509.CreateCertificate(
		rand.Reader,
		rootTmpl,
		rootTmpl,
		rootKey.Public(),
		rootKey)
	require.NoError(t, err)

	return rootKey, rootDer
}

func intermediateCertificate(t *testing.T, rootDer []byte, notBefore time.Time, notAfter time.Time, rootKey *rsa.PrivateKey) (*rsa.PrivateKey, []byte) {
	t.Helper()

	rootCert, err := x509.ParseCertificate(rootDer)
	require.NoError(t, err)

	intKey, err := rsa.GenerateKey(rand.Reader, 3072)
	require.NoError(t, err)

	intTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "Intermediate CA", Organization: []string{"Intermediate Organization"}},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	intDer, err := x509.CreateCertificate(rand.Reader,
		intTmpl,
		rootCert,
		intKey.Public(),
		rootKey)
	require.NoError(t, err)

	return intKey, intDer
}

func leafCertWithParams(t *testing.T, intDer []byte, notBefore time.Time, notAfter time.Time, intKey *rsa.PrivateKey, subject pkix.Name) (*rsa.PrivateKey, []byte) {
	t.Helper()

	intCert, err := x509.ParseCertificate(intDer)
	require.NoError(t, err)

	leafKey, err := rsa.GenerateKey(rand.Reader, 3072)
	require.NoError(t, err)

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      subject,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	leafDer, err := x509.CreateCertificate(rand.Reader,
		leafTmpl,
		intCert,
		leafKey.Public(),
		intKey)
	require.NoError(t, err)

	return leafKey, leafDer
}
