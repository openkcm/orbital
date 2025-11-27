package amqp_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	stdamqp "github.com/Azure/go-amqp"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
)

const (
	rootCACertFile     = "rootCA.pem"
	rootCAKeyFile      = "rootCA.key"
	serverCertFile     = "server.pem"
	serverKeyFile      = "server.key"
	clientCertFile     = "client.pem"
	clientKeyFile      = "client.key"
	rabbitMQConfigFile = "rabbitmq.conf"
)

var errBadOption = errors.New("bad option")

type tlsFiles struct {
	dir            string
	rootCACertPath string
	rootCAKeyPath  string
	serverCertPath string
	serverKeyPath  string
	clientCertPath string
	clientKeyPath  string
}

type mockCodec struct {
	FnDecodeTaskRequest  func([]byte) (orbital.TaskRequest, error)
	FnDecodeTaskResponse func([]byte) (orbital.TaskResponse, error)
	FnEncodeTaskRequest  func(orbital.TaskRequest) ([]byte, error)
	FnEncodeTaskResponse func(orbital.TaskResponse) ([]byte, error)
}

var _ orbital.Codec = &mockCodec{}

// DecodeTaskRequest implements orbital.Codec.
func (m *mockCodec) DecodeTaskRequest(bytes []byte) (orbital.TaskRequest, error) {
	return m.FnDecodeTaskRequest(bytes)
}

// DecodeTaskResponse implements orbital.Codec.
func (m *mockCodec) DecodeTaskResponse(bytes []byte) (orbital.TaskResponse, error) {
	return m.FnDecodeTaskResponse(bytes)
}

// EncodeTaskRequest implements orbital.Codec.
func (m *mockCodec) EncodeTaskRequest(request orbital.TaskRequest) ([]byte, error) {
	return m.FnEncodeTaskRequest(request)
}

// EncodeTaskResponse implements orbital.Codec.
func (m *mockCodec) EncodeTaskResponse(response orbital.TaskResponse) ([]byte, error) {
	return m.FnEncodeTaskResponse(response)
}

func createTLSFiles(t *testing.T) tlsFiles {
	t.Helper()
	dir := t.TempDir()

	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Root CA"},
		NotBefore:             time.Now().Add(-24 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	assert.NoError(t, err, "certificate creation should not fail")
	writePEM(t, dir, rootCACertFile, "CERTIFICATE", caDER, 0644)
	writePEM(t, dir, rootCAKeyFile, "RSA PRIVATE KEY",
		x509.MarshalPKCS1PrivateKey(caKey), 0600)
	caCert, err := x509.ParseCertificate(caDER)
	assert.NoError(t, err, "parsing CA certificate should not fail")

	srvKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	srvTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost", "rabbitmq"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, caCert, &srvKey.PublicKey, caKey)
	assert.NoError(t, err, "certificate creation should not fail")
	writePEM(t, dir, serverCertFile, "CERTIFICATE", srvDER, 0644)
	writePEM(t, dir, serverKeyFile, "RSA PRIVATE KEY",
		x509.MarshalPKCS1PrivateKey(srvKey), 0600)

	cliKey, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err, "client key generation should not fail")
	cliTpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "guest"},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTpl, caCert, &cliKey.PublicKey, caKey)
	assert.NoError(t, err, "client certificate creation should not fail")
	writePEM(t, dir, clientCertFile, "CERTIFICATE", cliDER, 0644)
	writePEM(t, dir, clientKeyFile, "RSA PRIVATE KEY",
		x509.MarshalPKCS1PrivateKey(cliKey), 0600)

	return tlsFiles{
		dir:            dir,
		rootCACertPath: filepath.Join(dir, rootCACertFile),
		rootCAKeyPath:  filepath.Join(dir, rootCAKeyFile),
		serverCertPath: filepath.Join(dir, serverCertFile),
		serverKeyPath:  filepath.Join(dir, serverKeyFile),
		clientCertPath: filepath.Join(dir, clientCertFile),
		clientKeyPath:  filepath.Join(dir, clientKeyFile),
	}
}

func writePEM(t *testing.T, dir, name, typ string, der []byte, mode os.FileMode) {
	t.Helper()
	p := filepath.Join(dir, name)
	err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), mode)
	assert.NoError(t, err)
}

func TestClientOptionBasics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  amqp.ClientOption
	}{
		{"anonymous auth", amqp.WithNoAuth()},
		{"basic auth", amqp.WithBasicAuth("user", "pw")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := &stdamqp.ConnOptions{}
			assert.NoError(t, tc.opt(opts), "option should not return error")
			assert.NotNil(t, opts.SASLType, "SASLType should be set")
		})
	}
}

func TestWithExternalMTLS(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		files := createTLSFiles(t)
		opts := &stdamqp.ConnOptions{}

		err := amqp.WithExternalMTLS(
			files.clientCertPath,
			files.clientKeyPath,
			files.rootCACertPath,
			"broker",
		)(opts)
		assert.NoError(t, err, "WithExternalMTLS should succeed")

		assert.NotNil(t, opts.SASLType, "SASLType should be set")
		assert.NotNil(t, opts.TLSConfig, "TLSConfig should be set")
		assert.Equal(t, "broker", opts.TLSConfig.ServerName, "ServerName should match")
		assert.Equal(t, tls.VersionTLS12, int(opts.TLSConfig.MinVersion), "MinVersion should be TLS1.2")
	})

	t.Run("key-pair load error", func(t *testing.T) {
		t.Parallel()

		err := amqp.WithExternalMTLS("missing", "missing", "", "")(&stdamqp.ConnOptions{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, amqp.ErrTLSPairLoad, "error should be ErrTLSPairLoad")
	})

	t.Run("CA read error", func(t *testing.T) {
		t.Parallel()
		files := createTLSFiles(t)

		err := amqp.WithExternalMTLS(
			files.clientCertPath,
			files.clientKeyPath,
			"missing",
			"",
		)(&stdamqp.ConnOptions{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, amqp.ErrCARead, "error should be ErrCARead")
	})

	t.Run("invalid CA", func(t *testing.T) {
		t.Parallel()
		files := createTLSFiles(t)

		badCA := "badCA.pem"
		writePEM(t, files.dir, badCA, "CERTIFICATE", []byte("bogus"), 0644)

		err := amqp.WithExternalMTLS(
			files.clientCertPath,
			files.clientKeyPath,
			filepath.Join(files.dir, badCA),
			"",
		)(&stdamqp.ConnOptions{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, amqp.ErrInvalidCACert, "error should be ErrInvalidCACert")
	})
}

func TestWithPropertiesMerges(t *testing.T) {
	t.Parallel()

	opts := &stdamqp.ConnOptions{Properties: map[string]any{"keep": 1}}
	props := map[string]any{"vpn": "corp", "client": "orbital"}

	err := amqp.WithProperties(props)(opts)
	assert.NoError(t, err, "WithProperties should not return error")

	expProps := map[string]any{"keep": 1, "vpn": "corp", "client": "orbital"}
	for k, v := range expProps {
		assert.Equal(t, v, opts.Properties[k], "property %q should match", k)
	}
}

func TestNewClient_WrapsOptionError(t *testing.T) {
	t.Parallel()

	badOpt := func(*stdamqp.ConnOptions) error { return errBadOption }

	cli, err := amqp.NewClient(t.Context(), codec.JSON{}, amqp.ConnectionInfo{}, badOpt)
	assert.Error(t, err)
	assert.Nil(t, cli)

	assert.ErrorIs(t, err, amqp.ErrApplyOption, "error should be ErrApplyOption")
	assert.ErrorIs(t, err, errBadOption, "wrapped error should match")
}

func TestNewClient_CodecError(t *testing.T) {
	t.Parallel()

	cli, err := amqp.NewClient(t.Context(), nil, amqp.ConnectionInfo{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, amqp.ErrCodecNotProvided, "error should be ErrCodecNotProvided")
	assert.Nil(t, cli)
}

func TestWithMessageBroker(t *testing.T) {
	t.Parallel()

	t.Run("RabbitMQ-Plain", func(t *testing.T) {
		t.Parallel()
		var protocol, port = "amqp", "5672"

		ctx := t.Context()
		container, err := startRabbitMQ(ctx)
		assert.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		tt := []struct {
			name     string
			queue    string
			testFunc func(t *testing.T, url, queue string)
		}{
			{
				name:     "send and receive task requests",
				testFunc: testSendReceiveTaskRequests,
				queue:    "rabbitmq-task-requests",
			},
			{
				name:     "send and receive task responses",
				testFunc: testSendReceiveTaskResponses,
				queue:    "rabbitmq-task-responses",
			},
		}

		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				tc.testFunc(t, url, tc.queue)
			})
		}
	})

	t.Run("RabbitMQ-mTLS", func(t *testing.T) {
		t.Parallel()
		var protocol, port = "amqps", "5671"

		ctx := t.Context()
		files := createTLSFiles(t)
		container, err := startRabbitMQ(ctx, withMTLSForRabbitMQ(files))
		assert.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		cli, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
			URL: url, Target: "mtls-q", Source: "mtls-q",
		}, amqp.WithExternalMTLS(
			files.clientCertPath,
			files.clientKeyPath,
			files.rootCACertPath,
			"localhost",
		))
		assert.NoError(t, err)
		defer closeClient(ctx, t, cli)

		exp := orbital.TaskRequest{TaskID: uuid.New()}
		assert.NoError(t, cli.SendTaskRequest(ctx, exp))
		got, err := cli.ReceiveTaskRequest(ctx)
		assert.NoError(t, err)
		assert.Equal(t, exp.TaskID, got.TaskID)
	})

	t.Run("Reconnection", func(t *testing.T) {
		t.Parallel()
		var protocol, port = "amqp", "5672"

		ctx := t.Context()
		// Start the container with a fixed port binding to be able to restart the container on the same port.
		container, err := startRabbitMQ(ctx, withPortBinding(port))
		assert.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		client, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
			URL: url, Target: "reconnect-q", Source: "reconnect-q",
		})
		assert.NoError(t, err)
		defer closeClient(ctx, t, client)

		err = container.Stop(ctx, nil)
		assert.NoError(t, err)

		err = client.SendTaskRequest(ctx, orbital.TaskRequest{TaskID: uuid.New()})
		assert.Error(t, err)
		assert.ErrorIs(t, err, amqp.ErrReconnectFailed)

		err = container.Start(ctx)
		assert.NoError(t, err)

		ctxCancel, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()

		for {
			select {
			case <-ctxCancel.Done():
				assert.Fail(t, "timed out waiting for reconnection")
				return
			default:
				err = client.SendTaskRequest(ctx, orbital.TaskRequest{TaskID: uuid.New()})
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return
			}
		}
	})

	t.Run("Close", func(t *testing.T) {
		t.Parallel()
		var protocol, port = "amqp", "5672"

		ctx := t.Context()
		container, err := startRabbitMQ(ctx)
		assert.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		tests := []struct {
			name         string
			blockReceive func(context.Context, *amqp.Client) error
		}{
			{
				name: "should unlock while receiving task requests",
				blockReceive: func(ctx context.Context, client *amqp.Client) error {
					_, err := client.ReceiveTaskRequest(ctx)
					return err
				},
			},
			{
				name: "should unlock while receiving task responses",
				blockReceive: func(ctx context.Context, client *amqp.Client) error {
					_, err := client.ReceiveTaskResponse(ctx)
					return err
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
					URL: url, Target: "test", Source: "test",
				})
				assert.NoError(t, err)

				go func() {
					err := tt.blockReceive(ctx, client)
					assert.ErrorIs(t, err, context.Canceled)
				}()

				// let the client block on receive
				time.Sleep(100 * time.Millisecond)

				err = client.Close(ctx)
				assert.NoError(t, err)
			})
		}
	})

	t.Run("Solace", func(t *testing.T) {
		t.Parallel()
		var protocol, port = "amqp", "5672"

		ctx := t.Context()
		container, err := startSolace(ctx)
		assert.NoError(t, err)
		ior, _ := container.Logs(ctx)
		if b, err := io.ReadAll(ior); err == nil {
			log.Println(string(b))
		}
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		// Even tought the container listens on port 5672, it may take more time
		// for Solace to be ready to accept connections.
		err = waitForClientReady(ctx, url)
		assert.NoError(t, err, "waiting for client to be ready should not fail")

		tt := []struct {
			name     string
			queue    string
			testFunc func(t *testing.T, url, queue string)
		}{
			{
				name:     "send and receive task requests",
				testFunc: testSendReceiveTaskRequests,
				queue:    "solace-task-requests",
			},
			{
				name:     "send and receive task responses",
				testFunc: testSendReceiveTaskResponses,
				queue:    "solace-task-responses",
			},
		}

		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				tc.testFunc(t, url, tc.queue)
			})
		}
	})
}

func TestReceiveTaskRequest(t *testing.T) {
	t.Run("should accept/ack the queue message even if there is an error while decoding the task request", func(t *testing.T) {
		// given
		var protocol, port = "amqp", "5672"

		ctx := t.Context()
		container, err := startRabbitMQ(ctx)
		assert.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		queue := uuid.New().String()

		mCodec := &mockCodec{}
		mCodec.FnEncodeTaskRequest = func(req orbital.TaskRequest) ([]byte, error) {
			// here we send the taskID in bytes to keep the track of the message.
			return []byte(req.TaskID.String()), nil
		}

		mCodec.FnDecodeTaskRequest = func(bytes []byte) (orbital.TaskRequest, error) {
			// here in the error we return a TaskID as an error message.
			return orbital.TaskRequest{}, errors.New(string(bytes)) //nolint:err113
		}

		cli, err := amqp.NewClient(ctx, mCodec, amqp.ConnectionInfo{
			URL: url, Target: queue, Source: queue,
		})

		assert.NoError(t, err)
		defer closeClient(ctx, t, cli)

		const noMsg = 5
		expErrMsg := make([]string, noMsg)
		// here we send noMsg messages with TaskID as message body.
		for i := range noMsg {
			req := orbital.TaskRequest{TaskID: uuid.New()}
			expErrMsg[i] = req.TaskID.String()
			assert.NoError(t, cli.SendTaskRequest(ctx, req))
		}

		// when
		// here we check if all the message are received with an error message and acknowledged.
		// If the message is not acknowledged, the client will hang in ReceiveTaskRequest.
		for i := range noMsg {
			got, err := cli.ReceiveTaskRequest(ctx)
			// then
			assert.Equal(t, orbital.TaskRequest{}, got)
			assert.Error(t, err)
			assert.ErrorContains(t, err, expErrMsg[i])
		}
	})
}

func TestReceiveTaskResponse(t *testing.T) {
	t.Run("should accept/ack the queue message even if there is an error while decoding the task response", func(t *testing.T) {
		// given
		var protocol, port = "amqp", "5672"

		ctx := t.Context()
		container, err := startRabbitMQ(ctx)
		assert.NoError(t, err)
		defer func() {
			err := container.Terminate(ctx)
			assert.NoError(t, err)
		}()

		url, err := getURL(ctx, container, protocol, port)
		assert.NoError(t, err)

		queue := uuid.New().String()

		mCodec := &mockCodec{}
		mCodec.FnEncodeTaskResponse = func(req orbital.TaskResponse) ([]byte, error) {
			// here we send the taskID in bytes to keep the track of the message.
			return []byte(req.TaskID.String()), nil
		}

		mCodec.FnDecodeTaskResponse = func(bytes []byte) (orbital.TaskResponse, error) {
			// here in the error we return a TaskID as an error message.
			return orbital.TaskResponse{}, errors.New(string(bytes)) //nolint:err113
		}

		cli, err := amqp.NewClient(ctx, mCodec, amqp.ConnectionInfo{
			URL: url, Target: queue, Source: queue,
		})

		assert.NoError(t, err)
		defer closeClient(ctx, t, cli)

		const noMsg = 5
		expErrMsg := make([]string, noMsg)
		// here we send noMsg messages with TaskID as message body.
		for i := range noMsg {
			req := orbital.TaskResponse{TaskID: uuid.New()}
			expErrMsg[i] = req.TaskID.String()
			assert.NoError(t, cli.SendTaskResponse(ctx, req))
		}

		// when
		// here we check if all the message are received with an error message and acknowledged.
		// If the message is not acknowledged, the client will hang in ReceiveTaskResponse.
		for i := range noMsg {
			got, err := cli.ReceiveTaskResponse(ctx)
			// then
			assert.Equal(t, orbital.TaskResponse{}, got)
			assert.Error(t, err)
			assert.ErrorContains(t, err, expErrMsg[i])
		}
	})
}

func startRabbitMQ(ctx context.Context, opts ...containerOpts) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4",
		ExposedPorts: []string{"5672/tcp", "5671/tcp", "15672/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5672/tcp"),
			wait.ForLog("Server startup complete"),
		),
	}

	for _, opt := range opts {
		if err := opt(&req); err != nil {
			return nil, err
		}
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

type containerOpts func(req *testcontainers.ContainerRequest) error

func withPortBinding(port string) containerOpts {
	return func(req *testcontainers.ContainerRequest) error {
		existingModifier := req.HostConfigModifier
		req.HostConfigModifier = func(hc *container.HostConfig) {
			if existingModifier != nil {
				existingModifier(hc)
			}
			hc.PortBindings = nat.PortMap{
				"5672/tcp": []nat.PortBinding{{HostPort: port}},
			}
		}
		return nil
	}
}

func withMTLSForRabbitMQ(files tlsFiles) containerOpts {
	return func(req *testcontainers.ContainerRequest) error {
		confFile := filepath.Join(files.dir, rabbitMQConfigFile)
		confContent := rabbitMQConfig(rootCACertFile, serverCertFile, serverKeyFile)
		err := os.WriteFile(confFile, []byte(confContent), 0600)
		if err != nil {
			return err
		}

		containerCertDir := "/certs"
		req.Files = []testcontainers.ContainerFile{
			{
				HostFilePath:      files.rootCACertPath,
				ContainerFilePath: filepath.Join(containerCertDir, rootCACertFile),
				FileMode:          0644,
			},
			{
				HostFilePath:      files.serverCertPath,
				ContainerFilePath: filepath.Join(containerCertDir, serverCertFile),
				FileMode:          0644,
			},
			{
				HostFilePath:      files.serverKeyPath,
				ContainerFilePath: filepath.Join(containerCertDir, serverKeyFile),
				FileMode:          0600,
			},
			{
				HostFilePath:      confFile,
				ContainerFilePath: "/etc/rabbitmq/rabbitmq.conf",
				FileMode:          0644,
			},
		}

		cmd := fmt.Sprintf(
			"chown rabbitmq:rabbitmq %s && chmod 600 %s && "+
				"rabbitmq-plugins enable --offline rabbitmq_auth_mechanism_ssl && "+
				"rabbitmq-server",
			filepath.Join(containerCertDir, serverKeyFile),
			filepath.Join(containerCertDir, serverKeyFile),
		)
		req.Cmd = []string{
			"sh", "-c", cmd,
		}

		req.WaitingFor = wait.ForAll(
			wait.ForListeningPort("5672/tcp"),
			wait.ForListeningPort("5671/tcp"),
			wait.ForLog("Server startup complete"),
		)

		return nil
	}
}

func rabbitMQConfig(caCert, serverCert, serverKey string) string {
	return fmt.Sprintf(`
listeners.tcp.default = 5672
listeners.ssl.default = 5671

ssl_options.cacertfile = /certs/%s
ssl_options.certfile   = /certs/%s
ssl_options.keyfile    = /certs/%s
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = true

auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = AMQPLAIN
auth_mechanisms.3 = EXTERNAL

ssl_cert_login_from = common_name
loopback_users = none

management.tcp.port = 15672
`, caCert, serverCert, serverKey)
}

func startSolace(ctx context.Context, opts ...containerOpts) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "solace/solace-pubsub-standard",
		ExposedPorts: []string{"5672/tcp"},
		Env: map[string]string{
			"username_admin_globalaccesslevel": "admin",
			"username_admin_password":          "admin",
			"msgVpnName":                       "default",
		},
		HostConfigModifier: func(hc *container.HostConfig) { hc.ShmSize = 2 << 40 },
		WaitingFor:         wait.ForListeningPort("5672/tcp").WithStartupTimeout(2 * time.Minute),
	}
	for _, opt := range opts {
		if err := opt(&req); err != nil {
			return nil, err
		}
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func getURL(ctx context.Context, container testcontainers.Container, protocol, port string) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	p, err := container.MappedPort(ctx, nat.Port(port))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s://%s/", protocol, net.JoinHostPort(host, p.Port())), nil
}

func waitForClientReady(ctx context.Context, url string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case <-timeoutCtx.Done():
			return fmt.Errorf("waiting for client to be ready: %w", lastErr)
		case <-ticker.C:
			cli, err := amqp.NewClient(timeoutCtx, codec.JSON{}, amqp.ConnectionInfo{
				URL:    url,
				Target: "ready-check",
				Source: "ready-check",
			})
			if err == nil {
				// Successfully connected, client is ready.
				return cli.Close(timeoutCtx)
			}
			lastErr = err
		}
	}
}

func testSendReceiveTaskRequests(t *testing.T, url, queue string) {
	t.Helper()

	ctx := t.Context()

	cli, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
		URL: url, Target: queue, Source: queue,
	})
	assert.NoError(t, err)
	defer closeClient(ctx, t, cli)

	const n = 5
	exp := make([]orbital.TaskRequest, n)
	for i := range n {
		exp[i] = orbital.TaskRequest{TaskID: uuid.New()}
		assert.NoError(t, cli.SendTaskRequest(ctx, exp[i]))
	}
	for i := range n {
		got, err := cli.ReceiveTaskRequest(ctx)
		assert.NoError(t, err)
		assert.Equal(t, exp[i].TaskID, got.TaskID)
	}
}

func testSendReceiveTaskResponses(t *testing.T, url, queue string) {
	t.Helper()

	ctx := t.Context()

	cli, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
		URL: url, Target: queue, Source: queue,
	})
	assert.NoError(t, err)
	defer closeClient(ctx, t, cli)

	const n = 5
	exp := make([]orbital.TaskResponse, n)
	for i := range n {
		exp[i] = orbital.TaskResponse{TaskID: uuid.New()}
		assert.NoError(t, cli.SendTaskResponse(ctx, exp[i]))
	}
	for i := range n {
		got, err := cli.ReceiveTaskResponse(ctx)
		assert.NoError(t, err)
		assert.Equal(t, exp[i].TaskID, got.TaskID)
	}
}

func closeClient(ctx context.Context, t *testing.T, client *amqp.Client) {
	t.Helper()

	err := client.Close(ctx)
	assert.NoError(t, err)
}
