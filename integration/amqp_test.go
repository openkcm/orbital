//go:build integration
// +build integration

package integration_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
)

func generateCertificates(t *testing.T) string {
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
	caDER, _ := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	writePEM(t, dir, "rootCA.pem", "CERTIFICATE", caDER, 0644)
	writePEM(t, dir, "rootCA.key", "RSA PRIVATE KEY",
		x509.MarshalPKCS1PrivateKey(caKey), 0600)
	caCert, _ := x509.ParseCertificate(caDER)

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
	srvDER, _ := x509.CreateCertificate(rand.Reader, srvTpl, caCert, &srvKey.PublicKey, caKey)
	writePEM(t, dir, "server.pem", "CERTIFICATE", srvDER, 0644)
	writePEM(t, dir, "server.key", "RSA PRIVATE KEY",
		x509.MarshalPKCS1PrivateKey(srvKey), 0600)

	cliKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	cliTpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "guest"},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, _ := x509.CreateCertificate(rand.Reader, cliTpl, caCert, &cliKey.PublicKey, caKey)
	writePEM(t, dir, "client.pem", "CERTIFICATE", cliDER, 0644)
	writePEM(t, dir, "client.key", "RSA PRIVATE KEY",
		x509.MarshalPKCS1PrivateKey(cliKey), 0600)

	confFile := filepath.Join(dir, "rabbitmq.conf")
	config := `
listeners.tcp.default = 5672
listeners.ssl.default = 5671

ssl_options.cacertfile = /certs/rootCA.pem
ssl_options.certfile   = /certs/server.pem
ssl_options.keyfile    = /certs/server.key
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = true

auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = AMQPLAIN
auth_mechanisms.3 = EXTERNAL

ssl_cert_login_from = common_name
loopback_users = none

management.tcp.port = 15672
`
	_ = os.WriteFile(confFile, []byte(config), 0644)

	return dir
}

func writePEM(t *testing.T, dir, name, typ string, der []byte, mode os.FileMode) {
	t.Helper()
	p := filepath.Join(dir, name)
	err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), mode)
	require.NoError(t, err)
}

type rabbitMQContainer struct {
	container testcontainers.Container
	amqpURL   string
	amqpsURL  string
}

func startRabbitMQ(ctx context.Context, t *testing.T, mtls bool, certDir string) (*rabbitMQContainer, func()) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4",
		ExposedPorts: []string{"5672/tcp", "5671/tcp", "15672/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp"),
	}

	if mtls {
		req.Files = []testcontainers.ContainerFile{
			{
				HostFilePath:      filepath.Join(certDir, "rootCA.pem"),
				ContainerFilePath: "/certs/rootCA.pem",
				FileMode:          0644,
			},
			{
				HostFilePath:      filepath.Join(certDir, "server.pem"),
				ContainerFilePath: "/certs/server.pem",
				FileMode:          0644,
			},
			{
				HostFilePath:      filepath.Join(certDir, "server.key"),
				ContainerFilePath: "/certs/server.key",
				FileMode:          0600,
			},
			{
				HostFilePath:      filepath.Join(certDir, "rabbitmq.conf"),
				ContainerFilePath: "/etc/rabbitmq/rabbitmq.conf",
				FileMode:          0644,
			},
		}

		req.Cmd = []string{
			"sh", "-c",
			"chown rabbitmq:rabbitmq /certs/server.key && chmod 600 /certs/server.key && " +
				"rabbitmq-plugins enable --offline rabbitmq_auth_mechanism_ssl && " +
				"rabbitmq-server",
		}

		req.WaitingFor = wait.ForAll(
			wait.ForListeningPort("5672/tcp"),
			wait.ForListeningPort("5671/tcp"),
		)
	}

	cont, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	require.NoError(t, err)

	host, _ := cont.Host(ctx)
	plain, _ := cont.MappedPort(ctx, "5672")
	res := &rabbitMQContainer{
		container: cont,
		amqpURL:   fmt.Sprintf("amqp://guest:guest@%s:%s/", host, plain.Port()),
	}
	if mtls {
		tls, _ := cont.MappedPort(ctx, "5671")
		res.amqpsURL = fmt.Sprintf("amqps://%s:%s/", host, tls.Port())
	}
	return res, func() { _ = cont.Terminate(ctx) }
}

func startSolace(ctx context.Context, t *testing.T) (string, func()) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "solace/solace-pubsub-standard",
		ExposedPorts: []string{"5672/tcp"},
		Env: map[string]string{
			"username_admin_globalaccesslevel": "admin",
			"username_admin_password":          "admin",
			"msgVpnName":                       "default",
		},
		HostConfigModifier: func(hc *container.HostConfig) { hc.ShmSize = 2 << 30 },
		WaitingFor: wait.ForListeningPort("5672/tcp").
			WithStartupTimeout(2 * time.Minute),
	}

	cont, _ := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	host, _ := cont.Host(ctx)
	port, _ := cont.MappedPort(ctx, "5672")

	return fmt.Sprintf("amqp://%s:%s/", host, port.Port()),
		func() { _ = cont.Terminate(ctx) }
}

func TestSendReceiveTaskRequests(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()
	rmq, cleanRMQ := startRabbitMQ(ctx, t, false, "")
	defer cleanRMQ()
	solURL, cleanSol := startSolace(ctx, t)
	defer cleanSol()

	for _, tc := range []struct{ name, url, q string }{
		{"RabbitMQ", rmq.amqpURL, "orbital-1"},
		{"Solace", solURL, "orbital-1"},
	} {

		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			cli, _ := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
				URL: tc.url, Target: tc.q, Source: tc.q,
			})
			const N = 5
			exp := make([]orbital.TaskRequest, N)
			for i := range N {
				exp[i] = orbital.TaskRequest{TaskID: uuid.New()}
				assert.NoError(t, cli.SendTaskRequest(ctx, exp[i]))
			}
			for i := range N {
				got, err := cli.ReceiveTaskRequest(ctx)
				assert.NoError(t, err)
				assert.Equal(t, exp[i].TaskID, got.TaskID)
			}
		})
	}
}

func TestSendReceiveTaskResponses(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()
	rmq, cleanRMQ := startRabbitMQ(ctx, t, false, "")
	defer cleanRMQ()
	solURL, cleanSol := startSolace(ctx, t)
	defer cleanSol()

	for _, tc := range []struct{ name, url, q string }{
		{"RabbitMQ", rmq.amqpURL, "orbital-2"},
		{"Solace", solURL, "orbital-2"},
	} {

		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			cli, _ := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
				URL: tc.url, Target: tc.q, Source: tc.q,
			})
			const N = 5
			exp := make([]orbital.TaskResponse, N)
			for i := range N {
				exp[i] = orbital.TaskResponse{TaskID: uuid.New()}
				assert.NoError(t, cli.SendTaskResponse(ctx, exp[i]))
			}
			for i := range N {
				got, err := cli.ReceiveTaskResponse(ctx)
				assert.NoError(t, err)
				assert.Equal(t, exp[i].TaskID, got.TaskID)
			}
		})
	}
}

func TestMTLSBrokers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := t.Context()
	certs := generateCertificates(t)
	rmq, clean := startRabbitMQ(ctx, t, true, certs)
	defer clean()

	time.Sleep(5 * time.Second)

	t.Run("RabbitMQ-mTLS", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		cli, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
			URL: rmq.amqpsURL, Target: "mtls-q", Source: "mtls-q",
		}, amqp.WithExternalMTLS(
			filepath.Join(certs, "client.pem"),
			filepath.Join(certs, "client.key"),
			filepath.Join(certs, "rootCA.pem"),
			"localhost",
		))
		require.NoError(t, err)

		exp := orbital.TaskRequest{TaskID: uuid.New()}
		assert.NoError(t, cli.SendTaskRequest(ctx, exp))
		got, err := cli.ReceiveTaskRequest(ctx)
		assert.NoError(t, err)
		assert.Equal(t, exp.TaskID, got.TaskID)
	})
}
