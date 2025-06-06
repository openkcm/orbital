package amqp_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"

	amqppkg "github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
)

var errBadOption = errors.New("bad option")

func writeTemp(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func selfSignedCert(t *testing.T) (string, string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	assert.NoError(t, err, "key generation should not fail")

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "unit-test"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	assert.NoError(t, err, "certificate creation should not fail")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	dir := t.TempDir()
	return writeTemp(t, dir, "cert.pem", certPEM),
		writeTemp(t, dir, "key.pem", keyPEM),
		writeTemp(t, dir, "ca.pem", certPEM)
}

func TestClientOptionBasics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		opt  amqppkg.ClientOption
	}{
		{"anonymous auth", amqppkg.WithNoAuth()},
		{"basic auth", amqppkg.WithBasicAuth("user", "pw")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			opts := &amqp.ConnOptions{}
			assert.NoError(t, tc.opt(opts), "option should not return error")
			assert.NotNil(t, opts.SASLType, "SASLType should be set")
		})
	}
}

func TestWithExternalMTLS(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		cert, key, ca := selfSignedCert(t)
		opts := &amqp.ConnOptions{}

		err := amqppkg.WithExternalMTLS(cert, key, ca, "broker")(opts)
		assert.NoError(t, err, "WithExternalMTLS should succeed")

		assert.NotNil(t, opts.SASLType, "SASLType should be set")
		assert.NotNil(t, opts.TLSConfig, "TLSConfig should be set")
		assert.Equal(t, "broker", opts.TLSConfig.ServerName, "ServerName should match")
		assert.Equal(t, tls.VersionTLS12, int(opts.TLSConfig.MinVersion), "MinVersion should be TLS1.2")
	})

	t.Run("key-pair load error", func(t *testing.T) {
		t.Parallel()

		err := amqppkg.WithExternalMTLS("missing", "missing", "missing", "")(&amqp.ConnOptions{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, amqppkg.ErrTLSPairLoad, "error should be ErrTLSPairLoad")
	})

	t.Run("invalid CA", func(t *testing.T) {
		t.Parallel()

		cert, key, _ := selfSignedCert(t)
		badCA := writeTemp(t, t.TempDir(), "ca.pem", []byte("bogus"))
		err := amqppkg.WithExternalMTLS(cert, key, badCA, "")(&amqp.ConnOptions{})
		assert.Error(t, err)
		assert.ErrorIs(t, err, amqppkg.ErrInvalidCACert, "error should be ErrInvalidCACert")
	})
}

func TestWithPropertiesMerges(t *testing.T) {
	t.Parallel()

	opts := &amqp.ConnOptions{Properties: map[string]any{"keep": 1}}
	props := map[string]any{"vpn": "corp", "client": "orbital"}

	err := amqppkg.WithProperties(props)(opts)
	assert.NoError(t, err, "WithProperties should not return error")

	expProps := map[string]any{"keep": 1, "vpn": "corp", "client": "orbital"}
	for k, v := range expProps {
		assert.Equal(t, v, opts.Properties[k], "property %q should match", k)
	}
}

func TestNewClient_WrapsOptionError(t *testing.T) {
	t.Parallel()

	badOpt := func(*amqp.ConnOptions) error { return errBadOption }

	_, err := amqppkg.NewClient(t.Context(), codec.JSON{}, amqppkg.ConnectionInfo{}, badOpt)

	assert.Error(t, err)

	assert.ErrorIs(t, err, amqppkg.ErrApplyOption, "error should be ErrApplyOption")
	assert.ErrorIs(t, err, errBadOption, "wrapped error should match")
}

func TestNewClient_CodecError(t *testing.T) {
	t.Parallel()

	_, err := amqppkg.NewClient(t.Context(), nil, amqppkg.ConnectionInfo{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, amqppkg.ErrCodecNotProvided, "error should be ErrCodecNotProvided")
}
