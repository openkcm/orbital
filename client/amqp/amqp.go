package amqp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/Azure/go-amqp"

	"github.com/openkcm/orbital"
)

var (
	// ErrApplyOption indicates that applying a ClientOption failed.
	ErrApplyOption = errors.New("amqp: failed to apply client option")
	// ErrTLSPairLoad indicates that loading the TLS certificate/key pair failed.
	ErrTLSPairLoad = errors.New("amqp: failed to load TLS key pair")
	// ErrCARead indicates that reading the CA certificate file failed.
	ErrCARead = errors.New("amqp: failed to read CA certificate file")
	// ErrInvalidCACert indicates that the CA certificate PEM could not be parsed.
	ErrInvalidCACert = errors.New("amqp: invalid CA certificate")
)

// AMQP is a client for sending and receiving messages using the AMQP protocol.
type AMQP struct {
	codec    orbital.Codec
	conn     *amqp.Conn
	sender   *amqp.Sender
	receiver *amqp.Receiver
}

// ConnectionInfo holds the connection details for the AMQP client.
type ConnectionInfo struct {
	URL    string
	Target string
	Source string
}

// ClientOption configures how the AMQP connection is established.
type ClientOption func(*amqp.ConnOptions) error

var (
	_ orbital.Initiator = &AMQP{}
	_ orbital.Responder = &AMQP{}
)

var ErrCodecNotProvided = errors.New("codec not provided")

// WithBasicAuth tells the client to use SASL PLAIN with user/password.
func WithBasicAuth(username, password string) ClientOption {
	return func(o *amqp.ConnOptions) error {
		o.SASLType = amqp.SASLTypePlain(username, password)
		return nil
	}
}

// WithExternalMTLS sets up mutual TLS + SASL EXTERNAL authentication.
func WithExternalMTLS(certFile, keyFile, caFile, serverName string) ClientOption {
	return func(o *amqp.ConnOptions) error {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrTLSPairLoad, err)
		}
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrCARead, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return ErrInvalidCACert
		}
		o.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			MinVersion:   tls.VersionTLS12,
			ServerName:   serverName,
		}
		o.SASLType = amqp.SASLTypeExternal("")
		return nil
	}
}

// WithNoAuth tells the client to use SASL ANONYMOUS (no credentials).
func WithNoAuth() ClientOption {
	return func(o *amqp.ConnOptions) error {
		o.SASLType = amqp.SASLTypeAnonymous()
		return nil
	}
}

// WithProperties lets you set custom connection properties (e.g., vpn-name).
func WithProperties(props map[string]any) ClientOption {
	return func(o *amqp.ConnOptions) error {
		maps.Copy(o.Properties, props)
		return nil
	}
}

// NewClient initializes and returns a new AMQP client instance
// configured with the provided codec for encoding and decoding messages,
// connection information (URL, target, and source),
// and optional client options.
func NewClient(ctx context.Context, codec orbital.Codec, connInfo ConnectionInfo, opts ...ClientOption) (*AMQP, error) {
	if codec == nil {
		return nil, ErrCodecNotProvided
	}

	connOpts := &amqp.ConnOptions{
		SASLType:   amqp.SASLTypeAnonymous(),
		Properties: map[string]any{},
	}

	for _, opt := range opts {
		if err := opt(connOpts); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrApplyOption, err)
		}
	}

	conn, err := amqp.Dial(ctx, connInfo.URL, connOpts)
	if err != nil {
		return nil, err
	}

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return nil, err
	}

	sender, err := session.NewSender(ctx, connInfo.Target, &amqp.SenderOptions{
		TargetDurability: amqp.DurabilityUnsettledState,
	})
	if err != nil {
		return nil, err
	}

	receiver, err := session.NewReceiver(ctx, connInfo.Source, &amqp.ReceiverOptions{
		SourceDurability: amqp.DurabilityUnsettledState,
	})
	if err != nil {
		return nil, err
	}

	return &AMQP{conn: conn, sender: sender, receiver: receiver, codec: codec}, nil
}

// SendTaskRequest sends an encoded TaskRequest message.
func (a *AMQP) SendTaskRequest(ctx context.Context, req orbital.TaskRequest) error {
	b, err := a.codec.EncodeTaskRequest(req)
	if err != nil {
		return err
	}
	return a.sender.Send(ctx, amqp.NewMessage(b), nil)
}

// ReceiveTaskRequest receives, decodes and acknowledges a TaskRequest message.
func (a *AMQP) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	msg, err := a.receiver.Receive(ctx, nil)
	if err != nil {
		return orbital.TaskRequest{}, err
	}

	req, err := a.codec.DecodeTaskRequest(msg.GetData())
	if err != nil {
		return orbital.TaskRequest{}, err
	}

	if err := a.receiver.AcceptMessage(ctx, msg); err != nil {
		return orbital.TaskRequest{}, err
	}

	return req, nil
}

// SendTaskResponse sends an encoded TaskResponse message.
func (a *AMQP) SendTaskResponse(ctx context.Context, resp orbital.TaskResponse) error {
	b, err := a.codec.EncodeTaskResponse(resp)
	if err != nil {
		return err
	}
	return a.sender.Send(ctx, amqp.NewMessage(b), nil)
}

// ReceiveTaskResponse receives, decodes and acknowledges a TaskResponse message.
func (a *AMQP) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	msg, err := a.receiver.Receive(ctx, nil)
	if err != nil {
		return orbital.TaskResponse{}, err
	}

	resp, err := a.codec.DecodeTaskResponse(msg.GetData())
	if err != nil {
		return orbital.TaskResponse{}, err
	}

	if err := a.receiver.AcceptMessage(ctx, msg); err != nil {
		return orbital.TaskResponse{}, err
	}

	return resp, nil
}
