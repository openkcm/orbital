package amqp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/Azure/go-amqp"

	slogctx "github.com/veqryn/slog-context"

	"github.com/openkcm/orbital"
)

var (
	// ErrCodecNotProvided indicates that no codec was provided to the client.
	ErrCodecNotProvided = errors.New("codec not provided")
	// ErrApplyOption indicates that applying a ClientOption failed.
	ErrApplyOption = errors.New("amqp: failed to apply client option")
	// ErrTLSPairLoad indicates that loading the TLS certificate/key pair failed.
	ErrTLSPairLoad = errors.New("amqp: failed to load TLS key pair")
	// ErrCARead indicates that reading the CA certificate file failed.
	ErrCARead = errors.New("amqp: failed to read CA certificate file")
	// ErrInvalidCACert indicates that the CA certificate PEM could not be parsed.
	ErrInvalidCACert = errors.New("amqp: invalid CA certificate")
	// ErrConnectInProgress indicates that a connection attempt is already in progress.
	ErrConnectInProgress = errors.New("amqp: connection already in progress")
	// ErrReconnectFailed indicates that the reconnection attempt has failed.
	ErrReconnectFailed = errors.New("amqp: reconnection failed")
)

// dialTimeout is the maximum time to wait for the AMQP connection to be established.
const dialTimeout = 5 * time.Second

type (
	// Client sends and receives messages using the AMQP protocol.
	Client struct {
		// codec is used to encode and decode messages.
		codec orbital.Codec
		// connInfo holds the connection details in case for reconnection attempts.
		connInfo ConnectionInfo
		// connOpts holds the connection options in case for reconnection attempts.
		connOpts *amqp.ConnOptions
		// amqp holds the underlying AMQP connection, sender, and receiver.
		amqp *conn
		// close is used to signal closure of the client.
		close chan struct{}
		// closeOnce ensures closure is only performed once to prevent panics.
		closeOnce sync.Once
	}

	// ClientOption configures how the AMQP connection is established.
	ClientOption func(*amqp.ConnOptions) error

	// ConnectionInfo holds the connection details for the AMQP client.
	ConnectionInfo struct {
		URL    string
		Target string
		Source string
	}

	conn struct {
		mu       sync.RWMutex
		conn     *amqp.Conn
		sender   *amqp.Sender
		receiver *amqp.Receiver
	}
)

var (
	_ orbital.Initiator = &Client{}
	_ orbital.Responder = &Client{}
)

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
func NewClient(ctx context.Context, codec orbital.Codec, connInfo ConnectionInfo, opts ...ClientOption) (*Client, error) {
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

	client := &Client{
		codec:    codec,
		connOpts: connOpts,
		connInfo: connInfo,
		amqp:     &conn{},
		close:    make(chan struct{}),
	}
	err := client.connect(ctx, connInfo, connOpts)
	if err != nil {
		return nil, err
	}

	return client, nil
}

// SendTaskRequest sends an encoded TaskRequest message.
func (c *Client) SendTaskRequest(ctx context.Context, req orbital.TaskRequest) error {
	b, err := c.codec.EncodeTaskRequest(req)
	if err != nil {
		return err
	}

	return c.retryOnConnError(ctx, func() error {
		return c.amqp.sender.Send(ctx, amqp.NewMessage(b), nil)
	})
}

// ReceiveTaskRequest receives, decodes and acknowledges a TaskRequest message.
func (c *Client) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	var req orbital.TaskRequest
	var errDec error

	err := c.retryOnConnError(ctx, func() error {
		msg, err := c.receive(ctx)
		if err != nil {
			return err
		}

		data := msg.GetData()
		req, errDec = c.codec.DecodeTaskRequest(data)
		if errDec != nil {
			slogctx.Error(ctx, "failed to decode task request", "data", string(data), "error", errDec)
		}

		if errAck := c.amqp.receiver.AcceptMessage(ctx, msg); errAck != nil {
			return errAck
		}

		return nil
	})
	if err != nil {
		return orbital.TaskRequest{}, err
	}
	if errDec != nil {
		return orbital.TaskRequest{}, errDec
	}

	return req, nil
}

// SendTaskResponse sends an encoded TaskResponse message.
func (c *Client) SendTaskResponse(ctx context.Context, resp orbital.TaskResponse) error {
	b, err := c.codec.EncodeTaskResponse(resp)
	if err != nil {
		return err
	}

	return c.retryOnConnError(ctx, func() error {
		return c.amqp.sender.Send(ctx, amqp.NewMessage(b), nil)
	})
}

// ReceiveTaskResponse receives, decodes and acknowledges a TaskResponse message.
func (c *Client) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	var resp orbital.TaskResponse
	var errDec error

	err := c.retryOnConnError(ctx, func() error {
		msg, err := c.receive(ctx)
		if err != nil {
			return err
		}

		data := msg.GetData()
		resp, errDec = c.codec.DecodeTaskResponse(data)
		if errDec != nil {
			slogctx.Error(ctx, "failed to decode task response", "data", string(data), "error", errDec)
		}

		if errAck := c.amqp.receiver.AcceptMessage(ctx, msg); errAck != nil {
			return errAck
		}

		return nil
	})
	if err != nil {
		return orbital.TaskResponse{}, err
	}
	if errDec != nil {
		return orbital.TaskResponse{}, errDec
	}

	return resp, nil
}

// Close closes the AMQP connection, sender, and receiver.
func (c *Client) Close(ctx context.Context) error {
	var errClose error
	c.closeOnce.Do(func() {
		close(c.close)

		c.amqp.mu.Lock()
		defer c.amqp.mu.Unlock()

		if c.amqp.receiver != nil {
			if err := c.amqp.receiver.Close(ctx); err != nil {
				errClose = fmt.Errorf("failed to close receiver: %w", err)
			}
		}
		if c.amqp.sender != nil {
			if err := c.amqp.sender.Close(ctx); err != nil {
				errClose = fmt.Errorf("failed to close sender: %w", err)
			}
		}
		if c.amqp.conn != nil {
			if err := c.amqp.conn.Close(); err != nil {
				errClose = fmt.Errorf("failed to close connection: %w", err)
			}
		}
	})
	return errClose
}

// connect establishes the AMQP connection, session, sender, and receiver.
func (c *Client) connect(ctx context.Context, info ConnectionInfo, opts *amqp.ConnOptions) error {
	if !c.amqp.mu.TryLock() {
		return ErrConnectInProgress
	}
	defer c.amqp.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	conn, err := amqp.Dial(dialCtx, info.URL, opts)
	if err != nil {
		return err
	}
	c.amqp.conn = conn

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		conn.Close()
		return err
	}

	sender, err := session.NewSender(ctx, info.Target, &amqp.SenderOptions{
		TargetDurability: amqp.DurabilityUnsettledState,
	})
	if err != nil {
		conn.Close()
		return err
	}
	c.amqp.sender = sender

	receiver, err := session.NewReceiver(ctx, info.Source, &amqp.ReceiverOptions{
		SourceDurability: amqp.DurabilityUnsettledState,
	})
	if err != nil {
		sender.Close(ctx)
		conn.Close()
		return err
	}
	c.amqp.receiver = receiver

	return nil
}

// retryOnConnError executes f and reconnects if a connection error occurs.
func (c *Client) retryOnConnError(ctx context.Context, f func() error) error {
	err := c.withRLock(f)
	var connErr *amqp.ConnError
	if errors.As(err, &connErr) {
		slogctx.Error(ctx, "AMQP connection error, attempt to reconnect", "description", connErr.RemoteErr.Description)

		err = c.connect(ctx, c.connInfo, c.connOpts)
		if err != nil {
			return fmt.Errorf("%w: %w: %w", ErrReconnectFailed, err, connErr)
		}

		err = c.withRLock(f)
	}
	return err
}

func (c *Client) withRLock(f func() error) error {
	c.amqp.mu.RLock()
	defer c.amqp.mu.RUnlock()
	return f()
}

// receive receives an AMQP message, respecting client closure.
func (c *Client) receive(ctx context.Context) (*amqp.Message, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-c.close:
			cancel()
		case <-ctx.Done():
		}
	}()

	return c.amqp.receiver.Receive(ctx, nil)
}
