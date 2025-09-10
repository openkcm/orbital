package solace

import (
	"context"
	"errors"
	"time"

	"solace.dev/go/messaging"
	"solace.dev/go/messaging/pkg/solace"
	"solace.dev/go/messaging/pkg/solace/config"
	"solace.dev/go/messaging/pkg/solace/message"
	"solace.dev/go/messaging/pkg/solace/resource"

	"github.com/openkcm/orbital"
)

const (
	terminateTimeout  = 5 * time.Second
	publishAckTimeout = 2 * time.Second
	protocol          = "protocol"
	smf               = "smf"
)

var (
	ErrFailedToGetPayloadBytes = errors.New("failed to get payload bytes")
	ErrPublisherNotReady       = errors.New("publisher is not ready")
)

type Solace struct {
	topic       string
	messageChan chan message.InboundMessage
	codec       orbital.Codec
	service     solace.MessagingService
	publisher   solace.PersistentMessagePublisher
	receiver    solace.PersistentMessageReceiver
}

type ConnectionInfo struct {
	Host   string
	VPN    string
	Target string
	Source string
}

type ClientOption func(solace.MessagingServiceBuilder)

// WithBasicAuth configures the client to use basic authentication with the given username and password.
// The caDir parameter specifies the directory containing CA certificates for TLS validation.
func WithBasicAuth(username, password, caDir string) ClientOption {
	return func(svc solace.MessagingServiceBuilder) {
		svc.WithAuthenticationStrategy(config.BasicUserNamePasswordAuthentication(
			username, password,
		)).WithTransportSecurityStrategy(config.NewTransportSecurityStrategy().
			WithCertificateValidation(false, true, caDir, ""))
	}
}

// WithExternalMTLS configures the client to use mutual TLS authentication with the given certificate, key, and CA files.
func WithExternalMTLS(certFile, keyFile, caDir string) ClientOption {
	return func(svc solace.MessagingServiceBuilder) {
		svc.WithAuthenticationStrategy(
			config.ClientCertificateAuthentication(certFile, keyFile, ""),
		)

		tls := config.NewTransportSecurityStrategy().
			WithCertificateValidation(true, true, caDir, "")
		svc.WithTransportSecurityStrategy(tls)
	}
}

// NewClient creates and returns a new Solace client.
func NewClient(codec orbital.Codec, connectionInfo ConnectionInfo, opts ...ClientOption) (*Solace, error) {
	messagingService, err := setupMessagingService(connectionInfo.Host, connectionInfo.VPN, opts...)
	if err != nil {
		return nil, err
	}

	publisher, err := messagingService.CreatePersistentMessagePublisherBuilder().Build()
	if err != nil {
		return nil, err
	}

	err = publisher.Start()
	if err != nil {
		return nil, err
	}

	sol := Solace{
		topic:       connectionInfo.Target,
		service:     messagingService,
		codec:       codec,
		messageChan: make(chan message.InboundMessage),
		publisher:   publisher,
	}

	err = sol.setupReceiver(messagingService, connectionInfo.Source)
	if err != nil {
		return nil, err
	}

	return &sol, nil
}

// Close terminates the Solace client, including the receiver, publisher, and messaging service.
func (s *Solace) Close() error {
	if s.receiver != nil {
		if err := s.receiver.Terminate(terminateTimeout); err != nil {
			return err
		}
	}

	if s.publisher != nil {
		if err := s.publisher.Terminate(terminateTimeout); err != nil {
			return err
		}
	}

	if s.service != nil {
		if err := s.service.Disconnect(); err != nil {
			return err
		}
	}

	if s.messageChan != nil {
		close(s.messageChan)
	}

	return nil
}

func (s *Solace) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	select {
	case msg := <-s.messageChan:
		payload, ok := msg.GetPayloadAsBytes()
		if !ok {
			return orbital.TaskRequest{}, ErrFailedToGetPayloadBytes
		}

		taskRequest, err := s.codec.DecodeTaskRequest(payload)
		if err != nil {
			return orbital.TaskRequest{}, err
		}

		return taskRequest, nil

	case <-ctx.Done():
		return orbital.TaskRequest{}, ctx.Err()
	}
}

func (s *Solace) SendTaskResponse(ctx context.Context, response orbital.TaskResponse) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		encoded, err := s.codec.EncodeTaskResponse(response)
		if err != nil {
			return err
		}

		msg, err := s.service.MessageBuilder().WithProperty(protocol, smf).BuildWithByteArrayPayload(encoded)
		if err != nil {
			return err
		}

		if !s.publisher.IsReady() {
			return ErrPublisherNotReady
		}

		err = s.publisher.PublishAwaitAcknowledgement(msg, resource.TopicOf(s.topic), publishAckTimeout, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Solace) SendTaskRequest(ctx context.Context, request orbital.TaskRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		encoded, err := s.codec.EncodeTaskRequest(request)
		if err != nil {
			return err
		}

		msg, err := s.service.MessageBuilder().WithProperty(protocol, smf).BuildWithByteArrayPayload(encoded)
		if err != nil {
			return err
		}

		if !s.publisher.IsReady() {
			return ErrPublisherNotReady
		}

		err = s.publisher.Publish(msg, resource.TopicOf(s.topic), nil, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Solace) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	select {
	case msg := <-s.messageChan:
		payload, ok := msg.GetPayloadAsBytes()
		if !ok {
			return orbital.TaskResponse{}, ErrFailedToGetPayloadBytes
		}

		taskResponse, err := s.codec.DecodeTaskResponse(payload)
		if err != nil {
			return orbital.TaskResponse{}, err
		}

		return taskResponse, nil

	case <-ctx.Done():
		return orbital.TaskResponse{}, ctx.Err()
	}
}

// setupReceiver sets up and starts a persistent message receiver for the given source (queue).
func (s *Solace) setupReceiver(messagingService solace.MessagingService, source string) error {
	queue := resource.QueueDurableNonExclusive(source)
	receiver, err := messagingService.
		CreatePersistentMessageReceiverBuilder().
		WithMessageAutoAcknowledgement().
		WithMissingResourcesCreationStrategy(config.PersistentReceiverDoNotCreateMissingResources).
		Build(queue)
	if err != nil {
		return err
	}

	if err := receiver.Start(); err != nil {
		return err
	}

	s.receiver = receiver

	err = receiver.ReceiveAsync(s.receiverCallback)
	if err != nil {
		return err
	}

	return nil
}

// receiverCallback is called when a message is received.
func (s *Solace) receiverCallback(message message.InboundMessage) {
	s.messageChan <- message
}

// setupMessagingService configures and connects to the Solace messaging service.
func setupMessagingService(host, vpn string, opts ...ClientOption) (solace.MessagingService, error) {
	brokerConfig := config.ServicePropertyMap{
		config.TransportLayerPropertyHost: host,
		config.ServicePropertyVPNName:     vpn,
	}

	svc := messaging.NewMessagingServiceBuilder().FromConfigurationProvider(brokerConfig)
	for _, opt := range opts {
		opt(svc)
	}

	messagingService, err := svc.Build()
	if err != nil {
		return nil, err
	}

	err = messagingService.Connect()
	if err != nil {
		return nil, err
	}

	return messagingService, nil
}
