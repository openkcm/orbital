package solace

import (
	"context"
	"errors"
	"sync"
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
	ErrReceiverChannelClosed   = errors.New("receiver channel is closed")
)

// QueueType represents the type of Solace queue to be used.
type QueueType uint

const (
	QueueTypeDurableNonExclusive QueueType = iota
	QueueTypeDurableExclusive
)

type Solace struct {
	topic       string
	messageChan chan message.InboundMessage
	codec       orbital.Codec
	service     solace.MessagingService
	publisher   solace.PersistentMessagePublisher
	receiver    solace.PersistentMessageReceiver
	close       sync.Once
}

// ConnectionInfo holds the connection details for the Solace messaging service.
type ConnectionInfo struct {
	Host      string     // Host is the Solace broker host address
	VPN       string     // VPN is the Solace VPN name
	Target    string     // Target is the topic to publish messages to
	Source    string     // Source is the queue to receive messages from
	QueueType *QueueType // QueueType specifies the type of queue to use, if nil it defaults to QueueTypeDurableNonExclusive.
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
			WithCertificateValidation(false, true, caDir, "")
		svc.WithTransportSecurityStrategy(tls)
	}
}

// NewClient creates and returns a new Solace client.
func NewClient(codec orbital.Codec, connInfo ConnectionInfo, opts ...ClientOption) (*Solace, error) {
	messagingService, err := setupMessagingService(connInfo.Host, connInfo.VPN, opts...)
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
		topic:       connInfo.Target,
		service:     messagingService,
		codec:       codec,
		messageChan: make(chan message.InboundMessage, 5),
		publisher:   publisher,
	}

	queueType := QueueTypeDurableNonExclusive
	if connInfo.QueueType != nil {
		queueType = *connInfo.QueueType
	}

	err = sol.setupReceiver(messagingService, queueType, connInfo.Source)
	if err != nil {
		return nil, err
	}

	return &sol, nil
}

// Close terminates the Solace client, including the receiver, publisher, and messaging service.
func (s *Solace) Close(_ context.Context) error {
	var err error

	s.close.Do(func() {
		if s.receiver != nil {
			if err = s.receiver.Terminate(terminateTimeout); err != nil {
				return
			}
		}

		if s.publisher != nil {
			if err = s.publisher.Terminate(terminateTimeout); err != nil {
				return
			}
		}

		if s.service != nil {
			if err = s.service.Disconnect(); err != nil {
				return
			}
		}

		if s.messageChan != nil {
			close(s.messageChan)
		}
	})

	return err
}

func (s *Solace) ReceiveTaskRequest(ctx context.Context) (orbital.TaskRequest, error) {
	select {
	case msg, isOpen := <-s.messageChan:
		if !isOpen {
			return orbital.TaskRequest{}, ErrReceiverChannelClosed
		}

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

		err = s.publisher.PublishAwaitAcknowledgement(msg, resource.TopicOf(s.topic), publishAckTimeout, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Solace) ReceiveTaskResponse(ctx context.Context) (orbital.TaskResponse, error) {
	select {
	case msg, isOpen := <-s.messageChan:
		if !isOpen {
			return orbital.TaskResponse{}, ErrReceiverChannelClosed
		}

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
func (s *Solace) setupReceiver(messagingService solace.MessagingService, queueType QueueType, source string) error {
	queue := resource.QueueDurableNonExclusive(source)
	if queueType != QueueTypeDurableNonExclusive {
		queue = resource.QueueDurableExclusive(source)
	}

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
