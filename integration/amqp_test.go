//go:build integration
// +build integration

package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/openkcm/orbital"
)

const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	solaceURL   = "amqp://localhost:5673/"
)

func createTestClient(t *testing.T, broker string, queue string) *amqp.AMQP {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var cl *amqp.AMQP
	var err error

	codec := codec.JSON{}
	switch broker {
	case "RabbitMQ":
		cl, err = amqp.NewClient(ctx, codec, amqp.ConnectionInfo{
			URL:    rabbitMQURL,
			Target: queue,
			Source: queue,
		})
		assert.NoError(t, err, "should create client without error")
	case "Solace":
		cl, err = amqp.NewClient(ctx, codec, amqp.ConnectionInfo{
			URL:    solaceURL,
			Target: queue,
			Source: queue,
		})
		assert.NoError(t, err, "should create client without error")
	default:
		t.Fatalf("unknown broker: %s", broker)
		return nil
	}

	return cl
}

func TestSendReceiveTaskRequests(t *testing.T) {
	cases := []struct {
		name  string
		queue string
	}{
		{"RabbitMQ", "orbital-1"},
		{"Solace", "orbital-1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			amqpClient := createTestClient(t, tc.name, tc.queue)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			const numMessages = 5
			expReqs := make([]orbital.TaskRequest, numMessages)

			for i := range numMessages {
				expReqs[i] = orbital.TaskRequest{TaskID: uuid.New()}
				assert.NoError(t,
					amqpClient.SendTaskRequest(ctx, expReqs[i]),
					"SendTaskRequest should succeed",
				)
			}

			for i := range numMessages {
				req, err := amqpClient.ReceiveTaskRequest(ctx)
				assert.NoError(t, err, "ReceiveTaskRequest should succeed")

				assert.NotNil(t, req, "request should not be nil")
				assert.Equal(t, expReqs[i].TaskID, req.TaskID, "TaskID should match")
			}
		})
	}
}

func TestSendReceiveTaskResponses(t *testing.T) {
	cases := []struct {
		name  string
		queue string
	}{
		{"RabbitMQ", "orbital-2"},
		{"Solace", "orbital-2"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			amqpClient := createTestClient(t, tc.name, tc.queue)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			const numMessages = 5
			expResps := make([]orbital.TaskResponse, numMessages)

			for i := range numMessages {
				expResps[i] = orbital.TaskResponse{TaskID: uuid.New()}
				assert.NoError(t,
					amqpClient.SendTaskResponse(ctx, expResps[i]),
					"SendTaskResponse should succeed",
				)
			}

			for i := range numMessages {
				resp, err := amqpClient.ReceiveTaskResponse(ctx)
				assert.NoError(t, err, "ReceiveTaskResponse should succeed (%s)", tc.name)

				assert.NotNil(t, resp, "response should not be nil")
				assert.Equal(t, expResps[i].TaskID, resp.TaskID, "TaskID should match")
			}
		})
	}
}

func TestMTLSBrokers(t *testing.T) {
	cases := []struct {
		name     string
		connInfo amqp.ConnectionInfo
	}{
		{
			"RabbitMQ-mTLS",
			amqp.ConnectionInfo{
				URL:    "amqps://localhost:5671",
				Target: "mtls-q",
				Source: "mtls-q",
			},
		},
	}

	codec := codec.JSON{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			certDir := filepath.Join("..", "certs")
			opts := amqp.WithExternalMTLS(
				filepath.Join(certDir, "client.pem"),
				filepath.Join(certDir, "client.key"),
				filepath.Join(certDir, "rootCA.pem"),
				"localhost",
			)
			client, err := amqp.NewClient(t.Context(), codec, tc.connInfo, opts)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			expReq := orbital.TaskRequest{TaskID: uuid.New()}
			assert.NoError(t, client.SendTaskRequest(ctx, expReq))
			req, err := client.ReceiveTaskRequest(ctx)
			assert.NoError(t, err)

			assert.NotNil(t, req, "request should not be nil")
			assert.Equal(t, expReq.TaskID, req.TaskID, "TaskID should match")
		})
	}
}
