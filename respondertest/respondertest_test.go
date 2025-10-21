package respondertest_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/respondertest"
)

func TestNewResponder(t *testing.T) {
	tests := []struct {
		name string
		opts []respondertest.Option
	}{
		{
			name: "without options",
			opts: []respondertest.Option{},
		},
		{
			name: "with options",
			opts: []respondertest.Option{
				respondertest.WithInputBufferSize(10),
				respondertest.WithOutputBufferSize(10),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responder := respondertest.NewResponder(tt.opts...)
			assert.NotNil(t, responder)
		})
	}
}

func TestReceiveTaskRequest(t *testing.T) {
	responder := respondertest.NewResponder()

	expReq := orbital.TaskRequest{
		TaskID: uuid.New(),
	}
	responder.NewRequest(expReq)

	req, err := responder.ReceiveTaskRequest(t.Context())

	assert.NoError(t, err)
	assert.Equal(t, expReq.TaskID, req.TaskID)
}

func TestSendTaskResponse(t *testing.T) {
	responder := respondertest.NewResponder()

	expResp := orbital.TaskResponse{
		TaskID: uuid.New(),
	}
	err := responder.SendTaskResponse(t.Context(), expResp)
	assert.NoError(t, err)

	resp := responder.NewResponse()
	assert.Equal(t, expResp.TaskID, resp.TaskID)
}
