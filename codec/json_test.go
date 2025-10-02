package codec_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/codec"
)

func TestJSON_TaskRequest(t *testing.T) {
	tts := []struct {
		name           string
		expTaskRequest orbital.TaskRequest
	}{
		{name: "with full value", expTaskRequest: expTaskRequest},
		{name: "without encrypt", expTaskRequest: expTaskRequestWithoutEncrypt},
		{name: "without signature", expTaskRequest: expTaskRequestWithoutSig},
		{name: "without signature type", expTaskRequest: expTaskRequestWithoutType},
	}
	// given
	codec := codec.JSON{}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			// when
			b, err := codec.EncodeTaskRequest(tt.expTaskRequest)
			assert.NoError(t, err)

			_, err = codec.DecodeTaskRequest([]byte("not a taskrequest"))
			assert.Error(t, err, "should return error for invalid data")

			actTaskRequest, err := codec.DecodeTaskRequest(b)
			assert.NoError(t, err)

			// then
			assert.Equal(t, tt.expTaskRequest, actTaskRequest)
		})
	}
}

func TestJSON_TaskResponse(t *testing.T) {
	tts := []struct {
		name            string
		expTaskResponse orbital.TaskResponse
	}{
		{name: "with full value", expTaskResponse: expTaskResponse},
		{name: "without encrypt", expTaskResponse: expTaskResponseWithoutEncrypt},
		{name: "without signature", expTaskResponse: expTaskResponseWithoutSig},
		{name: "without signature type", expTaskResponse: expTaskResponseWithoutType},
	}
	// given
	codec := codec.JSON{}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			// when
			b, err := codec.EncodeTaskResponse(tt.expTaskResponse)
			assert.NoError(t, err)

			_, err = codec.DecodeTaskResponse([]byte("not a taskresponse"))
			assert.Error(t, err, "should return error for invalid data")

			actTaskResponse, err := codec.DecodeTaskResponse(b)
			assert.NoError(t, err)

			// then
			assert.Equal(t, tt.expTaskResponse, actTaskResponse)
		})
	}
}
