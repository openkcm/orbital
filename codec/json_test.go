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
		{name: "without meta", expTaskRequest: expTaskRequestWithoutMeta},
	}
	// given
	subj := codec.JSON{}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			// when
			b, err := subj.EncodeTaskRequest(tt.expTaskRequest)
			assert.NoError(t, err)

			_, err = subj.DecodeTaskRequest([]byte("not a taskrequest"))
			assert.Error(t, err, "should return error for invalid data")

			actTaskRequest, err := subj.DecodeTaskRequest(b)
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
		{name: "without meta", expTaskResponse: expTaskResponseWithoutMeta},
	}
	// given
	subj := codec.JSON{}

	for _, tt := range tts {
		t.Run(tt.name, func(t *testing.T) {
			// when
			b, err := subj.EncodeTaskResponse(tt.expTaskResponse)
			assert.NoError(t, err)

			_, err = subj.DecodeTaskResponse([]byte("not a taskresponse"))
			assert.Error(t, err, "should return error for invalid data")

			actTaskResponse, err := subj.DecodeTaskResponse(b)
			assert.NoError(t, err)

			// then
			assert.Equal(t, tt.expTaskResponse, actTaskResponse)
		})
	}
}
