package codec_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital/codec"
)

func TestJSON_TaskRequest(t *testing.T) {
	// given
	codec := codec.JSON{}

	// when
	b, err := codec.EncodeTaskRequest(expTaskRequest)
	assert.NoError(t, err)

	_, err = codec.DecodeTaskRequest([]byte("not a taskrequest"))
	assert.Error(t, err, "should return error for invalid data")

	actTaskRequest, err := codec.DecodeTaskRequest(b)
	assert.NoError(t, err)

	// then
	assertTaskRequest(t, actTaskRequest)
}

func TestJSON_TaskResponse(t *testing.T) {
	// given
	codec := codec.JSON{}

	// when
	b, err := codec.EncodeTaskResponse(expTaskResponse)
	assert.NoError(t, err)

	_, err = codec.DecodeTaskResponse([]byte("not a taskresponse"))
	assert.Error(t, err, "should return error for invalid data")

	actTaskResponse, err := codec.DecodeTaskResponse(b)
	assert.NoError(t, err)

	// then
	assertTaskResponse(t, actTaskResponse)
}
