package codec_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/codec"
)

func TestJSON_TaskRequest(t *testing.T) {
	codec := codec.JSON{}
	req := orbital.TaskRequest{
		TaskID:       uuid.New(),
		Type:         "test",
		Data:         []byte("test data"),
		WorkingState: []byte("prev state"),
		ETag:         "etag",
	}

	b, err := codec.EncodeTaskRequest(req)
	assert.NoError(t, err)

	_, err = codec.DecodeTaskRequest([]byte("not a taskrequest"))
	assert.Error(t, err, "should return error for invalid data")

	decodedReq, err := codec.DecodeTaskRequest(b)
	assert.NoError(t, err)

	assert.Equal(t, req.TaskID, decodedReq.TaskID)
	assert.Equal(t, req.Type, decodedReq.Type)
	assert.Equal(t, req.Data, decodedReq.Data)
	assert.Equal(t, req.WorkingState, decodedReq.WorkingState)
	assert.Equal(t, req.ETag, decodedReq.ETag)
}

func TestJSON_TaskResponse(t *testing.T) {
	codec := codec.JSON{}
	resp := orbital.TaskResponse{
		TaskID:            uuid.New(),
		Type:              "test",
		Status:            string(orbital.ResultDone),
		WorkingState:      []byte("after state"),
		ETag:              "etag",
		ReconcileAfterSec: 10,
		ErrorMessage:      "no error",
	}

	b, err := codec.EncodeTaskResponse(resp)
	assert.NoError(t, err)

	_, err = codec.DecodeTaskResponse([]byte("not a taskresponse"))
	assert.Error(t, err, "should return error for invalid data")

	decodedResp, err := codec.DecodeTaskResponse(b)
	assert.NoError(t, err)

	assert.Equal(t, resp.TaskID, decodedResp.TaskID)
	assert.Equal(t, resp.Type, decodedResp.Type)
	assert.Equal(t, resp.Status, decodedResp.Status)
	assert.Equal(t, resp.WorkingState, decodedResp.WorkingState)
	assert.Equal(t, resp.ETag, decodedResp.ETag)
	assert.Equal(t, resp.ErrorMessage, decodedResp.ErrorMessage)
	assert.Equal(t, resp.ReconcileAfterSec, decodedResp.ReconcileAfterSec)
}
