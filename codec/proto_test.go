package codec_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/openkcm/orbital/codec"
	orbitalpb "github.com/openkcm/orbital/proto/orbital/v1"
)

func TestProto_TaskRequest(t *testing.T) {
	// given
	subj := codec.Proto{}

	// when
	b, err := subj.EncodeTaskRequest(expTaskRequest)
	assert.NoError(t, err)

	_, err = subj.DecodeTaskRequest([]byte("not a taskrequest"))
	assert.Error(t, err, "should return error for invalid data")

	decodedReq, err := subj.DecodeTaskRequest(b)
	assert.NoError(t, err)

	// then
	assertTaskRequest(t, decodedReq)
}

func TestProto_TaskResponse(t *testing.T) {
	// given
	subj := codec.Proto{}

	// when
	b, err := subj.EncodeTaskResponse(expTaskResponse)
	assert.NoError(t, err)

	_, err = subj.DecodeTaskResponse([]byte("not a taskresponse"))
	assert.Error(t, err, "should return error for invalid data")

	decodedResp, err := subj.DecodeTaskResponse(b)
	assert.NoError(t, err)

	// then
	assertTaskResponse(t, decodedResp)
}

func TestProto_Changes(t *testing.T) {
	t.Run("TaskRequest should check for new field mappings in proto", func(t *testing.T) {
		// given
		subj := codec.Proto{}

		// when
		b, err := subj.EncodeTaskRequest(expTaskRequest)
		assert.NoError(t, err)

		decodedReq, err := subj.DecodeTaskRequest(b)
		assert.NoError(t, err)

		// then
		b, err = json.Marshal(decodedReq)
		assert.NoError(t, err)

		mappedKeys := map[string]any{}
		err = json.Unmarshal(b, &mappedKeys)
		assert.NoError(t, err)

		protoFields := exportedProtoFields(t, orbitalpb.TaskRequest{})
		assert.Len(t, mappedKeys, len(protoFields), "should have same number of fields")
	})

	t.Run("TaskResponse should check for new field mappings in proto", func(t *testing.T) {
		// given
		subj := codec.Proto{}

		// when
		b, err := subj.EncodeTaskResponse(expTaskResponse)
		assert.NoError(t, err)

		decodedRes, err := subj.DecodeTaskResponse(b)
		assert.NoError(t, err)

		// then
		b, err = json.Marshal(decodedRes)
		assert.NoError(t, err)

		mappedKeys := map[string]any{}
		err = json.Unmarshal(b, &mappedKeys)
		assert.NoError(t, err)

		protoFields := exportedProtoFields(t, orbitalpb.TaskResponse{})
		assert.Len(t, mappedKeys, len(protoFields), "should have same number of fields")
	})
}

func TestOptionalField(t *testing.T) {
	t.Run("TaskResponse should not panic when", func(t *testing.T) {
		// given
		errMsg := "error message"
		tt := []struct {
			name  string
			input *orbitalpb.TaskResponse
		}{
			{
				name: "error message is nil",
				input: &orbitalpb.TaskResponse{
					TaskId:       uuid.New().String(),
					ErrorMessage: nil,
					WorkingState: []byte("some thing"),
				},
			},
			{
				name: "working state is nil",
				input: &orbitalpb.TaskResponse{
					TaskId:       uuid.New().String(),
					ErrorMessage: &errMsg,
					WorkingState: nil,
				},
			},
		}
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				encoded, err := proto.Marshal(tc.input)
				assert.NoError(t, err)
				subj := codec.Proto{}

				// then
				assert.NotPanics(t, func() {
					// when
					result, err := subj.DecodeTaskResponse(encoded)
					assert.NoError(t, err)
					assert.Equal(t, tc.input.TaskId, result.TaskID.String())
				})
			})
		}
	})
	t.Run("TaskRequest should not panic when", func(t *testing.T) {
		// given
		tt := []struct {
			name  string
			input *orbitalpb.TaskRequest
		}{
			{
				name: "data is nil",
				input: &orbitalpb.TaskRequest{
					TaskId: uuid.New().String(),
					Data:   nil,
				},
			},
		}
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				encoded, err := proto.Marshal(tc.input)
				assert.NoError(t, err)
				subj := codec.Proto{}

				// then
				assert.NotPanics(t, func() {
					// when
					result, err := subj.DecodeTaskRequest(encoded)
					assert.NoError(t, err)
					assert.Equal(t, tc.input.TaskId, result.TaskID.String())
				})
			})
		}
	})
}

func exportedProtoFields(t *testing.T, protoType any) map[string]any {
	t.Helper()
	fields := reflect.VisibleFields(reflect.TypeOf(protoType))
	exportedProtoFields := map[string]any{}
	for _, f := range fields {
		if f.IsExported() {
			exportedProtoFields[f.Name] = nil
		}
	}
	return exportedProtoFields
}
