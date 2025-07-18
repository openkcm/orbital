package codec

import (
	"encoding/json"

	"github.com/openkcm/orbital"
)

// JSON is a codec that encodes and decodes TaskRequest and TaskResponse in JSON format.
type JSON struct{}

var _ orbital.Codec = JSON{}

// EncodeTaskRequest encodes a TaskRequest into JSON format.
func (j JSON) EncodeTaskRequest(req orbital.TaskRequest) ([]byte, error) {
	return json.Marshal(req)
}

// EncodeTaskResponse encodes a TaskResponse into JSON format.
func (j JSON) EncodeTaskResponse(resp orbital.TaskResponse) ([]byte, error) {
	return json.Marshal(resp)
}

// DecodeTaskRequest decodes JSON data into a TaskRequest.
func (j JSON) DecodeTaskRequest(data []byte) (orbital.TaskRequest, error) {
	var req orbital.TaskRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return orbital.TaskRequest{}, err
	}
	return req, nil
}

// DecodeTaskResponse decodes JSON data into a TaskResponse.
func (j JSON) DecodeTaskResponse(data []byte) (orbital.TaskResponse, error) {
	var resp orbital.TaskResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return orbital.TaskResponse{}, err
	}
	return resp, nil
}
