package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/amqp"
	"github.com/openkcm/orbital/codec"
)

// This example uses RabbitMQ as an AMQP message broker.
// Start RabbitMQ server locally with `make docker-compose-up`.
//
// connInfo holds the connection details for the AMQP client.
var connInfo = amqp.ConnectionInfo{
	URL:    "amqp://guest:guest@localhost:5672/",
	Target: "target",
	Source: "source",
}

func main() {
	ctx := context.Background()

	// Initialize an AMQP client as the client for handling task requests
	client, err := amqp.NewClient(ctx, codec.JSON{}, connInfo)
	handleErr("initializing responder", err)
	defer client.Close(ctx)

	// Initialize an orbital operator that uses the responder
	operator, err := orbital.NewOperator(orbital.OperatorTarget{Client: client})
	handleErr("initializing operator", err)

	// Register a handler for the "example" task type
	err = operator.RegisterHandler("example", handleExample)
	handleErr("registering handler", err)

	// Start the operator to listen for task requests and respond
	operator.ListenAndRespond(ctx)

	// Send a task request to the operator via the AMQP message broker and wait for the response
	sendAndReceive()
}

func handleExample(_ context.Context, _ orbital.HandlerRequest) (orbital.HandlerResponse, error) {
	return orbital.HandlerResponse{
		Result:            orbital.ResultProcessing,
		ReconcileAfterSec: 10,
	}, nil
}

func sendAndReceive() {
	ctx := context.Background()
	initiator, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
		URL:    connInfo.URL,
		Target: connInfo.Source, // use the responder source as target for the initiator
		Source: connInfo.Target, // use the responder target as source for the initiator
	})
	handleErr("initializing initiator", err)

	req := orbital.TaskRequest{
		TaskID:     uuid.New(),
		ETag:       "example-etag",
		Type:       "example",
		ExternalID: "example-external-id",
		Data:       []byte("example data"),
	}

	reqJSON, err := json.MarshalIndent(req, "", "  ") // will base64 encode the data field
	handleErr("marshaling request to JSON", err)
	log.Printf("Sent request: %s\n", string(reqJSON))

	err = initiator.SendTaskRequest(ctx, req)
	handleErr("sending request", err)

	resp, err := initiator.ReceiveTaskResponse(ctx)
	handleErr("receiving response", err)

	respJSON, err := json.MarshalIndent(resp, "", "  ") // will base64 encode the working state field
	handleErr("marshaling response to JSON", err)
	log.Printf("Received response: %s\n", string(respJSON))
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
