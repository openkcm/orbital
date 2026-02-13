package main

import (
	"context"
	"log"
	"time"

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
	operator, err := orbital.NewOperator(orbital.TargetOperator{Client: client})
	handleErr("initializing operator", err)

	// Register a handler for the "example" task type
	err = operator.RegisterHandler("example", handlerExample)
	handleErr("registering handler", err)

	// Start the operator to listen for task requests and respond
	operator.ListenAndRespond(ctx)

	// Send a task request to the operator via the AMQP message broker and wait for the response
	sendAndReceive(ctx)
}

func handlerExample(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
	log.Printf("Received handler request:\n \tTaskID: %s\n \tType: %s\n \tData: %s\n \tWorkingState: %s\n \tTaskCreatedAt: %s\n \tTaskLastReconciledAt: %s\n",
		req.TaskID, req.TaskType, string(req.TaskData), string(req.TaskRawWorkingState), req.TaskCreatedAt, req.TaskLastReconciledAt)

	workingState, err := resp.WorkingState() // decode existing working state
	if err != nil {
		resp.Fail("invalid working state")
		return
	}

	counter := workingState.Inc("attempts")
	if counter > 3 {
		resp.Fail("too many attempts")
		return
	}

	workingState.Set("motivation", "keep trying")

	resp.ContinueAndWaitFor(5 * time.Second) // ask the manager to reconcile after 5 seconds
}

func sendAndReceive(ctx context.Context) {
	initiator, err := amqp.NewClient(ctx, codec.JSON{}, amqp.ConnectionInfo{
		URL:    connInfo.URL,
		Target: connInfo.Source, // use the responder source as target for the initiator
		Source: connInfo.Target, // use the responder target as source for the initiator
	})
	handleErr("initializing initiator", err)

	req := orbital.TaskRequest{
		TaskID:       uuid.New(),
		ETag:         "example-etag",
		Type:         "example",
		ExternalID:   "example-external-id",
		Data:         []byte("example data"),
		WorkingState: []byte(`{"someField":"someValue"}`), // example existing working state
	}

	log.Printf("Sent task request:\n \tTaskID: %s\n \tType: %s\n \tExternalID: %s\n \tData: %s\n \tWorkingState: %s\n",
		req.TaskID, req.Type, req.ExternalID, string(req.Data), string(req.WorkingState))

	err = initiator.SendTaskRequest(ctx, req)
	handleErr("sending request", err)

	resp, err := initiator.ReceiveTaskResponse(ctx)
	handleErr("receiving response", err)

	log.Printf("Received task response:\n \tTaskID: %s\n \tType: %s\n \tStatus: %s\n \tReconcileAfterSec: %d\n \tWorkingState: %s\n",
		resp.TaskID, resp.Type, resp.Status, resp.ReconcileAfterSec, string(resp.WorkingState))
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
