package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/client/rpc"
)

func main() {
	ctx := context.Background()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "localhost:50051")
	handleErr("creating listener", err)

	srv, err := rpc.NewServer(lis)
	handleErr("creating grpc server", err)

	operator, err := orbital.NewOperator(orbital.TargetOperator{Client: srv})
	handleErr("initializing operator", err)

	err = operator.RegisterHandler("example", handlerExample)
	handleErr("registering handler", err)

	// ListenAndRespond blocks (like http.ListenAndServe).
	log.Fatal(operator.ListenAndRespond(ctx))
}

func handlerExample(_ context.Context, req orbital.HandlerRequest, resp *orbital.HandlerResponse) {
	log.Printf("Received task: ID=%s Type=%s Data=%s",
		req.TaskID, req.TaskType, string(req.TaskData))

	workingState, err := resp.WorkingState()
	if err != nil {
		resp.Fail("invalid working state")
		return
	}

	workingState.Set("processed", true)
	workingState.Set("processedAt", time.Now().Format(time.RFC3339))

	resp.Complete()
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
