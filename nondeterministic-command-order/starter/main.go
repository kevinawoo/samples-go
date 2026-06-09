package main

import (
	"context"
	"log"

	sample "github.com/temporalio/samples-go/nondeterministic-command-order"
	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	we, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		//ID:        "nondeterministic-command-order-workflow-id",
		TaskQueue: sample.TaskQueue,
	}, sample.CommandOrderWorkflow)
	if err != nil {
		log.Fatalln("unable to start workflow", err)
	}

	log.Println("started workflow", "workflowID", we.GetID(), "runID", we.GetRunID())
}
