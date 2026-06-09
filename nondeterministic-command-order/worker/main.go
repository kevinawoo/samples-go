package main

import (
	"log"

	sample "github.com/temporalio/samples-go/nondeterministic-command-order"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("unable to create Temporal client", err)
	}
	defer c.Close()

	w := worker.New(c, sample.TaskQueue, worker.Options{})
	w.RegisterWorkflow(sample.CommandOrderWorkflow)
	w.RegisterWorkflow(sample.CommandOrderWithHiddenYieldWorkflow)
	w.RegisterWorkflow(sample.CommandOrderWithAwaitYieldWorkflow)
	w.RegisterWorkflow(sample.CommandOrderWithFutureGetYieldWorkflow)
	w.RegisterWorkflow(sample.CommandOrderWithSelectorYieldWorkflow)
	w.RegisterWorkflow(sample.CommandOrderWithChannelReceiveYieldWorkflow)
	w.RegisterWorkflow(sample.CommandOrderWithWaitGroupYieldWorkflow)
	w.RegisterWorkflow(sample.ChildWorkflow)
	w.RegisterActivity(sample.Activity)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("unable to start worker", err)
	}
}
