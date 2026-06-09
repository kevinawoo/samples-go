package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	sample "github.com/temporalio/samples-go/nondeterministic-command-order"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const expectedChildCommands = 10

type schedulingEvent struct {
	id   int64
	kind string
	name string
}

type workflowOrder struct {
	workflowID string
	runID      string
	events     []schedulingEvent
}

type batchResult struct {
	batch         int
	runs          int
	activityFirst int
	childFirst    int
	incomplete    int
}

type workflowVariant struct {
	name       string
	workflowFn interface{}
}

func main() {
	os.Exit(run())
}

func run() int {
	var batchSize int
	var maxBatches int
	var timeout time.Duration
	var startWorker bool
	var verbose bool
	var workflowName string
	var taskQueue string

	flag.IntVar(&batchSize, "runs", 20, "number of parent workflows to start per batch")
	flag.IntVar(&maxBatches, "max-batches", 0, "maximum number of batches to run; 0 keeps running until child-first ordering is found")
	flag.DurationVar(&timeout, "timeout", 30*time.Second, "maximum time to wait for scheduling events")
	flag.BoolVar(&startWorker, "worker", true, "start an in-process worker for the sample task queue")
	flag.BoolVar(&verbose, "verbose", false, "print scheduling events for every workflow, not only out-of-order or incomplete histories")
	flag.StringVar(&workflowName, "workflow", "control", "workflow variant to run: control, await, future-get, selector, channel-receive, waitgroup, or hidden-yield")
	flag.StringVar(&taskQueue, "task-queue", "", "task queue to use; defaults to an isolated per-process queue when -worker=true")
	flag.Parse()

	log.SetFlags(0)

	if batchSize <= 0 {
		log.Println("-runs must be greater than zero")
		return 1
	}
	if maxBatches < 0 {
		log.Println("-max-batches must be zero or greater")
		return 1
	}
	variant, err := selectWorkflowVariant(workflowName)
	if err != nil {
		log.Println(err)
		return 1
	}
	if taskQueue == "" {
		taskQueue = sample.TaskQueue
		if startWorker {
			taskQueue = fmt.Sprintf("%s-harness-%d", sample.TaskQueue, os.Getpid())
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := client.Dial(client.Options{Logger: noopLogger{}})
	if err != nil {
		log.Println("unable to create Temporal client", err)
		return 1
	}
	defer c.Close()

	var w worker.Worker
	if startWorker {
		w = worker.New(c, taskQueue, worker.Options{})
		w.RegisterWorkflow(sample.CommandOrderWorkflow)
		w.RegisterWorkflow(sample.CommandOrderWithHiddenYieldWorkflow)
		w.RegisterWorkflow(sample.CommandOrderWithAwaitYieldWorkflow)
		w.RegisterWorkflow(sample.CommandOrderWithFutureGetYieldWorkflow)
		w.RegisterWorkflow(sample.CommandOrderWithSelectorYieldWorkflow)
		w.RegisterWorkflow(sample.CommandOrderWithChannelReceiveYieldWorkflow)
		w.RegisterWorkflow(sample.CommandOrderWithWaitGroupYieldWorkflow)
		w.RegisterWorkflow(sample.ChildWorkflow)
		w.RegisterActivity(sample.Activity)
		if err := w.Start(); err != nil {
			log.Println("unable to start worker", err)
			return 1
		}
		defer w.Stop()
	}

	for batch := 1; maxBatches == 0 || batch <= maxBatches; batch++ {
		select {
		case <-ctx.Done():
			log.Println("stopped")
			return 1
		default:
		}

		result, err := runBatch(ctx, c, variant, taskQueue, batch, batchSize, timeout, verbose)
		if err != nil {
			log.Println("batch failed", err)
			return 1
		}

		fmt.Printf("batch=%d summary runs=%d activity_first=%d child_first=%d incomplete=%d\n", result.batch, result.runs, result.activityFirst, result.childFirst, result.incomplete)
		if result.childFirst > 0 {
			fmt.Printf("found out-of-order command scheduling in batch=%d\n", result.batch)
			return 2
		}
		if result.incomplete > 0 {
			fmt.Printf("stopping after incomplete history checks in batch=%d\n", result.batch)
			return 1
		}
	}

	return 0
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...interface{}) {}
func (noopLogger) Info(string, ...interface{})  {}
func (noopLogger) Warn(string, ...interface{})  {}
func (noopLogger) Error(string, ...interface{}) {}

func selectWorkflowVariant(name string) (workflowVariant, error) {
	switch name {
	case "control":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWorkflow}, nil
	case "hidden-yield":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWithHiddenYieldWorkflow}, nil
	case "await":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWithAwaitYieldWorkflow}, nil
	case "future-get":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWithFutureGetYieldWorkflow}, nil
	case "selector":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWithSelectorYieldWorkflow}, nil
	case "channel-receive":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWithChannelReceiveYieldWorkflow}, nil
	case "waitgroup":
		return workflowVariant{name: name, workflowFn: sample.CommandOrderWithWaitGroupYieldWorkflow}, nil
	default:
		return workflowVariant{}, fmt.Errorf("unknown -workflow %q; expected control, await, future-get, selector, channel-receive, waitgroup, or hidden-yield", name)
	}
}

func runBatch(ctx context.Context, c client.Client, variant workflowVariant, taskQueue string, batch, batchSize int, timeout time.Duration, verbose bool) (batchResult, error) {
	result := batchResult{batch: batch}

	started, err := startWorkflows(ctx, c, variant, taskQueue, batch, batchSize)
	if len(started) > 0 {
		defer terminateWorkflows(c, started)
	}
	if err != nil {
		return result, err
	}
	result.runs = len(started)

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, run := range started {
		order, err := waitForSchedulingEvents(waitCtx, c, run.GetID(), run.GetRunID())
		if err != nil {
			result.incomplete++
			fmt.Printf("INCOMPLETE batch=%d workflowID=%s runID=%s error=%v events=%s\n", batch, run.GetID(), run.GetRunID(), err, describeEvents(order.events))
			continue
		}

		orderClass := classification(order.events)
		switch orderClass {
		case "ACTIVITY_FIRST":
			result.activityFirst++
		case "CHILD_FIRST":
			result.childFirst++
		default:
			result.incomplete++
		}

		if verbose || orderClass != "ACTIVITY_FIRST" {
			fmt.Printf("%s batch=%d workflowID=%s runID=%s events=%s\n", orderClass, batch, order.workflowID, order.runID, describeEvents(order.events))
		}
	}

	return result, nil
}

func startWorkflows(ctx context.Context, c client.Client, variant workflowVariant, taskQueue string, batch, count int) ([]client.WorkflowRun, error) {
	batchID := strings.ReplaceAll(time.Now().UTC().Format("20060102T150405.000000000"), ".", "")
	runs := make([]client.WorkflowRun, 0, count)
	for i := 0; i < count; i++ {
		workflowID := fmt.Sprintf("nondeterministic-command-order-harness-%s-%06d-%s-%02d", variant.name, batch, batchID, i)
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID:                       workflowID,
			TaskQueue:                taskQueue,
			WorkflowExecutionTimeout: 2 * time.Minute,
		}, variant.workflowFn)
		if err != nil {
			return runs, fmt.Errorf("start workflow batch %d index %d: %w", batch, i, err)
		}
		runs = append(runs, run)
	}
	return runs, nil
}

func waitForSchedulingEvents(ctx context.Context, c client.Client, workflowID, runID string) (workflowOrder, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		order, err := getSchedulingEvents(ctx, c, workflowID, runID)
		if err != nil {
			return order, err
		}
		if hasCompleteSchedulingEvents(order.events) {
			return order, nil
		}

		select {
		case <-ctx.Done():
			return order, ctx.Err()
		case <-ticker.C:
		}
	}
}

func getSchedulingEvents(ctx context.Context, c client.Client, workflowID, runID string) (workflowOrder, error) {
	order := workflowOrder{
		workflowID: workflowID,
		runID:      runID,
	}

	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return order, err
		}
		schedulingEvent, ok := commandSchedulingEvent(event)
		if ok {
			order.events = append(order.events, schedulingEvent)
		}
	}

	return order, nil
}

func commandSchedulingEvent(event *historypb.HistoryEvent) (schedulingEvent, bool) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		attrs := event.GetActivityTaskScheduledEventAttributes()
		return schedulingEvent{
			id:   event.GetEventId(),
			kind: "activity",
			name: attrs.GetActivityId(),
		}, true
	case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
		attrs := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		return schedulingEvent{
			id:   event.GetEventId(),
			kind: "child",
			name: attrs.GetWorkflowId(),
		}, true
	default:
		return schedulingEvent{}, false
	}
}

func hasCompleteSchedulingEvents(events []schedulingEvent) bool {
	var activities, children int
	for _, event := range events {
		switch event.kind {
		case "activity":
			activities++
		case "child":
			children++
		}
	}
	return activities >= 1 && children >= expectedChildCommands
}

func classification(events []schedulingEvent) string {
	if !hasCompleteSchedulingEvents(events) {
		return "INCOMPLETE"
	}
	for _, event := range events {
		switch event.kind {
		case "activity":
			return "ACTIVITY_FIRST"
		case "child":
			return "CHILD_FIRST"
		}
	}
	return "INCOMPLETE"
}

func describeEvents(events []schedulingEvent) string {
	parts := make([]string, 0, len(events))
	for _, event := range events {
		parts = append(parts, fmt.Sprintf("%d:%s:%s", event.id, event.kind, event.name))
	}
	return strings.Join(parts, ",")
}

func terminateWorkflows(c client.Client, runs []client.WorkflowRun) {
	return
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, run := range runs {
		err := c.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "nondeterministic command order harness complete")
		if err != nil {
			log.Printf("unable to terminate workflowID=%s runID=%s: %v", run.GetID(), run.GetRunID(), err)
		}
	}
}
