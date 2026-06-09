package nondeterministic_command_order

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "nondeterministic-command-order"

// CommandOrderWorkflow intentionally demonstrates an unsafe workflow pattern.
//
// Do not copy this into production code: it uses real Go goroutines inside
// workflow code. The goroutines race to emit Temporal commands, so the command
// order can change across executions and replay.
func CommandOrderWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "ordering-activity",
		ScheduleToCloseTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: "ordering-child",
		TaskQueue:  TaskQueue,
	})

	var wg sync.WaitGroup
	var mu sync.Mutex
	start := make(chan struct{})

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		perturb()
		mu.Lock()
		defer mu.Unlock()
		_ = workflow.ExecuteActivity(ctx, Activity)
	}()
	go func() {
		defer wg.Done()
		<-start
		perturb()
		mu.Lock()
		defer mu.Unlock()
		_ = workflow.ExecuteChildWorkflow(childCtx, ChildWorkflow)
	}()

	close(start)
	wg.Wait()

	// Keep the workflow open after the first workflow task. The replay test only
	// needs the first batch of command events to show command-order drift.
	_ = workflow.Await(ctx, func() bool { return false })
	return nil
}

func perturb() {
	runtime.Gosched()
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
}

func ChildWorkflow(ctx workflow.Context) error {
	workflow.GetLogger(ctx).Info("child workflow ran")
	return nil
}

func Activity(ctx context.Context) error {
	return nil
}
