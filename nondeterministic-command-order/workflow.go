package nondeterministic_command_order

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "nondeterministic-command-order"
const childCount = 10

// CommandOrderWorkflow mirrors the production shape being investigated:
// schedule child workflows from workflow.Go coroutines, then schedule an
// activity from the root workflow coroutine.
func CommandOrderWorkflow(ctx workflow.Context) error {
	return commandOrderWorkflow(ctx, executeOrderingActivity)
}

// CommandOrderWithHiddenYieldWorkflow models an activity wrapper that performs
// a workflow-blocking preflight wait before it actually schedules the activity.
func CommandOrderWithHiddenYieldWorkflow(ctx workflow.Context) error {
	return CommandOrderWithAwaitYieldWorkflow(ctx)
}

func CommandOrderWithAwaitYieldWorkflow(ctx workflow.Context) error {
	return commandOrderWorkflow(ctx, executeOrderingActivityAfterAwait)
}

func CommandOrderWithFutureGetYieldWorkflow(ctx workflow.Context) error {
	return commandOrderWorkflow(ctx, executeOrderingActivityAfterFutureGet)
}

func CommandOrderWithSelectorYieldWorkflow(ctx workflow.Context) error {
	return commandOrderWorkflow(ctx, executeOrderingActivityAfterSelector)
}

func CommandOrderWithChannelReceiveYieldWorkflow(ctx workflow.Context) error {
	return commandOrderWorkflow(ctx, executeOrderingActivityAfterChannelReceive)
}

func CommandOrderWithWaitGroupYieldWorkflow(ctx workflow.Context) error {
	return commandOrderWorkflow(ctx, executeOrderingActivityAfterWaitGroup)
}

func commandOrderWorkflow(ctx workflow.Context, executeActivity func(workflow.Context) error) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "ordering-activity",
		ScheduleToCloseTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
	taskQueue := workflow.GetInfo(ctx).TaskQueueName

	for i := 0; i < childCount; i++ {
		i := i
		perturb(0)
		workflow.Go(ctx, func(ctx workflow.Context) {
			perturb(0)
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: childWorkflowID(i),
				TaskQueue:  taskQueue,
			})
			perturb(0)
			c := workflow.ExecuteChildWorkflow(childCtx, ChildWorkflow, i)
			perturb(0)
			c.Get(ctx, nil)
		})
	}

	if err := executeActivity(ctx); err != nil {
		return err
	}
	_ = workflow.Await(ctx, func() bool { return false })
	return nil
}

func executeOrderingActivity(ctx workflow.Context) error {
	act := workflow.ExecuteActivity(ctx, Activity)
	perturb(0)
	return act.Get(ctx, nil)
}

func executeOrderingActivityAfterAwait(ctx workflow.Context) error {
	preflight := workflowLocalFuture(ctx)
	if err := workflow.Await(ctx, func() bool {
		return preflight.IsReady()
	}); err != nil {
		return err
	}
	return executeOrderingActivity(ctx)
}

func executeOrderingActivityAfterFutureGet(ctx workflow.Context) error {
	preflight := workflowLocalFuture(ctx)
	var ignored bool
	if err := preflight.Get(ctx, &ignored); err != nil {
		return err
	}
	return executeOrderingActivity(ctx)
}

func executeOrderingActivityAfterSelector(ctx workflow.Context) error {
	preflight := workflowLocalFuture(ctx)
	selector := workflow.NewSelector(ctx)
	selector.AddFuture(preflight, func(workflow.Future) {})
	selector.Select(ctx)
	return executeOrderingActivity(ctx)
}

func executeOrderingActivityAfterChannelReceive(ctx workflow.Context) error {
	channel := workflow.NewChannel(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		channel.Send(ctx, true)
	})

	var ignored bool
	channel.Receive(ctx, &ignored)
	return executeOrderingActivity(ctx)
}

func executeOrderingActivityAfterWaitGroup(ctx workflow.Context) error {
	wg := workflow.NewWaitGroup(ctx)
	wg.Add(1)
	workflow.Go(ctx, func(ctx workflow.Context) {
		wg.Done()
	})
	wg.Wait(ctx)
	return executeOrderingActivity(ctx)
}

func workflowLocalFuture(ctx workflow.Context) workflow.Future {
	preflight, preflightDone := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		preflightDone.SetValue(true)
	})
	return preflight
}

func perturb(d time.Duration) {
	return // skip
	if d == 0 {
		d = time.Duration(rand.Intn(1000)) * time.Microsecond
	}

	if rand.Intn(100)%2 == 0 {
		runtime.Gosched()
	}
	if rand.Intn(100)%3 == 0 {
		time.Sleep(d)
	}
	if rand.Intn(100)%5 == 0 {
		runtime.Gosched()
	}
}

func childWorkflowID(i int) string {
	return fmt.Sprintf("ordering-child-%02d", i)
}

func ChildWorkflow(ctx workflow.Context, i int) error {
	workflow.GetLogger(ctx).Info("child workflow ran", "index", i)
	d := time.Duration(rand.Intn(10000)) * time.Microsecond
	perturb(d)
	return nil
}

func Activity(ctx context.Context) error {
	d := time.Duration(rand.Intn(10000)) * time.Microsecond
	perturb(d)
	return nil
}
