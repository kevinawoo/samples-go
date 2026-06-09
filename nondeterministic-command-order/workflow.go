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
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "ordering-activity",
		ScheduleToCloseTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})

	for i := 0; i < childCount; i++ {
		i := i
		workflow.Go(ctx, func(ctx workflow.Context) {
			perturb()
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				WorkflowID: childWorkflowID(i),
				TaskQueue:  TaskQueue,
			})
			_ = workflow.ExecuteChildWorkflow(childCtx, ChildWorkflow, i).Get(ctx, nil)
		})
	}

	_ = workflow.ExecuteActivity(ctx, Activity).Get(ctx, nil)
	_ = workflow.Await(ctx, func() bool { return false })
	return nil
}

func perturb() {
	runtime.Gosched()
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
}

func childWorkflowID(i int) string {
	return fmt.Sprintf("ordering-child-%02d", i)
}

func ChildWorkflow(ctx workflow.Context, i int) error {
	workflow.GetLogger(ctx).Info("child workflow ran", "index", i)
	return nil
}

func Activity(ctx context.Context) error {
	return nil
}
