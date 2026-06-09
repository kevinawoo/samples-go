# Nondeterministic Command Order

This sample mirrors the command-order shape of a workflow that starts child workflows inside `workflow.Go` coroutines, then schedules an activity on the root workflow coroutine.

The important behavior is that `workflow.Go` does not run the new coroutine body immediately at the call site. The root workflow coroutine keeps running, reaches `workflow.ExecuteActivity(...).Get(...)`, emits the activity command, and only then yields. After that yield, the child coroutines run and emit child workflow commands.

So this shape consistently sends:

```text
ScheduleActivityTask
StartChildWorkflowExecution
StartChildWorkflowExecution
...
```

Run the replay reproduction:

```bash
go test ./nondeterministic-command-order -count=1
```

The replay test runs the workflow repeatedly with scheduling perturbation and verifies that an activity-first history remains replay-compatible. It also verifies that a child-first history fails replay.

Run the server-history harness against a local Temporal server:

```bash
go run ./nondeterministic-command-order/harness
```

The harness runs batches of 20 parent workflows, fetches each event history from the server, and reports whether the first scheduling event is `ActivityTaskScheduled` or `StartChildWorkflowExecutionInitiated`. By default it keeps running until it finds child-first ordering. Use `-max-batches=N` for a bounded run and `-verbose` to print every workflow's scheduling events.

It starts an in-process worker on an isolated task queue by default; pass `-worker=false -task-queue=nondeterministic-command-order` if you already have a worker running for the sample task queue.

To model plausible activity wrappers that yield before scheduling the activity without creating a timer event, run one of:

```bash
go run ./nondeterministic-command-order/harness -workflow=control -verbose -max-batches 1 -runs 1
go run ./nondeterministic-command-order/harness -workflow=await -verbose -max-batches 1 -runs 1
go run ./nondeterministic-command-order/harness -workflow=future-get -verbose -max-batches 1 -runs 1
go run ./nondeterministic-command-order/harness -workflow=selector -verbose -max-batches 1 -runs 1
go run ./nondeterministic-command-order/harness -workflow=channel-receive -verbose -max-batches 1 -runs 1
go run ./nondeterministic-command-order/harness -workflow=waitgroup -verbose -max-batches 1 -runs 1
```

Each variant performs a workflow-local preflight before `workflow.ExecuteActivity`: `workflow.Await`, `Future.Get`, `Selector.Select`, channel receive, or `WaitGroup.Wait`. The preflight is satisfied by another workflow coroutine in the same workflow task, so it yields the root workflow coroutine without creating a `TimerStarted` event. The `workflow.Go` child coroutines then get a chance to schedule child workflows first.

If a real history has child workflow commands before the activity for this exact source shape, look for another yield point, helper behavior that schedules commands before the root activity, or a prior code version with different command order.
