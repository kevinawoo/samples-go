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

If a real history has child workflow commands before the activity for this exact source shape, look for another yield point, helper behavior that schedules commands before the root activity, or a prior code version with different command order.
