# Nondeterministic Command Order

This sample intentionally demonstrates an unsafe workflow pattern.

`CommandOrderWorkflow` starts real Go goroutines from workflow code. Those goroutines race to emit two Temporal commands: one activity command and one child workflow command. A normal mutex prevents concurrent SDK map writes, but the command that acquires the mutex first is still decided by the Go scheduler.

That means one execution can send:

```text
ScheduleActivityTask
StartChildWorkflowExecution
```

and another execution or replay can send:

```text
StartChildWorkflowExecution
ScheduleActivityTask
```

Run the replay reproduction:

```bash
go test ./nondeterministic-command-order -run TestReplayWithPerturbExposesNondeterministicCommandOrder -count=1
```

The test replays a small synthetic history where the activity was scheduled before the child workflow. The perturbation loop eventually produces both activity-first and child-first command orders, proving the command order is not stable.

The fix is to emit Temporal commands from a workflow coroutine in a deterministic order, store the returned futures, and only use `workflow.Go`, selectors, channels, or wait groups for waiting/handling after the commands have been scheduled.
