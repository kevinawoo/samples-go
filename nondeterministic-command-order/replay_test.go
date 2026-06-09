package nondeterministic_command_order

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestReplayWithPerturbExposesNondeterministicCommandOrder(t *testing.T) {
	history := activityThenChildHistory()

	var replayMatchedHistory bool
	var replayMismatchedHistory bool
	for i := 0; i < 500; i++ {
		err := replay(history)
		if err == nil {
			replayMatchedHistory = true
			continue
		}
		if isCommandOrderMismatch(err) {
			replayMismatchedHistory = true
		} else {
			t.Fatalf("unexpected replay error: %v", err)
		}
		if replayMatchedHistory && replayMismatchedHistory {
			return
		}
	}

	require.True(t, replayMatchedHistory, "perturbation never produced activity-first command order")
	require.True(t, replayMismatchedHistory, "perturbation never produced child-first command order")
}

func isCommandOrderMismatch(err error) bool {
	errText := err.Error()
	return strings.Contains(errText, "nondeterministic workflow") ||
		strings.Contains(errText, "lookup failed for scheduledEventID to activityID")
}

func replay(history *historypb.History) error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(CommandOrderWorkflow)
	replayer.RegisterWorkflow(ChildWorkflow)
	return replayer.ReplayWorkflowHistory(nil, history)
}

func activityThenChildHistory() *historypb.History {
	ts := timestamppb.New(time.Unix(0, 0))
	return &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventTime: ts,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType:        &commonpb.WorkflowType{Name: "CommandOrderWorkflow"},
					TaskQueue:           &taskqueuepb.TaskQueue{Name: TaskQueue},
					WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				},
			},
		},
		{
			EventId:   2,
			EventTime: ts,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue:           &taskqueuepb.TaskQueue{Name: TaskQueue},
					StartToCloseTimeout: durationpb.New(10 * time.Second),
				},
			},
		},
		{
			EventId:   3,
			EventTime: ts,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
					ScheduledEventId: 2,
					Identity:         "replay-test",
				},
			},
		},
		{
			EventId:   4,
			EventTime: ts,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
					Identity:         "replay-test",
				},
			},
		},
		{
			EventId:   5,
			EventTime: ts,
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId:                   "ordering-activity",
					ActivityType:                 &commonpb.ActivityType{Name: "Activity"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: TaskQueue},
					ScheduleToCloseTimeout:       durationpb.New(time.Minute),
					StartToCloseTimeout:          durationpb.New(time.Minute),
					WorkflowTaskCompletedEventId: 4,
				},
			},
		},
		{
			EventId:   6,
			EventTime: ts,
			EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
			Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
				StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
					WorkflowId:                   "ordering-child",
					WorkflowType:                 &commonpb.WorkflowType{Name: "ChildWorkflow"},
					TaskQueue:                    &taskqueuepb.TaskQueue{Name: TaskQueue},
					WorkflowTaskCompletedEventId: 4,
				},
			},
		},
	}}
}
