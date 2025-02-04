package helloworld

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/worker"
)

type replayTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller
	service  *workflowservicemock.MockWorkflowServiceClient
}

func TestReplayTestSuite(t *testing.T) {
	s := new(replayTestSuite)
	suite.Run(t, s)
}

func (s *replayTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())
	s.service = workflowservicemock.NewMockWorkflowServiceClient(s.mockCtrl)
}

func (s *replayTestSuite) TearDownTest() {
	s.mockCtrl.Finish() // assert mock’s expectations
}

// This replay test is the recommended way to make sure changing workflow code is backward compatible without non-deterministic errors.
// "helloworld.json" can be downloaded from Temporal CLI:
//
//	tctl wf show -w hello_world_workflowID --output_filename ./helloworld.json
//
// Or from Temporal Web UI. And you may need to change workflowType in the first event.
func (s *replayTestSuite) TestReplayWorkflowHistoryFromFile() {
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{
		EnableLoggingInReplay:    true,
		DisableDeadlockDetection: true,
	})
	require.NoError(s.T(), err)

	worker.EnableVerboseLogging(true)
	l := slog.Default()
	slog.SetLogLoggerLevel(slog.LevelDebug)

	replayer.RegisterWorkflow(Workflow)

	entries, err := os.ReadDir(".")
	require.NoError(s.T(), err)

	//err = replayer.ReplayWorkflowHistoryFromJSONFile(l, "runId-events.json")
	//require.NoError(s.T(), err)

	for _, e := range entries {
		if strings.Contains(e.Name(), ".json") {
			fmt.Println("running", e.Name())
			err = replayer.ReplayWorkflowHistoryFromJSONFile(l, e.Name())
			require.NoError(s.T(), err)
		}
	}
}
