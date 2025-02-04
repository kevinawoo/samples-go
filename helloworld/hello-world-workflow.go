package helloworld

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/temporal"
	"math"
	"strconv"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Workflow is a Hello World workflow definition.
func Workflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumInterval: 3 * time.Second,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("HelloWorld workflow started", "name", name)

	var result string
	get := workflow.ExecuteActivity(ctx, Activity, 10000) // you can change the activity function to Anything!😲, it doesn't do a match by name

	fmt.Println("hello-world-workflow.go:30")

	err := get.Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
	}

	for i := 0; i < math.MaxInt; i++ {
		fmt.Println("i", i)
		var result struct {
			Something int
		}
		err := fmt.Errorf("start")
		for err != nil {
			name := fmt.Sprintf("v%d", i)
			//name := "remove-panic"
			v := workflow.GetVersion(ctx, name, workflow.DefaultVersion, 1)
			fmt.Println("version", v)
			if v == workflow.DefaultVersion {
				fmt.Println("default", i)
				if i > 25 {
					panic(">25")
				}
				//args := &IntStruct{
				//	SomeIntVal: i,
				//}
				err = workflow.ExecuteActivity(ctx, Activity, "0").Get(ctx, &result)
				if err != nil {
					logger.Error("Activity failed.", "Error", err)
				}
				fmt.Println("results", result)
			} else {
				fmt.Println("else", i)
				err = workflow.ExecuteActivity(ctx, ActivitySuccess, i).Get(ctx, &result)
				if err != nil {
					logger.Error("Activity failed.", "Error", err)
				}
			}
		}

		fmt.Println("")
	}

	return "", nil
}

type IntStruct struct {
	SomeIntVal int
	anotherVal map[int]string
}
type StringStruct struct {
	SomeStringVal string
}

func Activity(ctx context.Context, i string) (string, error) {
	time.Sleep(1 * time.Second)
	//if i > 51 {
	//	return "", fmt.Errorf("ahh")
	//}
	//return strconv.Itoa(s.SomeIntVal), nil
	return "999", nil
}

func ActivitySuccess(ctx context.Context, i int) (string, error) {
	if i > 60 {
		return "", fmt.Errorf("ahh")
	}
	return strconv.Itoa(i), nil
}
func ActivitySuccessDone(ctx context.Context, i int) (string, error) {
	if false {
		return "", fmt.Errorf("ahh")
	}
	return strconv.Itoa(i), nil
}
