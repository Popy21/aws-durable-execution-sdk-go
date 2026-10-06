// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package extest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func echoHandler(ctx durable.Context, event string) (string, error) {
	return durable.Step(ctx, "echo", func(_ durable.StepContext) (string, error) {
		return "echo: " + event, nil
	})
}

func TestNewDefaultsToLocal(t *testing.T) {
	t.Setenv(EnvRunner, "")
	r := New(t, echoHandler)
	if !r.Local() || r.Cloud() || IsCloud() {
		t.Fatalf("unset %s must select the local runner", EnvRunner)
	}

	result := r.RunUntilComplete(t, "hi")
	if result.Status != durabletest.Succeeded {
		t.Fatalf("expected Succeeded, got %s", result.Status)
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil || out != "echo: hi" {
		t.Fatalf("result = %q, %v; want %q", out, err, "echo: hi")
	}

	// Local-only steps are available and do not fail the test.
	if got := r.OpenCallbacks(); len(got) != 0 {
		t.Fatalf("expected no open callbacks, got %v", got)
	}
	if r.CompletePendingTimers() {
		t.Fatal("expected no pending timers")
	}
}

func TestNewLocalModeExplicit(t *testing.T) {
	t.Setenv(EnvRunner, "local")
	if r := New(t, echoHandler); !r.Local() {
		t.Fatalf("%s=local must select the local runner", EnvRunner)
	}
}

func TestTargetFunction(t *testing.T) {
	t.Setenv(EnvFunctionPrefix, "")
	if got, want := TargetFunction("invoke-simple-target"), "v2-go-invoke-simple-target:$LATEST"; got != want {
		t.Errorf("unset prefix: got %q, want %q", got, want)
	}
	t.Setenv(EnvFunctionPrefix, "ci-")
	if got, want := TargetFunction("invoke-simple-target"), "ci-go-invoke-simple-target:$LATEST"; got != want {
		t.Errorf("prefix ci-: got %q, want %q", got, want)
	}
}

func TestExampleNameIsPackageDirectory(t *testing.T) {
	if got := exampleName(t); got != "extest" {
		t.Fatalf("exampleName = %q, want the package directory name", got)
	}
}

// fakeAPI is a DurableExecutionAPI whose executions succeed immediately
// with a fixed result. It counts invocations.
type fakeAPI struct {
	durabletest.DurableExecutionAPI
	invokes int
	result  string
}

func (f *fakeAPI) Invoke(context.Context, *lambda.InvokeInput, ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	f.invokes++
	return &lambda.InvokeOutput{DurableExecutionArn: aws.String(fmt.Sprintf("arn:fake:%d", f.invokes))}, nil
}

func (f *fakeAPI) GetDurableExecution(context.Context, *lambda.GetDurableExecutionInput, ...func(*lambda.Options)) (*lambda.GetDurableExecutionOutput, error) {
	return &lambda.GetDurableExecutionOutput{Status: types.ExecutionStatusSucceeded, Result: aws.String(f.result)}, nil
}

func (f *fakeAPI) GetDurableExecutionHistory(context.Context, *lambda.GetDurableExecutionHistoryInput, ...func(*lambda.Options)) (*lambda.GetDurableExecutionHistoryOutput, error) {
	return &lambda.GetDurableExecutionHistoryOutput{}, nil
}

func TestCloudRunUntilCompleteCachesTerminalResult(t *testing.T) {
	api := &fakeAPI{result: `"done"`}
	r := newCloud[string, string](t, api, "fn:$LATEST")
	if !r.Cloud() || r.Local() {
		t.Fatal("newCloud must produce a cloud runner")
	}

	first := r.RunUntilComplete(t, "in")
	second := r.RunUntilComplete(t, "in")
	third := r.Run(t, "in")
	if api.invokes != 1 {
		t.Fatalf("expected one invocation for repeated runs of a terminal execution, got %d", api.invokes)
	}
	if first != second || first != third {
		t.Fatal("repeated runs must return the cached terminal result")
	}
	if first.Status != durabletest.Succeeded || first.RawResult != `"done"` {
		t.Fatalf("unexpected result %+v", first)
	}

	r.Reset()
	r.RunUntilComplete(t, "in")
	if api.invokes != 2 {
		t.Fatalf("Reset must allow a new execution, got %d invocations", api.invokes)
	}

	// RegisterFunction is a no-op in cloud mode: the deployed companion is
	// the target.
	r.RegisterFunction("x", durabletest.DurableFunction(echoHandler))
}

// recordingTB captures the first Fatalf so a local-only method's refusal
// can be asserted instead of ending the test.
type recordingTB struct {
	testing.TB
	fatal string
}

type stop struct{}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.fatal = fmt.Sprintf(format, args...)
	panic(stop{})
}

func (r *recordingTB) Fatal(args ...any) {
	r.fatal = fmt.Sprint(args...)
	panic(stop{})
}

// fatalOf runs call and returns the message it failed tb with, or "" when
// it did not fail.
func fatalOf(tb *recordingTB, call func()) string {
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				if _, ok := rec.(stop); !ok {
					panic(rec)
				}
			}
		}()
		call()
	}()
	return tb.fatal
}

// TestRunnerErrorFailsTest checks that a runner error, here an event that
// does not marshal to JSON, fails the test through the testing.TB passed
// to Run and RunUntilComplete.
func TestRunnerErrorFailsTest(t *testing.T) {
	handler := func(_ durable.Context, _ chan int) (string, error) { return "", nil }
	t.Setenv(EnvRunner, "")
	for name, call := range map[string]func(*Runner[chan int, string], testing.TB){
		"Run":              func(r *Runner[chan int, string], tb testing.TB) { r.Run(tb, make(chan int)) },
		"RunUntilComplete": func(r *Runner[chan int, string], tb testing.TB) { r.RunUntilComplete(tb, make(chan int)) },
	} {
		t.Run(name, func(t *testing.T) {
			tb := &recordingTB{TB: t}
			r := New(t, handler)
			if got := fatalOf(tb, func() { call(r, tb) }); !strings.Contains(got, "marshal event") {
				t.Fatalf("%s: fatal = %q, want the runner's marshal error", name, got)
			}
		})
	}
}

func TestCloudRefusesLocalOnlyMethods(t *testing.T) {
	calls := map[string]func(r *Runner[string, string]){
		"CompleteChainedInvoke": func(r *Runner[string, string]) { _ = r.CompleteChainedInvoke("n", nil) },
		"FailChainedInvoke":     func(r *Runner[string, string]) { _ = r.FailChainedInvoke("n", "E", "m") },
		"OpenCallbacks":         func(r *Runner[string, string]) { _ = r.OpenCallbacks() },
		"SendCallbackSuccess":   func(r *Runner[string, string]) { _ = r.SendCallbackSuccess("id", nil) },
		"SendCallbackFailure":   func(r *Runner[string, string]) { _ = r.SendCallbackFailure("id", "E", "m") },
		"SendCallbackHeartbeat": func(r *Runner[string, string]) { _ = r.SendCallbackHeartbeat("id") },
		"TimeoutCallback":       func(r *Runner[string, string]) { _ = r.TimeoutCallback("id") },
		"CompletePendingTimers": func(r *Runner[string, string]) { _ = r.CompletePendingTimers() },
		"OmitTokenOnCheckpoint": func(r *Runner[string, string]) { r.OmitTokenOnCheckpoint(1) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			tb := &recordingTB{TB: t}
			r := newCloud[string, string](tb, &fakeAPI{}, "fn:$LATEST")
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						if _, ok := rec.(stop); !ok {
							panic(rec)
						}
					}
				}()
				call(r)
			}()
			if !strings.Contains(tb.fatal, name) || !strings.Contains(tb.fatal, "only available with the local runner") {
				t.Fatalf("expected %s to fail the test in cloud mode, got %q", name, tb.fatal)
			}
		})
	}
}
