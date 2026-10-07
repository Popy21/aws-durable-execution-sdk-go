package durable_test

// A CheckpointDurableExecution response with no CheckpointToken means the
// service accepts no further checkpoints from this invocation. These tests
// assert how the invocation ends in each case:
//
//   - TestTerminalTokenlessResponseSucceeds: a token-less response to the
//     handler's terminal EXECUTION/SUCCEED checkpoint reports SUCCEEDED.
//   - TestInFlightTokenlessResponsePends: a token-less response to a branch
//     checkpoint still in flight when the handler returns reports PENDING.
//   - TestInFlightTokenlessResponseWithErrorPends: the same with a handler
//     that returns an error reports PENDING.
//   - TestMidRunTokenlessResponsePends: a token-less response to a mid-run
//     step checkpoint reports PENDING and does not start dependent work.
//   - TestRevokePathLogsOneWarn: a revoke path emits exactly one WARN through
//     the user logger.
//
// All run without AWS through a fake ExecutionClient.

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

type tokenlessClient struct {
	mu    sync.Mutex
	calls int
	drop  func(call int, in durable.CheckpointInput) bool
	// started, when non-nil, is closed when the first dropped checkpoint
	// call begins. That call then blocks until release is closed.
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *tokenlessClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *tokenlessClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	c.calls++
	n := c.calls
	c.mu.Unlock()
	if c.drop(n, in) {
		if c.started != nil {
			c.once.Do(func() { close(c.started) })
			<-c.release
		}
		return durable.CheckpointOutput{CheckpointToken: ""}, nil
	}
	return durable.CheckpointOutput{CheckpointToken: "tok-" + string(rune('a'+n))}, nil
}

func execInput() []byte {
	b, _ := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:account:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{
				"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": "{}"},
			}},
			"NextMarker": "",
		},
	})
	return b
}

func runInvocation(t *testing.T, h durable.Handler[struct{}, string], c *tokenlessClient, opts ...durable.HandlerOption) string {
	t.Helper()
	opts = append(opts, durable.WithExecutionClient(c))
	resp, err := durable.Wrap(h, opts...)(context.Background(), execInput())
	if err != nil {
		return "INVOCATION_ERROR: " + err.Error()
	}
	var r struct {
		Status string `json:"Status"`
	}
	if jerr := json.Unmarshal(resp, &r); jerr != nil {
		t.Fatalf("parse response %q: %v", resp, jerr)
	}
	return r.Status
}

func hasExecSucceed(in durable.CheckpointInput) bool {
	for _, u := range in.Updates {
		if u.Type == durable.OperationTypeExecution && u.Action == durable.OperationActionSucceed {
			return true
		}
	}
	return false
}

func hasStepSucceed(in durable.CheckpointInput, name string) bool {
	for _, u := range in.Updates {
		if u.Action == durable.OperationActionSucceed && u.Name != nil && *u.Name == name {
			return true
		}
	}
	return false
}

// A token-less response to the terminal EXECUTION/SUCCEED checkpoint must
// report SUCCEEDED.
func TestTerminalTokenlessResponseSucceeds(t *testing.T) {
	c := &tokenlessClient{drop: func(_ int, in durable.CheckpointInput) bool { return hasExecSucceed(in) }}
	big := strings.Repeat("x", 6*1024*1024+1000)
	h := func(ctx durable.Context, _ struct{}) (string, error) { return big, nil }
	status := runInvocation(t, h, c)
	t.Logf("terminal status=%s", status)
	if status != "SUCCEEDED" {
		t.Errorf("terminal token-less response: got %s, want SUCCEEDED", status)
	}
}

// A token-less response to a branch checkpoint in flight at handler return
// must report PENDING.
func TestInFlightTokenlessResponsePends(t *testing.T) {
	c := &tokenlessClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
		drop:    func(call int, _ durable.CheckpointInput) bool { return call == 1 },
	}
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		// The deferred close runs as the handler returns, so the dropped
		// checkpoint is answered only after the handler has returned.
		defer close(c.release)
		_ = durable.StepAsync(ctx, "background", func(durable.StepContext) (string, error) { return "bg", nil })
		<-c.started
		return "finished", nil
	}
	status := runInvocation(t, h, c)
	t.Logf("in-flight status=%s", status)
	if status != "PENDING" {
		t.Errorf("in-flight token-less response: got %s, want PENDING", status)
	}
}

// A token-less response to a branch checkpoint in flight when the handler
// returns an ERROR must report PENDING, not FAILED.
func TestInFlightTokenlessResponseWithErrorPends(t *testing.T) {
	c := &tokenlessClient{
		started: make(chan struct{}),
		release: make(chan struct{}),
		drop:    func(call int, _ durable.CheckpointInput) bool { return call == 1 },
	}
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		// The deferred close runs as the handler returns, so the dropped
		// checkpoint is answered only after the handler has returned.
		defer close(c.release)
		_ = durable.StepAsync(ctx, "background", func(durable.StepContext) (string, error) { return "bg", nil })
		<-c.started
		return "", errors.New("handler failed")
	}
	status := runInvocation(t, h, c)
	t.Logf("in-flight-error status=%s", status)
	if status != "PENDING" {
		t.Errorf("in-flight token-less response with handler error: got %s, want PENDING", status)
	}
}

// A token-less response to a mid-run step checkpoint reports PENDING and does
// not start the dependent step.
func TestMidRunTokenlessResponsePends(t *testing.T) {
	var secondRuns atomic.Int32
	c := &tokenlessClient{drop: func(_ int, in durable.CheckpointInput) bool { return hasStepSucceed(in, "first") }}
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		if _, err := durable.Step(ctx, "first", func(durable.StepContext) (string, error) { return "a", nil }); err != nil {
			return "", err
		}
		if _, err := durable.Step(ctx, "second", func(durable.StepContext) (string, error) {
			secondRuns.Add(1)
			return "b", nil
		}); err != nil {
			return "", err
		}
		return "done", nil
	}
	status := runInvocation(t, h, c)
	t.Logf("mid-run status=%s secondRuns=%d", status, secondRuns.Load())
	if status != "PENDING" {
		t.Errorf("mid-run token-less response: got %s, want PENDING", status)
	}
	if secondRuns.Load() != 0 {
		t.Errorf("dependent step ran %d times, want 0", secondRuns.Load())
	}
}

// warnCounter counts WARN-level records.
type warnCounter struct {
	n atomic.Int32
}

func (h *warnCounter) Enabled(context.Context, slog.Level) bool { return true }
func (h *warnCounter) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		h.n.Add(1)
	}
	return nil
}
func (h *warnCounter) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnCounter) WithGroup(string) slog.Handler      { return h }

// The revoke path must log exactly one WARN through the user logger.
func TestRevokePathLogsOneWarn(t *testing.T) {
	wc := &warnCounter{}
	c := &tokenlessClient{drop: func(_ int, in durable.CheckpointInput) bool { return hasStepSucceed(in, "first") }}
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "first", func(durable.StepContext) (string, error) { return "a", nil })
	}
	status := runInvocation(t, h, c, durable.WithLogHandler(wc))
	t.Logf("revoke status=%s warnCount=%d", status, wc.n.Load())
	if wc.n.Load() != 1 {
		t.Errorf("revoke path WARN count = %d, want 1", wc.n.Load())
	}
}
