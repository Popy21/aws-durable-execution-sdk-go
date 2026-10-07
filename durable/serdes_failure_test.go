package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// failingMarshalSerdes is a Serdes whose Marshal always fails and whose Unmarshal
// delegates to encoding/json. It makes a step's result impossible to
// serialize.
type failingMarshalSerdes struct{}

func (failingMarshalSerdes) Marshal(context.Context, durable.SerdesContext, any) ([]byte, error) {
	return nil, errors.New("marshal boom")
}

func (failingMarshalSerdes) Unmarshal(_ context.Context, _ durable.SerdesContext, data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// serdesFailureClient is an ExecutionClient that records nothing and never fails.
// The input-decode and output-encode cases below reach no operation, so it is
// never asked to checkpoint.
type serdesFailureClient struct{}

func (serdesFailureClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (serdesFailureClient) Checkpoint(context.Context, durable.CheckpointInput) (durable.CheckpointOutput, error) {
	return durable.CheckpointOutput{CheckpointToken: "tok-next"}, nil
}

// serdesFailureResponse is the parsed Lambda response of one invocation.
type serdesFailureResponse struct {
	Status string
	Error  *struct {
		ErrorType    string
		ErrorMessage string
	}
}

// runSerdesFailureCase invokes a handler once with a crafted initial execution state
// whose customer input payload is inputPayload. It returns the parsed response
// and any invocation error.
func runSerdesFailureCase[I, O any](h durable.Handler[I, O], inputPayload string) (serdesFailureResponse, error) {
	in, _ := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{
				"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": inputPayload},
			}},
			"NextMarker": "",
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out, err := durable.Wrap(h, durable.WithExecutionClient(serdesFailureClient{}))(ctx, in)
	if err != nil {
		return serdesFailureResponse{}, err
	}
	var r serdesFailureResponse
	_ = json.Unmarshal(out, &r)
	return r, nil
}

// A step whose result cannot be serialized must fail the operation once and
// must not consult the retry strategy. The required behavior is one body
// run.
func TestStepSerializeFailureIsNotRetried(t *testing.T) {
	var runs int64
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(_ durable.StepContext) (string, error) {
			atomic.AddInt64(&runs, 1)
			return "value", nil
		}, durable.WithStepSerdes(failingMarshalSerdes{})) // default retry strategy
	}
	r, err := durabletest.NewLocalRunner(h).RunUntilComplete(struct{}{})
	if err != nil {
		t.Fatal(err)
	}

	if r.Status != durabletest.Failed {
		t.Errorf("status = %s, want FAILED", r.Status)
	}
	if r.Error == nil || r.Error.Type != "StepError" {
		t.Errorf("error type = %v, want StepError", r.Error)
	}
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Errorf("step body ran %d times, want 1", got)
	}
}

// A handler input that does not decode into the event type must fail the
// execution at once with a recorded SerdesError whose direction is
// "unmarshal".
func TestHandlerInputDecodeFailureFailsExecution(t *testing.T) {
	type order struct {
		ID int `json:"id"`
	}
	h := func(_ durable.Context, _ order) (string, error) { return "ok", nil }

	// The customer input payload is the JSON string "hello"; it cannot decode
	// into order.
	r, err := runSerdesFailureCase(h, `"hello"`)
	if err != nil {
		t.Fatalf("invocation error, not a FAILED response: %v", err)
	}
	if r.Status != "FAILED" {
		t.Errorf("status = %q, want FAILED", r.Status)
	}
	if r.Error == nil || r.Error.ErrorType != "SerdesError" {
		t.Errorf("recorded error = %+v, want ErrorType SerdesError", r.Error)
	}
	if r.Error != nil && !strings.Contains(r.Error.ErrorMessage, "unmarshal") {
		t.Errorf("error message = %q, want it to name the unmarshal direction", r.Error.ErrorMessage)
	}
}

// A handler result that cannot be encoded must fail the execution at once with
// a recorded SerdesError whose direction is "marshal". A float64 NaN cannot be
// encoded by encoding/json.
func TestHandlerResultEncodeFailureFailsExecution(t *testing.T) {
	h := func(_ durable.Context, _ struct{}) (float64, error) { return math.NaN(), nil }

	r, err := runSerdesFailureCase(h, "{}")
	if err != nil {
		t.Fatalf("invocation error, not a FAILED response: %v", err)
	}
	if r.Status != "FAILED" {
		t.Errorf("status = %q, want FAILED", r.Status)
	}
	if r.Error == nil || r.Error.ErrorType != "SerdesError" {
		t.Errorf("recorded error = %+v, want ErrorType SerdesError", r.Error)
	}
	if r.Error != nil && !strings.Contains(r.Error.ErrorMessage, "marshal") {
		t.Errorf("error message = %q, want it to name the marshal direction", r.Error.ErrorMessage)
	}
}
