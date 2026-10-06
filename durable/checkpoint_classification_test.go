package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// classifyClient is a fake ExecutionClient. failCheckpoint returns the error
// to fail a checkpoint call with, or nil to accept it. failLoad fails the
// state load.
type classifyClient struct {
	mu             sync.Mutex
	tokens         int
	failCheckpoint func(in durable.CheckpointInput) error
	failLoad       error
}

func (c *classifyClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	if c.failLoad != nil {
		return durable.GetExecutionStateOutput{}, c.failLoad
	}
	return durable.GetExecutionStateOutput{}, nil
}

func (c *classifyClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	c.mu.Lock()
	c.tokens++
	tok := fmt.Sprintf("tok-%d", c.tokens)
	c.mu.Unlock()
	if c.failCheckpoint != nil {
		if err := c.failCheckpoint(in); err != nil {
			return durable.CheckpointOutput{}, err
		}
	}
	return durable.CheckpointOutput{CheckpointToken: tok}, nil
}

// failsStepSucceed reports whether a checkpoint call carries a step's SUCCEED
// update.
func failsStepSucceed(in durable.CheckpointInput) bool {
	for _, u := range in.Updates {
		if u.Type == durable.OperationTypeStep && u.Action == durable.OperationActionSucceed {
			return true
		}
	}
	return false
}

func classifyInput(nextMarker string) []byte {
	b, _ := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:account:function:fn:$LATEST/durable-execution/e/1",
		"CheckpointToken":     "tok-0",
		"InitialExecutionState": map[string]any{
			"Operations": []map[string]any{{
				"Id": "exec", "Type": "EXECUTION", "Status": "STARTED",
				"ExecutionDetails": map[string]any{"InputPayload": "{}"},
			}},
			"NextMarker": nextMarker,
		},
	})
	return b
}

type classifyResult struct {
	status  string // "FAILED", "SUCCEEDED", or "RESUME"
	result  string
	errType string
	errMsg  string
}

func classifyRun(t *testing.T, h durable.Handler[struct{}, string], c *classifyClient, nextMarker string) classifyResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resp, err := durable.Wrap(h, durable.WithExecutionClient(c))(ctx, classifyInput(nextMarker))
	if err != nil {
		return classifyResult{status: "RESUME", errMsg: err.Error()}
	}
	var r struct {
		Status string `json:"Status"`
		Result string `json:"Result"`
		Error  *struct {
			ErrorType    string `json:"ErrorType"`
			ErrorMessage string `json:"ErrorMessage"`
		} `json:"Error"`
	}
	if jerr := json.Unmarshal(resp, &r); jerr != nil {
		t.Fatalf("parse response %q: %v", resp, jerr)
	}
	out := classifyResult{status: r.Status, result: r.Result}
	if r.Error != nil {
		out.errType = r.Error.ErrorType
		out.errMsg = r.Error.ErrorMessage
	}
	return out
}

func classifyStep(ctx durable.Context, _ struct{}) (string, error) {
	return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
		return "ok", nil
	})
}

func classifyCatchStep(ctx durable.Context, _ struct{}) (string, error) {
	v, err := durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
		return "ok", nil
	})
	var ce *durable.CheckpointError
	if errors.As(err, &ce) {
		return "recovered", nil
	}
	return v, err
}

func classifyAPIErr(code, msg string, fault smithy.ErrorFault) error {
	return &smithy.GenericAPIError{Code: code, Message: msg, Fault: fault}
}

func classifyHTTPErr(status int) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      errors.New(http.StatusText(status)),
	}
}

func onStepSucceed(err error) func(durable.CheckpointInput) error {
	return func(in durable.CheckpointInput) error {
		if failsStepSucceed(in) {
			return err
		}
		return nil
	}
}

func TestCheckpointClassification(t *testing.T) {
	t.Run("enforcement_caught", func(t *testing.T) {
		c := &classifyClient{failCheckpoint: onStepSucceed(
			classifyAPIErr("InvalidParameterValueException", "The provided checkpoint request is not valid.", smithy.FaultClient))}
		got := classifyRun(t, classifyCatchStep, c, "")
		if got.status != "FAILED" {
			t.Errorf("status=%q result=%q, want FAILED", got.status, got.result)
		}
	})

	for _, code := range []string{"KMSAccessDeniedException", "KMSDisabledException", "KMSInvalidStateException", "KMSNotFoundException"} {
		t.Run("kms_"+code, func(t *testing.T) {
			c := &classifyClient{failCheckpoint: onStepSucceed(
				classifyAPIErr(code, "Lambda could not access the KMS key.", smithy.FaultServer))}
			got := classifyRun(t, classifyStep, c, "")
			if got.status != "FAILED" {
				t.Errorf("status=%q errMsg=%q, want FAILED", got.status, got.errMsg)
			}
		})
	}

	t.Run("stale_lowercase", func(t *testing.T) {
		c := &classifyClient{failCheckpoint: onStepSucceed(
			classifyAPIErr("InvalidParameterValueException", "invalid checkpoint token: superseded", smithy.FaultClient))}
		got := classifyRun(t, classifyStep, c, "")
		if got.status != "FAILED" {
			t.Errorf("status=%q, want FAILED", got.status)
		}
	})

	t.Run("bare_429", func(t *testing.T) {
		c := &classifyClient{failCheckpoint: onStepSucceed(classifyHTTPErr(429))}
		got := classifyRun(t, classifyStep, c, "")
		if got.status != "RESUME" {
			t.Errorf("status=%q, want RESUME", got.status)
		}
	})

	t.Run("stale_canonical_control", func(t *testing.T) {
		c := &classifyClient{failCheckpoint: onStepSucceed(
			classifyAPIErr("InvalidParameterValueException", "Invalid checkpoint token has been superseded.", smithy.FaultClient))}
		got := classifyRun(t, classifyStep, c, "")
		if got.status != "RESUME" {
			t.Errorf("status=%q, want RESUME", got.status)
		}
	})

	t.Run("stateload_errortype", func(t *testing.T) {
		c := &classifyClient{failLoad: &durable.ClientError{
			Scope: durable.ErrorScopeExecution,
			Err:   errors.New("kms: access denied"),
		}}
		got := classifyRun(t, classifyStep, c, "page-2")
		if got.status != "FAILED" {
			t.Fatalf("status=%q, want FAILED", got.status)
		}
		if got.errType != "ClientError" {
			t.Errorf("errType=%q, want ClientError", got.errType)
		}
		if !strings.Contains(got.errMsg, "kms: access denied") {
			t.Errorf("errMsg=%q, want it to contain the client error message", got.errMsg)
		}
	})
}
