// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durable_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// retryCountClient is a fake ExecutionClient that counts how often each kind
// of checkpoint call is sent and fails the step SUCCEED call every time with
// a retryable server fault.
type retryCountClient struct {
	mu               sync.Mutex
	tokens           int
	stepSucceedCalls int
}

func (c *retryCountClient) GetExecutionState(context.Context, durable.GetExecutionStateInput) (durable.GetExecutionStateOutput, error) {
	return durable.GetExecutionStateOutput{}, nil
}

func (c *retryCountClient) Checkpoint(_ context.Context, in durable.CheckpointInput) (durable.CheckpointOutput, error) {
	stepSucceed := false
	for _, u := range in.Updates {
		if u.Type == durable.OperationTypeStep && u.Action == durable.OperationActionSucceed {
			stepSucceed = true
		}
	}
	c.mu.Lock()
	if stepSucceed {
		c.stepSucceedCalls++
	}
	c.tokens++
	tok := fmt.Sprintf("tok-%d", c.tokens)
	c.mu.Unlock()
	if stepSucceed {
		return durable.CheckpointOutput{}, &smithy.GenericAPIError{
			Code:    "ServiceException",
			Message: "The service is temporarily unavailable.",
			Fault:   smithy.FaultServer,
		}
	}
	return durable.CheckpointOutput{CheckpointToken: tok}, nil
}

func retryCountInput() []byte {
	b, _ := json.Marshal(map[string]any{
		"DurableExecutionArn": "arn:aws:lambda:us-west-2:123:function:fn:$LATEST/durable-execution/e/1",
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

func TestCheckpointSingleCall(t *testing.T) {
	c := &retryCountClient{}
	h := func(ctx durable.Context, _ struct{}) (string, error) {
		return durable.Step(ctx, "s", func(durable.StepContext) (string, error) {
			return "ok", nil
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := durable.Wrap(h, durable.WithExecutionClient(c))(ctx, retryCountInput())

	c.mu.Lock()
	stepSucceedCalls := c.stepSucceedCalls
	c.mu.Unlock()

	// The SDK must send the failing batch exactly once and let the client's
	// own retryer, which a fake client does not have, be the only retry.
	if stepSucceedCalls != 1 {
		t.Errorf("step SUCCEED checkpoint sent %d times, want 1", stepSucceedCalls)
	}
	// A retryable checkpoint failure ends the invocation so the service
	// invokes the execution again.
	if err == nil {
		t.Errorf("invocation did not end with an error; a retryable checkpoint failure must end the invocation")
	}
}
