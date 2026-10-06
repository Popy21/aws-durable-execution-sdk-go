// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package durabletest_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

// This example demonstrates the canonical usage of the local testing
// runner: create a runner, run a handler to completion, and inspect the
// typed result.
func Example() {
	// Define a durable handler that processes an order through multiple steps.
	handler := func(ctx durable.Context, event string) (string, error) {
		validated, err := durable.Step[string](ctx, "validate", func(_ durable.StepContext) (string, error) {
			return "valid:" + event, nil
		})
		if err != nil {
			return "", err
		}

		if err := durable.Wait(ctx, "cooldown", 5*time.Second); err != nil {
			return "", err
		}

		confirmed, err := durable.Step[string](ctx, "confirm", func(_ durable.StepContext) (string, error) {
			return "confirmed:" + validated, nil
		})
		if err != nil {
			return "", err
		}

		return confirmed, nil
	}

	// Create a local runner and run the handler to completion.
	// RunUntilComplete automatically advances the wait timer between
	// invocations. It returns an error only when the runner itself fails;
	// the handler's outcome is in the result.
	runner := durabletest.NewLocalRunner(handler)
	result, err := runner.RunUntilComplete("order-42")
	if err != nil {
		fmt.Println("Runner error:", err)
		return
	}

	fmt.Println("Status:", result.Status)

	output, err := durabletest.ResultAs[string](result)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Result:", output)

	// Inspect individual operations.
	validateOp := result.Operation("validate")
	fmt.Println("Validate status:", validateOp.Status)

	// Output:
	// Status: SUCCEEDED
	// Result: confirmed:valid:order-42
	// Validate status: SUCCEEDED
}

// This example registers the functions a workflow invokes so the chained
// invokes run for real instead of being stubbed. The pricing function is a
// durable handler with its own checkpoint log; the tax function is a plain
// handler called once.
func ExampleLocalRunner_RegisterFunction() {
	type order struct {
		Quantity int `json:"quantity"`
	}

	pricing := func(ctx durable.Context, o order) (int, error) {
		return durable.Step(ctx, "lookup", func(durable.StepContext) (int, error) {
			return o.Quantity * 10, nil
		})
	}
	tax := func(_ context.Context, subtotal int) (int, error) {
		return subtotal / 10, nil
	}

	handler := func(ctx durable.Context, o order) (int, error) {
		subtotal, err := durable.Invoke[int](ctx, "price", "pricing-function", o)
		if err != nil {
			return 0, err
		}
		taxDue, err := durable.Invoke[int](ctx, "tax", "tax-function", subtotal)
		if err != nil {
			return 0, err
		}
		return subtotal + taxDue, nil
	}

	runner := durabletest.NewLocalRunner(handler)
	runner.RegisterFunction("pricing-function", durabletest.DurableFunction(pricing))
	runner.RegisterFunction("tax-function", durabletest.PlainFunction(tax))

	result, err := runner.RunUntilComplete(order{Quantity: 3})
	if err != nil {
		fmt.Println("Runner error:", err)
		return
	}
	fmt.Println("Status:", result.Status)

	total, err := durabletest.ResultAs[int](result)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println("Total:", total)
	fmt.Println("Price invoke:", result.Operation("price").Status)

	// Output:
	// Status: SUCCEEDED
	// Total: 33
	// Price invoke: SUCCEEDED
}

// This example runs a handler that fails. The runner's error is nil,
// because the runner did its job; the result reports the failure, and
// ResultAs returns an error that names the handler's error.
func ExampleResultAs_failedHandler() {
	handler := func(ctx durable.Context, _ string) (string, error) {
		return "", errors.New("card declined")
	}

	result, err := durabletest.NewLocalRunner(handler).RunUntilComplete("order-42")
	if err != nil {
		fmt.Println("Runner error:", err)
		return
	}
	fmt.Println("Status:", result.Status)

	if _, err := durabletest.ResultAs[string](result); err != nil {
		fmt.Println("Error:", err)
	}

	// Output:
	// Status: FAILED
	// Error: durabletest: cannot deserialize result from FAILED execution: Error: card declined
}
