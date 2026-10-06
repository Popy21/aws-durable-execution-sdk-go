// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Command standalone runs a durable handler with the local runner from a
// plain main package, outside any test.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
	"github.com/aws/aws-durable-execution-sdk-go/durable/durabletest"
)

func handler(ctx durable.Context, name string) (string, error) {
	if err := durable.Wait(ctx, "cooldown", time.Hour); err != nil {
		return "", err
	}
	return durable.Step(ctx, "greet", func(durable.StepContext) (string, error) {
		return "hello, " + name, nil
	})
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	result, err := durabletest.NewLocalRunner(handler).RunUntilComplete("world")
	if err != nil {
		return err
	}
	out, err := durabletest.ResultAs[string](result)
	if err != nil {
		return err
	}
	fmt.Println("result:", out)
	return nil
}
