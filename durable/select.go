package durable

import (
	"errors"
	"fmt"
	"slices"
)

// selectOutcome is the checkpointed result of a [Select] operation: the
// name of the winning branch and its settled outcome. The outcome reuses
// [Settled], so a failed winner's error is recorded with its wire type and
// rebuilt on replay the same way [AllSettled] rebuilds a rejected outcome.
type selectOutcome[O any] struct {
	Winner  string     `json:"winner"`
	Outcome Settled[O] `json:"outcome"`
}

// Select runs the branches concurrently, each in its own child context
// named by [Branch].Name, and returns the name and value of the first
// branch to settle with a terminal outcome (success or non-suspension
// error). It is [Race] for callers who need to know which branch won: the
// value alone cannot tell a primary quote from a fallback, but winner can.
//
// A terminal outcome observed before any suspension wins immediately,
// without awaiting the remaining branches. Once a suspension is observed,
// Select awaits all remaining branches so they reach their blocking
// points, then propagates the suspension; suspension takes precedence
// over terminal outcomes observed after it because the suspended branch
// completes only on a later invocation.
//
// If the winning branch fails, err is that branch's error, a
// [*ChildContextError] naming the branch, and winner names it too. The
// Select operation itself is recorded as SUCCEEDED in that case, with the
// winner's failure as part of its result, so replay returns the same
// winner and the same error; the failing branch's own child context is
// recorded as FAILED.
//
// Select uses [RunInChildContext] internally, so the winner's name and
// value are checkpointed together: on replay, the same winner is returned
// even when a different branch would finish first if the branches ran
// again. opts configure that child-context operation; [WithChildSerdes]
// selects the serializer for the checkpointed record, an object with a
// "winner" field holding the name and an "outcome" field holding the
// value or error in the shape [AllSettled] stores.
//
// Empty branches returns an error immediately, without recording an
// operation, because no branch could ever win. Duplicate branch names are
// rejected the same way, because the winner would be ambiguous.
func Select[O any](ctx Context, name string, branches []Branch[O], opts ...ChildOption) (winner string, value O, err error) {
	var zero O
	if len(branches) == 0 {
		return "", zero, fmt.Errorf("durable: Select %q: no branches", name)
	}
	seen := make(map[string]struct{}, len(branches))
	for _, b := range branches {
		if _, dup := seen[b.Name]; dup {
			return "", zero, fmt.Errorf("durable: Select %q: duplicate branch name %q", name, b.Name)
		}
		seen[b.Name] = struct{}{}
	}

	out, err := RunInChildContext(ctx, name, func(childCtx Context) (selectOutcome[O], error) {
		var none selectOutcome[O]

		// Every branch runs in its own child context.
		fs := make([]*Future[O], len(branches))
		for i, b := range branches {
			fs[i] = Go(childCtx, b.Name, b.Func)
		}

		// The branch futures are awaited together on this goroutine: it
		// parks until one settles, then reads every settled one in branch
		// order.
		observe := combinatorObserver(childCtx)
		var sawSuspend bool
		pending := slices.Clone(fs)
		idx := make([]int, len(fs))
		for i := range idx {
			idx[i] = i
		}
		for len(pending) > 0 {
			awaitAnySettled(pending)
			keptF, keptI := pending[:0], idx[:0]
			for j, f := range pending {
				if !f.isDone() {
					keptF, keptI = append(keptF, f), append(keptI, idx[j])
					continue
				}
				observe(f.err)
				if f.err != nil && errors.Is(f.err, errSuspendExecution) {
					sawSuspend = true
					continue
				}
				if !sawSuspend {
					outcome := Settled[O]{Value: f.value}
					if f.err != nil {
						outcome = Settled[O]{Err: f.err}
					}
					return selectOutcome[O]{Winner: branches[idx[j]].Name, Outcome: outcome}, nil
				}
				// A terminal outcome after a suspension is discarded: the
				// loop keeps draining so every branch reaches a blocking
				// point, and the suspension propagates below. On the
				// resume invocation the branches replay and the terminal
				// outcome is returned then.
			}
			pending, idx = keptF, keptI
		}
		return none, errSuspendExecution
	}, opts...)
	if err != nil {
		return "", zero, err
	}
	return out.Winner, out.Outcome.Value, out.Outcome.Err
}
