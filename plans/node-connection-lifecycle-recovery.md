---
read_when:
  - Implementing the Node fix for Gateway reconciliation requests that hang on DELETE /connection?id=-1
  - Changing connection-manager concurrency, cancellation, or Node shutdown
---

# Node connection lifecycle recovery plan

Status: implementation required

Repository: `shellroute/node` only

Base requirement: the implementation branch must contain merge `c035faac` and its lifecycle commit `0a2d83d5`.

## Goal

Make connection cleanup authoritative without allowing one stuck tunnel operation to freeze the Node control plane.

After this change:

- `DELETE /connection?id=-1` returns success only after every pre-existing connection is actually retired.
- A canceled or stuck connect cannot appear after bulk cleanup and escape retirement.
- Request cancellation bounds the HTTP handler even when a low-level dependency ignores cancellation.
- Concurrent Gateway retries join one cleanup attempt; they do not create more blocked handlers or cleanup calls.
- A stuck connection does not hold a global or per-port mutex needed by other control-plane operations.
- SIGTERM proceeds to stop the API and the rest of the Node after a bounded connection-cleanup wait.
- A timeout emits enough state and one goroutine dump to identify the exact stuck cleanup callback next time.

## Incident and confirmed root cause

Incident logs: `/tmp/shellroute-logs/sh-logs-20260902-155254-wjFhOv`.

What happened:

1. Gateway started at `2026-09-02 12:24:34Z` and resolved both `myst-1:4050` and `myst-2:4050`.
2. Its authoritative startup cleanup, `DELETE /connection?id=-1`, reached the old Myst containers but timed out for both nodes on every attempt.
3. The old containers received SIGTERM but did not exit within Docker's 10-second grace period, so Docker force-killed them.
4. A pending request received EOF at the moment its Myst container was killed. This proves the failure was inside the old Node process, not DNS or container discovery.
5. The unchanged Gateway recovered after the Myst containers were recreated.

The liveness regression is in `core/connection/multi.go`, introduced by `0a2d83d5`:

- `Connect`, single-port `Disconnect`, and `Reconnect` hold `bulkMu.RLock()` for the complete low-level manager operation.
- Bulk `Disconnect(-1)` waits for `bulkMu.Lock()` and then holds it through every low-level disconnect.
- Those low-level operations include network waits and arbitrary cleanup callbacks.
- TequilAPI does not pass `c.Request.Context()` into the manager, so Gateway's 10-second cancellation does not release the Node operation.
- `Node.Kill()` begins with the same unbounded bulk disconnect and returns early on error, before stopping the HTTP API and other Node components.

The old process was destroyed before a goroutine dump was captured, so the exact low-level callback is unknown. It could have been a writer waiting for a stale reader, or a writer already inside a stuck cleanup. The replacement must handle both; identifying only one callback is not sufficient.

## Scope

Required Node changes:

- Replace `bulkMu` and striped operation locks with a short-lock lifecycle coordinator.
- Add context to the `MultiManager` lifecycle methods and update all Node call sites.
- Make the production `connectionManager` interruptible and make concurrent disconnect callers wait for the real shared cleanup result.
- Make TequilAPI return a retryable failure when cleanup is still unresolved.
- Bound connection cleanup during Node shutdown and continue the remaining shutdown sequence.
- Add deterministic race, timeout, endpoint, and shutdown regression tests.
- Add targeted lifecycle diagnostics and a timeout-only goroutine dump.

Out of scope:

- No Gateway behavior changes. Its startup cleanup must remain fail-closed.
- No ShellRoute API, Caddy, firewall, certificate, Docker restart-script, or host logging changes.
- No automatic process exit, container restart, health-policy change, new endpoint, or new dependency.
- No general rewrite of tunnel cleanup callbacks. This change contains a non-cooperative callback and exposes it in logs; a separately proven callback bug can be fixed afterward.
- No unrelated upstream refactor.

## Required invariants

Treat these as acceptance rules, not implementation suggestions:

1. No coordinator/lifecycle mutex may be held while calling `Manager.Connect`, `Manager.Disconnect`, `Manager.Reconnect`, a proposal lookup, a cleanup callback, or while waiting on a channel/context.
2. A successful bulk disconnect covers every manager whose lifecycle began before that bulk generation. A late connect/reconnect result from an older generation is never published and must be retired.
3. Bulk success means cleanup completed. Detaching a manager, requesting cancellation, or reaching a timeout is not success.
4. While authoritative bulk reconciliation is active or unresolved, new connects/reconnects fail promptly with a retryable lifecycle-busy error. This preserves Gateway readiness semantics.
5. Only one cleanup call may run against a manager at a time. Concurrent deletes/retries attach to its existing result.
6. A port cannot be reused while an older manager for that port is still connecting, reconnecting, or retiring.
7. `ErrNoConnection` during retirement counts as successful/idempotent cleanup. Any other cleanup error remains unresolved until a later retry succeeds or the process restarts.
8. `Status` and `Stats` never call a manager while holding the coordinator mutex.
9. Every wait exposed to HTTP or shutdown observes a context. Internal cleanup may outlive an HTTP request, but it has a separate hard deadline and remains tracked.
10. No timeout path may report success, lose the unresolved manager, or allow it to be replaced on the same port.

## Design to implement

### 1. Context-aware public coordinator API

In `core/connection/interface.go`, change only `MultiManager` lifecycle methods:

```go
Connect(context.Context, identity.Identity, common.Address, ProposalLookup, ConnectParams) error
Disconnect(context.Context, int) error
Reconnect(context.Context, int) error
```

Keep `Status`, `Stats`, and `CheckChannel` unchanged. Keep the low-level `Manager` interface source-compatible to limit fork divergence and avoid rewriting unrelated manager tests.

Add these sentinel errors in `core/connection`:

- `ErrLifecycleBusy`: an authoritative bulk cleanup is active/unresolved, the requested port is still retiring, or an in-flight operation was superseded by bulk cleanup.
- Continue using `ErrConnectionCancelled` for canceled connection establishment. A connect superseded by bulk must match both `ErrConnectionCancelled` and `ErrLifecycleBusy`, for example with `errors.Join`.

Use `errors.Is`, never direct equality, at API boundaries.

Update all production `MultiManager` callers:

- `tequilapi/endpoints/connection.go`: pass `c.Request.Context()` to create and delete.
- `cmd/node.go`: use a shutdown timeout context.
- `mobile/mysterium/entrypoint.go`: use `context.Background()` because this API exposes no caller context.
- `sleep/sleep.go`: pass an explicit context to reconnect; preserve existing wake/check behavior.
- Update mocks and compile-time interface assertions.

### 2. Replace lock barriers with a generation-fenced state machine

Refactor `core/connection/multi.go`. Remove `bulkMu`, `stripeCount`, `stripes`, and `stripe()` completely.

Keep one `mu` used only for in-memory state. Represent each port with an entry containing at least:

- port and generation;
- manager;
- phase: connecting, active, reconnecting, or retiring;
- completion/result of the current low-level call;
- whether cleanup is running, its completion/result, and last error.

The coordinator must maintain:

- current generation;
- current entries by port;
- retiring entries by port;
- `reconcileRequired` flag;
- at most one active shared bulk operation;
- a state-change notification channel, replaced and closed under `mu`, so waiters can re-check state without polling or spawning one waiter goroutine per entry;
- a configurable-on-the-instance bulk cleanup limit, default `30s`, so tests use milliseconds without mutating a package global.

Put helper types in `core/connection/multi_lifecycle.go` if needed to keep each file below 500 lines. All mutable entry fields are protected by the coordinator `mu`; document this next to the types.

Do not use fixed lock stripes. They serialize unrelated ports after hash collisions and still let a non-cooperative operation poison a port indefinitely.

### 3. Connect lifecycle

Implement this sequence:

1. Check the caller context.
2. Create the low-level manager outside `mu`.
3. Under `mu`, reject with `ErrLifecycleBusy` if reconciliation is active/unresolved or that port is retiring; reject with `ErrAlreadyExists` if the port is current; otherwise reserve the port with a connecting entry and the current generation.
4. Run the manager's context-aware connect capability in one owned worker, falling back to `Manager.Connect` only for legacy/fake managers. No coordinator lock is held.
5. The caller waits for the worker result or its context.
6. On caller cancellation, atomically detach the exact entry, mark it retiring, request a non-blocking low-level cancellation, and return a wrapped `ErrConnectionCancelled` preserving `errors.Is(err, context.Canceled/DeadlineExceeded)`.
7. On worker success, publish as active only if the entry is still exact, its generation is current, its context is live, and reconciliation is not required.
8. A stale/late success is retired; it is never put back into the current map.
9. On worker error, retire defensively. The original connect error is returned to its live caller.

Use buffered result channels so a worker can finish after its original caller has left. The worker owns final state publication; no goroutine may block trying to send to an abandoned request.

### 4. Individual disconnect and reconnect

For `Disconnect(ctx, port)`:

1. Under `mu`, detach the exact current entry and mark it retiring. If it is already retiring, attach to that same retirement. Unknown ports still return nil.
2. If a connect/reconnect call is in flight, request cancellation immediately, but wait for that call to finish before starting low-level cleanup. Do not run `Connect` and `Disconnect` concurrently on the same manager.
3. Start at most one cleanup attempt outside `mu`.
4. Wait for successful retirement, a real cleanup error, or the caller context.
5. Remove the retiring entry only after nil/`ErrNoConnection`. Keep it reserved after timeout/error.

For `Reconnect(ctx, port)`:

- Track it as an entry operation exactly like connect.
- If bulk cleanup advances the generation, its late result cannot restore the entry; retire it after the reconnect call returns.
- Return `error` from `MultiManager.Reconnect`; log the result at the sleep call site.
- Do not implement reconnect as an untracked goroutine.

### 5. Shared authoritative bulk cleanup

For `Disconnect(ctx, id)` where `id < 0`:

1. If a bulk operation is already active, attach the caller to its `done` result. Do not start another operation.
2. Otherwise, under `mu`, set `reconcileRequired`, advance the generation only when transitioning from reconciled to unresolved, detach all current entries into retirement, include entries already retiring, publish the shared bulk operation, and notify state waiters. Retries of the same unresolved cleanup keep the same generation.
3. Outside `mu`, request cancellation for in-flight connect/reconnect operations and start missing cleanup attempts.
4. The shared operation uses its own `context.Background()`-derived 30-second deadline. An HTTP request only stops waiting for the result; it does not abandon authoritative server-side cleanup.
5. Wait by re-checking tracked state after notifications. Do not use a `WaitGroup` helper that leaks forever when one manager never returns.
6. Complete successfully only when all covered entries retired successfully. Then clear `reconcileRequired` and allow new connections.
7. On timeout or non-idempotent cleanup error, return failure, retain `reconcileRequired` and every unresolved entry, and clear only the active attempt. The next bulk request retries an entry whose previous cleanup call returned an error, or attaches to cleanup still running.
8. A retry must never start a second `Manager.Disconnect` while the previous call is still running.

This gives Gateway the required contract: `202` proves the node is clean; `503`, a client timeout, or transport failure keeps the node unavailable and retryable.

### 6. Make the production manager cancellable and truthful

In `core/connection/manager.go`, add an unexported capability used by the multi-manager through type assertion:

```go
type lifecycleManager interface {
    Manager
    ConnectContext(context.Context, identity.Identity, common.Address, ProposalLookup, ConnectParams) error
    CancelCurrentOperation()
    DisconnectContext(context.Context) error
    ReconnectContext(context.Context) error
}
```

Production `connectionManager` must implement it; legacy/fake `Manager` implementations may use the coordinator's isolated worker fallback.

`CancelCurrentOperation` requirements:

- copy the current cancel function under `ctxLock`, release the lock, then invoke it;
- nil-safe;
- no status transition, cleanup, network call, or wait;
- safe to call repeatedly.

Preserve the old public methods as compatibility wrappers: `Connect` calls `ConnectContext(context.Background(), ...)`, `Disconnect` calls `DisconnectContext(context.Background())`, and `Reconnect` calls `ReconnectContext(context.Background())` and logs its error.

For `ConnectContext`, create the manager's lifetime context after the not-connected guard but before proposal lookup/validation. Never overwrite it later in the same connect. Use `context.AfterFunc` to cancel that lifetime context if the request context ends during establishment. Before publishing success, stop the callback and resolve its cancellation race; if cancellation already won, return `ErrConnectionCancelled`. After success, request cancellation must no longer affect the connection. Check cancellation after external stages, and cancel the lifetime context on every error path after creating it.

The manager's lifetime context must remain separate from the HTTP request context: deriving it directly from the request would tear down every successful tunnel as soon as the handler returned.

Refactor disconnect coordination so `DisconnectContext`:

- starts cleanup once;
- attaches to the actual existing cleanup when state is already `Disconnecting`;
- returns nil only after that cleanup completes;
- returns the context error when its caller stops waiting while cleanup continues and remains observable;
- lets a later caller attach again;
- preserves legacy `Disconnect()` as `DisconnectContext(context.Background())`.

`ReconnectContext` must use the same deadline for the shared disconnect wait and subsequent reconnect. It must return an error instead of hiding it. It must not start a new connect after its context or generation was canceled.

Route every existing cleanup entry point (`Connect` failure, `Cancel`, payment/keepalive failure, explicit disconnect, and reconnect) through the same single-flight disconnect primitive. The old `disconnect()` becomes the private cleanup runner invoked once by that primitive; no caller may bypass completion tracking. Publish the new completion channel before exposing `Disconnecting`, and never hold its coordination mutex while running cleanup.

Replace the current behavior where `Disconnecting` immediately returns nil. That behavior would let bulk reconciliation falsely succeed while old cleanup is still stuck. Also replace the fragile recreation/read ordering of `cleanupFinished` with one clearly owned completion channel per cleanup attempt.

Do not make cleanup callbacks run concurrently. Preserve their reverse registration order and current error logging.

### 7. TequilAPI response behavior

In `tequilapi/endpoints/connection.go`:

- Pass the request context into lifecycle calls.
- Check context errors and `ErrLifecycleBusy` before `ErrConnectionCancelled`; map them to HTTP `503 Service Unavailable` using the existing `apierror.ServiceUnavailable()` helper. A disconnected client may never receive this response; the important requirement is that its handler returns.
- Preserve current mappings for `ErrAlreadyExists`, non-request `ErrConnectionCancelled`, `ErrNoConnection`, and other internal errors.
- Add `503` to the Swagger response comments for create/delete. Regenerate Swagger only if the repository's normal generator requires generated artifacts to change.

The API must not return `202` for a detached-but-unresolved manager.

### 8. Bounded Node shutdown

In `cmd/node.go`:

- Give bulk connection cleanup a five-second context deadline, held in an injectable/testable `Node` field with a five-second default.
- Record the disconnect error, but always stop the HTTP API, UI server, and sleep notifier.
- Return the collected error after all stops, using `errors.Join` where appropriate.
- Preserve `ErrNoConnection` as informational, not a failure.

Five seconds leaves time inside Docker's observed 10-second stop grace for the rest of `Dependencies.Shutdown`. A stuck cleanup worker may remain in memory, but process exit reclaims it; it must not prevent graceful process exit.

### 9. Diagnostics for the next incident

Add structured logs at bulk start, completion, failure, and timeout. Include:

- operation (`connect`, `disconnect`, `reconnect`, `disconnect_all`);
- port where applicable;
- generation;
- elapsed duration;
- counts and sorted port lists for current, connecting, reconnecting, retiring, and cleanup-running entries;
- whether the caller timed out while shared cleanup continued;
- wrapped error.

When a bulk generation first reaches its internal 30-second timeout, write one full goroutine dump to the normal Node log with `runtime/pprof.Lookup("goroutine").WriteTo(..., 2)`. Rate-limit it to once per unresolved generation; Gateway retries must not dump repeatedly. Emit another concise timeout log during shutdown, but do not dump again for the same generation.

No tokens, identities, proposal payloads, or credentials in these logs.

## Expected file set and implementation order

Expected edits:

- `core/connection/interface.go`
- `core/connection/multi.go`
- `core/connection/manager.go`
- `core/connection/multi_test.go`
- `core/connection/manager_test.go`
- `tequilapi/endpoints/connection.go`
- `tequilapi/endpoints/connection_test.go`
- `cmd/node.go` and a focused Node shutdown test file
- `sleep/sleep.go` and its affected tests/mocks
- `mobile/mysterium/entrypoint.go` and its affected tests/mocks
- generated TequilAPI docs only if the normal Swagger generator changes them

Allowed new focused files: `core/connection/multi_lifecycle.go`, `core/connection/multi_lifecycle_test.go`, and `cmd/node_test.go` when no equivalent test file exists.

Implementation order:

1. Add channel-controlled regression tests for the current deadlock and false-success cases.
2. Add the low-level context lifecycle capability and truthful single-flight disconnect completion.
3. Replace the multi-manager locks with the generation coordinator; make all new core tests pass under `-race`.
4. Change the `MultiManager` signatures and update every compiler-discovered caller/mock.
5. Add endpoint `503` behavior and endpoint tests.
6. Add bounded shutdown, shutdown tests, and timeout diagnostics.
7. Run formatting, targeted stress/race tests, build checks, the full repository gate, then staging acceptance.

Do not commit a knowingly non-building intermediate state. A single cohesive `fix: make node connection lifecycle bounded` commit is acceptable; otherwise every intermediate commit must compile and pass its affected tests.

## Tests to add or update

Keep all existing tests. Convert their signatures and expectations; do not delete them. Move new lifecycle tests to `core/connection/multi_lifecycle_test.go` so test files remain below 500 lines. Replace current busy-spin synchronization with explicit channels and bounded test deadlines.

Required deterministic tests:

1. Blocked connect plus bulk cleanup: bulk waiter returns by context deadline; after connect is released, the stale manager is disconnected exactly once and never published.
2. Blocked connect ignores cancellation: unrelated `Status`, `Stats`, and individual operations remain responsive; no coordinator lock is held by the worker.
3. Bulk generation race: a connect completing after generation advance cannot survive cleanup.
4. Bulk single-flight: concurrent bulk requests cause one low-level cleanup attempt.
5. Gateway-style retry: first request times out, cleanup later completes, next bulk request succeeds.
6. Stuck cleanup: bulk returns failure, new connects fail promptly with `ErrLifecycleBusy`, same-port reuse is impossible, and retries do not duplicate the running cleanup.
7. Cleanup error: error is returned and retained; a later successful retry clears reconciliation.
8. Individual disconnect racing connect: no orphan, false success, duplicate cleanup, or port reuse.
9. Reconnect racing bulk: late reconnect is retired and cannot restore the old generation.
10. Two unrelated ports operate concurrently; removal of stripes introduces no race.
11. `go test -race` stress loop over connect/disconnect/bulk operations leaves no current or retiring entries after successful reconciliation.
12. Low-level manager: cancellation while waiting for connected state returns `ErrConnectionCancelled` and cleanup completion is observable.
13. Low-level manager: two concurrent disconnect callers share cleanup; neither receives nil before cleanup completes.
14. Endpoint create/delete pass request cancellation and map lifecycle busy/deadline to `503`; successful authoritative delete remains `202`.
15. `Node.Kill`: a blocking multi-manager reaches its deadline, all three stop methods are still called, and the error is returned within a small injected test timeout.

Avoid `time.Sleep` as race coordination. Use started/release/done channels and `select` with a short safety timeout.

## Verification gates

Run from `shellroute/node`:

```bash
gofmt -w <changed-go-files>
go test -race ./core/connection -count=20 -timeout=120s
go test -race ./tequilapi/endpoints ./cmd ./sleep -count=1 -timeout=120s
go test ./mobile/mysterium -count=1 -timeout=120s
./scripts/run-tests.sh
go build ./core/connection/... ./tequilapi/... ./cmd/... ./sleep/... ./mobile/mysterium/...
```

Then run the repository's normal full test/lint gate (`make test` or the CI-equivalent command documented by the repo). Record any platform-only failure verbatim; do not silently skip it.

Manual staging acceptance using the sibling ShellRoute deployment:

1. Start Myst nodes and Gateway with the candidate Node image.
2. Establish at least one tunnel on each Myst node.
3. Restart Gateway only. Confirm each startup `DELETE /connection?id=-1` either returns `202` after real cleanup or keeps that node unavailable with an explicit retryable error.
4. Confirm successful cleanup allows pool creation without restarting Myst.
5. During an intentionally blocked test cleanup, confirm repeated Gateway retries do not increase Node cleanup-worker count and no new tunnel is accepted.
6. Send SIGTERM during the blocked cleanup. Confirm the Node exits inside Docker's 10-second grace period without `signal: killed`/forced-kill evidence.
7. Confirm timeout logs contain the lifecycle snapshot and one goroutine dump, and ordinary successful operation produces no dump.

## Definition of done

- Every invariant above is covered by code and deterministic tests.
- Gateway's fail-closed cleanup contract is unchanged.
- Normal connect, disconnect, reconnect, mobile, sleep/wake, and shutdown call sites compile and retain behavior.
- No global/per-port operation lock spans external work.
- A non-cooperative manager cannot block an HTTP handler past its context or block Node shutdown past five seconds.
- Bulk success cannot precede real cleanup completion.
- Race tests and repository gates pass.
- Changes are committed with Conventional Commit messages; only Node repository files are staged; nothing is pushed.

## Prompt for the implementing agent

Implement `plans/node-connection-lifecycle-recovery.md` exactly in the `shellroute/node` repository.

Work only in Node; do not modify `shellroute`, `gateway`, Caddy, deployment scripts, or monitoring policy. Before editing, verify the current branch is a non-main implementation branch and contains `c035faac`/`0a2d83d5`. Per repository rules, do not create or switch branches yourself; if the supplied branch is wrong, stop and report it.

Replace the lock-spanning lifecycle implementation with the generation-fenced coordinator described here. Preserve authoritative fail-closed bulk cleanup: never return success for detached, canceled, timed-out, failed, or still-running cleanup. Propagate request contexts, make production manager cancellation non-blocking, make disconnect completion truthful, bound shutdown, and add the timeout diagnostics and every listed regression test. Keep the fork delta targeted; do not redesign unrelated connection code.

Run all verification gates and the staging acceptance that is available. Fix failures within this scope. Commit only your changed Node files using concise Conventional Commit messages. Do not push. In the handoff, list commits, tests, any unavailable staging step, and a direct production-readiness verdict.
