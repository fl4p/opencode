# Persistent `Monitor` wake-loop for opencode

> **Upstream repo:** `https://github.com/anomalyco/opencode` (origin remote confirmed by `git remote -v`).

## Problem

`opencode "monitor file ./f for changes and tell me when it happens"` cannot work today.

The CLI has a **background subagent** primitive (`task` with `background=true`, added in #15994 / #31173 / #13261) that keeps the process alive while a child session runs and wakes the parent when results arrive. But there is no generic **background shell monitor** primitive: a tool that spawns an arbitrary long-lived command (e.g. `fswatch`, `inotifywait`, `tail -f`) and re-invokes the model on every stdout line.

## Related work (archived PRs on `anomalyco/opencode`)

A number of **archived / stale PRs** explored the **wake loop** and **keep-alive** concepts, but **none are merged** into the current `dev` branch. They are all scoped to **background subagents** (`task` tool with `background=true`), not to a generic shell monitor:

| PR | Status | What it does | Why it is not a `Monitor` tool |
|---|---|---|---|
| #15994 | Archived/open | Background subagent execution (`task` + `background=true`). Forks a child session, wakes parent on completion. | Watches a **child session**, not an arbitrary shell command. |
| #31173 | Archived/open | V2 `task` tool with background child sessions. | Same as above. |
| #13261 | Archived/open | `BackgroundTaskCompleted` event flushed into parent session. | Same as above. |
| #28047 | Archived/open | Keep-alive: prevents the CLI from exiting while background subagents are still running. | **Not in `dev`**. The current `run.ts` still breaks on the first `session.status` idle. |
| #29759 | Archived/open | UX fix for continuing an already-running background task. | No new primitive. |
| #31973 | Archived/open | Provider model refresh running in background. | Unrelated. |
| #17198 | Archived/open | `/btw` TUI command for background sessions. | UI-only, no new tool. |
| #29831, #29108 | Archived/open | Spawn exit/close handling fixes. | Reliability improvements a monitor manager would rely on, but not merged. |

**Key insight:** the current `dev` branch has **no** CLI keep-alive for background work. The non-interactive loop in `packages/opencode/src/cli/cmd/run.ts` breaks on the first `session.status` idle and exits immediately. We must implement the keep-alive logic ourselves.

The `Monitor` tool proposed here is a **new, distinct primitive**: it watches an arbitrary shell process (not a child session) and re-invokes the model on every stdout line.

## Existing infrastructure we can reuse

- **Wake loop** — `SessionExecution.wake(sessionID, seq?)` (`packages/core/src/session/execution.ts`) schedules a drain of the session runner. `SessionRunCoordinator` (`packages/core/src/session/run-coordinator.ts`) coalesces multiple wakes, runs at most one drain per session at a time, and automatically queues a follow-up run if a new wake arrives while a drain is active.
- **Durable prompt admission** — `SessionPrompt.prompt({ sessionID, parts, noReply: true })` (`packages/opencode/src/session/prompt.ts`) admits a synthetic user message into the session input table without immediately invoking the loop. When `SessionExecution.wake` is called, the runner promotes the admitted prompt and starts a new turn.
- **Background subagents** — the `task` tool with `background=true` uses `BackgroundJob` (`packages/core/src/background-job.ts`) to fork child sessions. This proves the architecture works end-to-end, but it is session-to-session, not process-to-session.

**What is missing in `dev`:**
- **CLI keep-alive for background work** — the non-interactive loop breaks on first idle; no code defers exit while background subagents or monitors are alive.
- **Generic process monitor** — `BackgroundJob` tracks child sessions, not arbitrary long-lived shell commands.
- **Monitor built-in tool** — no tool exists to arm a shell watcher and wake the model on stdout events.

So the missing pieces are:
- a **process registry** that keeps arbitrary child shell processes alive across turns (distinct from `BackgroundJob`, which tracks subagent sessions),
- a **`Monitor`** built-in tool that can start a shell process and inject prompts on stdout events,
- and **CLI keep-alive wiring** that defers exit while monitors are active.

## Design

### 1. `BackgroundMonitorManager` (new core service)

`packages/core/src/background-monitor.ts`

A process-local, session-scoped registry of long-lived child processes. Uses Effect's `ChildProcessSpawner` and `Scope` for lifecycle management.

```ts
export interface MonitorInfo {
  id: string
  sessionID: SessionSchema.ID
  command: string
  description: string
  cwd: string
  status: "running" | "exited" | "stopped"
  exitReason?: string
}

export interface Interface {
  readonly start: (input: {
    sessionID: SessionSchema.ID
    command: string
    description: string
    cwd: string
    timeoutMs?: number
    onEvent: (line: string) => Effect.Effect<void>
    onExit: (reason: string) => Effect.Effect<void>
  }) => Effect.Effect<MonitorInfo>

  readonly stop: (id: string) => Effect.Effect<void>
  readonly stopAllForSession: (sessionID: SessionSchema.ID) => Effect.Effect<void>
  readonly countForSession: (sessionID: SessionSchema.ID) => Effect.Effect<number>
  readonly whenIdleForSession: (
    sessionID: SessionSchema.ID,
    signal: AbortSignal,
  ) => Effect.Effect<void>
}
```

- `start` spawns a detached shell via `ChildProcessSpawner` (reusing `Shell.preferred` / `Shell.args` from `packages/core/src/shell.ts`), `stdio: ["ignore", "pipe", "pipe"]`, process-group so we can kill the tree.
- Buffers stdout lines, calls `onEvent(line)` for **every** non-empty line. Does NOT kill after the first line.
- On exit/timeout, calls `onExit(reason)` and deregisters.
- `whenIdleForSession` resolves when the session has zero active monitors. If `signal` aborts, it resolves immediately after stopping the session's monitors.
- Instance-scoped via `InstanceState` so each open project gets its own registry, automatically cleaned up on disposal.

**Layer wiring**: add to `packages/core/src/background-monitor.ts` with `defaultLayer`, `node`, and provide it in `packages/opencode/src/tool/registry.ts` node dependencies.

### 2. `Monitor` tool (new built-in)

`packages/opencode/src/tool/monitor.ts`

A host-backed, non-blocking built-in tool. It yields `BackgroundMonitorManager`, `Session`, and `RuntimeFlags` at init time, and reuses the existing `ctx.extra.promptOps` wake loop (the same mechanism used by `TaskTool`) to admit synthetic messages and wake the session. This avoids adding a hard dependency on `SessionPrompt` / `SessionExecution` to the tool init, because those would create a dependency cycle (`SessionPrompt` depends on `ToolRegistry`).

```ts
export const MonitorTool = Tool.define(
  "monitor",
  Effect.gen(function* () {
    const manager = yield* BackgroundMonitorManager.Service
    const sessions = yield* Session.Service
    const flags = yield* RuntimeFlags.Service

    return {
      description: DESCRIPTION,
      parameters: Schema.Struct({
        command: Schema.String,
        description: Schema.String,
        timeoutMs: Schema.optional(Schema.Number),
      }),
      execute: (params, ctx) =>
        Effect.gen(function* () {
          if (!flags.experimentalMonitor) {
            return yield* Effect.die(new Error("Monitor tool requires OPENCODE_EXPERIMENTAL_MONITOR=true"))
          }

          const ops = ctx.extra?.promptOps as TaskPromptOps
          // ... permission ask ...

          const session = yield* sessions.get(ctx.sessionID).pipe(Effect.orDie)

          yield* manager.stopAllForSession(ctx.sessionID)
          yield* manager.start({
            sessionID: ctx.sessionID,
            command: params.command,
            description: params.description,
            cwd: session.directory,
            timeoutMs: params.timeoutMs,
            onEvent: (line) =>
              Effect.gen(function* () {
                yield* ops.prompt({ ... event message ... }).pipe(Effect.ignore)
                yield* ops.wake(ctx.sessionID)
              }),
            onExit: (reason) =>
              Effect.gen(function* () {
                yield* ops.prompt({ ... exit message ... }).pipe(Effect.ignore)
                yield* ops.wake(ctx.sessionID)
              }),
          }).pipe(Effect.provide(CrossSpawnSpawner.defaultLayer))

          return {
            title: params.description,
            metadata: { monitor: true, monitorId: info.id, description: params.description },
            output: `Monitor armed (${info.id}) for "${params.description}". Events will arrive as new messages. Do not re-arm it unless you need a different watch.`,
          }
        }),
    }
  }),
)
```

To make `ops.wake` available, `TaskPromptOps` was extended with a `wake(sessionID)` method and `SessionPrompt.ops` now provides it.

Key semantics:
- **Returns immediately** — the monitor is armed, the tool call ends, the assistant turn ends.
- **Persistent** — runs until the command exits, the session aborts, or the CLI process exits.
- **Wake-on-exit** — if the command exits (e.g. `inotifywait` missing on macOS), the model is woken so it can re-arm with a working command.
- The tool is **idempotent-ish** — if the model calls `Monitor` again for the same description, we should either ignore (already armed) or stop the old one and start a new one. Implement `stopAllForSession` before re-arming, or key monitors by `sessionID + description`.

### 3. Wiring into the tool registry

`packages/opencode/src/tool/registry.ts`:

1. Import `MonitorTool`.
2. In the `Effect.all` tool init block, add `monitor: Tool.init(monitor)`.
3. In the `builtin` array, add `tool.monitor` after `tool.patch` (or near the end).
4. Add `BackgroundMonitorManager` to the `defaultLayer` and `node` dependencies of `ToolRegistry`.

### 4. Keep the CLI process alive (non-interactive mode)

`packages/opencode/src/cli/cmd/run.ts` (non-interactive branch, around line 763-807):

The non-interactive loop currently breaks on the first `session.status` idle event and exits. We need to defer exit while monitors are active.

**Approach: simple deferred promise, no HTTP polling.**

1. After `session.prompt()` or `session.command()` is sent, the event loop subscribes to `client.event.subscribe()` and processes events.
2. When the session goes `idle` for the first time, do NOT break immediately. Instead, create a `Promise.race`:
   - `BackgroundMonitorManager.whenIdleForSession(sessionID)` resolves when the monitor count hits 0 (or immediately if no monitors are active).
   - If a new `session.status` event arrives with `busy`, the monitor manager is still alive, so we just keep the loop going.
3. Only break and exit when the session is idle **and** `whenIdleForSession` has resolved.
4. On SIGINT / `--timeout` / abort, call `BackgroundMonitorManager.stopAllForSession(sessionID)` (which causes `whenIdleForSession` to resolve immediately) before exiting.

Because `whenIdleForSession` is an Effect operation, we can run it via `Effect.runPromise` in the CLI async code:

```ts
// In the event loop, on session.status idle
if (event.type === "session.status" && event.properties.sessionID === sessionID) {
  if (event.properties.status.type === "idle") {
    const isIdle = await Effect.runPromise(
      manager.whenIdleForSession(sessionID).pipe(Effect.timeout("1 second")),
    ).catch(() => true)
    if (isIdle) {
      break
    }
    // Monitors are still active, continue the event loop
    continue
  }
}
```

Or simpler: don't `break` on the first idle at all. Instead, wrap the loop in an outer `while` that checks `manager.countForSession` after the first idle completes:

```ts
// After the initial turn settles (loop returns on idle)
await Effect.runPromise(
  manager.whenIdleForSession(sessionID),
)
// Only now can we safely exit
```

### 5. Skip HTTP API for V1

No new HTTP endpoints needed. The CLI keep-alive logic will use a direct `BackgroundMonitorManager.whenIdleForSession(sessionID)` call (or equivalent promise/effect) instead of polling via the SDK.

The manager is a core Effect service, and in local mode the CLI runs in the same process. A direct method call is cleaner than HTTP polling. If attach mode or UI introspection is needed later, the HTTP API can be added as a follow-up.

### 6. Session cleanup — stop monitors on abort / dispose

`packages/opencode/src/session/session.ts` (or the session disposal path):

When a session is removed, aborted, or the instance is disposed, stop all monitors for that session.

- In `Session.abort` / `Session.dispose`, if available, yield `BackgroundMonitorManager.stopAllForSession(sessionID)`.
- In the CLI `onInterrupt` handler (`packages/opencode/src/cli/cmd/run/runtime.ts`), add `manager.stopAllForSession(state.sessionID)` before `ctx.sdk.session.abort(...)`.
- In `BackgroundMonitorManager`'s own `Effect.addFinalizer`, stop all monitors on scope closure.

### 7. System prompt / catalog

`packages/opencode/src/session/prompt/anthropic.txt` (and `default.txt`, `gpt.txt`, etc.):

Add a short description of the `Monitor` tool to the available tools section, or rely on the tool description injected by the registry. The registry description is sufficient for modern models.

No routing alias is needed — `monitor` is already a clean name.

### 8. E2E test

`packages/opencode/test/cli/monitor.e2e.test.ts` (or `apps/cli/src/cli.monitor.e2e.test.ts` if a CLI package exists):

1. Create a temp directory with a file `./f`.
2. Spawn the real CLI: `opencode "watch file ./f and tell me when it changes"` (with `--auto-approve` and `--timeout 120s`).
3. Wait until stdout shows the monitor armed message.
4. Append to `./f` twice with a 2-second gap.
5. Assert stdout contains **≥2** assistant responses reacting to the changes (proves the wake loop re-invoked the model each time without the process exiting).
6. Kill the CLI / let timeout end; assert no leaked child processes.
7. Gate by `OPENCODE_LIVE_MONITOR_TEST=1` so it only runs in CI when explicitly enabled.

### 9. Tests touched

- `packages/opencode/test/tool/registry.test.ts` — add `MonitorTool` to the expected built-in list.
- `packages/opencode/test/session/prompt.test.ts` — if any tests assert exact tool counts, update them.
- `packages/core/test/background-monitor.test.ts` — unit tests for `BackgroundMonitorManager` start/stop/count/whenIdle.

## Status / checklist

- [x] `BackgroundMonitorManager` core service (`packages/core/src/background-monitor.ts`)
- [x] `Monitor` built-in tool (`packages/opencode/src/tool/monitor.ts`)
- [x] Tool registry wiring (`packages/opencode/src/tool/registry.ts`)
- [ ] CLI non-interactive keep-alive (`packages/opencode/src/cli/cmd/run.ts`) — started by other agent; imports `getMonitorCount` / `stopAllForSessionSync` from core
- [ ] Interactive mode cleanup (`packages/opencode/src/cli/cmd/run/runtime.ts` onInterrupt) — other agent
- [ ] Session disposal cleanup (`packages/opencode/src/session/session.ts` or abort path) — other agent
- [x] `RuntimeFlags.experimentalMonitor` toggle (`OPENCODE_EXPERIMENTAL_MONITOR=true`)
- [ ] E2E test green (gated by `OPENCODE_LIVE_MONITOR_TEST=1`) — other agent
- [x] Core unit tests for `BackgroundMonitorManager` (`packages/core/test/background-monitor.test.ts`)
- [x] Tool unit test (`packages/opencode/test/tool/monitor.test.ts`)

## Implementation notes / refinements

- **Wake loop via `ctx.extra.promptOps`**: to avoid a dependency cycle (`SessionPrompt` depends on `ToolRegistry`), the `Monitor` tool does not directly yield `SessionPrompt.Service` or `SessionExecution.Service`. Instead it reuses the `promptOps` object already passed to tools by `SessionTools.resolve`. `TaskPromptOps` was extended with `wake(sessionID)` so the monitor can both admit a synthetic message and wake the session runner.
- **ChildProcessSpawner lifecycle**: `BackgroundMonitorManager.start` spawns via `ChildProcessSpawner` and requires `ChildProcessSpawner` in its service interface. The `Monitor` tool satisfies this internally with `Effect.provide(CrossSpawnSpawner.defaultLayer)`. `stop` explicitly calls `handle.kill()` to terminate the process tree before closing the monitor scope.
- **Monitor idempotency**: the tool stops all existing monitors for the session before arming a new one, so re-arming replaces the previous watch rather than stacking monitors.
- **CLI keep-alive edge case**: if the model goes idle and the monitor fires immediately, the `session.status` event stream may emit `idle` then `busy` very quickly. The CLI loop should not race and miss the transition. Use `shouldExit` deferred + explicit `countForSession` check on every idle event rather than a simple `continue`.
- **stderr handling**: the monitor manager swallows stderr; `ChildProcessSpawner` gives us separate streams if we want to log it later.
- **Timeout semantics**: `timeoutMs` defaults to omitted (run until process exit). In practice, most CLI monitor commands will use a short timeout during testing.
- **No `PushNotification`**: per requirements, we are not adding a `PushNotification` tool. The model should simply use the normal text response to inform the user. If a desktop ping is needed later, it can be added as a separate tool without changing the monitor architecture.