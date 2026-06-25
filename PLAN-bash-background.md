# Plan: add a "run command in background" capability to opencode

## Status: IMPLEMENTED (experimental, behind `OPENCODE_EXPERIMENTAL_BACKGROUND_RUN`)

Done: `kind` tagging in `BackgroundMonitorManager` (+ `stopAllForSessionByKind`); `monitor`
re-arm now stops only `kind:"monitor"`; `experimentalBackgroundRun` flag; `bash_background` +
`bash_background_stop` tools; registry wiring; core + tool unit tests green; typecheck clean.

**Gotcha found during impl:** the manager runs commands via `eval <JSON.stringify(command)>`
(see `Shell.args`), so the output-capture wrapper MUST be a single line — a literal newline gets
re-escaped to a `\n` token and corrupts the command. The wrapper is `( <cmd> ) > '<log>' 2>&1`.

## Context

opencode has a `monitor` tool (experimental, `OPENCODE_EXPERIMENTAL_MONITOR`) that spawns a
long-lived shell command and wakes the model **once per stdout line**. What it lacks is the
Claude Code **`Bash run_in_background`** semantic: run a command detached, return immediately,
capture its full output to a file the model can read anytime, and wake the model **exactly once
when the command exits** (with the exit code). This is the natural tool for "kick off the build /
test run / long script and tell me when it's done" — where per-line events would be noise.

This same capability is already designed in the **cline** harness: a separate `start_monitor` /
`stop_monitor` / `list_monitors` tool family on a `BackgroundMonitorManager`, with a designed
"Bash mode" (`wakeOnLine:false`, tee output to a logfile, notify once on exit). We mirror that
"Bash mode" in opencode.

Crucially, **all the hard plumbing already exists in opencode** and is reused unchanged:
- `BackgroundMonitorManager` (`packages/core/src/background-monitor.ts`) — detached spawn,
  process-group tree-kill, PID tracking, `onEvent`/`onExit` callbacks, `timeoutMs`, per-session
  registry, finalizer cleanup.
- The async wake-loop: `ctx.extra.promptOps.prompt(...)` → `prompt()` → `loop()` injects a
  synthetic user message and re-invokes the model (`packages/opencode/src/session/prompt.ts`).
  Already injected into every tool context by `SessionTools.resolve` (`session/tools.ts:46`).
- CLI keep-alive: `getMonitorCount` / `stopAllForSessionSync` / `whenIdleForSession` already keep
  the non-interactive process alive while any manager entry is active — background runs are
  counted automatically, so the CLI won't exit out from under them.

So this is **new surface over existing infra**, not new infrastructure.

## Recommended approach

A new flag-gated tool **`bash_background`** plus a companion **`bash_background_stop`**, both
backed by the existing `BackgroundMonitorManager`. Output is captured by redirecting the command's
combined stdout+stderr to a logfile (no per-line events), and the model is woken once on exit.

### Behavior (model-facing)

- `bash_background({ command, description })` →
  - asks permission (same `ctx.ask` pattern as `monitor`/`shell`),
  - arms a detached run, returns **immediately** with:
    `Background run armed (bg-0) for "<description>". PID <pid>. Output streaming to <logpath>. You'll be notified when it exits; Read the logfile to check progress.`
  - On exit, a synthetic message is injected:
    `[Background: <description>] exited (<exit code N>). Output in <logpath>.`
- Read progress anytime via the existing **Read** tool on `<logpath>`.
- `bash_background_stop({ id })` → tree-kills that run (no exit notification, since it's intentional).
- **No auto-timeout in v1** (matches Claude `run_in_background`: runs until it exits or the session
  ends). Use `bash_background_stop` to cancel.

### Output capture (no manager change needed for this part)

Reuse `BackgroundMonitorManager.start` but wrap the user command so all output lands in a logfile:

```
{ <user command> ; } > '<logpath>' 2>&1
```

- `<logpath>` = a session-scoped temp file under opencode's `Global` temp dir (no spaces → safe to
  quote). Mirror cline's `createTempFilePath("agent-background")`.
- Because output is redirected to the file, the manager's stdout stream stays empty → `onEvent`
  never fires (no per-line wake spam). `onExit(reason)` still fires on process exit (it awaits
  `handle.exitCode` independent of stdout), carrying `exit code <N>`.

### Required manager change: tag entries by kind (small)

`packages/core/src/background-monitor.ts`:
- Add `kind: "monitor" | "background"` to `Info` and `StartInput` (default `"monitor"`).
- Add `stopForSessionByKind(sessionID, kind)` (or a `kind?` filter on `stopAllForSession`).
- **Why:** the `monitor` tool calls `manager.stopAllForSession()` on arm (`monitor.ts:67`) to enforce
  one monitor per session. Background runs share this manager, so without a kind filter, arming a
  monitor would kill all background runs (and vice-versa). Fix: `monitor.ts` re-arm stops only
  `kind:"monitor"`; full session cleanup (`stopAllForSession`, abort/dispose) still stops everything.
- Background runs do **not** call any stop-all on arm — they're concurrent and coexist with monitors
  and with each other.

### New tool files

- `packages/opencode/src/tool/bash-background.ts` — modeled on `tool/monitor.ts`:
  - yields `BackgroundMonitorManager.Service`, `Session.Service`, `RuntimeFlags.Service`,
  - gate: `if (!flags.experimentalBackgroundRun) Effect.die(...)`,
  - reads `ctx.extra.promptOps`, builds the wrapped command + logpath,
  - `manager.start({ kind:"background", command: wrapped, description, cwd, onEvent: noop,
    onExit: reason => ops.prompt({ parts:[synthetic exit message + logpath] }) then ops.wake })`,
  - returns the arm message with id/pid/logpath.
- `packages/opencode/src/tool/bash-background.txt` — model-facing description. Must state: returns
  immediately; one notification on exit; read the logfile for progress; **prefer plain `bash` for
  short commands**; use `monitor` (not this) when you need an event per line; combined stdout+stderr
  is captured.
- `packages/opencode/src/tool/bash-background-stop.ts` — tiny tool: `{ id }` → `manager.stop(id)`.

### Feature flag

`packages/opencode/src/effect/runtime-flags.ts` — one line, same pattern as `experimentalMonitor`:
```ts
experimentalBackgroundRun: enabledByExperimental("OPENCODE_EXPERIMENTAL_BACKGROUND_RUN"),
```

### Registry wiring

`packages/opencode/src/tool/registry.ts`:
- `import { BashBackgroundTool } from "./bash-background"` and the stop tool.
- `const bashbg = yield* BashBackgroundTool` (+ stop) near the other `yield*` tool inits (~line 101).
- Add to the `Effect.all({...})` init block: `bashBackground: Tool.init(bashbg), ...`.
- Add to the `builtin` array gated by the flag (mirror line 240):
  `...(flags.experimentalBackgroundRun ? [tool.bashBackground, tool.bashBackgroundStop] : [])`.
- The `ToolRegistry` already provides `BackgroundMonitorManager` in its node deps (used by `monitor`),
  so no new layer wiring is required.

### Keep-alive / cleanup (mostly free)

- CLI non-interactive keep-alive already defers exit while `getMonitorCount(sessionID) > 0`; background
  runs increment that count via `manager.start`, so they keep the process alive with no change.
- Session abort/dispose already calls `stopAllForSession` (all kinds) → background runs are tree-killed
  on cleanup. Verify the abort path in `cli/cmd/run/runtime.ts` `onInterrupt` and the manager finalizer
  cover background kind (they call `stopAllForSession`, which stops all kinds — OK).

## Files to modify / create

- **create** `packages/opencode/src/tool/bash-background.ts`
- **create** `packages/opencode/src/tool/bash-background.txt`
- **create** `packages/opencode/src/tool/bash-background-stop.ts`
- **edit** `packages/core/src/background-monitor.ts` — add `kind` + kind-filtered stop
- **edit** `packages/opencode/src/tool/monitor.ts` — re-arm stops only `kind:"monitor"`
- **edit** `packages/opencode/src/effect/runtime-flags.ts` — add `experimentalBackgroundRun`
- **edit** `packages/opencode/src/tool/registry.ts` — import, init, flag-gated builtin spread
- **create** `packages/core/test/background-monitor.test.ts` additions — kind filter unit test
- **create** `packages/opencode/test/tool/bash-background.test.ts` — arm/return/onExit notify
- **edit** `packages/opencode/test/tool/registry.test.ts` — if it asserts exact builtin counts

(After approval, also copy this plan into the opencode repo per global convention, e.g.
`opencode-source/opencode/PLAN-bash-background.md`.)

## Open / deferred (not in v1)

- **`list` tool** — `manager.list`/`listForSession` already exist; expose as `bash_background_list`
  later if the model struggles to track ids.
- **Timeout that notifies** — current manager suppresses `onExit` on intentional stop (incl. timeout).
  v1 has no auto-timeout, so this is moot; if added later, distinguish "timeout" from "user stop" and
  fire `onExit` for timeout.
- **stderr separation** — v1 merges stderr into the logfile via `2>&1` (what you usually want). If
  separate streams are ever needed, the manager exposes them.

## Verification (end to end)

1. Build the workspace (bun) and run the unit tests:
   `bun test packages/core/test/background-monitor.test.ts packages/opencode/test/tool/bash-background.test.ts packages/opencode/test/tool/registry.test.ts`
   - Asserts: kind-filtered stop leaves background runs alive when a monitor re-arms; arming returns
     immediately with id/pid/logpath; `onExit` injects exactly one synthetic message with the code.
2. Live smoke test (gated, like `OPENCODE_LIVE_MONITOR_TEST`):
   `OPENCODE_EXPERIMENTAL_BACKGROUND_RUN=true opencode "run 'sleep 2; echo hi; exit 3' in the background and tell me when it finishes"` (with `--auto-approve --timeout 60s`).
   - Expect: immediate "armed" reply with a logfile path; ~2s later one assistant message reporting
     exit code 3; the logfile contains `hi`; no leaked child processes after exit.
3. Coexistence: arm a `monitor` and a `bash_background` in the same session; re-arm the monitor;
   confirm the background run is **not** killed (proves the kind filter), and that session abort kills
   both.
