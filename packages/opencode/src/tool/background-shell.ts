import { Cause, Effect, Schema, Scope, Stream } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { Shell } from "@opencode-ai/core/shell"
import { getMonitorCount, monitorPid, monitorStarted, monitorStopped } from "@opencode-ai/core/background-monitor"
import { EventV2 } from "@opencode-ai/core/event"
import { WakeScope } from "@opencode-ai/core/background-job"

// Live count of running background jobs (monitor + bash_background) for a session.
// Published so the MAIN-thread TUI footer can render the count: the module-global
// shim (getMonitorCount/_sessionCounts) lives in the WORKER thread that runs the
// tools, so the renderer can't read it directly — but the worker forwards every
// EventV2 to the TUI client, so the count rides across the boundary as an event.
export const BackgroundJobsEvent = EventV2.define({
  type: "session.background-jobs",
  schema: {
    sessionID: Schema.String,
    count: Schema.Number,
  },
})

// Coalesce lines arriving within this window into one wake (one model turn).
const BATCH_WINDOW_MS = 200
// Kill a runaway watcher after this many lines rather than flood the session.
const FLOOD_MAX_LINES = 5000

/** Build a detached login-shell command (same env/flags the old monitor used). */
export function makeShellCommand(command: string, cwd: string): ChildProcess.Command {
  const shell = Shell.acceptable()
  // Parent-death watchdog. The child is `detached` (its own process group / session
  // leader via setsid) so graceful teardown can tree-kill it with `kill -- -pid`.
  // But on NON-graceful opencode death (SIGKILL / tile-close / crash) no JS finalizer
  // runs, the child reparents to launchd, and the watcher leaks forever (seen live:
  // 8-9 day old `while :; ... stat ./f` orphans). Fix: spawn a tiny background guard
  // that polls opencode's pid (passed in as OPENCODE_PARENT_PID, unambiguous vs $PPID)
  // and SIGTERMs our own process group once it disappears, bounding orphan life to
  // ~the poll interval. Must be a SINGLE line: Shell.args runs `eval <JSON.stringify>`,
  // so a literal newline corrupts into a `\n` token.
  // The watchdog's stdio is redirected to /dev/null: as a background job it inherits
  // the command's stdout pipe, and leaving it attached would hold the pipe open so the
  // reader never sees EOF when a short command exits (hanging exit-notify).
  const guarded =
    process.platform === "win32"
      ? command
      : `( while kill -0 "$OPENCODE_PARENT_PID" 2>/dev/null; do sleep 2; done; kill -- -$$ 2>/dev/null ) </dev/null >/dev/null 2>&1 & ${command}`
  const args = Shell.args(shell, guarded, cwd)
  return ChildProcess.make(shell, args, {
    cwd,
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      OPENCODE_PARENT_PID: String(process.pid),
      TERM: "xterm-256color",
      PAGER: "cat",
      GIT_PAGER: "cat",
    },
  })
}

/**
 * A fully-provided (`R = never`) Effect suitable as a `BackgroundJob` `run`:
 * spawns `command`, tracks session liveness + PID (so the CLI defers exit and
 * SIGINT can kill it), and (when `onBatch` is given) delivers stdout as
 * coalesced line batches. The process is killed when the job's scope closes
 * (i.e. on `BackgroundJob.cancel` / session teardown) via the scoped spawn + an
 * explicit kill finalizer. Resolves to the exit reason string.
 */
export function runShellJob(opts: {
  sessionID: string
  command: ChildProcess.Command
  // Called with a BATCH of stdout lines (joined by "\n") coalesced over a short
  // window. Receiving batches — not single lines — is what bounds wake frequency.
  onBatch?: (batch: string) => Effect.Effect<void>
  // Called with the live running-job count right after this job is registered and
  // again right after it ends, so the caller can publish it (e.g. as an EventV2 the
  // TUI footer consumes). Must be fully-provided (R = never) — build it from an
  // already-resolved EventV2Bridge in the tool, not from ambient services here.
  onCount?: (count: number) => Effect.Effect<void>
}): Effect.Effect<string, unknown, ChildProcessSpawner> {
  return Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner
      // The job's own scope (provided by Effect.scoped). The reader and the debounce
      // timer fork into THIS — not detached: forkDetach makes daemon fibers no scope
      // ever interrupts (proven against the Effect internals), leaking a ref'd timer
      // past teardown (hang) and orphaning in-flight work. job-scoping keeps them
      // interruptible on cancel/teardown. (The model WAKE forks into wakeScope below,
      // for a different reason — see there.)
      const jobScope = yield* Scope.Scope
      // Session-lifetime scope (provided by BackgroundJob.start). Model wakes fork
      // into THIS, not jobScope, so cancelling this job (re-arm) can't interrupt a
      // wake that is itself running the re-arm turn (the self-cancel deadlock). Falls
      // back to jobScope when run outside start. Reader + debounce timer stay jobScope.
      const wakeScope = (yield* WakeScope) ?? jobScope
      monitorStarted(opts.sessionID)
      if (opts.onCount) yield* opts.onCount(getMonitorCount(opts.sessionID)).pipe(Effect.ignore)
      let pid: number | undefined
      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          monitorStopped(opts.sessionID, pid)
          // Publish the post-decrement count so the footer clears/updates when this
          // job ends — even if the session is idle (no other event to ride on).
          if (opts.onCount) yield* opts.onCount(getMonitorCount(opts.sessionID)).pipe(Effect.ignore)
        }),
      )

      const handle = yield* spawner.spawn(opts.command)
      pid = Number(handle.pid)
      monitorPid(opts.sessionID, pid)
      yield* Effect.addFinalizer(() => handle.kill().pipe(Effect.ignore))

      const onBatch = opts.onBatch
      if (onBatch) {
        let carry = ""
        let pending: string[] = []
        let total = 0
        let timerArmed = false

        // Fire-and-forget (forked, NOT awaited): onBatch calls ops.prompt, which
        // AWAITS the model turn. Forked so the reader never blocks on a turn. We fork
        // into wakeScope (session-lifetime), NOT jobScope: when the model re-arms a
        // monitor from inside a wake turn, that cancels THIS job — and if the wake ran
        // in jobScope, cancel's Scope.close would interrupt+await the very fiber
        // running the turn that issued the cancel (self-cancel deadlock). wakeScope
        // outlives the job, so the turn completes; the wake is still reaped on session
        // teardown (no daemon leak). This also makes the trailing exit batch (below)
        // survive job-scope close instead of being dropped.
        const emit = (batch: ReadonlyArray<string>) =>
          onBatch(batch.join("\n")).pipe(Effect.forkIn(wakeScope, { startImmediately: true }))

        // Flush whatever has accumulated as one batch (one wake).
        const flush = Effect.suspend(() => {
          timerArmed = false
          if (pending.length === 0) return Effect.void
          const batch = pending
          pending = []
          return emit(batch)
        })

        yield* Stream.runForEach(handle.stdout, (chunk) =>
          Effect.gen(function* () {
            carry += new TextDecoder().decode(chunk as Uint8Array)
            const lines = carry.split(/\r?\n/)
            carry = lines.pop() ?? ""
            for (const line of lines) {
              const trimmed = line.trim()
              if (!trimmed) continue
              pending.push(trimmed)
              total += 1
            }
            // Flood guard: flush what we have, tell the model, then kill (via scope).
            if (total >= FLOOD_MAX_LINES) {
              if (pending.length > 0) {
                const batch = pending
                pending = []
                yield* emit(batch)
              }
              yield* emit([`[flood guard] watcher stopped: emitted ${total}+ lines too fast`])
              return yield* Effect.interrupt
            }
            // Arm a single debounce timer; it flushes everything buffered so far.
            if (pending.length > 0 && !timerArmed) {
              timerArmed = true
              yield* flush.pipe(Effect.delay(`${BATCH_WINDOW_MS} millis`), Effect.forkIn(jobScope, { startImmediately: true }))
            }
          }),
        )

        // Process exited: deliver any buffered lines (incl. a trailing partial).
        // emit() forks into wakeScope (session-lifetime), so this trailing batch
        // survives the job-scope close that follows exit — and the model may safely
        // re-arm on the exit note without the exit-then-rearm self-cancel deadlock.
        const tail = carry.trim()
        if (tail) pending.push(tail)
        if (pending.length > 0) yield* emit(pending)
      }

      return yield* handle.exitCode.pipe(
        Effect.matchCause({
          onSuccess: (code) => `exit code ${code}`,
          onFailure: (cause) => `signal or error: ${Cause.squash(cause)}`,
        }),
      )
    }),
  )
}
