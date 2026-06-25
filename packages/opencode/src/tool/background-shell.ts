import { Cause, Effect, Scope, Stream } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import { Shell } from "@opencode-ai/core/shell"
import { monitorPid, monitorStarted, monitorStopped } from "@opencode-ai/core/background-monitor"

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
}): Effect.Effect<string, unknown, ChildProcessSpawner> {
  return Effect.scoped(
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner
      // The job's own scope (provided by Effect.scoped). Per-line/batch wakes are
      // forked into THIS scope, not detached: forkDetach makes daemon fibers that
      // no scope ever interrupts (proven against the Effect internals), which both
      // leaks a ref'd debounce timer past teardown (hang) and orphans in-flight
      // wakes. forkIn(jobScope) keeps them interruptible on cancel/teardown while
      // still NOT blocking the reader (preserving the re-arm deadlock fix).
      const jobScope = yield* Scope.Scope
      monitorStarted(opts.sessionID)
      let pid: number | undefined
      yield* Effect.addFinalizer(() => Effect.sync(() => monitorStopped(opts.sessionID, pid)))

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
        // AWAITS the model turn. If the reader awaited that, cancelling/re-arming a
        // monitor (Scope.close from inside the very turn the reader is awaiting)
        // would deadlock. Forking into jobScope keeps the reader interruptible AND
        // ties the wake fiber's lifetime to the job (interrupted on teardown).
        const emit = (batch: ReadonlyArray<string>) =>
          onBatch(batch.join("\n")).pipe(Effect.forkIn(jobScope, { startImmediately: true }))

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
