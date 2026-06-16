export * as BackgroundMonitorManager from "./background-monitor"

import { Cause, Clock, Context, Effect, Exit, Layer, Scope, Stream, SynchronizedRef } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner"
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner"
import { Shell } from "./shell"

export type Status = "running" | "exited" | "stopped"

export type Info = {
  readonly id: string
  readonly sessionID: string
  readonly command: string
  readonly description: string
  readonly cwd: string
  readonly status: Status
  readonly exitReason?: string
  readonly startedAt: number
}

export type StartInput = {
  readonly sessionID: string
  readonly command: string
  readonly description: string
  readonly cwd: string
  readonly timeoutMs?: number
  readonly onEvent: (line: string) => Effect.Effect<void>
  readonly onExit: (reason: string) => Effect.Effect<void>
}

export interface Interface {
  readonly list: () => Effect.Effect<Info[]>
  readonly get: (id: string) => Effect.Effect<Info | undefined>
  readonly start: (input: StartInput) => Effect.Effect<Info>
  readonly stop: (id: string) => Effect.Effect<void>
  readonly stopAllForSession: (sessionID: string) => Effect.Effect<void>
  readonly stopAll: () => Effect.Effect<void>
  readonly countForSession: (sessionID: string) => Effect.Effect<number>
  readonly listForSession: (sessionID: string) => Effect.Effect<Info[]>
  readonly whenIdleForSession: (sessionID: string, signal?: AbortSignal) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/BackgroundMonitorManager") {}

type ActiveMonitor = {
  readonly info: Info
  readonly handle: ChildProcessHandle
  readonly stdoutBuffer: { buffer: string }
  readonly intentional: boolean
  readonly scope: Scope.Closeable
  readonly onEvent: (line: string) => Effect.Effect<void>
  readonly onExit: (reason: string) => Effect.Effect<void>
}

type State = {
  readonly monitors: Map<string, ActiveMonitor>
  readonly counter: number
}

function makeCommand(input: StartInput): ChildProcess.Command {
  const shell = Shell.acceptable()
  const args = Shell.args(shell, input.command, input.cwd)
  return ChildProcess.make(shell, args, {
    cwd: input.cwd,
    detached: process.platform !== "win32",
    env: {
      ...process.env,
      TERM: "xterm-256color",
      PAGER: "cat",
      GIT_PAGER: "cat",
    },
  })
}

function snapshot(info: Info, status: Status, exitReason?: string): Info {
  return { ...info, status, exitReason }
}

function waitForAbort(signal: AbortSignal): Effect.Effect<void> {
  return Effect.callback<void, never>((resume) => {
    if (signal.aborted) {
      resume(Effect.void)
      return
    }
    const onAbort = () => resume(Effect.void)
    signal.addEventListener("abort", onAbort, { once: true })
    return Effect.sync(() => signal.removeEventListener("abort", onAbort))
  })
}

// Global mutable state for synchronous external access (e.g. CLI keep-alive polling).
// Updated atomically inside Effect fibers; safe because the map itself is not
// shared across Effect boundaries, only read from plain JS.
const _sessionMonitorCounts = new Map<string, number>()
const _sessionMonitorPids = new Map<string, Array<number>>()

export function getMonitorCount(sessionID: string): number {
  return _sessionMonitorCounts.get(sessionID) ?? 0
}

export function stopAllForSessionSync(sessionID: string): void {
  const pids = _sessionMonitorPids.get(sessionID)
  if (!pids) return
  for (const pid of pids) {
    try {
      process.kill(pid, "SIGTERM")
    } catch {
      // ignore already-exited or permission errors
    }
  }
  _sessionMonitorPids.delete(sessionID)
}

function addPid(sessionID: string, pid: number): void {
  const list = _sessionMonitorPids.get(sessionID) ?? []
  list.push(pid)
  _sessionMonitorPids.set(sessionID, list)
}

function removePid(sessionID: string, pid: number): void {
  const list = _sessionMonitorPids.get(sessionID)
  if (!list) return
  const filtered = list.filter((p) => p !== pid)
  if (filtered.length === 0) {
    _sessionMonitorPids.delete(sessionID)
  } else {
    _sessionMonitorPids.set(sessionID, filtered)
  }
}

export const make = Effect.gen(function* () {
  const state = yield* SynchronizedRef.make<State>({ monitors: new Map(), counter: 0 })
  const scope = yield* Scope.Scope
  const spawner = yield* ChildProcessSpawner

  const updateCount = (sessionID: string, delta: number) => {
    const next = (_sessionMonitorCounts.get(sessionID) ?? 0) + delta
    if (next <= 0) {
      _sessionMonitorCounts.delete(sessionID)
    } else {
      _sessionMonitorCounts.set(sessionID, next)
    }
  }

  const deregister = (id: string) =>
    SynchronizedRef.update(state, (s) => ({
      ...s,
      monitors: new Map(Array.from(s.monitors.entries()).filter(([key]) => key !== id)),
    }))

  const start = Effect.fn("BackgroundMonitorManager.start")(function* (input: StartInput) {
    const current = yield* SynchronizedRef.get(state)
    const id = `monitor-${current.counter.toString(36)}`
    yield* SynchronizedRef.update(state, (s) => ({ ...s, counter: s.counter + 1 }))

    const command = makeCommand(input)
    const startedAt = yield* Clock.currentTimeMillis
    const info: Info = {
      id,
      sessionID: input.sessionID,
      command: input.command,
      description: input.description,
      cwd: input.cwd,
      status: "running",
      startedAt,
    }

    const monitorScope = yield* Scope.fork(scope, "parallel")

    const handle = yield* spawner
      .spawn(command)
      .pipe(Effect.provideService(Scope.Scope, monitorScope))
      .pipe(
        Effect.matchCauseEffect({
          onSuccess: (handle) => Effect.succeed(handle),
          onFailure: (cause) =>
            Effect.die(new Error(`Monitor failed to start: ${Cause.squash(cause)}`)),
        }),
      )

    const monitor: ActiveMonitor = {
      info,
      handle,
      stdoutBuffer: { buffer: "" },
      intentional: false,
      scope: monitorScope,
      onEvent: input.onEvent,
      onExit: input.onExit,
    }

    yield* SynchronizedRef.update(state, (s) => ({
      ...s,
      monitors: new Map(s.monitors).set(id, monitor),
    }))
    updateCount(input.sessionID, 1)
    addPid(input.sessionID, Number(handle.pid))

    const processStdout = Effect.fnUntraced(function* () {
      yield* Stream.runForEach(handle.stdout, (chunk) =>
        Effect.gen(function* () {
          const text = new TextDecoder().decode(chunk as Uint8Array)
          monitor.stdoutBuffer.buffer += text
          const lines = monitor.stdoutBuffer.buffer.split(/\r?\n/)
          monitor.stdoutBuffer.buffer = lines.pop() ?? ""
          for (const line of lines) {
            const trimmed = line.trim()
            if (!trimmed) continue
            yield* input.onEvent(trimmed)
          }
        }),
      )
    })

    const processExit = Effect.fnUntraced(function* () {
      const reason = yield* handle.exitCode.pipe(
        Effect.matchCauseEffect({
          onSuccess: (code) => Effect.succeed(`exit code ${code}`),
          onFailure: (cause) => Effect.succeed(`signal or error: ${Cause.squash(cause)}`),
        }),
      )
      yield* SynchronizedRef.update(state, (s) => {
        const existing = s.monitors.get(id)
        if (!existing) return s
        return {
          ...s,
          monitors: new Map(s.monitors).set(id, {
            ...existing,
            info: snapshot(existing.info, "exited", reason),
          }),
        }
      })
      const trailing = monitor.stdoutBuffer.buffer.trim()
      if (trailing) yield* input.onEvent(trailing)
      if (!monitor.intentional) {
        yield* input.onExit(reason)
      }
      yield* deregister(id)
      updateCount(input.sessionID, -1)
      removePid(input.sessionID, Number(handle.pid))
    })

    yield* processStdout().pipe(Effect.forkIn(monitorScope, { startImmediately: true }))
    yield* processExit().pipe(Effect.forkIn(monitorScope, { startImmediately: true }))

    if (input.timeoutMs && input.timeoutMs > 0) {
      yield* Effect.sleep(input.timeoutMs).pipe(
        Effect.tap(() => stop(id)),
        Effect.forkIn(monitorScope, { startImmediately: true }),
      )
    }

    return info
  })

  const stop = Effect.fn("BackgroundMonitorManager.stop")(function* (id: string) {
    const monitor = yield* SynchronizedRef.modify(state, (s): readonly [ActiveMonitor | undefined, State] => {
      const existing = s.monitors.get(id)
      if (!existing) return [undefined, s]
      const updated = { ...existing, intentional: true }
      return [updated, { ...s, monitors: new Map(s.monitors).set(id, updated) }]
    })
    if (!monitor) return
    yield* monitor.handle.kill().pipe(Effect.ignore)
    yield* Scope.close(monitor.scope, Exit.void).pipe(Effect.ignore)
    yield* deregister(id)
    updateCount(monitor.info.sessionID, -1)
    removePid(monitor.info.sessionID, Number(monitor.handle.pid))
  })

  const stopAll = Effect.fn("BackgroundMonitorManager.stopAll")(function* () {
    const ids = yield* SynchronizedRef.get(state).pipe(Effect.map((s) => Array.from(s.monitors.keys())))
    yield* Effect.forEach(ids, (id) => stop(id), { concurrency: "unbounded", discard: true })
  })

  const stopAllForSession = Effect.fn("BackgroundMonitorManager.stopAllForSession")(function* (sessionID: string) {
    const ids = yield* listForSession(sessionID).pipe(Effect.map((monitors) => monitors.map((m) => m.id)))
    yield* Effect.forEach(ids, (id) => stop(id), { concurrency: "unbounded", discard: true })
  })

  const list = Effect.fn("BackgroundMonitorManager.list")(function* () {
    return Array.from((yield* SynchronizedRef.get(state)).monitors.values())
      .map((m) => m.info)
      .sort((a, b) => a.startedAt - b.startedAt)
  })

  const get = Effect.fn("BackgroundMonitorManager.get")(function* (id: string) {
    return (yield* SynchronizedRef.get(state)).monitors.get(id)?.info
  })

  const countForSession = Effect.fn("BackgroundMonitorManager.countForSession")(function* (sessionID: string) {
    let count = 0
    for (const monitor of (yield* SynchronizedRef.get(state)).monitors.values()) {
      if (monitor.info.sessionID === sessionID) count += 1
    }
    return count
  })

  const listForSession = Effect.fn("BackgroundMonitorManager.listForSession")(function* (sessionID: string) {
    return Array.from((yield* SynchronizedRef.get(state)).monitors.values())
      .filter((m) => m.info.sessionID === sessionID)
      .map((m) => m.info)
      .sort((a, b) => a.startedAt - b.startedAt)
  })

  const whenIdleForSession = Effect.fn("BackgroundMonitorManager.whenIdleForSession")(function* (
    sessionID: string,
    signal?: AbortSignal,
  ) {
    const wait = Effect.fnUntraced(function* () {
      while (true) {
        const count = yield* countForSession(sessionID)
        if (count === 0) return
        yield* Effect.sleep("100 millis")
      }
    })
    const idle = wait()
    if (!signal) return yield* idle
    return yield* idle.pipe(Effect.raceFirst(waitForAbort(signal)))
  })

  yield* Effect.addFinalizer(
    Effect.fnUntraced(function* () {
      yield* stopAll()
    }),
  )

  return Service.of({
    list,
    get,
    start,
    stop,
    stopAllForSession,
    stopAll,
    countForSession,
    listForSession,
    whenIdleForSession,
  })
})

export const layer = Layer.effect(Service, make)

export const defaultLayer = layer

export * as BackgroundMonitor from "./background-monitor"
