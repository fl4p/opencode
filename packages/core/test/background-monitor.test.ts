import { describe, expect } from "bun:test"
import { BackgroundMonitorManager } from "../src/background-monitor"
import { CrossSpawnSpawner } from "../src/cross-spawn-spawner"
import { Deferred, Effect, Layer } from "effect"
import { testEffect } from "./lib/effect"

const it = testEffect(
  BackgroundMonitorManager.layer.pipe(Layer.provide(CrossSpawnSpawner.defaultLayer)),
)

describe("BackgroundMonitorManager", () => {
  it.live("starts a monitor and stops", () =>
    Effect.gen(function* () {
      const monitors = yield* BackgroundMonitorManager.Service
      const exited = yield* Deferred.make<string>()

      const info = yield* monitors.start({
        sessionID: "test-session",
        command: process.platform === "win32" ? "cmd /c timeout /t 30 >nul" : "sleep 30",
        description: "test monitor",
        cwd: process.cwd(),
        onEvent: () => Effect.void,
        onExit: (reason) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(exited, reason)
          }),
      })

      expect(info.sessionID).toBe("test-session")
      expect(info.status).toBe("running")

      yield* Effect.sleep("100 millis")
      yield* monitors.stop(info.id)

      const reason = yield* Deferred.await(exited)
      expect(typeof reason).toBe("string")

      const count = yield* monitors.countForSession("test-session")
      expect(count).toBe(0)
    }),
  )

  it.live("starts a monitor, emits events, and stops", () =>
    Effect.gen(function* () {
      const monitors = yield* BackgroundMonitorManager.Service
      const event = yield* Deferred.make<string>()
      const exited = yield* Deferred.make<string>()

      const info = yield* monitors.start({
        sessionID: "test-session",
        command: process.platform === "win32" ? "cmd /c echo hello && timeout /t 30 >nul" : "bash -c 'echo hello && sleep 30'",
        description: "test monitor",
        cwd: process.cwd(),
        onEvent: (line) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(event, line)
          }),
        onExit: (reason) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(exited, reason)
          }),
      })

      expect(info.sessionID).toBe("test-session")
      expect(info.status).toBe("running")

      const line = yield* Deferred.await(event)
      expect(line).toBe("hello")

      yield* monitors.stop(info.id)

      const reason = yield* Deferred.await(exited)
      expect(typeof reason).toBe("string")

      const count = yield* monitors.countForSession("test-session")
      expect(count).toBe(0)
    }),
  )

  it.live("stopAllForSessionByKind stops only the matching kind", () =>
    Effect.gen(function* () {
      const monitors = yield* BackgroundMonitorManager.Service
      const sleep = process.platform === "win32" ? "cmd /c timeout /t 30 >nul" : "sleep 30"

      const mon = yield* monitors.start({
        sessionID: "kind-session",
        command: sleep,
        description: "a monitor",
        cwd: process.cwd(),
        kind: "monitor",
        onEvent: () => Effect.void,
        onExit: () => Effect.void,
      })
      const bg = yield* monitors.start({
        sessionID: "kind-session",
        command: sleep,
        description: "a background run",
        cwd: process.cwd(),
        kind: "background",
        onEvent: () => Effect.void,
        onExit: () => Effect.void,
      })

      expect(mon.kind).toBe("monitor")
      expect(bg.kind).toBe("background")

      yield* Effect.sleep("100 millis")
      expect(yield* monitors.countForSession("kind-session")).toBe(2)

      // Re-arming a monitor must not kill concurrent background runs.
      yield* monitors.stopAllForSessionByKind("kind-session", "monitor")
      yield* Effect.sleep("50 millis")

      expect(yield* monitors.countForSession("kind-session")).toBe(1)
      const stillRunning = yield* monitors.get(bg.id)
      expect(stillRunning?.status).toBe("running")
      expect(stillRunning?.kind).toBe("background")

      yield* monitors.stopAllForSession("kind-session")
      expect(yield* monitors.countForSession("kind-session")).toBe(0)
    }),
  )

  it.live("defaults kind to monitor when omitted", () =>
    Effect.gen(function* () {
      const monitors = yield* BackgroundMonitorManager.Service
      const info = yield* monitors.start({
        sessionID: "default-kind",
        command: process.platform === "win32" ? "cmd /c timeout /t 30 >nul" : "sleep 30",
        description: "no kind",
        cwd: process.cwd(),
        onEvent: () => Effect.void,
        onExit: () => Effect.void,
      })
      expect(info.kind).toBe("monitor")
      yield* monitors.stop(info.id)
    }),
  )
})
