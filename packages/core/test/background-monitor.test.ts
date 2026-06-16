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
})
